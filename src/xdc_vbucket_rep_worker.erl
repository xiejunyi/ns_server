%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.

-module(xdc_vbucket_rep_worker).

%% public API
-export([start_link/1]).

-include("xdc_replicator.hrl").

%% in XDCR the Source should always from local with record #db{}, while
%% the target should always from remote with record #httpdb{}. There is
%% no intra-cluster XDCR
start_link(#rep_worker_option{cp = Cp, source = Source, target = Target,
                              worker_id  = WorkerID,
                              changes_manager = ChangesManager,
                              opt_rep_threshold = OptRepThreshold,
                              xmem_server = XMemSrv,
                              batch_size = BatchSize,
                              batch_items = BatchItems} = _WorkerOption) ->
    Pid = spawn_link(fun() ->
                             erlang:monitor(process, ChangesManager),
                             queue_fetch_loop(WorkerID, Source, Target, Cp,
                                              ChangesManager, OptRepThreshold,
                                              BatchSize, BatchItems, XMemSrv)
                     end),


    ?xdcr_debug("create queue_fetch_loop process (worker_id: ~p, pid: ~p) within replicator (pid: ~p) "
                "Source: ~p, Target: ~p, ChangesManager: ~p, latency optimized: ~p",
                [WorkerID, Pid, Cp, Source#db.name, misc:sanitize_url(Target#httpdb.url), ChangesManager, OptRepThreshold]),

    {ok, Pid}.

-spec queue_fetch_loop(integer(), #db{}, #httpdb{}, pid(), pid(),
                       integer(), integer(), integer(), pid() | nil) -> ok.
queue_fetch_loop(WorkerID, Source, Target, Cp, ChangesManager,
                 OptRepThreshold, BatchSize, BatchItems, nil) ->
    ?xdcr_trace("fetch changes from changes manager at ~p (target: ~p)",
                [ChangesManager, misc:sanitize_url(Target#httpdb.url)]),
    ChangesManager ! {get_changes, self()},
    receive
        {'DOWN', _, _, _, _} ->
            ok = gen_server:call(Cp, {worker_done, self()}, infinity);
        {changes, ChangesManager, Changes, ReportSeq} ->


 ?xdcr_debug("XDCR-UPR: received ~p mutations from UPR changes manager ~p",
             [ChangesManager, length(Changes)]),

            %% get docinfo of missing ids
            {MissingDocInfoList, MetaLatency, NumDocsOptRepd} =
                find_missing(Changes, Target, OptRepThreshold, nil),
            NumChecked = length(Changes),
            NumWritten = length(MissingDocInfoList),
            %% use ptr in docinfo to fetch document from storage
            Start = now(),
            {ok, DataRepd} = local_process_batch(
                               MissingDocInfoList, Cp, Source, Target,
                               #batch{}, BatchSize, BatchItems, nil),

            %% the latency returned should be coupled with batch size, for example,
            %% if we send N docs in a batch, the latency returned to stats should be the latency
            %% for all N docs. XMem mode XDCR is now single doc based, therefore the latency
            %% should be single doc replication latency latency in millisecond.
            DocLatency = timer:now_diff(now(), Start) div 1000,

            %% report seq done and stats to vb replicator
            ok = gen_server:call(Cp, {report_seq_done,
                                      #worker_stat{
                                        worker_id = WorkerID,
                                        seq = ReportSeq,
                                        worker_meta_latency_aggr = MetaLatency*NumChecked,
                                        worker_docs_latency_aggr = DocLatency*NumWritten,
                                        worker_data_replicated = DataRepd,
                                        worker_item_opt_repd = NumDocsOptRepd,
                                        worker_item_checked = NumChecked,
                                        worker_item_replicated = NumWritten}}, infinity),

            ?xdcr_trace("Worker reported completion of seq ~p, num docs written: ~p "
                        "data replicated: ~p bytes, latency: ~p ms.",
                        [ReportSeq, NumWritten, DataRepd, DocLatency]),
            queue_fetch_loop(WorkerID, Source, Target, Cp, ChangesManager,
                             OptRepThreshold, BatchSize, BatchItems, nil)
    end;

queue_fetch_loop(WorkerID, Source, Target, Cp, ChangesManager,
                 OptRepThreshold, BatchSize, BatchItems, XMemSrv) ->
    ?xdcr_trace("fetch changes from changes manager at ~p (target: ~p)",
                [ChangesManager, misc:sanitize_url(Target#httpdb.url)]),
    ChangesManager ! {get_changes, self()},
    receive
        {'DOWN', _, _, _, _} ->
            ok = gen_server:call(Cp, {worker_done, self()}, infinity);
        {changes, ChangesManager, Changes, ReportSeq} ->
            %% get docinfo of missing ids
            {MissingDocInfoList, MetaLatency, NumDocsOptRepd} =
                find_missing(Changes, Target, OptRepThreshold, XMemSrv),
            NumChecked = length(Changes),
            NumWritten = length(MissingDocInfoList),
            %% use ptr in docinfo to fetch document from storage
            Start = now(),
            {ok, DataRepd} = local_process_batch(
                               MissingDocInfoList, Cp, Source, Target,
                               #batch{}, BatchSize, BatchItems, XMemSrv),

            %% the latency returned should be coupled with batch size, for example,
            %% if we send N docs in a batch, the latency returned to stats should be the latency
            %% for all N docs. XMem mode XDCR is now single doc based, therefore the latency
            %% should be single doc replication latency latency in millisecond.
            BatchLatency = timer:now_diff(now(), Start) div 1000,
            DocLatency = try BatchLatency / NumWritten
                         catch error:badarith -> 0
                         end,

            %% report seq done and stats to vb replicator
            ok = gen_server:call(Cp, {report_seq_done,
                                      #worker_stat{
                                        worker_id  = WorkerID,
                                        seq = ReportSeq,
                                        worker_meta_latency_aggr = MetaLatency*NumChecked,
                                        worker_docs_latency_aggr = DocLatency*NumWritten,
                                        worker_data_replicated = DataRepd,
                                        worker_item_opt_repd = NumDocsOptRepd,
                                        worker_item_checked = NumChecked,
                                        worker_item_replicated = NumWritten}}, infinity),

            ?xdcr_trace("Worker reported completion of seq ~p, num docs written: ~p "
                        "data replicated: ~p bytes, latency: ~p ms.",
                        [ReportSeq, NumWritten, DataRepd, DocLatency]),
            queue_fetch_loop(WorkerID, Source, Target, Cp, ChangesManager,
                             OptRepThreshold, BatchSize, BatchItems, XMemSrv)
    end.


local_process_batch([], _Cp, _Src, _Tgt, #batch{docs = []}, _BatchSize, _BatchItems, _XMemSrv) ->
    {ok, 0};
local_process_batch([], Cp, #db{} = Source, #httpdb{} = Target,
                    #batch{docs = Docs, size = Size}, BatchSize, BatchItems, XMemSrv) ->
    ?xdcr_trace("worker process flushing a batch docs of total size ~p bytes",
                [Size]),
    ok = flush_docs_helper(Target, Docs, XMemSrv),
    {ok, DataRepd1} = local_process_batch([], Cp, Source, Target, #batch{}, BatchSize, BatchItems, XMemSrv),
    {ok, DataRepd1 + Size};

local_process_batch([DocInfo | Rest], Cp, #db{} = Source,
                    #httpdb{} = Target, Batch, BatchSize, BatchItems, XMemSrv) ->
    {ok, {_, DocsList, _}} = fetch_doc(
                                      Source, DocInfo, fun local_doc_handler/2,
                                      {Target, [], Cp}),
    {Batch2, DataFlushed} = lists:foldl(
                         fun(Doc, {Batch0, DataFlushed1}) ->
                                 maybe_flush_docs(Target, Batch0, Doc, DataFlushed1, BatchSize, BatchItems, XMemSrv)
                         end,
                         {Batch, 0}, DocsList),
    {ok, DataFlushed2} = local_process_batch(Rest, Cp, Source, Target, Batch2, BatchSize, BatchItems, XMemSrv),
    %% return total data flushed
    {ok, DataFlushed + DataFlushed2}.


%% fetch doc using doc info
fetch_doc(Source, #xdc_doc{docinfo = DocInfo} = _XDCDoc, DocHandler, Acc) ->
    couch_api_wrap:open_doc(Source, DocInfo, [deleted], DocHandler, Acc).

local_doc_handler({ok, Doc}, {Target, DocsList, Cp}) ->
    {ok, {Target, [Doc | DocsList], Cp}};
local_doc_handler(_, Acc) ->
    {ok, Acc}.

-spec maybe_flush_docs(#httpdb{}, #batch{}, #doc{}, integer(), integer(), integer(), pid() | nil) ->
                              {#batch{}, integer()}.
maybe_flush_docs(#httpdb{} = Target, Batch, Doc, DataFlushed, BatchSize, BatchItems, nil) ->
    maybe_flush_docs_capi(Target, Batch, Doc, DataFlushed, BatchSize, BatchItems);

maybe_flush_docs(#httpdb{} = _Target, Batch, Doc, DataFlushed, BatchSize, BatchItems, XMemSrv) ->
    maybe_flush_docs_xmem(XMemSrv, Batch, Doc, DataFlushed, BatchSize, BatchItems).

-spec flush_docs_helper(any(), list(), pid() | nil) -> ok.
flush_docs_helper(Target, DocsList, nil) ->
    {RepMode,RV} = {"capi", flush_docs_capi(Target, DocsList)},

    case RV of
        ok ->
            ?xdcr_trace("replication mode: ~p, worker process replicated ~p docs to target ~p",
                        [RepMode, length(DocsList), misc:sanitize_url(Target#httpdb.url)]),
            ok;
        {failed_write, Error} ->
            ?xdcr_error("replication mode: ~p, unable to replicate ~p docs to target ~p",
                        [RepMode, length(DocsList), misc:sanitize_url(Target#httpdb.url)]),
            exit({failed_write, Error})
    end;

flush_docs_helper(Target, DocsList, XMemSrv) ->
    {RepMode,RV} = {"xmem", flush_docs_xmem(XMemSrv, DocsList)},

    case RV of
        ok ->
            ?xdcr_trace("replication mode: ~p, worker process replicated ~p docs to target ~p",
                        [RepMode, length(DocsList), misc:sanitize_url(Target#httpdb.url)]),
            ok;
        {failed_write, Error} ->
            ?xdcr_error("replication mode: ~p, unable to replicate ~p docs to target ~p",
                        [RepMode, length(DocsList), misc:sanitize_url(Target#httpdb.url)]),
            exit({failed_write, Error})
    end.

%% return list of Docsinfos of missing keys
-spec find_missing(list(), #httpdb{}, integer(), pid() | nil) -> {list(), integer(), integer()}.
find_missing(DocInfos, Target, OptRepThreshold, XMemSrv) ->
    Start = now(),

    %% depending on doc body size, we separate all keys into two groups:
    %% keys with doc body size greater than the threshold, and keys with doc body
    %% smaller than or equal to threshold.
    {BigDocIdRevs, SmallDocIdRevs, DelCount, BigDocCount,
     SmallDocCount, AllRevsCount} = lists:foldr(
                                      fun(#xdc_doc{docinfo = DocInfo},
                                          {BigIdRevAcc, SmallIdRevAcc, DelAcc, BigAcc, SmallAcc, CountAcc}) ->

                                              #doc_info{id = Id, rev = Rev, deleted = Deleted,
                                                        size = DocSize} = DocInfo,

                                              %% deleted doc is always treated as small doc, regardless of doc size
                                              {BigIdRevAcc1, SmallIdRevAcc1, DelAcc1, BigAcc1, SmallAcc1} =
                                                  case Deleted of
                                                      true ->
                                                          {BigIdRevAcc, [{Id, Rev} | SmallIdRevAcc], DelAcc + 1,
                                                           BigAcc, SmallAcc + 1};
                                                      _ ->
                                                          %% for all other mutations, check its doc size
                                                          case DocSize > OptRepThreshold of
                                                              true ->
                                                                  {[{Id, Rev} | BigIdRevAcc], SmallIdRevAcc, DelAcc,
                                                                   BigAcc + 1, SmallAcc};
                                                              _  ->
                                                                  {BigIdRevAcc, [{Id, Rev} | SmallIdRevAcc], DelAcc,
                                                                   BigAcc, SmallAcc + 1}
                                                          end
                                                  end,
                                              {BigIdRevAcc1, SmallIdRevAcc1, DelAcc1, BigAcc1, SmallAcc1, CountAcc + 1}
                                      end,
                                      {[], [], 0, 0, 0, 0}, DocInfos),

    %% metadata operation for big docs only
    {Missing, MissingBigDocCount} =
        case length(BigDocIdRevs) of
            V when V > 0 ->
                MissingBigIdRevs = find_missing_helper(Target, BigDocIdRevs, XMemSrv),
                {lists:flatten([SmallDocIdRevs | MissingBigIdRevs]), length(MissingBigIdRevs)};
            _ ->
                {SmallDocIdRevs, 0}
        end,

    %% build list of docinfo for all missing keys
    MissingDocInfoList = lists:filter(
                           fun(#xdc_doc{docinfo = DocInfo} = _XDCDoc) ->
                                   #doc_info{id = Id} = DocInfo,
                                   case lists:keyfind(Id, 1, Missing) of
                                       %% not a missing key
                                       false ->
                                           false;
                                       %% a missing key
                                       _  -> true
                                   end
                           end,
                           DocInfos),

    %% latency in millisecond
    TotalLatency = round(timer:now_diff(now(), Start) div 1000),
    Latency = case is_pid(XMemSrv) of
                  true ->
                      %% xmem is single doc based
                      try (TotalLatency div BigDocCount) of
                          X -> X
                      catch
                          error:badarith -> 0
                      end;
                  _ ->
                      TotalLatency
              end,


    RepMode = case is_pid(XMemSrv) of
                  true ->
                      "xmem";
                  _ ->
                      "capi"
              end,
    ?xdcr_trace("[replication mode: ~p] out of all ~p docs, number of small docs (including dels: ~p) is ~p, "
                "number of big docs is ~p, threshold is ~p bytes, ~n\t"
                "after conflict resolution at target (~p), out of all big ~p docs "
                "the number of docs we need to replicate is: ~p; ~n\t "
                "total # of docs to be replicated is: ~p, total latency: ~p ms",
                [RepMode, AllRevsCount, DelCount, SmallDocCount, BigDocCount, OptRepThreshold,
                 misc:sanitize_url(Target#httpdb.url), BigDocCount, MissingBigDocCount,
                 length(MissingDocInfoList), Latency]),

    {MissingDocInfoList, Latency, length(SmallDocIdRevs)}.

-spec find_missing_helper(#httpdb{}, list(), pid() | nil) -> list().
find_missing_helper(Target, BigDocIdRevs, XMemSrv) ->
    MissingIdRevs = case XMemSrv of
                        nil ->
                            {ok, IdRevs} = couch_api_wrap:get_missing_revs(Target, BigDocIdRevs),
                            IdRevs;
                        Pid when is_pid(Pid) ->
                            {ok, IdRevs} = xdc_vbucket_rep_xmem_srv:find_missing(XMemSrv, BigDocIdRevs),
                            IdRevs
                    end,
    MissingIdRevs.

%% ================================================= %%
%% ========= FLUSHING DOCS USING CAPI ============== %%
%% ================================================= %%
-spec maybe_flush_docs_capi(#httpdb{}, #batch{}, #doc{},
                            integer(), integer(), integer()) -> {#batch{}, integer()}.
maybe_flush_docs_capi(#httpdb{} = Target, Batch, Doc, DataFlushed, BatchSize, BatchItems) ->
    #batch{docs = DocAcc, size = SizeAcc, items = ItemsAcc} = Batch,
    JsonDoc = couch_doc:to_json_base64(Doc),

    SizeAcc2 = SizeAcc + iolist_size(JsonDoc),
    ItemsAcc2 = ItemsAcc + 1,
    case ItemsAcc2 >= BatchItems orelse SizeAcc2 > BatchSize of
        true ->
            ?xdcr_trace("Worker flushing ~b docs, batch of size ~b bytes "
                        "(batch size ~b, batch items ~b)",
                        [ItemsAcc2, SizeAcc2, BatchSize, BatchItems]),
            flush_docs_capi(Target, [JsonDoc | DocAcc]),
            %% data flushed, return empty batch and size of data flushed
            {#batch{}, SizeAcc2 + DataFlushed};
        false ->            %% no data flushed in this turn, return the new batch
            {#batch{docs = [JsonDoc | DocAcc], size = SizeAcc2, items = ItemsAcc2}, DataFlushed}
    end.

-spec flush_docs_capi(#httpdb{}, list()) -> ok | {failed_write, term()}.
flush_docs_capi(_Target, []) ->
    ok;
flush_docs_capi(Target, DocsList) ->
    case couch_api_wrap:update_docs(Target, DocsList, [delay_commit],
                                    replicated_changes) of
        ok ->
            ok;
        {ok, {Props}} ->
            DbUri = couch_api_wrap:db_uri(Target),
            ?xdcr_error("Replicator: couldn't write document `~s`, revision `~s`,"
                        " to target database `~s`. Error: `~s`, reason: `~200s`.",
                        [get_value(<<"id">>, Props, ""), get_value(<<"rev">>, Props, ""), DbUri,
                         get_value(<<"error">>, Props, ""), get_value(<<"reason">>, Props, "")]),
            {failed_write, Props}
    end.


%% ================================================= %%
%% ========= FLUSHING DOCS USING XMEM ============== %%
%% ================================================= %%
-spec flush_docs_xmem(pid(), list()) -> ok | {failed_write, term()}.
flush_docs_xmem(_XMemSrv, []) ->
    ok;
flush_docs_xmem(XMemSrv, DocsList) ->
    {WorkerPid, Pipeline} = xdc_vbucket_rep_xmem_srv:get_worker(XMemSrv),
    TimeStart = now(),
    RV =
        case Pipeline of
            false ->
                xdc_vbucket_rep_xmem_worker:flush_docs(WorkerPid, DocsList);
            _ ->
                xdc_vbucket_rep_xmem_worker:flush_docs_pipeline(WorkerPid, DocsList)
        end,
    TimeSpent = timer:now_diff(now(), TimeStart) div 1000,
    AvgLatency = TimeSpent div length(DocsList),

    case RV of
        {ok, NumDocRepd, NumDocRejected} ->
            ?xdcr_trace("out of total ~p docs, "
                        "# of docs accepted by remote: ~p "
                        "# of docs rejected by remote: ~p"
                        "(xmem worker: ~p,"
                        "time spent in ms: ~p, avg latency per doc in ms: ~p)",
                        [length(DocsList),
                         NumDocRepd, NumDocRejected,
                         WorkerPid, TimeSpent, AvgLatency]),
            ok;
        {error, Msg} ->
            {failed_write, Msg}
    end.

-spec maybe_flush_docs_xmem(pid(), #batch{}, #doc{},
                            integer(), integer(), integer()) -> {#batch{}, integer()}.
maybe_flush_docs_xmem(XMemSrv, Batch, Doc0, DocsFlushed, BatchSize, BatchItems) ->
    #batch{docs = DocAcc, size = SizeAcc, items = ItemsAcc} = Batch,

    %% uncompress it if necessary
    Doc =  couch_doc:with_uncompressed_body(Doc0),
    DocSize = case Doc#doc.deleted of
                  true ->
                      0;
                  _ ->
                      iolist_size(Doc#doc.body)
              end,

    SizeAcc2 = SizeAcc + DocSize,
    ItemsAcc2 = ItemsAcc + 1,

    %% if reach the limit in terms of docs, flush them
    case ItemsAcc2 >= BatchItems orelse SizeAcc2 >= BatchSize of
        true ->
            ?xdcr_trace("Worker flushing ~b docs, batch of ~p bytes "
                        "(batch size ~b, batch items ~b)",
                        [ItemsAcc2, SizeAcc2, BatchSize, BatchItems]),
            DocsList = [Doc| DocAcc],
            ok = flush_docs_xmem(XMemSrv, DocsList),
            %% data flushed, return empty batch and size of # of docs flushed
            {#batch{}, DocsFlushed + SizeAcc2};
        _ ->            %% no data flushed in this turn, return the new batch
            {#batch{docs = [Doc | DocAcc], size = SizeAcc2, items = ItemsAcc2}, DocsFlushed}
    end.
