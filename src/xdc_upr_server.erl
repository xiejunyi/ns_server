% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(xdc_upr_server).
-behaviour(gen_server).

-include("xdc_replicator.hrl").

% Public API
-export([start/4]).
-export([init/1, terminate/2, handle_call/3]).
-export([handle_cast/2, handle_info/2, code_change/3]).
-export([connect/1, disconnect/1]).
-export([stream_mutations/2]).

%% copy from couchdb
-record(state, {
    socket = nil           :: socket(),
    timeout = 5000         :: timeout(),
    request_id = 0         :: request_id(),
    callbacks = dict:new() :: dict(),
    % Data that was received but not parsed yet
    data = <<>>            :: binary()
}).

%%-type mutations_fold_fun() :: fun().
%%-type mutations_fold_acc() :: any().

% Public API
start(Bucket, Parent, ServName, Vbs) ->
    gen_server:start_link(?MODULE, {Bucket, Parent, ServName, Vbs}, []).

init({Bucket, Parent,  ServName, Vbs}) ->
    Timeout = case couch_config:get("upr", "connection_timeout") of
                  undefined ->
                      ?XDCR_UPR_DEFAULT_CONNECTION_TIMEOUT_MS;
                  T ->
                      list_to_integer(T)
              end,

    %% always streamming from local upr
    Ip = "127.0.0.1",
    Port = case couch_config:get("upr", "port") of
               undefined ->
                   ?XDCR_UPR_DEFAULT_PORT;
               P ->
                   list_to_integer(P)
           end,

    VbVersions = [{P, [{0, 0}]} || P <- Vbs],
    InitState = #xdc_upr_server_state{
                   vbs = Vbs,
                   bucket = Bucket,
                   parent = Parent,
                   name = ServName,
                   timeout = Timeout,
                   port =  Port,
                   ip = Ip,
                   user = Bucket,
                   passwd = [],
                   vb_versions = VbVersions,
                   vb_endseq = orddict:new(),
                   vb_rollback = orddict:new(),
                   stats = #xdc_upr_server_stats{},
                   socket = undefined,
                   status = idle,
                   error_reports = ringbuffer:new(?XDCR_ERROR_HISTORY)
                  },

    OptionStr = ?format_msg("node: ~s, port: ~p, timeout: ~p ms",
                           [Ip, Port, Timeout]),
    ?xdcr_debug("XDCR-UPR server for bucket ~p initialized under bucket replicator ~p, "
                "with option: ~s.",
                [Bucket, Parent, OptionStr]),
    {ok, InitState}.

-spec connect(pid() | nil) -> ok.
connect(nil) ->
    ok;
connect(Server) ->
    gen_server:call(Server, connect, infinity).

-spec disconnect(pid()| nil) -> ok.
disconnect(nil) ->
    ok;
disconnect(Server) ->
    gen_server:call(Server, disconnect, infinity).


stream_mutations(UprPid, #xdc_upr_stream_request_option{
                            vb = Vb,
                            start_seq = StartSeq,
                            cbk_func = ChangesCbk} = _RequestOption) ->
%% maintain all failover log state in xdc
%% 1. get end seq
%% 2. get partition version
%% call couch_upr:enum_docs_since
%% 4 parse result, update failover log in state
%% 5. rollback
    {ok, EndSeq} = couch_upr:get_sequence_number(UprPid, Vb),
    ?xdcr_debug("XDCR-UPR: get endseq ~p for vb ~p", [EndSeq, Vb]),
    RV =
        case EndSeq =:= StartSeq of
            true ->
                {ok, 0, StartSeq};
            _ ->
                VbVersions = gen_server:call(UprPid, {get_vb_versions, Vb}),
                ?xdcr_debug("XDCR-UPR: for vb ~p, Vb versions: ~p", [Vb, VbVersions]),


                Result = couch_upr:enum_docs_since(
                           UprPid, Vb, VbVersions, StartSeq, EndSeq,
                           ChangesCbk, 0),

                ?xdcr_debug("XDCR-UPR: for vb ~p, enum_docs results: ~p", [Result]),

                case Result of
                    {ok, AccCount, NewVbVersions} ->
                        ok = gen_server:call(UprPid, {update_vb_versions_endseq, Vb,
                                                      NewVbVersions, EndSeq}),
                        {ok, AccCount, EndSeq};
                    {rollback, RollbackSeq} ->
                        ok = gen_server:call(UprPid, {update_vb_rollback_seq, Vb,
                                                      RollbackSeq}),
                        {rollback, RollbackSeq}
                end
        end,
    ?xdcr_debug("XDCR-UPR: for vb ~p, RV: ~p", [Vb, RV]),
    RV.


%% --- handle_call functions --- %%
handle_call(connect, _From,
            #xdc_upr_server_state{name = Name, ip = Host, port = Port,
                                  timeout = TimeOut,
                                  bucket = Bucket} =  State) ->

    {ok, Socket} = gen_tcp:connect(Host, Port,
                                   [binary, {packet, raw},
                                    {active, false}, {reuseaddr, true}]),
    S1 = #state{
        socket = Socket,
        timeout = TimeOut,
        request_id = 0
    },
    % Authentication is used to specify from which bucket the data should
    % come from
    {RV, NewState} =
        case couch_upr:sasl_auth(list_to_binary(Bucket), S1) of
            {ok, S2} ->
                 ?xdcr_debug("XDCR-UPR: SASL authed succesfully for bucket :~p.", [Bucket]),
                 S3 = couch_upr:open_connection(list_to_binary(Name), S2),
                 ?xdcr_debug("XDCR-UPR: open connection created for bucket ~p with state: ~p.",
                             [Bucket, S3]),
                 {ok, State#xdc_upr_server_state{socket = S3#state.socket,
                                                 status = active,
                                                 request_id = S3#state.request_id}};
             {stop, sasl_auth_failed} ->
                 ?xdcr_debug("XDCR-UPR: SASL auth failed for bucket :~p.", [Bucket]),
                 {stop, State}
         end,

    case RV of
        ok ->
            {reply, ok, NewState};
        stop ->
            {reply, sasl_auth_failed, NewState}
    end;

handle_call(disconnect, {_Pid, _Tag}, #xdc_upr_server_state{} =  State) ->
    State1 = close_connection(State),
    Name = State1#xdc_upr_server_state.name,
    ?xdcr_debug("XDCR-UPR: upr server ~p disconnected from memcached.", Name),
    {reply, ok, State1, hibernate};

handle_call({get_vb_versions, Vb}, _From, State) ->
    AccVersions = State#xdc_upr_server_state.vb_versions,
    VbVersions = couch_util:get_value(Vb, AccVersions),
    {reply, VbVersions, State};

handle_call({update_vb_versions_endseq, Vb, NewVbVer, EndSeq}, _From,
            #xdc_upr_server_state{vb_versions = AllVbVersions,
                                  vb_endseq = AllVbEndSeq} = State) ->
    AllVbEndSeq2 = orddict:store(Vb, EndSeq, AllVbEndSeq),
    AllVbVersions2 = lists:ukeymerge(1, [{Vb, NewVbVer}], AllVbVersions),
    {reply, ok, State#xdc_upr_server_state{vb_versions = AllVbVersions2, vb_endseq = AllVbEndSeq2}};

handle_call({update_vb_rollback_seq, Vb, RollbackSeq}, _From,
            #xdc_upr_server_state{vb_rollback = AllVbRollback} = State) ->
    CurrVbRollback = case orddict:find(Vb, AllVbRollback) of
                         {ok, Value} ->
                             Value;
                         error ->
                             []
                     end,
    AllVbRollback2 = orddict:store(Vb, [RollbackSeq | CurrVbRollback], AllVbRollback),
    {reply, ok, State#xdc_upr_server_state{vb_rollback = AllVbRollback2}};

handle_call(get_request_id, _From, State) ->
    RequestId = case State#xdc_upr_server_state.request_id of
    RequestId0 when RequestId0 < 1 bsl (?UPR_SIZES_OPAQUE + 1) ->
        RequestId0;
    _ ->
        0
    end,
    {reply, RequestId, State#xdc_upr_server_state{request_id=RequestId + 1}};

handle_call(get_socket_and_timeout, _From,
            #xdc_upr_server_state{socket = Socket, timeout = Timeout} = State) ->
    {reply, {Socket, Timeout}, State};

handle_call(Msg, From, State) ->
    ?xdcr_debug("XDCR-UPR: receive unexpected call ~p from ~p, ignore.", [Msg, From]),
    {reply, ok, State}.

%% --- handle_cast functions --- %%
handle_cast(stop, #xdc_upr_server_state{} = State) ->
    %% let terminate() do the cleanup
    {stop, normal, State};
handle_cast({report_error, Err}, #xdc_upr_server_state{error_reports = Errs} = State) ->
    {noreply, State#xdc_upr_server_state{error_reports = ringbuffer:add(Err, Errs)}};
handle_cast(Msg, #xdc_upr_server_state{bucket = Bucket} = State) ->
    ?xdcr_error("[xdcr-upr server for bucket ~p]: received unexpected cast ~p",
                [Bucket, Msg]),
    {stop, {error, {unexpected_cast, Msg}}, State}.

%% --- handle_info --- %%
handle_info({'EXIT',_Pid, normal}, St) ->
    {noreply, St};
handle_info({'EXIT',_Pid, Reason}, St) ->
    {stop, Reason, St};
handle_info(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.

%% --- default gen_server callbacks --- %%
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, State) when Reason == normal orelse Reason == shutdown ->
    terminate_cleanup(State);
terminate(Reason, #xdc_upr_server_state{bucket = Bucket,
                                        parent = Par} = State) ->
    report_error(Reason, Bucket, Par),
    ?xdcr_error("[xdcr-upr server for vb ~p]: shutdown xdcr-upr server, error reported to"
                "parent: ~p", [Bucket, Par]),
    terminate_cleanup(State),
    ok.

%% ------------------------------------------ %%
%%           internal help functions          %%
%% ------------------------------------------ %%
report_error(Err, _Vb, _Parent) when Err == normal orelse Err == shutdown ->
    ok;
report_error(Err, Vb, Parent) ->
     %% return raw erlang time to make it sortable
    RawTime = erlang:localtime(),
    Time = misc:iso_8601_fmt(RawTime),
    String = iolist_to_binary(io_lib:format("~s - [XDCR-UPR] Error replicating "
                                            "vbucket ~p: ~p",
                                            [Time, Vb, Err])),
    gen_server:cast(Parent, {report_error, {RawTime, String}}).

close_connection(#xdc_upr_server_state{socket = Socket} = State) ->
    case Socket of
        undefined ->
            ok;
        _ ->
            ok = gen_tcp:close(Socket)
    end,
    State#xdc_upr_server_state{socket = undefined, status = idle}.

terminate_cleanup(#xdc_upr_server_state{} = State) ->
    close_connection(State),
    ok.


