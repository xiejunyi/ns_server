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

-ifndef(_XDC_COMMON__HRL_).
-define(_XDC_COMMON__HRL_,).

%% couchdb headers
-include("couch_db.hrl").
-include("couch_js_functions.hrl").
-include("couch_api_wrap.hrl").
-include("../lhttpc/lhttpc.hrl").
-include_lib("couch_upr/include/couch_upr.hrl").

%% ns_server headers
-include("ns_common.hrl").

%% imported functions
-import(couch_util, [
                     get_value/2,
                     get_value/3,
                     to_binary/1
                    ]).

%% ------------------------------------%%
%%  constants and macros used by XDCR  %%
%% ------------------------------------%%
-define(REP_ID_VERSION, 2).
%% capture the last 10 entries of checkpoint history per bucket replicator
-define(XDCR_CHECKPOINT_HISTORY, 10).
%% capture the last 10 entries of error history per bucket replicator
-define(XDCR_ERROR_HISTORY, 10).
%% interval (millisecs) to compute rate stats
-define(XDCR_RATE_STAT_INTERVAL_MS, 1000).
%% sleep time in secs before retry
-define(XDCR_SLEEP_BEFORE_RETRY, 30).
%% constants used by XMEM
-define(XDCR_XMEM_CONNECTION_ATTEMPTS, 16).
-define(XDCR_XMEM_CONNECTION_TIMEOUT, 120000).  %% timeout in ms
%% builder of error/warning/debug msgs
-define(format_msg(Msg, Args), lists:flatten(io_lib:format(Msg, Args))).

-define(xdcr_trace(Format, Args),
        case random:uniform(xdc_rep_utils:get_trace_dump_invprob()) of
            1 ->
                ?xdcr_debug(Format, Args);
            _ ->
                ok
        end).

%% number of memcached errors before stop replicator
-define(XDCR_XMEM_MEMCACHED_ERRORS, 1).


%% concurrency throttle type
-define(XDCR_INIT_CONCUR_THROTTLE, "xdcr-init").
-define(XDCR_REPL_CONCUR_THROTTLE, "xdcr-repl").

%% data source of streaming mutations
-define(XDCR_FROM_UPR, "xdcr-from-upr").
-define(XDCR_FROM_COUCHDB,   "xdcr-from-couchdb").


%% -------------------------%%
%%   XDCR data structures   %%
%% -------------------------%%

%% replication settings used by bucket level and vbucket level replicators
-record(rep, {
          id,
          source,
          target,
          replication_mode,
          options
         }).

%% rate of replicaiton stat maintained in bucket replicator
-record(ratestat, {
          timestamp = now(),
          item_replicated = 0,
          data_replicated = 0,
          curr_rate_item = 0,
          curr_rate_data = 0
}).

%% vbucket replication status and statistics, used by xdc_vbucket_rep
-record(rep_vb_status, {
          vb,
          pid,
          status = idle,

          %% following stats initialized to 0 when vb replicator starts, and refreshed
          %% when update stat to bucket replicator. The bucket replicator is responsible
          %% for aggretating the statistics for each vb. These stats may be from different
          %% vb replicator processes. We do not need to persist these stats in checkpoint
          %% doc. Consequently the lifetime of these stats at vb replicator level is the
          %% same as that of its parent vb replicator process.

          %% # of docs have been checked for eligibility of replication
          docs_checked = 0,
          %% of docs have been replicated
          docs_written = 0,
          %% of docs have been replicated optimistically
          docs_opt_repd = 0,
          %% bytes of data replicated
          data_replicated = 0,
          %% num of checkpoints issued successfully
          num_checkpoints = 0,
          %% total num of failed checkpoints
          num_failedckpts = 0,
          work_time = 0, % in MS
          commit_time = 0,  % in MS

          %% following stats are handled differently from above. They will not be
          %% aggregated at bucket replicator, instead, each vb replicator will
          %% fetch these stats from couchdb and worker_queue, and publish them
          %% directly to bucket replicator

          %% # of docs to replicate
          num_changes_left = 0,
          %% num of docs in changes queue
          docs_changes_queue = 0,
          %% size of changes queues
          size_changes_queue = 0,

          %% following are per vb stats since the replication starts
          %% from the very beginning. They are persisted in the checkpoint
          %% documents and may span the lifetime of multiple vb replicators
          %% for the same vbucket
          total_docs_checked = 0,
          total_docs_written = 0,
          total_docs_opt_repd = 0,
          total_data_replicated = 0,

          %% latency stats
          meta_latency_aggr = 0,
          meta_latency_wt = 0,
          docs_latency_aggr = 0,
          docs_latency_wt = 0,

          %% worker stats
          workers_stat = dict:new() %% dict of each worker's latency stats (key = pid, value = #worker_stat{})
 }).

%% vbucket checkpoint status used by each vbucket replicator and status reporting
%% to bucket replicator
-record(rep_checkpoint_status, {
          %% timestamp of the checkpoint from now() with granularity of microsecond, used
          %% as key for ordering
          ts,
          time,   % human readable local time
          vb,     % vbucket id
          succ,   % true if a succesful checkpoint, false otherwise
          error   % error msg
 }).

%% batch of documents usd by vb replicator worker process
-record(batch, {
          docs = [],
          size = 0,
          items = 0
         }).

%% bucket level replication state used by module xdc_replication
-record(replication, {
          rep = #rep{},                    % the basic replication settings
          mode,                            % replication mode
          vbucket_sup,                     % the supervisor for vb replicators
          vbs = [],                        % list of vb we should be replicating
          num_tokens = 0,                  % number of available tokens used by throttles
          init_throttle,                   % limits # of concurrent vb replicators initializing
          work_throttle,                   % limits # of concurrent vb replicators working
          num_active = 0,                  % number of active replicators
          num_waiting = 0,                 % number of waiting replicators
          vb_rep_dict = dict:new(),        % contains state and stats for each replicator

          %% rate of replication
          ratestat = #ratestat{},
          %% history of last N errors
          error_reports = ringbuffer:new(?XDCR_ERROR_HISTORY),
          %% history of last N checkpoints
          checkpoint_history = ringbuffer:new(?XDCR_CHECKPOINT_HISTORY),
          %% UPR server
          upr_server = nil
         }).

%% vbucket level replication state used by module xdc_vbucket_rep
-record(rep_state, {
          rep_details = #rep{},
          %% vbreplication stats
          status = #rep_vb_status{},
          %% time the vb replicator intialized
          rep_start_time,

          %% xmem server process
          xmem_srv,
          %% remote node
          xmem_remote,

          throttle,
          parent,
          source_name,
          target_name,
          source,
          target,
          src_master_db,
          tgt_master_db,
          history,
          checkpoint_history,
          start_seq,
          committed_seq,
          current_through_seq,
          source_cur_seq,
          seqs_in_progress = [],
          highest_seq_done = ?LOWEST_SEQ,
          source_log,
          target_log,
          rep_starttime,
          src_starttime,
          tgt_starttime,
          timer = nil, %% checkpoint timer

          %% timer to account the working time, reset every time we publish stats to
          %% bucket replicator
          work_start_time,
          last_checkpoint_time,
          workers,
          changes_queue,
          session_id,
          source_seq = nil,
          upr_server = nil,

          %% a boolean variable indicating that the rep has already
          %% behind db purger, at least one deletion has been lost.
          behind_purger
         }).

%% vbucket replicator worker process state used by xdc_vbucket_rep_worker
-record(rep_worker_state, {
          cp,
          loop,
          max_parallel_conns,
          source,
          target,
          readers = [],
          writer = nil,
          pending_fetch = nil,
          flush_waiter = nil,
          source_db_compaction_notifier = nil,
          target_db_compaction_notifier = nil,
          batch = #batch{}
         }).

%% concurrency throttle state used by module concurrency_throttle
-record(concurrency_throttle_state, {
          %% parent process creating the throttle server
          parent,
          %% type of concurrency throttle
          type,
          %% total number of tokens
          total_tokens,
          %% number of available tokens
          avail_tokens,
          %% table of waiting requests to be scheduled
          %% (key = Pid, value = {Signal, LoadKey})
          waiting_pool,
          %% table of active, scheduled requests
          %% (key = Pid, value = LoadKey)
          active_pool,
          %% table of load at target node
          %% (key = TargetNode, value = number of active requests on that node)
          target_load,
          %% table of monitoring refs
          %% (key = Pid, value = monitoring reference)
          monitor_dict
         }).

%% options to start xdc replication worker process
-record(rep_worker_option, {
          worker_id,               %% unique id of worker process starting from 1
          cp,                      %% parent vb replicator process
          source = #db{},          %% source db
          target = #httpdb{},      %% target db
          changes_manager,         %% process to queue changes from storage
          max_conns,               %% max connections
          xmem_server,             %% XMem server process
          opt_rep_threshold,       %% optimistic replication threshold
          batch_size,              %% batch size (in bytes)
          batch_items              %% batch items
         }).

%% statistics reported from worker process to its parent vbucket replicator
-record(worker_stat, {
          worker_id,
          seq = 0,
          worker_meta_latency_aggr = 0,
          worker_docs_latency_aggr = 0,
          worker_data_replicated = 0,
          worker_item_checked = 0,
          worker_item_replicated = 0,
          worker_item_opt_repd = 0
         }).

%% option to start vbucket replicator
-record(xdc_vb_rep_start_option, {
          sup,
          vb,
          rep = #rep{},
          mode,
          parent,
          init_throttle,
          work_throttle,
          upr_server
         }).

%%-----------------------------------------%%
%%            XDCR-MEMCACHED               %%
%%-----------------------------------------%%
% statistics
-record(xdc_vb_rep_xmem_statistics, {
          item_replicated = 0,
          data_replicated = 0,
          ckpt_issued = 0,
          ckpt_failed = 0
          }).

%% information needed talk to remote memcached
-record(xdc_rep_xmem_remote, {
          ip, %% inet:ip_address(),
          port, %% inet:port_number(),
          bucket = "default",
          username = "_admin",
          password = "_admin",
          options = []
         }).

%% xmem server state
-record(xdc_vb_rep_xmem_srv_state, {
          vb,
          parent_vb_rep,
          num_workers,
          pid_workers,
          statistics = #xdc_vb_rep_xmem_statistics{},
          remote = #xdc_rep_xmem_remote{},
          seed,
          enable_pipeline = false,
          error_reports
         }).

%% xmem worker state
-record(xdc_vb_rep_xmem_worker_state, {
          id,
          vb,
          parent_server_pid,
          status,
          statistics = #xdc_vb_rep_xmem_statistics{},
          socket, %% inet:socket(),
          time_connected,
          time_init,
          error_reports,
          local_conflict_resolution,
          connection_timeout,
          mcd_loc
         }).


%% -------------------------------------------- %%
%%     GENERIC UPR OPS CODES AND CONSTANTS      %%
%% -------------------------------------------- %%
%%--- START OF COPY FROM COUCHDB_URP.HRL --%%
%%-define(UPR_HEADER_LEN, 24).
%%-define(UPR_MAGIC_REQUEST, 16#80).
%%-define(UPR_MAGIC_RESPONSE, 16#81).
%%-define(UPR_OPCODE_OPEN_CONNECTION, 16#50).
%%-define(UPR_OPCODE_STREAM_REQUEST, 16#53).
%%-define(UPR_OPCODE_FAILOVER_LOG_REQUEST, 16#54).
%%-define(UPR_OPCODE_STREAM_END, 16#55).
%%-define(UPR_OPCODE_SNAPSHOT_MARKER, 16#56).
%%-define(UPR_OPCODE_MUTATION, 16#57).
%%-define(UPR_OPCODE_DELETION, 16#58).
%%-define(UPR_OPCODE_EXPIRATION, 16#59).
%%-define(UPR_OPCODE_STATS, 16#10).
%%-define(UPR_OPCODE_SASL_AUTH, 16#21).
%%-define(UPR_FLAG_OK, 16#00).
%%-define(UPR_FLAG_STATE_CHANGED, 16#01).
%%-define(UPR_FLAG_CONSUMER, 16#00).
%%-define(UPR_FLAG_PRODUCER, 16#01).
%%-define(UPR_REQUEST_TYPE_MUTATION, 16#03).
%%-define(UPR_REQUEST_TYPE_DELETION, 16#04).
%%-define(UPR_STATUS_OK, 16#00).
%%-define(UPR_STATUS_KEY_NOT_FOUND, 16#01).
%%-define(UPR_STATUS_ROLLBACK, 16#23).
%%-define(UPR_STATUS_NOT_MY_VBUCKET, 16#07).
%%-define(UPR_STATUS_ERANGE, 16#22).
%%-define(UPR_STATUS_SASL_AUTH_FAILED, 16#20).
%%% The sizes are in bits
%%-define(UPR_SIZES_KEY_LENGTH, 16).
%%-define(UPR_SIZES_PARTITION, 16).
%%-define(UPR_SIZES_BODY, 32).
%%-define(UPR_SIZES_OPAQUE, 32).
%%-define(UPR_SIZES_CAS, 64).
%%-define(UPR_SIZES_BY_SEQ, 64).
%%-define(UPR_SIZES_REV_SEQ, 64).
%%-define(UPR_SIZES_FLAGS, 32).
%%-define(UPR_SIZES_EXPIRATION, 32).
%%-define(UPR_SIZES_LOCK, 32).
%%-define(UPR_SIZES_KEY, 40).
%%-define(UPR_SIZES_VALUE, 56).
%%-define(UPR_SIZES_PARTITION_UUID, 64).
%%-define(UPR_SIZES_RESERVED, 32).
%%-define(UPR_SIZES_STATUS, 16).
%%-define(UPR_SIZES_SEQNO, 32).
%%-define(UPR_SIZES_METADATA_LENGTH, 16).
%%-define(UPR_SIZES_NRU_LENGTH, 8).
%%
% NOTE vmx 2014-01-16: In ep-engine the maximum size is currently 25
%%-define(UPR_MAX_FAILOVER_LOG_SIZE, 25).
%%
%%-type upr_status() :: non_neg_integer().
%%-type request_id() :: non_neg_integer().
%%-type size()       :: non_neg_integer().
%%-type socket()     :: port().
%%
%%% Those types are duplicates from couch_set_view.hrl
%%-type uint64()                   :: 0..18446744073709551615.
%%-type partition_id()             :: non_neg_integer().
%%-type update_seq()               :: non_neg_integer().
%%-type uuid()                     :: uint64().
%%-type partition_version()        :: [{uuid(), update_seq()}].
%%% Manipulate via ordsets or orddict, keep it ordered by partition id.
%%-type partition_versions()       :: ordsets:ordset({partition_id(), partition_version()}).
%%
%%--- END OF COPY FROM COUCHDB_URP.HRL --%%

%% -------------------------- %%
%%  XDCR-UPR constants        %%
%% -------------------------- %%
-define(XDCR_UPR_MAX_FAILOVER_HISTORY, 100).
-define(XDCR_UPR_DEFAULT_CONNECTION_TIMEOUT_MS, 30000).
-define(XDCR_UPR_DEFAULT_PORT, 11210).

%% -------------------------- %%
%%  XDCR-UPR data structures  %%
%% -------------------------- %%
%%-record(xdc_upr_server_option, {
%%          vbs = [],
%%          name = nil,
%%          timeout = 0,
%%          ip = nil,
%%          port = nil,
%%          user = nil,
%%          passwd = nil,
%%          other = []
%%         }).
%%
-record(xdc_upr_server_stats, {
          num_mutations_streamed = 0,
          data_bytes_streamed = 0
         }).

-record(xdc_upr_server_state, {
          name = nil,
          vbs = [],
          bucket = nil,

          timeout = 0,
          ip = nil,
          port = nil,
          user = nil,
          passwd = nil,

          parent = nil,
          socket = nil,
          request_id = 0,
          vb_versions = [],
          vb_endseq = nil,
          vb_rollback = nil,
          stats = #xdc_upr_server_stats{},
          status = undefined,
          error_reports = nil
         }).

-record(xdc_upr_stream_request_option, {
          vb = nil,
          version = nil,
          start_seq = 0,
          end_seq = 0,
          cbk_func = undefined,
          aggregator = undefined
         }).

%% mutations streaming from UPR servre
-record(xdc_upr_mutation, {
          seq = 0        :: non_neg_integer(),
          rev_seq = 0    :: non_neg_integer(),
          cas = 0        :: non_neg_integer(),
          flags = 0      :: non_neg_integer(),
          expiration = 0 :: non_neg_integer(),
          locktime = 0   :: non_neg_integer(),
          key = <<>>     :: binary(),
          val = <<>>     :: binary(),
          key_length = 0    :: non_neg_integer(),
          val_length = 0    :: non_neg_integer(),
          metadata       :: binary()
         }).

%% end of XDCR-UPR

%% an XDCR doc wrapper for doc_info and doc in couchdb
%% this is to unify the doc structure to stream from couchdb and upr
-record(xdc_doc, {
          docinfo = #doc_info{},

          %% the binary body
          body = <<"{}">>,
          %% unused, copy from couchdb.doc
          meta = []
         }).

-endif.
%% end of xdc_replicator.hrl

