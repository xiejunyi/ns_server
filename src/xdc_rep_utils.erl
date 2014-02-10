%% @author Couchbase <info@couchbase.com>
%% @copyright 2011 Couchbase, Inc.
%%
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

%% XDC Replication Specific Utility Functions

-module(xdc_rep_utils).

-export([parse_rep_doc/2]).
-export([local_couch_uri_for_vbucket/2]).
-export([remote_couch_uri_for_vbucket/3, my_active_vbuckets/1]).
-export([parse_rep_db/3]).
-export([split_dbname/1]).
-export([get_master_db/1, get_checkpoint_log_id/2]).
-export([get_trace_dump_invprob/0]).
-export([sanitize_status/3, get_rep_info/1]).
-export([buildXDCDocFromMutation/2]).

-include("xdc_replicator.hrl").

%% Given a Bucket name and a vbucket id, this function computes the Couch URI to
%% locally access it.
local_couch_uri_for_vbucket(BucketName, VbucketId) ->
    iolist_to_binary([BucketName, $/, integer_to_list(VbucketId)]).


%% Given the vbucket map and node list of a remote bucket and a vbucket id, this
%% function computes the CAPI URI to access it.
remote_couch_uri_for_vbucket(VbucketMap, NodeList, VbucketId) ->
    [Owner | _ ] = lists:nth(VbucketId+1, VbucketMap),
    {OwnerNodeProps} = lists:nth(Owner+1, NodeList),
    CapiBase = couch_util:get_value(<<"couchApiBase">>, OwnerNodeProps),
    iolist_to_binary([CapiBase, "%2F", integer_to_list(VbucketId)]).


%% Given a bucket config, this function computes a list of active vbuckets
%% currently owned by the executing node.
my_active_vbuckets(BucketConfig) ->
    VBucketMap = couch_util:get_value(map, BucketConfig),
    [Ordinal-1 ||
        {Ordinal, Owner} <- misc:enumerate([Head || [Head|_] <- VBucketMap]),
        Owner == node()].

%% Parse replication document
parse_rep_doc(DocId, {Props}) ->
    ProxyParams = parse_proxy_params(get_value(<<"proxy">>, Props, <<>>)),
    Options = xdc_settings:get_all_settings_snapshot(Props),
    Source = parse_rep_db(get_value(<<"source">>, Props), ProxyParams, Options),
    Target = parse_rep_db(get_value(<<"target">>, Props), ProxyParams, Options),
    RepMode = case get_value(<<"type">>, Props) of
                  <<"xdc">> ->
                      "capi";
                  <<"xdc-xmem">> ->
                      "xmem"
              end,
    #rep{id = DocId,
         source = Source,
         target = Target,
         replication_mode = RepMode,
         options = Options}.

parse_proxy_params(ProxyUrl) when is_binary(ProxyUrl) ->
    parse_proxy_params(?b2l(ProxyUrl));
parse_proxy_params([]) ->
    [];
parse_proxy_params(ProxyUrl) ->
    [{proxy, ProxyUrl}].

parse_rep_db({Props}, _ProxyParams, Options) ->
    Url = maybe_add_trailing_slash(get_value(<<"url">>, Props)),
    {AuthProps} = get_value(<<"auth">>, Props, {[]}),
    {BinHeaders} = get_value(<<"headers">>, Props, {[]}),
    Headers = lists:ukeysort(1, [{?b2l(K), ?b2l(V)} || {K, V} <- BinHeaders]),
    DefaultHeaders = (#httpdb{})#httpdb.headers,
    OAuth = case get_value(<<"oauth">>, AuthProps) of
                undefined ->
                    nil;
                {OauthProps} ->
                    #oauth{
                  consumer_key = ?b2l(get_value(<<"consumer_key">>, OauthProps)),
                  token = ?b2l(get_value(<<"token">>, OauthProps)),
                  token_secret = ?b2l(get_value(<<"token_secret">>, OauthProps)),
                  consumer_secret = ?b2l(get_value(<<"consumer_secret">>, OauthProps)),
                  signature_method =
                      case get_value(<<"signature_method">>, OauthProps) of
                          undefined ->        hmac_sha1;
                          <<"PLAINTEXT">> ->  plaintext;
                          <<"HMAC-SHA1">> ->  hmac_sha1;
                          <<"RSA-SHA1">> ->   rsa_sha1
                      end
                 }
            end,
    SslParams = case Url of
                    "http://" ++ _ ->
                        [];
                    "https://" ++ _ ->
                        %% we cannot allow https without cert. If cert
                        %% is missing, requests must not go through
                        %% (undefined will cause crash in call below)
                        CertPEM = get_value(xdcr_cert, Options, undefined),
                        ns_ssl:cert_pem_to_ssl_verify_options(CertPEM)
                end,
    ConnectOpts = get_value(socket_options, Options) ++ SslParams,
    Timeout = get_value(connection_timeout, Options) * 1000,
    LhttpcOpts = lists:keysort(1, [
                                   {connect_options, ConnectOpts},
                                   {connect_timeout, Timeout},
                                   {send_retry, 0}
                                  ]),

    #httpdb{
             url = Url,
             oauth = OAuth,
             headers = lists:ukeymerge(1, Headers, DefaultHeaders),
             lhttpc_options = LhttpcOpts,
             timeout = Timeout,
             httpc_pool = xdc_lhttpc_pool,
             http_connections = get_value(http_connections, Options),
             retries = get_value(retries_per_request, Options)
           };
parse_rep_db(<<"http://", _/binary>> = Url, ProxyParams, Options) ->
    parse_rep_db({[{<<"url">>, Url}]}, ProxyParams, Options);
parse_rep_db(<<"https://", _/binary>> = Url, ProxyParams, Options) ->
    parse_rep_db({[{<<"url">>, Url}]}, ProxyParams, Options);
parse_rep_db(<<DbName/binary>>, _ProxyParams, _Options) ->
    DbName.

maybe_add_trailing_slash(Url) when is_binary(Url) ->
    maybe_add_trailing_slash(?b2l(Url));
maybe_add_trailing_slash(Url) ->
    case lists:last(Url) of
        $/ ->
            Url;
        _ ->
            Url ++ "/"
    end.


get_checkpoint_log_id(#db{name = DbName0}, LogId0) ->
    get_checkpoint_log_id(?b2l(DbName0), LogId0);
get_checkpoint_log_id(#httpdb{url = DbUrl0}, LogId0) ->
    [_, _, DbName0] = string:tokens(DbUrl0, "/"),
    get_checkpoint_log_id(couch_httpd:unquote(DbName0), LogId0);
get_checkpoint_log_id(DbName0, LogId0) ->
    {DbName, _UUID} = split_uuid(DbName0),
    {_, VBucket} = split_dbname(DbName),
    ?l2b([?LOCAL_DOC_PREFIX, integer_to_list(VBucket), "-", LogId0]).

get_master_db(#db{name = DbName}) ->
    ?l2b(get_master_db(?b2l(DbName)));
get_master_db(#httpdb{url=DbUrl0}=Http) ->
    [Scheme, Host, DbName0] = string:tokens(DbUrl0, "/"),
    DbName = get_master_db(couch_httpd:unquote(DbName0)),

    DbUrl = Scheme ++ "//" ++ Host ++ "/" ++ couch_httpd:quote(DbName) ++ "/",
    Http#httpdb{url = DbUrl};
get_master_db(DbName0) ->
    {DbName, UUID} = split_uuid(DbName0),
    {Bucket, _} = split_dbname(DbName),
    MasterDbName = Bucket ++ "/master",
    unsplit_uuid({MasterDbName, UUID}).

split_dbname(DbName) ->
    %% couchbase does not support slashes in bucket names; thus we can have only
    %% two items in this list
    [BucketName, VBucketStr] = string:tokens(DbName, [$/]),
    {BucketName, list_to_integer(VBucketStr)}.

split_uuid(DbName) ->
    case string:tokens(DbName, [$;]) of
        [RealDbName, UUID] ->
            {RealDbName, UUID};
        _ ->
            {DbName, undefined}
    end.

unsplit_uuid({DbName, undefined}) ->
    DbName;
unsplit_uuid({DbName, UUID}) ->
    DbName ++ ";" ++ UUID.

%% inverse probability to dump non-critical datapath trace,
%% trace will be dumped by probability 1/N
-spec get_trace_dump_invprob() -> integer().
get_trace_dump_invprob() ->
    xdc_settings:get_global_setting(trace_dump_invprob).

sanitize_url(Url) when is_binary(Url) ->
    ?l2b(sanitize_url(?b2l(Url)));
sanitize_url(Url) when is_list(Url) ->
    I = string:rchr(Url, $@),
    case I of
        0 ->
            Url;
        _ ->
            "*****" ++ string:sub_string(Url, I)
    end;
sanitize_url(Url) ->
    Url.

sanitize_state(State) ->
    misc:rewrite_tuples(fun (T) ->
                                case T of
                                    #xdc_rep_xmem_remote{} = Remote ->
                                        {stop, Remote#xdc_rep_xmem_remote{password = "*****"}};
                                    #rep_state{} = RepState ->
                                        {continue,
                                         RepState#rep_state{target_name = sanitize_url(RepState#rep_state.target_name)}};
                                    #httpdb{} = HttpDb ->
                                        {stop,
                                         HttpDb#httpdb{url = sanitize_url(HttpDb#httpdb.url)}};
                                    _ ->
                                        {continue, T}
                                end
                        end, State).

sanitize_status(_Opt, _PDict, State) ->
    [{data, [{"State", sanitize_state(State)}]}].

get_rep_info(#rep{source = Src, target = Tgt, replication_mode = Mode}) ->
    ?format_msg("from ~p to ~p in mode: ~p", [Src, Tgt, Mode]).

buildXDCDocFromMutation(#doc{} = Doc, Seq) ->
    #doc{id = Key,
         rev = {RevSeq, <<CAS:64, Exp:32, Flag:32>>},
         body = Value,
         deleted = Deletion} = Doc,

    DocInfo = #doc_info{
                 id = Key,
                 deleted = Deletion,
                 local_seq = Seq,
                 rev = {RevSeq, <<CAS:64, Exp:32, Flag:32>>},
                 size = byte_size(Value)
                },

    XDCDoc = #xdc_doc{docinfo = DocInfo, body = Value, meta = []},
    ?xdcr_debug("xdcr-junyi: build xdcodc for key: ~p", [Key]),
    XDCDoc.
