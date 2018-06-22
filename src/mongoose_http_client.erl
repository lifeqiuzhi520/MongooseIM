%%==============================================================================
%% Copyright 2016 Erlang Solutions Ltd.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%==============================================================================
-module(mongoose_http_client).
-include("mongoose.hrl").

%% API
-export([start/0, stop/0, start_pool/2, stop_pool/1, get_pool/1, get/3, post/4]).

%% Exported for testing
-export([start/1]).

%%------------------------------------------------------------------------------
%% API

-spec start() -> any().
start() ->
    case ejabberd_config:get_local_option(http_connections) of
        undefined -> ok;
        Opts -> start(Opts)
    end.

-spec stop() -> any().
stop() ->
    ok.

-spec start_pool(atom(), list()) -> ok.
start_pool(Name, Opts) ->
    do_start_pool(Name, Opts).

-spec stop_pool(atom()) -> ok.
stop_pool(Name) ->
    wpool:stop_pool(pool_proc_name(Name)).

-spec get(pool(), binary(), list()) ->
                 {ok, {binary(), binary()}} | {error, any()}.
get(Pool, Path, Headers) ->
    make_request(Pool, Path, <<"GET">>, Headers, <<>>).

-spec post(pool(), binary(), list(), binary()) ->
                 {ok, {binary(), binary()}} | {error, any()}.
post(Pool, Path, Headers, Query) ->
    make_request(Pool, Path, <<"POST">>, Headers, Query).

%%------------------------------------------------------------------------------
%% exported for testing

start(Opts) ->
    wpool:start(),
    [start_pool(Name, PoolOpts) || {Name, PoolOpts} <- Opts],
    ok.

%%------------------------------------------------------------------------------
%% internal functions

do_start_pool(Name, Opts) ->
    Server = gen_mod:get_opt(server, Opts, "http://localhost"),
    Size = gen_mod:get_opt(pool_size, Opts, 20),
    PathPrefix = list_to_binary(gen_mod:get_opt(path_prefix, Opts, "/")),
    SelectionStrategy = gen_mod:get_opt(selection_strategy, Opts, available_worker),
    SupervisorStrategy = gen_mod:get_opt(supervisor_strategy, Opts, {one_for_one, 5, 60}),
    WpoolOpts = [{workers, Size},
                 {worker, {mongoose_http_client_worker, [Server, PathPrefix, SelectionStrategy]}},
                 {strategy, SupervisorStrategy}],
    ProcName = pool_proc_name(Name),
    wpool:start_pool(ProcName, WpoolOpts),
    ok.

pool_proc_name(PoolName) ->
    list_to_atom("mongoose_http_client_pool_" ++ atom_to_list(PoolName)).

make_request(Pool, Path, Method, Headers, Query) ->
    case catch wpool:call(pool_proc_name(Pool), {Path, Method, Headers, Query}) of
        {'EXIT', {timeout, _}} ->
            {error, pool_timeout};
        {ok, {{Code, _Reason}, _RespHeaders, RespBody, _, _}} ->
            {ok, {Code, RespBody}};
        {error, timeout} ->
            {error, request_timeout};
        {'EXIT', Reason} ->
            {error, {'EXIT', Reason}};
        {error, Reason} ->
            {error, Reason}
    end.


