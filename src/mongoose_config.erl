%% @doc Pure config logic.
%% No ets table manipulations, no Mnesia, no starting modules, no file reading here.
%% Everything here is safe, side effect free.
%% OK, logging is possible, but keep it to minimum.
-module(mongoose_config).
-export([parse_terms/1]).
-export([get_config_diff/2]).
-export([compute_config_version/2]).
-export([compute_config_file_version/1]).
-export([check_hosts/2]).
-export([can_be_ignored/1]).
-export([compare_modules/2]).
-export([compare_listeners/2]).
-export([is_not_host_specific/1]).

-export([allow_override_all/1,
         allow_override_local_only/1,
         state_to_opts/1,
         can_override/2]).

%% for unit tests
-export([group_host_changes/1]).

-include("mongoose.hrl").
-include("ejabberd_config.hrl").

-type key() :: atom()
             | {key(), jid:server() | atom() | list()}
             | {atom(), atom(), atom()}
             | binary(). % TODO: binary is questionable here

-type value() :: atom()
               | binary()
               | integer()
               | string()
               | [value()]
               | tuple().

-export_type([key/0, value/0]).

-record(state, {opts = [] :: list(),
                hosts = [] :: [host()],
                odbc_pools = [] :: [atom()],
                override_local = false :: boolean(),
                override_global = false :: boolean(),
                override_acls = false :: boolean()}).

-record(compare_result, {to_start = [] :: list(),
                         to_stop = [] :: list(),
                         to_reload = [] :: list()}).

-type compare_result() :: #compare_result{}.

-type host() :: any(). % TODO: specify this
-type state() :: #state{}.
-type macro() :: {macro_key(), macro_value()}.

%% The atom must have all characters in uppercase.
-type macro_key() :: atom().

-type macro_value() :: term().

-type known_term() :: override_global
                    | override_local
                    | override_acls
                    | {acl, _, _}
                    | {alarms, _}
                    | {access, _, _}
                    | {shaper, _, _}
                    | {host, _}
                    | {hosts, _}
                    | {host_config, _, _}
                    | {listen, _}
                    | {language, _}
                    | {sm_backend, _}
                    | {outgoing_s2s_port, integer()}
                    | {outgoing_s2s_options, _, integer()}
                    | {{s2s_addr, _}, _}
                    | {s2s_dns_options, [tuple()]}
                    | {s2s_use_starttls, integer()}
                    | {s2s_certfile, _}
                    | {domain_certfile, _, _}
                    | {node_type, _}
                    | {cluster_nodes, _}
                    | {registration_timeout, integer()}
                    | {mongooseimctl_access_commands, list()}
                    | {loglevel, _}
                    | {max_fsm_queue, _}
                    | {sasl_mechanisms, _}
                    | host_term().

-type host_term() :: {acl, _, _}
                   | {access, _, _}
                   | {shaper, _, _}
                   | {host, _}
                   | {hosts, _}
                   | {odbc_server, _}.

-callback stop(host()) -> any().


-spec search_hosts_and_pools({host|hosts, [host()] | host()}
                             | {pool, odbc, atom()}
                             | {pool, odbc, atom(), list()}, state()) -> any().
search_hosts_and_pools(Term, State) ->
    case Term of
        {host, Host} ->
            case State of
                #state{hosts = []} ->
                    add_hosts_to_option([Host], State);
                _ ->
                    ?ERROR_MSG("Can't load config file: "
                               "too many hosts definitions", []),
                    exit("too many hosts definitions")
            end;
        {hosts, Hosts} ->
            case State of
                #state{hosts = []} ->
                    add_hosts_to_option(Hosts, State);
                _ ->
                    ?ERROR_MSG("Can't load config file: "
                               "too many hosts definitions", []),
                    exit("too many hosts definitions")
            end;
        {pool, PoolType, PoolName, _Options} ->
            search_hosts_and_pools({pool, PoolType, PoolName}, State);
        {pool, odbc, PoolName} ->
            add_odbc_pool_to_option(PoolName, State);
        _ ->
            State
    end.


-spec add_hosts_to_option(Hosts :: [host()],
                          State :: state()) -> state().
add_hosts_to_option(Hosts, State) ->
    PrepHosts = normalize_hosts(Hosts),
    add_option(hosts, PrepHosts, State#state{hosts = PrepHosts}).

add_odbc_pool_to_option(PoolName, State) ->
    Pools = State#state.odbc_pools,
    State#state{odbc_pools = [PoolName | Pools]}.

-spec normalize_hosts([host()]) -> [binary() | tuple()].
normalize_hosts(Hosts) ->
    normalize_hosts(Hosts, []).


normalize_hosts([], PrepHosts) ->
    lists:reverse(PrepHosts);
normalize_hosts([Host | Hosts], PrepHosts) ->
    case jid:nodeprep(host_to_binary(Host)) of
        error ->
            ?ERROR_MSG("Can't load config file: "
                       "invalid host name [~p]", [Host]),
            exit("invalid hostname");
        PrepHost ->
            normalize_hosts(Hosts, [PrepHost | PrepHosts])
    end.

host_to_binary(Host) ->
    unicode:characters_to_binary(Host).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Support for Macro

%% @doc Replace the macros with their defined values.
-spec replace_macros(Terms :: [term()]) -> [term()].
replace_macros(Terms) ->
    {TermsOthers, Macros} = split_terms_macros(Terms),
    replace(TermsOthers, Macros).


%% @doc Split Terms into normal terms and macro definitions.
-spec split_terms_macros(Terms :: [term()]) -> {[term()], [macro()]}.
split_terms_macros(Terms) ->
    lists:foldl(fun split_terms_macros_fold/2, {[], []}, Terms).

-spec split_terms_macros_fold(any(), Acc) -> Acc when
      Acc :: {[term()], [{Key :: any(), Value :: any()}]}.
split_terms_macros_fold({define_macro, Key, Value} = Term, {TOs, Ms}) ->
    case is_atom(Key) and is_all_uppercase(Key) of
        true ->
            {TOs, Ms ++ [{Key, Value}]};
        false ->
            exit({macro_not_properly_defined, Term})
    end;
split_terms_macros_fold(Term, {TOs, Ms}) ->
    {TOs ++ [Term], Ms}.


%% @doc Recursively replace in Terms macro usages with the defined value.
-spec replace(Terms :: [term()],
              Macros :: [macro()]) -> [term()].
replace([], _) ->
    [];
replace([Term | Terms], Macros) ->
    [replace_term(Term, Macros) | replace(Terms, Macros)].


replace_term(Key, Macros) when is_atom(Key) ->
    case is_all_uppercase(Key) of
        true ->
            case proplists:get_value(Key, Macros) of
                undefined -> exit({undefined_macro, Key});
                Value -> Value
            end;
        false ->
            Key
    end;
replace_term({use_macro, Key, Value}, Macros) ->
    proplists:get_value(Key, Macros, Value);
replace_term(Term, Macros) when is_list(Term) ->
    replace(Term, Macros);
replace_term(Term, Macros) when is_tuple(Term) ->
    List = tuple_to_list(Term),
    List2 = replace(List, Macros),
    list_to_tuple(List2);
replace_term(Term, _) ->
    Term.


-spec is_all_uppercase(atom()) -> boolean().
is_all_uppercase(Atom) ->
    String = erlang:atom_to_list(Atom),
    lists:all(fun(C) when C >= $a, C =< $z -> false;
                 (_) -> true
              end, String).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Process terms

-spec process_term(Term :: known_term(),
                   State :: state()) -> state().
process_term(Term, State) ->
    case Term of
        override_global ->
            State#state{override_global = true};
        override_local ->
            State#state{override_local = true};
        override_acls ->
            State#state{override_acls = true};
        {acl, _ACLName, _ACLData} ->
            process_host_term(Term, global, State);
        {alarms, Env} ->
            add_option(alarms, Env, State);
        {access, _RuleName, _Rules} ->
            process_host_term(Term, global, State);
        {shaper, _Name, _Data} ->
            %%lists:foldl(fun(Host, S) -> process_host_term(Term, Host, S) end,
            %%          State, State#state.hosts);
            process_host_term(Term, global, State);
        {host, _Host} ->
            State;
        {hosts, _Hosts} ->
            State;
        {host_config, Host, Terms} ->
            lists:foldl(fun(T, S) ->
                            process_host_term(T, list_to_binary(Host), S) end,
                        State, Terms);
        {pool, odbc, _PoolName} ->
            State;
        {pool, odbc, PoolName, Options} ->
            lists:foldl(fun(T, S) ->
                            process_db_pool_term(T, PoolName, S)
                        end,
                        State, Options);
        {listen, Listeners} ->
            Listeners2 =
                lists:map(
                  fun({PortIP, Module, Opts}) ->
                      {Port, IPT, _, _, Proto, OptsClean} =
                          ejabberd_listener:parse_listener_portip(PortIP, Opts),
                      {{Port, IPT, Proto}, Module, OptsClean}
                  end,
                  Listeners),
            add_option(listen, Listeners2, State);
        {language, Val} ->
            add_option(language, list_to_binary(Val), State);
        {sm_backend, Val} ->
            add_option(sm_backend, Val, State);
        {outgoing_s2s_port, Port} ->
            add_option(outgoing_s2s_port, Port, State);
        {outgoing_s2s_options, Methods, Timeout} ->
            add_option(outgoing_s2s_options, {Methods, Timeout}, State);
        {{s2s_addr, Host}, Addr} ->
            add_option({s2s_addr, list_to_binary(Host)}, Addr, State);
        {{global_distrib_addr, Host}, Addr} ->
            add_option({global_distrib_addr, list_to_binary(Host)}, Addr, State);
        {s2s_dns_options, PropList} ->
            add_option(s2s_dns_options, PropList, State);
        {s2s_use_starttls, Port} ->
            add_option(s2s_use_starttls, Port, State);
        {s2s_ciphers, Ciphers} ->
            add_option(s2s_ciphers, Ciphers, State);
        {s2s_certfile, CertFile} ->
            case ejabberd_config:is_file_readable(CertFile) of
                true -> add_option(s2s_certfile, CertFile, State);
                false ->
                    ErrorText = "There is a problem in the configuration: "
                        "the specified file is not readable: ",
                    throw({error, ErrorText ++ CertFile})
            end;
        {domain_certfile, Domain, CertFile} ->
            case ejabberd_config:is_file_readable(CertFile) of
                true -> add_option({domain_certfile, Domain}, CertFile, State);
                false ->
                    ErrorText = "There is a problem in the configuration: "
                        "the specified file is not readable: ",
                    throw({error, ErrorText ++ CertFile})
            end;
        {node_type, NodeType} ->
            add_option(node_type, NodeType, State);
        {cluster_nodes, Nodes} ->
            add_option(cluster_nodes, Nodes, State);
        {watchdog_admins, Admins} ->
            add_option(watchdog_admins, Admins, State);
        {watchdog_large_heap, LH} ->
            add_option(watchdog_large_heap, LH, State);
        {registration_timeout, Timeout} ->
            add_option(registration_timeout, Timeout, State);
        {mongooseimctl_access_commands, ACs} ->
            add_option(mongooseimctl_access_commands, ACs, State);
        {routing_modules, Mods} ->
            add_option(routing_modules, Mods, State);
        {loglevel, Loglevel} ->
            ejabberd_loglevel:set(Loglevel),
            State;
        {max_fsm_queue, N} ->
            add_option(max_fsm_queue, N, State);
        {sasl_mechanisms, Mechanisms} ->
            add_option(sasl_mechanisms, Mechanisms, State);
        {http_connections, HttpConnections} ->
            add_option(http_connections, HttpConnections, State);
        {all_metrics_are_global, Value} ->
            add_option(all_metrics_are_global, Value, State);
        {services, Value} ->
            add_option(services, Value, State);
        {_Opt, _Val} ->
            process_term_for_hosts_and_pools(Term, State)
    end.

process_term_for_hosts_and_pools(Term = {Key, _Val}, State) ->
    BKey = atom_to_binary(Key, utf8),
    case get_key_group(BKey, Key) of
        odbc ->
            ok = check_pools(State#state.odbc_pools),
            lists:foldl(fun(Pool, S) -> process_db_pool_term(Term, Pool, S) end,
                        State, State#state.odbc_pools);
        _ ->
            lists:foldl(fun(Host, S) -> process_host_term(Term, Host, S) end,
                        State, State#state.hosts)
    end.

check_pools([]) ->
    ?CRITICAL_MSG("Config file invalid: ODBC defined with no pools", []),
    exit(no_odbc_pools);
check_pools(Pools) when is_list(Pools) ->
    ok.

-spec process_host_term(Term :: host_term(),
                        Host :: acl:host(),
                        State :: state()) -> state().
process_host_term(Term, Host, State) ->
    case Term of
        {acl, ACLName, ACLData} ->
            State#state{opts =
                            [acl:to_record(Host, ACLName, ACLData) | State#state.opts]};
        {access, RuleName, Rules} ->
            State#state{opts = [#config{key   = {access, RuleName, Host},
                                        value = Rules} |
                                State#state.opts]};
        {shaper, Name, Data} ->
            State#state{opts = [#config{key   = {shaper, Name, Host},
                                        value = Data} |
                                State#state.opts]};
        {host, Host} ->
            State;
        {hosts, _Hosts} ->
            State;
        {odbc_pool, Pool} when is_atom(Pool) ->
            add_option({odbc_pool, Host}, Pool, State);
        {riak_server, RiakConfig} ->
            add_option(riak_server, RiakConfig, State);
        {cassandra_servers, CassandraConfig} ->
            add_option(cassandra_servers, CassandraConfig, State);
        {elasticsearch_server, ESConfig} ->
            add_option(elasticsearch_server, ESConfig, State);
        {Opt, Val} ->
            add_option({Opt, Host}, Val, State)
    end.

process_db_pool_term({Opt, Val}, Pool, State) when is_atom(Pool) ->
    add_option({Opt, odbc_pool, Pool}, Val, State).

-spec add_option(Opt :: key(),
                 Val :: value(),
                 State :: state()) -> state().
add_option(Opt, Val, State) ->
    Table = case Opt of
                hosts ->
                    config;
                language ->
                    config;
                sm_backend ->
                    config;
                _ ->
                    local_config
            end,
    case Table of
        config ->
            State#state{opts = [#config{key = Opt, value = Val} |
                                State#state.opts]};
        local_config ->
            case Opt of
                {{add, OptName}, Host} ->
                    State#state{opts = compact({OptName, Host}, Val,
                                               State#state.opts, [])};
                _ ->
                    State#state{opts = [#local_config{key = Opt, value = Val} |
                                        State#state.opts]}
            end
    end.


compact({OptName, Host} = Opt, Val, [], Os) ->
    ?WARNING_MSG("The option '~p' is defined for the host ~p using host_config "
                 "before the global '~p' option. This host_config option may "
                 "get overwritten.", [OptName, Host, OptName]),
    [#local_config{key = Opt, value = Val}] ++ Os;
%% Traverse the list of the options already parsed
compact(Opt, Val, [O | Os1], Os2) ->
    case catch O#local_config.key of
        %% If the key of a local_config matches the Opt that wants to be added
        Opt ->
            %% Then prepend the new value to the list of old values
            Os2 ++ [#local_config{key = Opt,
                                  value = Val ++ O#local_config.value}
                   ] ++ Os1;
        _ ->
            compact(Opt, Val, Os1, Os2 ++ [O])
    end.


%%--------------------------------------------------------------------
%% Configuration parsing
%%--------------------------------------------------------------------

-spec parse_terms(term()) -> state().
parse_terms(Terms) ->
    State = lists:foldl(fun search_hosts_and_pools/2, #state{}, Terms),
    TermsWExpandedMacros = replace_macros(Terms),
    lists:foldl(fun process_term/2,
                add_option(odbc_pools, State#state.odbc_pools, State),
                TermsWExpandedMacros).

-spec get_config_diff(state(), map()) -> map().
get_config_diff(State, #{global_config := Config,
                         local_config := Local,
                         host_local_config := HostsLocal}) ->
                                                % New Options
    {NewConfig, NewLocal, NewHostsLocal} = categorize_options(State#state.opts),
                                                % Current Options
    %% global config diff
    CC = compare_terms(Config, NewConfig, 2, 3),
    LC = compare_terms(Local, NewLocal, 2, 3),
    LHC = compare_terms(group_host_changes(HostsLocal), group_host_changes(NewHostsLocal), 1, 2),
    #{config_changes => CC,
      local_config_changes => LC,
      local_hosts_changes => LHC}.



compute_config_version(LC, LCH) ->
    L0 = lists:filter(mk_node_start_filter(), LC ++ LCH),
    L1 = sort_config(L0),
    crypto:hash(sha, term_to_binary(L1)).

compute_config_file_version(#state{opts = Opts, hosts = Hosts}) ->
    L = sort_config(Opts ++ Hosts),
    crypto:hash(sha, term_to_binary(L)).

-spec check_hosts([jid:server()], [jid:server()]) -> {[jid:server()],
                                                                [jid:server()]}.
check_hosts(NewHosts, OldHosts) ->
    Old = sets:from_list(OldHosts),
    New = sets:from_list(NewHosts),
    ListToAdd = sets:to_list(sets:subtract(New, Old)),
    ListToDel = sets:to_list(sets:subtract(Old, New)),
    {ListToDel, ListToAdd}.


-spec can_be_ignored(Key :: atom() | tuple()) -> boolean().
can_be_ignored(Key) when is_atom(Key);
                         is_tuple(Key) ->
    L = [domain_certfile, s2s, all_metrics_are_global, odbc],
    lists:member(Key, L).


-spec compare_modules(term(), term()) -> compare_result().
compare_modules(OldMods, NewMods) ->
    compare_terms(OldMods, NewMods, 1, 2).

-spec compare_listeners(term(), term()) -> compare_result().
compare_listeners(OldListeners, NewListeners) ->
    compare_terms(map_listeners(OldListeners), map_listeners(NewListeners), 1, 2).

map_listeners(Listeners) ->
    lists:map(fun({PortIP, Module, Opts}) ->
                  {{PortIP, Module}, Opts}
              end, Listeners).

                                                % group values which can be grouped like odbc ones
-spec group_host_changes([term()]) -> [{atom(), [term()]}].
group_host_changes(Changes) when is_list(Changes) ->
    D = lists:foldl(fun(#local_config{key = {Key, Host}, value = Val}, Dict) ->
                        BKey = atom_to_binary(Key, utf8),
                        case get_key_group(BKey, Key) of
                            Key ->
                                dict:append({Key, Host}, Val, Dict);
                            NewKey ->
                                dict:append({NewKey, Host}, {Key, Val}, Dict)
                        end
                    end, dict:new(), Changes),
    [{Group, lists:sort(lists:flatten(MaybeDeepList))}
     || {Group, MaybeDeepList} <- dict:to_list(D)].


-spec is_not_host_specific(atom()
                           | {atom(), jid:server()}
                           | {atom(), atom(), atom()}) -> boolean().
is_not_host_specific(Key) when is_atom(Key) ->
    true;
is_not_host_specific({Key, Host}) when is_atom(Key), is_binary(Host) ->
    false;
is_not_host_specific({Key, PoolType, PoolName})
  when is_atom(Key), is_atom(PoolType), is_atom(PoolName) ->
    true.

-spec categorize_options([term()]) -> {GlobalConfig, LocalConfig, HostsConfig} when
      GlobalConfig :: list(),
      LocalConfig :: list(),
      HostsConfig :: list().
categorize_options(Opts) ->
    lists:foldl(fun({config, _, _} = El, Acc) ->
                    as_global(El, Acc);
                   ({local_config, {Key, Host}, _} = El, Acc)
                      when is_atom(Key), is_binary(Host) ->
                    as_hosts(El, Acc);
                   ({local_config, _, _} = El, Acc) ->
                    as_local(El, Acc);
                   ({acl, _, _}, R) ->
                    %% no need to do extra work here
                    R;
                   (R, R2) ->
                    ?ERROR_MSG("not matched ~p", [R]),
                    R2
                end, {[], [], []}, Opts).

as_global(El, {Config, Local, HostLocal}) -> {[El | Config], Local, HostLocal}.
as_local(El, {Config, Local, HostLocal}) -> {Config, [El | Local], HostLocal}.
as_hosts(El, {Config, Local, HostLocal}) -> {Config, Local, [El | HostLocal]}.

-spec get_key_group(binary(), atom()) -> atom().
get_key_group(<<"ldap_", _/binary>>, _) ->
    ldap;
get_key_group(<<"odbc_", _/binary>>, _) ->
    odbc;
get_key_group(<<"pgsql_", _/binary>>, _) ->
    odbc;
get_key_group(<<"auth_", _/binary>>, _) ->
    auth;
get_key_group(<<"ext_auth_", _/binary>>, _) ->
    auth;
get_key_group(<<"s2s_", _/binary>>, _) ->
    s2s;
get_key_group(_, Key) when is_atom(Key) ->
    Key.

-spec compare_terms(OldTerms :: [tuple()],
                    NewTerms :: [tuple()],
                    KeyPos :: non_neg_integer(),
                    ValuePos :: non_neg_integer()) -> compare_result().
compare_terms(OldTerms, NewTerms, KeyPos, ValuePos)
  when is_integer(KeyPos), is_integer(ValuePos) ->
    {ToStop, ToReload} = lists:foldl(pa:bind(fun find_modules_to_change/5,
                                             KeyPos, NewTerms, ValuePos),
                                     {[], []}, OldTerms),
    ToStart = lists:foldl(pa:bind(fun find_modules_to_start/4,
                                  KeyPos, OldTerms), [], NewTerms),
    #compare_result{to_start  = ToStart,
                    to_stop   = ToStop,
                    to_reload = ToReload}.

find_modules_to_start(KeyPos, OldTerms, Element, ToStart) ->
    case lists:keyfind(element(KeyPos, Element), KeyPos, OldTerms) of
        false -> [Element | ToStart];
        _ -> ToStart
    end.

find_modules_to_change(KeyPos, NewTerms, ValuePos,
                       Element, {ToStop, ToReload}) ->
    case lists:keyfind(element(KeyPos, Element), KeyPos, NewTerms) of
        false ->
            {[Element | ToStop], ToReload};
        NewElement ->
            OldVal = element(ValuePos, Element),
            NewVal = element(ValuePos, NewElement),
            case OldVal == NewVal of
                true ->
                    {ToStop, ToReload};
                false ->
                    %% add also old value
                    {ToStop,
                     [{element(KeyPos, Element), OldVal, NewVal} | ToReload]}
            end
    end.

mk_node_start_filter() ->
    fun(#local_config{key = node_start}) ->
        false;
       (_) ->
        true
    end.

sort_config(Config) when is_list(Config) ->
    L = lists:map(fun(ConfigItem) when is_list(ConfigItem) ->
                      sort_config(ConfigItem);
                     (ConfigItem) when is_tuple(ConfigItem) ->
                      sort_config(tuple_to_list(ConfigItem));
                     (ConfigItem) ->
                      ConfigItem
                  end, Config),
    lists:sort(L).


%% -----------------------------------------------------------------
%% State API
%% -----------------------------------------------------------------

allow_override_all(State = #state{}) ->
    State#state{override_global = true,
                override_local  = true,
                override_acls   = true}.

allow_override_local_only(State = #state{}) ->
    State#state{override_global = false,
                override_local  = true,
                override_acls   = false}.

state_to_opts(#state{opts = Opts}) ->
    lists:reverse(Opts).

can_override(global, #state{override_global = Override}) ->
    Override;
can_override(local, #state{override_local = Override}) ->
    Override;
can_override(acls, #state{override_acls = Override}) ->
    Override.
