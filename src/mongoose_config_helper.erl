%% @doc Helper functions to play with options in the shell.
-module(mongoose_config_helper).
-export([get_flatten_opts/0,
         get_opts/0,
         get_expanded_opts/0,
         diff_expanded/0]).

get_flatten_opts() ->
    LC = ejabberd_config:get_local_config(),
    LCH = ejabberd_config:get_host_local_config(),
    mongoose_config:flatten_opts(LC, LCH).

get_opts() ->
    LC = ejabberd_config:get_local_config(),
    LCH = ejabberd_config:get_host_local_config(),
    {LC, LCH}.

%% @doc It should be the same as `get_opts', just slower :)
get_expanded_opts() ->
    mongoose_config:expand_opts(get_flatten_opts()).

%% Helps to debug option expansion
diff_expanded() ->
    {LC, LCH} = get_opts(),
    {ELC, ELCH} = get_expanded_opts(),
    #{diff_local => LC -- ELC, %% in LC, but not in ELC
      diff_local_host => LCH -- ELCH, %% in LCH, but not in ELCH
      diff_local_expanded => ELC -- LC,
      diff_local_host_expanded => ELCH -- LCH}.
