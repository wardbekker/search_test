
-module(search_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

    SearchIntakeServer = {
      search_intake_ser, {search_intake_ser, start_link, []},
      Restart, Shutdown, Type, [search_intake_ser]
     },

    StackOverflowImporterSer = {
      search_so_import, {search_so_import, start_link, []},
      Restart, Shutdown, Type, [search_so_import]
     },

    SearchIndexSup = {
      search_index_sup, {search_index_sup, start_link, []},
      Restart, Shutdown, Type, [search_index_sup]
     },

    {ok, { {one_for_one, 5, 10}, [SearchIntakeServer, StackOverflowImporterSer, SearchIndexSup]} }.

