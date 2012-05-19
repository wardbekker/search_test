%%%-------------------------------------------------------------------
%%% @author Ward Bekker <>
%%% @copyright (C) 2012, Ward Bekker
%%% @doc
%%%
%%% @end
%%% Created : 15 May 2012 by Ward Bekker <>
%%%-------------------------------------------------------------------
-module(search_index_ser).

-behaviour(gen_server).

%% API
-export([start_link/1, add/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 
-define(PARTITIONS, 4). 


-record(state, { term_frequencies }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(ServerName) ->
    gen_server:start_link({local, ServerName}, ?MODULE, [], []).


import() ->
    {ok,Bin} = file:read_file("dump.bin"),
    lists:foreach(
      fun({DocId, Attributes}) ->
              {ok, Body} = dict:find("Body", Attributes),
              Body1 = re:replace(Body, "\\W", " ", [{return,list},global,unicode]),
              Tokens = string:tokens(Body1, " .,-;"),
              [add(Term, DocId) || Term <- Tokens]
      end,
      binary_to_term(Bin)
     ).

import_faster() ->
    {ok,Bin} = file:read_file("dump.bin"),
    Terms = binary_to_term(Bin),
    %% about 500 ms up to here
    _Pairs = lists:map(
      fun({DocId, Attributes}) ->
              {ok, Body} = dict:find("Body", Attributes),
              Body1 = re:replace(Body, "[^\\W\\d]", " ", [{return,list},global,unicode]),
              Tokens = string:tokens(Body1, " .,-;"),
              [{Term, DocId} || Term <- Tokens]
      end,
      Terms
     ),
    ok.
    %% up to 8 sec up to here
    %% dict:from_list(Pairs).

import_faster_p() ->
    {ok,Bin} = file:read_file("dump.bin"),
    Terms = binary_to_term(Bin),
    %% about 500 ms up to here
    _Pairs = plists:map(
               fun({DocId, Attributes}) ->
                       {ok, Body} = dict:find("Body", Attributes),
                       Body1 = re:replace(Body, "[^\\W\\d]", " ", [{return,list},global,unicode]),
                       Tokens = string:tokens(Body1, " .,-;"),
                       [{Term, DocId} || Term <- Tokens]
               end,
               Terms,
               {processes, 8}
              ),
    ok.


add(Term, DocumentId) ->
    case length(Term) > 2 of
        true ->
            Pid = get_index_partition(Term),
            gen_server:cast(Pid, {add, Term, DocumentId});
        false ->
            ok
    end.

do_query(Terms) ->
    Sets = [ gen_server:call(get_index_partition(Term), {do_query, Term})|| Term <- Terms],
    sets:intersection(Sets).

get_all_partitions() ->
    [ get_partition(P) || P <- lists:seq(0, ?PARTITIONS-1)].

get_partition(Partition) ->
    ServerName = "index_ser_" ++ integer_to_list(Partition),
    ServerProcessName = list_to_atom(ServerName),
    case whereis(ServerProcessName) of
	undefined ->
            {ok, Pid} = supervisor:start_child(search_index_sup, [ServerProcessName]);
        Pid ->
            Pid
    end,
    Pid.    

get_index_partition(Term) ->
    Partition = erlang:phash2(Term) rem ?PARTITIONS,
    get_partition(Partition).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    State = #state{ term_frequencies = dict:new() },
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({add, Term, DocumentId}, _From, State) ->
    %% update term frequency
    NewFrequencies = case dict:find( Term, State#state.term_frequencies ) of
                         { ok, Frequencies } -> 
                             dict:update_counter(DocumentId, 1, Frequencies);
                         _ ->
                             dict:update_counter(DocumentId, 1, dict:new())
               end,
    NewTermFrequencies =
        orddict:store(Term, NewFrequencies, State#state.term_frequencies),
    %% update   
    {reply, ok, State#state{ term_frequencies = NewTermFrequencies }};
handle_call({do_query, Term}, _From, State) ->
    io:format("do_query for term ~p ~n", [Term]),
    Reply = case orddict:find(Term, State#state.term_frequencies) of
        {ok, D1} ->
                    D1;
        _ -> 
                    dict:new()
            end,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.



%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
