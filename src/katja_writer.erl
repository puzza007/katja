%%% Copyright (c) 2014-2015, Daniel Kempkens <daniel@kempkens.io>
%%%
%%% Permission to use, copy, modify, and/or distribute this software
%%% for any purpose with or without fee is hereby granted, provided
%%% that the above copyright notice and this permission notice appear
%%% in all copies.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
%%% WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
%%% WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE
%%% AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
%%% CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
%%% LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
%%% NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
%%% CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%%%
%%% @author Daniel Kempkens <daniel@kempkens.io>
%%% @copyright {@years} Daniel Kempkens
%%% @version {@version}
%%% @doc The `katja_writer' module is responsible for sending metrics to Riemann.
-module(katja_writer).
-behaviour(gen_server).

-include("katja_types.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEFAULT_DEFAULTS, []).
-define(DEFAULT_SEND_EVENT_TIMEOUT, 30000).
-define(COMMON_FIELDS, [time, state, service, host, description, tags, ttl]).

-export([send_entities/2]).
-export([send_event/2]).
-export([send_state/2]).
-export([start_link/0]).
-export([start_link/1]).
-export([stop/1]).

-export([code_change/3]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([init/1]).
-export([terminate/2]).


-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link(?MODULE, [], []).

-spec start_link(register) -> {ok, pid()} | ignore | {error, term()}.
start_link(register) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop(katja:process()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, terminate).

-spec send_event(katja:process(), katja:event()) -> ok | {error, term()}.
send_event(Pid, Data) ->
    send_event(Pid, Data, ?DEFAULT_SEND_EVENT_TIMEOUT).

send_event(Pid, Data, Timeout) ->
    Timer = quintana:begin_timed(<<"katja.send_event.time">>),
    ok = quintana:notify_spiral({<<"katja.send_event.num">>, 1}),
    Event = create_event(Data),
    Res = gen_server:call(Pid, {send_message, event, Event}, Timeout),
    ok = quintana:notify_timed(Timer),
    Res.

-spec send_state(katja:process(), katja:event()) -> ok | {error, term()}.
send_state(Pid, Data) ->
    Timer = quintana:begin_timed(<<"katja.send_state.time">>),
    ok = quintana:notify_spiral({<<"katja.send_state.num">>, 1}),
    State = create_state(Data),
    Res = gen_server:call(Pid, {send_message, state, State}),
    ok = quintana:notify_timed(Timer),
    Res.

-spec send_entities(katja:process(), katja:entities()) -> ok | {error, term()}.
send_entities(Pid, Data) ->
    Timer = quintana:begin_timed(<<"katja.send_entities.time">>),
    ok = quintana:notify_spiral({<<"katja.send_entities.num">>, 1}),
    {EventEntities, StateEntities} = create_events_and_states(Data),
    Entities = StateEntities ++ EventEntities,
    Res = gen_server:call(Pid, {send_message, entities, Entities}),
    ok = quintana:notify_timed(Timer),
    Res.

init([]) ->
    {ok, _} = katja_connection:connect().

handle_call({send_message, _Type, Data}, _From, State) ->
    Msg = create_message(Data),
    {Reply, State2} = send_message(Msg, State),
    {reply, Reply, State2};
handle_call(terminate, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(normal, State) ->
    ok = katja_connection:disconnect(State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec create_event(katja:event()) -> riemannpb_event().
create_event(Data) ->
    Event = #riemannpb_event{},
    lists:foldl(fun(K, E) ->
                        case lists:keyfind(K, 1, Data) of
                            {K, V} -> set_event_field(K, V, E);
                            false -> set_event_field(K, undefined, E)
                        end
                end, Event, [attributes, metric|?COMMON_FIELDS]).

-spec create_state(katja:state()) -> riemannpb_state().
create_state(Data) ->
    State = #riemannpb_state{},
    lists:foldl(fun(K, E) ->
                        case lists:keyfind(K, 1, Data) of
                            {K, V} -> set_state_field(K, V, E);
                            false -> set_state_field(K, undefined, E)
                        end
                end, State, [once|?COMMON_FIELDS]).

-spec create_events_and_states(katja:entities()) -> {[riemannpb_event()], [riemannpb_state()]}.
create_events_and_states(Data) ->
    EventEntities = case lists:keyfind(events, 1, Data) of
                        {events, Events} -> [create_event(E) || E <- Events];
                        false -> []
                    end,
    StateEntities = case lists:keyfind(states, 1, Data) of
                        {states, States} -> [create_state(S) || S <- States];
                        false -> []
                    end,
    {EventEntities, StateEntities}.

-spec set_event_field(atom(), term(), riemannpb_event()) -> riemannpb_event().
set_event_field(time, undefined, E) -> E#riemannpb_event{time=current_timestamp()};
set_event_field(time, riemann, E) -> E#riemannpb_event{time=undefined};
set_event_field(time, V, E) -> E#riemannpb_event{time=V};
set_event_field(state, V, E) -> E#riemannpb_event{state=V};
set_event_field(service, V, E) -> E#riemannpb_event{service=V};
set_event_field(host, undefined, E) -> E#riemannpb_event{host=default_hostname()};
set_event_field(host, V, E) -> E#riemannpb_event{host=V};
set_event_field(description, V, E) -> E#riemannpb_event{description=V};
set_event_field(tags, undefined, E) -> E#riemannpb_event{tags=default_tags()};
set_event_field(tags, V, E) -> E#riemannpb_event{tags=V};
set_event_field(ttl, undefined, E) -> E#riemannpb_event{ttl=default_ttl()};
set_event_field(ttl, V, E) -> E#riemannpb_event{ttl=V};
set_event_field(attributes, undefined, E) -> E#riemannpb_event{attributes=[]};
set_event_field(attributes, V, E) ->
    Attrs = [#riemannpb_attribute{key=AKey, value=AVal} || {AKey, AVal} <- V],
    E#riemannpb_event{attributes=Attrs};
set_event_field(metric, undefined, E) -> E#riemannpb_event{metric_f = 0.0, metric_sint64 = 0};
set_event_field(metric, V, E) when is_integer(V) -> E#riemannpb_event{metric_f = V * 1.0, metric_sint64 = V};
set_event_field(metric, V, E) -> E#riemannpb_event{metric_f = V, metric_d = V}.

-spec set_state_field(atom(), term(), riemannpb_state()) -> riemannpb_state().
set_state_field(time, undefined, S) -> S#riemannpb_state{time=current_timestamp()};
set_state_field(time, riemann, S) -> S#riemannpb_state{time=undefined};
set_state_field(time, V, S) -> S#riemannpb_state{time=V};
set_state_field(state, V, S) -> S#riemannpb_state{state=V};
set_state_field(service, V, S) -> S#riemannpb_state{service=V};
set_state_field(host, undefined, S) -> S#riemannpb_state{host=default_hostname()};
set_state_field(host, V, S) -> S#riemannpb_state{host=V};
set_state_field(description, V, S) -> S#riemannpb_state{description=V};
set_state_field(tags, undefined, S) -> S#riemannpb_state{tags=default_tags()};
set_state_field(tags, V, S) -> S#riemannpb_state{tags=V};
set_state_field(ttl, undefined, S) -> S#riemannpb_state{ttl=default_ttl()};
set_state_field(ttl, V, S) -> S#riemannpb_state{ttl=V};
set_state_field(once, V, S) -> S#riemannpb_state{once=V}.

-spec default_hostname() -> iolist().
default_hostname() ->
    Defaults = application:get_env(katja, defaults, ?DEFAULT_DEFAULTS),
    case lists:keyfind(host, 1, Defaults) of
        {host, Host} -> Host;
        false ->
            {ok, Host} = inet:gethostname(),
            Host
    end.

-spec default_tags() -> [iolist()].
default_tags() ->
    Defaults = application:get_env(katja, defaults, ?DEFAULT_DEFAULTS),
    case lists:keyfind(tags, 1, Defaults) of
        {tags, Tags} -> Tags;
        false -> []
    end.

-spec default_ttl() -> float() | undefined.
default_ttl() ->
    Defaults = application:get_env(katja, defaults, ?DEFAULT_DEFAULTS),
    case lists:keyfind(ttl, 1, Defaults) of
        {ttl, TTL} -> TTL;
        false -> undefined
    end.

-spec current_timestamp() -> pos_integer().
current_timestamp() ->
    {MegaSecs, Secs, _MicroSecs} = os:timestamp(),
    MegaSecs * 1000000 + Secs.

-spec create_message(riemannpb_entity() | [riemannpb_entity()]) -> riemannpb_message().
create_message(Entity) when not is_list(Entity) ->
    create_message([Entity]);
create_message(Entities) ->
    {Events, States} = lists:splitwith(fun(Entity) -> is_record(Entity, riemannpb_event) end, Entities),
    #riemannpb_msg{events=Events, states=States}.

-spec send_message(riemannpb_message(), katja_connection:state()) -> {ok, katja_connection:state()} |
                                                                                                   {{error, term()}, katja_connection:state()}.
send_message(Msg, State) ->
    Msg2 = katja_pb:encode_riemannpb_msg(Msg),
    BinMsg = iolist_to_binary(Msg2),
    case katja_connection:send_message(BinMsg, State) of
        {{ok, _RetMsg}, State2} -> {ok, State2};
        {{error, _Reason}, _State2}=E -> E
    end.

-ifdef(TEST).
-define(TEST_DATA, [
                    {time, 1},
                    {state, "online"},
                    {service, "katja"},
                    {host, "localhost"},
                    {description, "katja test"},
                    {tags, ["foo", "bar"]}
                   ]).

create_event_test() ->
    DefaultHost = default_hostname(),
    ?assertMatch(#riemannpb_event{time=1, state="online", service="katja", host="localhost", description="katja test", tags=["foo", "bar"]}, create_event(?TEST_DATA)),
    ?assertMatch(#riemannpb_event{time=1, state="online", service="katja", host="localhost", description="katja test", tags=[]},
                 create_event(lists:keydelete(tags, 1, ?TEST_DATA))),
    ?assertMatch(#riemannpb_event{time=1, state="online", service="katja", host=DefaultHost, description="katja test", tags=["foo", "bar"]},
                 create_event(lists:keydelete(host, 1, ?TEST_DATA))),
    ?assertMatch(#riemannpb_event{metric_f=0.0, metric_sint64=0}, create_event(?TEST_DATA)),
    ?assertMatch(#riemannpb_event{metric_f=1.0, metric_sint64=1}, create_event(?TEST_DATA ++ [{metric, 1}])),
    ?assertMatch(#riemannpb_event{metric_f=2.0, metric_d=2.0}, create_event(?TEST_DATA ++ [{metric, 2.0}])),
    ?assertMatch(#riemannpb_event{ttl=900.1, attributes=[#riemannpb_attribute{key="foo", value="bar"}]},
                 create_event(?TEST_DATA ++ [{ttl, 900.1}, {attributes, [{"foo", "bar"}]}])),
    ?assertMatch(#riemannpb_event{time=undefined}, create_event([{time, riemann}])).

create_state_test() ->
    DefaultHost = default_hostname(),
    ?assertMatch(#riemannpb_state{time=1, state="online", service="katja", host="localhost", description="katja test", tags=["foo", "bar"]}, create_state(?TEST_DATA)),
    ?assertMatch(#riemannpb_state{time=1, state="online", service="katja", host="localhost", description="katja test", tags=[]},
                 create_state(lists:keydelete(tags, 1, ?TEST_DATA))),
    ?assertMatch(#riemannpb_state{time=1, state="online", service="katja", host=DefaultHost, description="katja test", tags=["foo", "bar"]},
                 create_state(lists:keydelete(host, 1, ?TEST_DATA))),
    ?assertMatch(#riemannpb_state{time=undefined}, create_state([{time, riemann}])).

create_message_test() ->
    Event = create_event(?TEST_DATA),
    State = create_state(?TEST_DATA),
    ?assertMatch(#riemannpb_msg{events=[#riemannpb_event{service="katja", host="localhost", description="katja test"}]}, create_message(Event)),
    ?assertMatch(#riemannpb_msg{events=[#riemannpb_event{service="katja", host="localhost", description="katja test"}]}, create_message([Event])),
    ?assertMatch(#riemannpb_msg{states=[#riemannpb_state{service="katja", host="localhost", description="katja test"}]}, create_message(State)),
    ?assertMatch(#riemannpb_msg{states=[#riemannpb_state{service="katja", host="localhost", description="katja test"}]}, create_message([State])),
    ?assertMatch(#riemannpb_msg{
                    events=[#riemannpb_event{service="katja", host="localhost", description="katja test"}],
                    states=[#riemannpb_state{service="katja", host="localhost", description="katja test"}]
                   }, create_message([Event, State])).

default_hostname_test() ->
    Host = "an.host",
    ok = application:set_env(katja, defaults, [{host, Host}]),
    ?assertEqual(Host, default_hostname()),
    ok = application:unset_env(katja, defaults),
    ?assertNotEqual(Host, default_hostname()).

default_tags_test() ->
    Tags = ["some", "tags"],
    ok = application:set_env(katja, defaults, [{tags, Tags}]),
    ?assertEqual(Tags, default_tags()),
    ok = application:unset_env(katja, defaults),
    ?assertEqual([], default_tags()).

default_ttl_test() ->
    TTL = 60.0,
    ok = application:set_env(katja, defaults, [{ttl, TTL}]),
    ?assertEqual(TTL, default_ttl()),
    ok = application:unset_env(katja, defaults),
    ?assertEqual(undefined, default_ttl()).
-endif.
