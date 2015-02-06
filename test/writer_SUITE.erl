% Copyright (c) 2014-2015, Daniel Kempkens <daniel@kempkens.io>
%
% Permission to use, copy, modify, and/or distribute this software for any purpose with or without fee is hereby granted,
% provided that the above copyright notice and this permission notice appear in all copies.
%
% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
% WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL
% DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
% NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%
% This suite expects Riemann to be running on 127.0.0.1:5555.

-module(writer_SUITE).

-include_lib("common_test/include/ct.hrl").

% Common Test
-export([
  all/0,
  init_per_suite/1,
  init_per_testcase/2,
  end_per_suite/1,
  end_per_testcase/2
]).

% Tests
-export([
  send_event/1,
  send_events/1,
  send_events_pid/1,
  send_events_tcp/1,
  send_state/1,
  send_states/1,
  send_states_pid/1,
  send_states_tcp/1,
  send_entities/1,
  send_entities_pid/1,
  ignore_unknown_messages/1
]).

% Common Test

all() ->
  [
    send_event,
    send_events,
    send_events_pid,
    send_events_tcp,
    send_state,
    send_states,
    send_states_pid,
    send_states_tcp,
    send_entities,
    send_entities_pid,
    ignore_unknown_messages
  ].

init_per_suite(Config) ->
  ok = katja:start(),
  Config.

init_per_testcase(Test, Config) when Test =:= send_events_pid; Test =:= send_events_tcp; Test =:= send_events_udp; Test =:= send_events_async;
                                     Test =:= send_states_pid; Test =:= send_states_tcp; Test =:= send_states_udp; Test =:= send_states_async;
                                     Test =:= send_entities_pid; Test =:= send_entities_async; Test =:= ignore_unknown_messages ->
  {ok, WPid} = katja_writer:start_link(),
  [{pid_writer, WPid} | Config];
init_per_testcase(_Test, Config) ->
  Config.

end_per_suite(_Config) ->
  ok = katja:stop(),
  ok.

end_per_testcase(Test, Config) when Test =:= send_events_pid; Test =:= send_events_tcp; Test =:= send_events_udp; Test =:= send_events_async;
                                    Test =:= send_states_pid; Test =:= send_states_tcp; Test =:= send_states_udp; Test =:= send_states_async;
                                    Test =:= send_entities_pid; Test =:= send_entities_async; Test =:= ignore_unknown_messages ->
  WPid = ?config(pid_writer, Config),
  ok = katja_writer:stop(WPid),
  ok;
end_per_testcase(_Test, _Config) ->
  ok.

% Tests

send_event(_Config) ->
  ok = katja:send_event([{service, "katja 1"}, {metric, 9001}]),
  ok = katja:send_event([{service, <<"katja 1">>}, {metric, 9001}]),
  ok = katja:send_event([{service, ["kat", $j, $a, " 1"]}, {metric, 9001}]),
  Description = lists:flatten(lists:duplicate(4096, "abcd")),
  ok = katja:send_event([{service, "katja 1"}, {metric, 9001}, {description, Description}, {ttl, 60}]),
  ok = katja:send_event([{service, "katja 1"}, {metric, 9002}, {attributes, [{"foo", "bar"}]}]).

send_events(_Config) ->
  Description = lists:flatten(lists:duplicate(4096, "abcd")),
  Event = [{service, "katja 1"}, {metric, 9001}, {description, Description}],
  ok = katja:send_events([Event, Event]).

send_events_pid(Config) ->
  WPid = ?config(pid_writer, Config),
  ok = katja:send_event(WPid, [{service, "katja 1"}, {metric, 9001}]),
  Description = lists:flatten(lists:duplicate(4096, "abcd")),
  Event = [{service, "katja 1"}, {metric, 9001}, {description, Description}],
  ok = katja:send_event(WPid, Event),
  ok = katja:send_events(WPid, [Event, Event]).

send_events_tcp(Config) ->
  WPid = ?config(pid_writer, Config),
  Event = [{service, "katja 1"}, {metric, 9001}],
  ok = katja:send_event(katja_writer, Event),
  ok = katja:send_event(WPid, Event),
  ok = katja:send_events(katja_writer, [Event, Event]),
  ok = katja:send_events(WPid, [Event, Event]).

send_state(_Config) ->
  ok = katja:send_state([{service, "katja 1"}, {state, "testing"}]),
  ok = katja:send_state([{service, "katja 1"}, {state, "testing"}, {ttl, 60}]).

send_states(_Config) ->
  State = [{service, "katja 1"}, {state, "testing"}],
  ok = katja:send_states([State, State]).

send_states_pid(Config) ->
  WPid = ?config(pid_writer, Config),
  State = [{service, "katja 1"}, {state, "testing"}],
  ok = katja:send_state(WPid, State),
  ok = katja:send_states(WPid, [State, State]).

send_states_tcp(Config) ->
  WPid = ?config(pid_writer, Config),
  State = [{service, "katja 1"}, {state, "testing"}],
  ok = katja:send_state(WPid, State),
  ok = katja:send_states(katja_writer, [State, State]),
  ok = katja:send_states(WPid, [State, State]).

send_entities(_Config) ->
  Description = lists:flatten(lists:duplicate(4096, "abcd")),
  Event = [{service, "katja 1"}, {metric, 9001}, {description, Description}],
  State = [{service, "katja 1"}, {state, "testing"}],
  ok = katja:send_entities([{events, [Event]}]),
  ok = katja:send_entities([{states, [State]}]),
  ok = katja:send_entities([{states, [State, State]}, {events, [Event, Event]}]).

send_entities_pid(Config) ->
  WPid = ?config(pid_writer, Config),
  Event = [{service, "katja 1"}, {metric, 9001}],
  State = [{service, "katja 1"}, {state, "testing"}],
  ok = katja:send_entities(WPid, [{events, [Event]}]),
  ok = katja:send_entities(WPid, [{states, [State]}]),
  ok = katja:send_entities(WPid, [{states, [State, State]}, {events, [Event, Event]}]).

ignore_unknown_messages(Config) ->
  WPid = ?config(pid_writer, Config),
  ignored = gen_server:call(WPid, foobar),
  ok = gen_server:cast(WPid, foobar),
  WPid ! foobar.
