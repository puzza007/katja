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

-module(connection_SUITE).

-include_lib("common_test/include/ct.hrl").

% Common Test
-export([
  all/0
]).

% Tests
-export([
  connect/1,
  send_invalid_tcp/1
]).

% Common Test

all() ->
  [
    connect,
    send_invalid_tcp
  ].

% Tests

connect(_Config) ->
  {ok, State} = katja_connection:connect(),
  ok = katja_connection:disconnect(State),
  Host = "10.99.99.99",
  Port = 9001,
  {ok, {connection_state, undefined, Host, Port}} =
    katja_connection:connect("10.99.99.99", 9001).

send_invalid_tcp(_Config) ->
  {ok, State} = katja_connection:connect(),
  {{error, closed}, State2} = katja_connection:send_message(<<"invalid">>, State),
  {{error, closed}, State3} = katja_connection:send_message(<<"still invalid">>, State2),
  ok = katja_connection:disconnect(State3).
