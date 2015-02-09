%%% Copyright (c) 2014, Daniel Kempkens <daniel@kempkens.io>
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
%%% @doc Handles connections to Riemann.
-module(katja_connection).

-include("katja_types.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEFAULT_HOST, "127.0.0.1").
-define(DEFAULT_PORT, 5555).

-record(connection_state, {
          socket :: gen_tcp:socket(),
          host :: string(),
          port :: pos_integer()
         }).

-opaque state() :: #connection_state{}.

-export_type([state/0]).

-export([connect/0]).
-export([connect/2]).
-export([disconnect/1]).
-export([send_message/2]).
-export([is_connected/1]).


-spec connect() -> {ok, state()} | {error, term()}.
connect() ->
    Host = application:get_env(katja, host, ?DEFAULT_HOST),
    Port = application:get_env(katja, port, ?DEFAULT_PORT),
    connect(Host, Port).

-spec connect(string(), pos_integer()) -> {ok, state()} | {error, term()}.
connect(Host, Port) ->
    State = #connection_state{host=Host, port=Port},
    maybe_connect(State).

-spec disconnect(state()) -> ok.
disconnect(#connection_state{socket=undefined}) ->
    ok;
disconnect(#connection_state{socket=Socket}) ->
    gen_tcp:close(Socket).

-spec maybe_connect(state()) -> {ok, state()} | {error, term()}.
maybe_connect(#connection_state{host=Host, port=Port}=S) ->
    Options = [binary,
               {active, false},
               {nodelay, true},
               {send_timeout, 4000}],
    case gen_tcp:connect(Host, Port, Options, 4000) of
        {ok, Socket} ->
            S2 = S#connection_state{socket=Socket},
            {ok, S2};
        {error, Reason} when Reason == econnrefused orelse
                             Reason == closed orelse
                             Reason == timeout  ->
            S2 = S#connection_state{socket=undefined},
            error_logger:error_msg("Error sending event to riemann: ~p", [Reason]),
            {ok, S2}
    end.

is_connected(#connection_state{socket=undefined}) ->
    false;
is_connected(#connection_state{socket=Socket}) when is_port(Socket) ->
    true.

-spec
send_message(binary(), state()) ->
    {{ok, riemannpb_message()}, state()} |
    {{error, term()}, state()}.
send_message(Msg, #connection_state{host=Host, port=Port, socket=Socket}=S)
  when Socket /= undefined ->
    MsgSize = byte_size(Msg),
    Msg2 = <<MsgSize:32/integer-big, Msg/binary>>,
    case gen_tcp:send(Socket, Msg2) of
        ok ->
            case receive_reply(Socket) of
                {ok, _RetMsg}=O -> {O, S};
                {error, _Reason}=E -> {E, S}
            end;
        {error, Reason} when Reason == econnrefused orelse
                             Reason == closed orelse
                             Reason == timeout  ->
            gen_tcp:close(Socket),
            error_logger:error_msg("Error sending event to riemann: ~p", [Reason]),
            case connect(Host, Port) of
                {ok, S2 = #connection_state{socket=undefined}} ->
                    Error = {error, connection_error},
                    {Error, S2};
                {ok, #connection_state{socket=NewSocket}} ->
                    S2 = S#connection_state{socket=NewSocket},
                    send_message(Msg, S2)
            end;
        {error, _Reason}=E -> {E, S}
    end;
send_message(_Msg, #connection_state{socket=undefined}=S) ->
    {{error, connection_error}, S}.

-spec receive_reply(gen_tcp:socket()) -> {ok, term()} | {error, term()}.
receive_reply(Socket) ->
    receive_reply(Socket, <<>>).

-spec receive_reply(gen_tcp:socket(), binary()) -> {ok, term()} | {error, term()}.
receive_reply(Socket, Buffer) ->
    case gen_tcp:recv(Socket, 0, 4000) of
        {ok, BinMsg} ->
            BinMsg2 = <<Buffer/binary, BinMsg/binary>>,
            case decode_message(BinMsg2) of
                too_short -> receive_reply(Socket, BinMsg2);
                #riemannpb_msg{ok=true}=Msg -> {ok, Msg};
                #riemannpb_msg{ok=false, error=Reason} -> {error, Reason}
            end;
        {error, _Reason}=E -> E
    end.

-spec decode_message(binary()) -> riemannpb_message() | too_short.
decode_message(<<MsgSize:32/integer-big, Msg/binary>>) when MsgSize > byte_size(Msg) ->
    too_short;
decode_message(<<MsgSize:32/integer-big, Msg/binary>>) ->
    case Msg of
        <<Msg2:MsgSize/binary, _Rest/binary>> -> katja_pb:decode_riemannpb_msg(Msg2);
        _ -> #riemannpb_msg{ok=false, error="Response could not be decoded"}
    end.
