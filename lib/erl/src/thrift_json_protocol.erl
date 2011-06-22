%%
%% Licensed to the Apache Software Foundation (ASF) under one
%% or more contributor license agreements. See the NOTICE file
%% distributed with this work for additional information
%% regarding copyright ownership. The ASF licenses this file
%% to you under the Apache License, Version 2.0 (the
%% "License"); you may not use this file except in compliance
%% with the License. You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%

-module(thrift_json_protocol).

-behaviour(thrift_protocol).

-include("thrift_constants.hrl").
-include("thrift_protocol.hrl").

-export([new/1, new/2,
         read/2,
         write/2,
         flush_transport/1,
         close_transport/1,

         new_protocol_factory/2
        ]).

-record(json_context, {
    % the type of json_context: array or object
    type,
    % fields read or written
    fields_processed = 0
}).

-record(json_protocol, {
    transport,
    context_stack = [],
    jsx
}).
-type state() :: #json_protocol{}.
-include("thrift_protocol_behaviour.hrl").

-define(VERSION_1, 1).
-define(JSON_DOUBLE_PRECISION, 16).

typeid_to_list(TypeId) ->
    erlang:atom_to_list(thrift_protocol:typeid_to_atom(TypeId)).

start_context(object) -> "{";
start_context(array) -> "[".

end_context(object) -> "}";
end_context(array) -> "]".


new(Transport) ->
    new(Transport, _Options = []).

new(Transport, Options) ->
    State  = #json_protocol{transport = Transport},
    State1 = parse_options(Options, State),
    thrift_protocol:new(?MODULE, State1).

% No options currently
parse_options([], State) ->
    State.

flush_transport(This = #json_protocol{transport = Transport}) ->
    {NewTransport, Result} = thrift_transport:flush(Transport),
    {This#json_protocol{
            transport = NewTransport,
            context_stack = []
        }, Result}.

close_transport(This = #json_protocol{transport = Transport}) ->
    {NewTransport, Result} = thrift_transport:close(Transport),
    {This#json_protocol{
            transport = NewTransport,
            context_stack = [],
            jsx = undefined
        }, Result}.

%%%
%%% instance methods
%%%
% places a new context on the stack:
write(#json_protocol{context_stack = Stack} = State, {enter_context, Type}) ->
    NewState = State#json_protocol{context_stack = [
        #json_context{type=Type}|Stack]},
    write_values(NewState,  list_to_binary(start_context(Type)));

% removes the topmost context from stack    
write(#json_protocol{context_stack = [CurrCtxt|Stack]} = State, {exit_context}) ->
    Type = CurrCtxt#json_context.type,
    NewState = State#json_protocol{context_stack = Stack},
    write_values(NewState, list_to_binary(end_context(Type)));

write(This0, #protocol_message_begin{
    name = Name,
    type = Type,
    seqid = Seqid}) ->
    write_values(This0, [
        {enter_context, array},
        {i32, ?VERSION_1},
        {i32, Type},
        {string, Name},
        {i32, Seqid}
    ]);

write(This, message_end) ->  
    write_values(This, {exit_context});

% Example field expression: "1":{"dbl":3.14}
write(This0, #protocol_field_begin{
       name = _Name,
       type = Type,
       id = Id}) ->
    write_values(This0, [
        % entering 'outer' object
        {i16, Id},
        % entering 'outer' object
        {enter_context, object},
        {string, typeid_to_list(Type)}
    ]);

write(This, field_stop) -> 
    {This, ok};

write(This, field_end) -> 
    write_values(This,[
        {exit_context},
    ]);
% Example message with map: [1,"testMap",1,0,{"1":{"map":["i32","i32",3,{"7":77,"8":88,"9":99}]}}]
write(This0, #protocol_map_begin{
       ktype = Ktype,
       vtype = Vtype,
       size = Size}) ->
    write_values(This0, [
        {enter_context, array},
        {string, typeid_to_list(Ktype)},
        {string, typeid_to_list(Vtype)},
        {i32, Size},
        {enter_context, object}
    ]);

write(This, map_end) -> 
    write_values(This,[
        {exit_context},
        {exit_context}
    ]);

write(This0, #protocol_list_begin{
        etype = Etype,
        size = Size}) ->
    write_values(This0, [
        {enter_context, array},
        {string, typeid_to_list(Etype)},
        {i32, Size}
    ]);

write(This, list_end) -> 
    write_values(This,[
        {exit_context}
    ]);

% example message with set: [1,"testSet",1,0,{"1":{"set":["i32",3,1,2,3]}}]
write(This0, #protocol_set_begin{
        etype = Etype,
        size = Size}) ->
    write_values(This0, [
        {enter_context, array},
        {string, typeid_to_list(Etype)},
        {i32, Size}
    ]);

write(This, set_end) -> 
    write_values(This,[
        {exit_context}
    ]);
% example message with struct: [1,"testStruct",1,0,{"1":{"rec":{"1":{"str":"worked"},"4":{"i8":1},"9":{"i32":1073741824},"11":{"i64":1152921504606847000}}}}]
write(This, #protocol_struct_begin{}) -> 
    write_values(This, [
        {enter_context, object}
    ]);

write(This, struct_end) -> 
    write_values(This,[
        {exit_context}
    ]);

write(This, {bool, true})  -> write_field(This, <<"true">>);
write(This, {bool, false}) -> write_field(This, <<"false">>);

write(This, {byte, Byte}) ->
    write_field(This, list_to_binary(integer_to_list(Byte)));

write(This, {i16, I16}) ->
    write(This, {byte, I16});

write(This, {i32, I32}) ->
    write(This, {byte, I32});

write(This, {i64, I64}) ->
    write(This, {byte, I64});

write(This, {double, Double}) ->
    write_field(This, list_to_binary(io_lib:format("~.*f", [?JSON_DOUBLE_PRECISION,Double])));


write(This0, {string, Str}) ->
    write_field(This0, case is_binary(Str) of
        true -> Str;
        false -> jsx:term_to_json(list_to_binary([$", Str, $"])))
        end
    ).
%% TODO: binary data should be base64 encoded!

%% Data :: iolist()
write(This = #json_protocol{transport = Trans}, Data) ->
    {NewTransport, Result} = thrift_transport:write(Trans, Data),
    {This#json_protocol{transport = NewTransport}, Result}.

%% prepends ',' ':' or '"' if necessary, etc.
write_field(This0, Value) ->
    FieldNo = This0#json_context.fields_processed,
    CtxtType = This0#json_context.type,
    Rem = FieldNo rem 2,
    % write pre-field content
    {This1, ok} = case {CtxtType, FieldNo, Rem} of
        {array, N, _} when N > 0 ->
            write(This0, <<",">>);
        {object, 0, 0} -> % object key
            write(This0, <<"\"">>);
        {object, _, 0} -> % object key
            write(This0, <<",\"">>);
        _ ->
            {This0, ok} % no pre-field necessary
    end,
    % write actual field value
    {This2, ok} = write(This1, Value),
    % write post-field content
    {This3, ok} = case {CtxtType, Rem} of
        {object, 0} -> % object key
            write(This2, <<"\":">>);
        _ ->
            {This2, ok} % no pre-field necessary
    end,
    % increment # of processed fields
    {This3#json_context{fields_processed = FieldNo + 1}, ok}.

write_values(This0, ValueList) ->
    FinalState = lists:foldl(
        fun(Val, ThisIn) ->
            {ThisOut, ok} = write(ThisIn, Val),
            ThisOut
        end,
        This0,
        ValueList),
    {FinalState, ok}.

%% I wish the erlang version of the transport interface included a 
%% read_all function (like eg. the java implementation). Since it doesn't,
%% here's my version (even though it probably shouldn't be in this file).
%%
%% The resulting binary is immediately send to the JSX stream parser.
%% Subsequent calls to read actually operate on the events returned by JSX.
read_all(#json_protocol{transport = Transport0} = State) ->
    {Transport1, Bin} = read_all_1(Transport0, []),
    P = jsx:decoder(),
    State#json_protocol{
        transport = Transport1,
        jsx = P(Bin)
    }.

read_all_1(Transport0, IoList) ->
    Bin = case thrift_transport:read(Transport0, 1) of
        {Transport1, {ok, Data}} -> % character successfully read; read more
            read_all_1(Transport1, [Data|IoList]);
        {Transport1, {error, 'EOF'}} ->
            iolist_to_binary(lists:reverse(IoList))
    end,
    {Transport1, Bin}.

% Expect reads an event from the JSX event stream. It receives an event or data
% type as input. Comparing the read event from the one is was passed, it
% returns an error if something other than the expected value is encountered.
% Expect also maintains the context stack in #json_protocol.
expect(#json_protocol{jsx={event, {Type, Data}, Next}}=State, ExpectedType) ->
    NextState = State#json_protocol{jsx=Next()},
    case Type == ExpectedType of
        true -> 
            {handle_special_event(NextState, Type), {ok, Data}};
        false ->
            {NextState, {error, unexpected_json_event}}
    end;

expect(#json_protocol{jsx={event, Event, Next}}=State, ExpectedEvent) ->
     expect(State#json_protocol{jsx={event, {Event, none}, Next}}, ExpectedEvent).

handle_special_event(#json_protocol{context_stack = CtxtStack} = State, Event) ->
    case Event of
        start_object ->
            State#json_protocol{context_stack = [#json_context{
                    type=object
                }|CtxtStack]};
        start_array ->
            State#json_protocol{context_stack = [#json_context{
                    type=array
                }|CtxtStack]};
        end_object ->
            State#json_protocol{context_stack = tail(CtxtStack)};
        end_array ->
            State#json_protocol{context_stack = tail(CtxtStack)};
        _ ->
            State
    end.

tail([]) -> [];
tail([Head|Tail]) -> Tail.

expect_many(State, ExpectedList) ->
    {State1, ResultList, Status} = expect_many_1(State, ExpectedList, [], ok),
    % use return value format used elsewhere
    {State1, {Status, ResultList}}.

expect_many_1(State, [], ResultList, Status) ->
    {State, lists:reverse(ResultList), Status};
expect_many_1(State, [Expected|ExpTail], ResultList, Status) ->
    {State1, {Status, Data}=Result} = expect(State, Expected),
    NewResultList = [Result|ResultList],
    case Status of
        % in case of error, end prematurely
        error -> expect_many_1(State1, [], NewResultList, Status);
        ok -> expect_many_1(State1, ExpTail, NewResultList, Status);
    end.

% wrapper around expect to make life easier for container opening/closing functions
expect_nodata(This, Expected) ->
    {This1, Ret} = expect(This, Expected),
    {This1, case Ret of
        {ok, _} -> 
            ok;
        _Error -> 
            Ret
    end}.
    
read(This0, message_begin) ->
    % call read_all to get the contents of the transport buffer into JSX.
    This1 = read_all(This0),
    case expect_many(This1, 
            [start_array, integer, integer, string, integer]) of
        {This2, {ok, [_, Version, Type, Name, Seqid]}} ->
            case Version =:= ?VERSION_1 of
                true ->
                    {This2, #protocol_message_begin{name  = binary_to_list(Name),
                                                    type  = Type,
                                                    seqid = SeqId}};
                false ->
                    {This2, {error, no_json_protocol_version}}
            end;
        Other -> Other
    end;

read(This, message_end) -> 
    expect_nodata(This, end_object);

read(This, struct_begin) -> {This, ok};
    expect_nodata(This, start_object);

read(This, struct_end) -> {This, ok};
    expect_nodata(This, end_object);

read(This0, field_begin) ->
    {This1, Result} = read(This0, byte),
    case Result of
        {ok, Type = ?tType_STOP} ->
            {This1, #protocol_field_begin{type = Type}};
        {ok, Type} ->
            {This2, {ok, Id}} = read(This1, i16),
            {This2, #protocol_field_begin{type = Type,
                                          id = Id}}
    end;

read(This, field_end) -> {This, ok};

read(This0, map_begin) ->
    {This1, {ok, Ktype}} = read(This0, byte),
    {This2, {ok, Vtype}} = read(This1, byte),
    {This3, {ok, Size}}  = read(This2, i32),
    {This3, #protocol_map_begin{ktype = Ktype,
                                vtype = Vtype,
                                size = Size}};
read(This, map_end) -> {This, ok};

read(This0, list_begin) ->
    {This1, {ok, Etype}} = read(This0, byte),
    {This2, {ok, Size}}  = read(This1, i32),
    {This2, #protocol_list_begin{etype = Etype,
                                 size = Size}};
read(This, list_end) -> {This, ok};

read(This0, set_begin) ->
    {This1, {ok, Etype}} = read(This0, byte),
    {This2, {ok, Size}}  = read(This1, i32),
    {This2, #protocol_set_begin{etype = Etype,
                                 size = Size}};
read(This, set_end) -> {This, ok};

read(This0, field_stop) ->
    {This1, {ok, ?tType_STOP}} = read(This0, byte),
    {This1, ok};

%%

read(This0, bool) ->
    {This1, Result} = read(This0, byte),
    case Result of
        {ok, Byte} -> {This1, {ok, Byte /= 0}};
        Else -> {This1, Else}
    end;

read(This0, byte) ->
    {This1, Bytes} = read_data(This0, 1),
    case Bytes of
        {ok, <<Val:8/integer-signed-big, _/binary>>} -> {This1, {ok, Val}};
        Else -> {This1, Else}
    end;

read(This0, i16) ->
    {This1, Bytes} = read_data(This0, 2),
    case Bytes of
        {ok, <<Val:16/integer-signed-big, _/binary>>} -> {This1, {ok, Val}};
        Else -> {This1, Else}
    end;

read(This0, i32) ->
    {This1, Bytes} = read_data(This0, 4),
    case Bytes of
        {ok, <<Val:32/integer-signed-big, _/binary>>} -> {This1, {ok, Val}};
        Else -> {This1, Else}
    end;

%% unsigned ints aren't used by thrift itself, but it's used for the parsing
%% of the packet version header. Without this special function BEAM works fine
%% but hipe thinks it received a bad version header.
read(This0, ui32) ->
    {This1, Bytes} = read_data(This0, 4),
    case Bytes of
        {ok, <<Val:32/integer-unsigned-big, _/binary>>} -> {This1, {ok, Val}};
        Else -> {This1, Else}
    end;

read(This0, i64) ->
    {This1, Bytes} = read_data(This0, 8),
    case Bytes of
        {ok, <<Val:64/integer-signed-big, _/binary>>} -> {This1, {ok, Val}};
        Else -> {This1, Else}
    end;

read(This0, double) ->
    {This1, Bytes} = read_data(This0, 8),
    case Bytes of
        {ok, <<Val:64/float-signed-big, _/binary>>} -> {This1, {ok, Val}};
        Else -> {This1, Else}
    end;

% returns a binary directly, call binary_to_list if necessary
read(This0, string) ->
    {This1, {ok, Sz}}  = read(This0, i32),
    read_data(This1, Sz).

-spec read_data(#json_protocol{}, non_neg_integer()) ->
    {#json_protocol{}, {ok, binary()} | {error, _Reason}}.
read_data(This, 0) -> {This, {ok, <<>>}};
read_data(This = #json_protocol{transport = Trans}, Len) when is_integer(Len) andalso Len > 0 ->
    {NewTransport, Result} = thrift_transport:read(Trans, Len),
    {This#json_protocol{transport = NewTransport}, Result}.


%%%% FACTORY GENERATION %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(tbp_opts, {strict_read = true,
                   strict_write = true}).

parse_factory_options([], Opts) ->
    Opts;
parse_factory_options([{strict_read, Bool} | Rest], Opts) when is_boolean(Bool) ->
    parse_factory_options(Rest, Opts#tbp_opts{strict_read=Bool});
parse_factory_options([{strict_write, Bool} | Rest], Opts) when is_boolean(Bool) ->
    parse_factory_options(Rest, Opts#tbp_opts{strict_write=Bool}).


%% returns a (fun() -> thrift_protocol())
new_protocol_factory(TransportFactory, Options) ->
    ParsedOpts = parse_factory_options(Options, #tbp_opts{}),
    F = fun() ->
                {ok, Transport} = TransportFactory(),
                thrift_json_protocol:new(
                  Transport,
                  [{strict_read,  ParsedOpts#tbp_opts.strict_read},
                   {strict_write, ParsedOpts#tbp_opts.strict_write}])
        end,
    {ok, F}.

