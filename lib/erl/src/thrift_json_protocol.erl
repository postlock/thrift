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

new(Transport, _Options) ->
    State  = #json_protocol{transport = Transport},
    %State1 = parse_options(Options, State),
    thrift_protocol:new(?MODULE, State).

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
    write_values(This,[{exit_context}]);
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

write(This, {bool, true})  -> write_field(This, <<"true">>, false);
write(This, {bool, false}) -> write_field(This, <<"false">>, false);

write(This, {byte, Byte}) ->
    write_field(This, list_to_binary(integer_to_list(Byte)), false);

write(This, {i16, I16}) ->
    write(This, {byte, I16});

write(This, {i32, I32}) ->
    write(This, {byte, I32});

write(This, {i64, I64}) ->
    write(This, {byte, I64});

write(This, {double, Double}) ->
    write_field(This, list_to_binary(io_lib:format("~.*f",
        [?JSON_DOUBLE_PRECISION,Double])), false);


write(This0, {string, Str}) ->
    write_field(This0, case is_binary(Str) of
        true -> Str;
        false -> jsx:term_to_json(list_to_binary([$", Str, $"]))
        end, true);
%% TODO: binary data should be base64 encoded

%% Data :: iolist()
write(This = #json_protocol{transport = Trans}, Data) ->
    {NewTransport, Result} = thrift_transport:write(Trans, Data),
    {This#json_protocol{transport = NewTransport}, Result}.

%% prepends ',' ':' or '"' if necessary, etc.
write_field(This0, Value, HasQuotes) ->
    FieldNo = This0#json_context.fields_processed,
    CtxtType = This0#json_context.type,
    Rem = FieldNo rem 2,
    {This2, ok} = case {CtxtType, FieldNo, Rem, HasQuotes} of
        {array, N, _, _} when N > 0 ->  % array element (not first)
            write(This0, [<<",">>, Value]);
        {object, 0, _, false} -> % non-string object key (first)
            write(This0, [<<"\"">>, Value, <<"\":">>]);
        {object, 0, _, true} -> % string object key (first)
            write(This0, [Value,<<":">>]);
        {object, _, 0, false} -> % non-string object key (not first)
            write(This0, [<<",\"">>, Value, <<"\":">>]);
        {object, _, 0, true} -> % string object key (not first)
            write(This0, [<<",">>, Value,<<":">>]);
        _ -> % no pre-field necessary
            write(This0, [Value])
    end,
   {This2#json_context{fields_processed = FieldNo + 1}, ok}.

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
            {NextState, {ok, Data}};
        false ->
            {NextState, {error, unexpected_json_event}}
    end;

expect(#json_protocol{jsx={event, Event, Next}}=State, ExpectedEvent) ->
     expect(State#json_protocol{jsx={event, {Event, none}, Next}}, ExpectedEvent).

expect_many(State, ExpectedList) ->
    {State1, ResultList, Status} = expect_many_1(State, ExpectedList, [], ok),
    % use return value format used elsewhere
    {State1, {Status, ResultList}}.

expect_many_1(State, [], ResultList, Status) ->
    {State, lists:reverse(ResultList), Status};
expect_many_1(State, [Expected|ExpTail], ResultList, Status) ->
    {State1, {Status, Data}} = expect(State, Expected),
    NewResultList = [Data|ResultList],
    case Status of
        % in case of error, end prematurely
        error -> expect_many_1(State1, [], NewResultList, Status);
        ok -> expect_many_1(State1, ExpTail, NewResultList, Status)
    end.

% wrapper around expect to make life easier for container opening/closing functions
expect_nodata(This, ExpectedList) ->
    case expect_many(This, ExpectedList) of
        {State, {ok, _}} -> 
            {State, ok};
        Error -> 
            Error
    end.

read_field(#json_protocol{jsx={event, Field, Next}} = State) ->
    NewState = State#json_protocol{jsx=Next()},
    {NewState, Field}.

read(This0, message_begin) ->
    % call read_all to get the contents of the transport buffer into JSX.
    This1 = read_all(This0),
    case expect_many(This1, 
            [start_array, integer, integer, string, integer]) of
        {This2, {ok, [_, Version, Type, Name, SeqId]}} ->
            case Version =:= ?VERSION_1 of
                true ->
                    {This2, #protocol_message_begin{name  = Name,
                                                    type  = Type,
                                                    seqid = SeqId}};
                false ->
                    {This2, {error, no_json_protocol_version}}
            end;
        Other -> Other
    end;

read(This, message_end) -> 
    expect_nodata(This, [end_object]);

read(This, struct_begin) -> 
    expect_nodata(This, [start_object]);

read(This, struct_end) -> 
    expect_nodata(This, [end_object]);

read(This0, field_begin) ->
    case expect_many(This0, 
            [%field id
             key, 
             % {} surrounding field
             start_object, 
             % type of field
             key]) of
        {This1, {ok, [FieldIdStr, _, FieldType]}} ->
             {This1, #protocol_field_begin{type = FieldType, id =
             list_to_integer(FieldIdStr)}}; % TODO: do we need to wrap this in a try/catch?
        Other -> Other
    end;

read(This, field_end) -> 
    expect_nodata(This, [end_object]);

% Example message with map: [1,"testMap",1,0,{"1":{"map":["i32","i32",3,{"7":77,"8":88,"9":99}]}}]
read(This0, map_begin) ->
    case expect_many(This0, 
            [start_array,
             % key type
             string, 
             % value type
             string, 
             % size
             integer,
             % the following object contains the map
             start_object]) of
        {This1, {ok, [_, Ktype, Vtype, Size, _]}} ->
            {This1, #protocol_map_begin{ktype = Ktype,
                                vtype = Vtype,
                                size = Size}};
        Other -> Other
    end;

read(This, map_end) -> 
    expect_nodata(This, [end_object, end_array]);

read(This0, list_begin) ->
    case expect_many(This0, 
            [start_array,
             % element type
             string, 
             % size
             integer]) of
        {This1, {ok, [_, Etype, Size]}} ->
            {This1, #protocol_list_begin{
                etype = Etype,
                size = Size}};
        Other -> Other
    end;

read(This, list_end) -> 
    expect_nodata(This, [end_array]);

% example message with set: [1,"testSet",1,0,{"1":{"set":["i32",3,1,2,3]}}]
read(This0, set_begin) ->
    case expect_many(This0, 
            [start_array,
             % element type
             string, 
             % size
             integer]) of
        {This1, {ok, [_, Etype, Size]}} ->
            {This1, #protocol_set_begin{
                etype = Etype,
                size = Size}};
        Other -> Other
    end;

read(This, set_end) -> 
    expect_nodata(This, [end_array]);

read(This0, field_stop) ->
    {This0, ok};
%%

read(This0, bool) ->
    {This1, Field} = read_field(This0),
    Value = case Field of
        {literal, I} -> 
            {ok, I}; 
        _Other ->
            {error, unexpected_event_for_boolean}
    end,
    {This1, Value};

read(This0, byte) ->
    {This1, Field} = read_field(This0),
    Value = case Field of
        {key, K} ->
            {ok, list_to_integer(K)};
        {integer, I} -> 
            {ok, I}; 
        _Other ->
            {error, unexpected_event_for_integer}
    end,
    {This1, Value};

read(This0, i16) ->
    read(This0, byte);

read(This0, i32) ->
    read(This0, byte);

read(This0, i64) ->
    read(This0, byte);

read(This0, double) ->
    {This1, Field} = read_field(This0),
    Value = case Field of
        {float, I} -> 
            {ok, I}; 
        _Other ->
            {error, unexpected_event_for_double}
    end,
    {This1, Value};

% returns a binary directly, call binary_to_list if necessary
read(This0, string) ->
    {This1, Field} = read_field(This0),
    Value = case Field of
        {string, I} -> 
            {ok, I}; 
        {key, J} -> 
            {ok, J}; 
        _Other ->
            {error, unexpected_event_for_string}
    end,
    {This1, Value}.

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
    %ParsedOpts = parse_factory_options(Options, #tbp_opts{}),
    ParsedOpts = #tbp_opts{},
    F = fun() ->
                {ok, Transport} = TransportFactory(),
                thrift_json_protocol:new(
                  Transport,
                  [{strict_read,  ParsedOpts#tbp_opts.strict_read},
                   {strict_write, ParsedOpts#tbp_opts.strict_write}])
        end,
    {ok, F}.

