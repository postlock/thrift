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
    context_stack
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
    {This#json_protocol{transport = NewTransport}, Result}.

close_transport(This = #json_protocol{transport = Transport}) ->
    {NewTransport, Result} = thrift_transport:close(Transport),
    {This#json_protocol{transport = NewTransport}, Result}.

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

% Example field expression: {"1":{"dbl":3.14}}
write(This0, #protocol_field_begin{
       name = _Name,
       type = Type,
       id = Id}) ->
    write_values(This0, [
        % entering 'outer' object
        {enter_context, object},
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
        {exit_context}
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

write(This, {bool, true})  -> write(This, <<"true">>);
write(This, {bool, false}) -> write(This, <<"false">>);

write(This, {byte, Byte}) ->
    write(This, list_to_binary(integer_to_list(Byte)));

write(This, {i16, I16}) ->
    write(This, {byte, I16});

write(This, {i32, I32}) ->
    write(This, {byte, I32});

write(This, {i64, I64}) ->
    write(This, {byte, I64});

write(This, {double, Double}) ->
    write(This, list_to_binary(io_lib:format("~.*f", [?JSON_DOUBLE_PRECISION,Double])));


write(This0, {string, Str}) ->
    write(This0, [$", list_to_binary(Str), $"]);

%% Data :: iolist()
write(This = #json_protocol{transport = Trans}, Data) ->
    {NewTransport, Result} = thrift_transport:write(Trans, Data),
    {This#json_protocol{transport = NewTransport}, Result}.


write_values(This0, ValueList) ->
    FinalState = lists:foldl(
        fun(Val, ThisIn) ->
            {ThisOut, ok} = write(ThisIn, Val),
            ThisOut
        end,
        This0,
        ValueList),
    {FinalState, ok}.

read(This0, message_begin) ->
    {This1, Initial} = read(This0, ui32),
    case Initial of
        {ok, Sz} when Sz =:= ?VERSION_1 ->
            %% we're at version 1
            {This2, {ok, Name}}  = read(This1, string),
            {This3, {ok, SeqId}} = read(This2, i32),
            Type                 = Sz,
            {This3, #protocol_message_begin{name  = binary_to_list(Name),
                                            type  = Type,
                                            seqid = SeqId}};

        {ok, Sz} when Sz < 0 ->
            %% there's a version number but it's unexpected
            {This1, {error, {bad_json_protocol_version, Sz}}};

        {ok, _Sz} ->
            %% strict_read is true and there's no version header; that's an error
            {This1, {error, no_json_protocol_version}};

        Else ->
            {This1, Else}
    end;

read(This, message_end) -> {This, ok};

read(This, struct_begin) -> {This, ok};
read(This, struct_end) -> {This, ok};

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

