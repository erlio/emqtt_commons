%% This file is a copy of `rabbitmq_mqtt_frame.erl' from rabbitmq.
%% License:
%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%
-module(emqtt_frame).

-include("emqtt_frame.hrl").

-export([parse/2, initial_state/0]).
-export([serialise/1]).

-define(RESERVED, 0).
-define(PROTOCOL_MAGIC_31, "MQIsdp").
-define(PROTOCOL_MAGIC_311, "MQTT").
-define(PROTOCOL_MAGIC_131, "MQIsdp131"). %% used for bridges
-define(MAX_LEN, 16#fffffff).
-define(HIGHBIT, 2#10000000).
-define(LOWBITS, 2#01111111).

initial_state() -> none.

parse(<<>>, none) ->
    {more, fun(Bin) -> parse(Bin, none) end};
parse(<<MessageType:4, Dup:1, QoS:2, Retain:1, Rest/binary>>, none) ->
    parse_remaining_len(Rest, #mqtt_frame_fixed{ type   = MessageType,
                                                 dup    = bool(Dup),
                                                 qos    = QoS,
                                                 retain = bool(Retain) });
parse(Bin, Cont) -> Cont(Bin).

parse_remaining_len(<<>>, Fixed) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Fixed) end};
parse_remaining_len(Rest, Fixed) ->
    parse_remaining_len(Rest, Fixed, 1, 0).

parse_remaining_len(_Bin, _Fixed, _Multiplier, Length)
  when Length > ?MAX_LEN ->
    {error, invalid_mqtt_frame_len};
parse_remaining_len(<<>>, Fixed, Multiplier, Length) ->
    {more, fun(Bin) -> parse_remaining_len(Bin, Fixed, Multiplier, Length) end};
parse_remaining_len(<<1:1, Len:7, Rest/binary>>, Fixed, Multiplier, Value) ->
    parse_remaining_len(Rest, Fixed, Multiplier * ?HIGHBIT, Value + Len * Multiplier);
parse_remaining_len(<<0:1, Len:7, Rest/binary>>, Fixed,  Multiplier, Value) ->
    parse_frame(Rest, Fixed, Value + Len * Multiplier).

parse_frame(Bin, #mqtt_frame_fixed{ type = Type,
                                    qos  = Qos } = Fixed, Length) ->
    case {Type, Bin} of
        {?CONNECT, <<FrameBin:Length/binary, Rest/binary>>} ->
            {ProtocolMagic, Rest1} = parse_utf(FrameBin),
            <<ProtoVersion : 8, Rest2/binary>> = Rest1,
            <<UsernameFlag : 1,
              PasswordFlag : 1,
              WillRetain   : 1,
              WillQos      : 2,
              WillFlag     : 1,
              CleanSession : 1,
              _Reserved    : 1,
              KeepAlive    : 16/big,
              Rest3/binary>>   = Rest2,
            {ClientId,  Rest4} = parse_client_id(Rest3),
            {WillTopic, Rest5} = parse_utf(Rest4, WillFlag),
            {WillMsg,   Rest6} = parse_msg(Rest5, WillFlag),
            {UserName,  Rest7} = parse_utf(Rest6, UsernameFlag),
            {PasssWord, <<>>}  = parse_utf(Rest7, PasswordFlag),
            case (ProtocolMagic == ?PROTOCOL_MAGIC_31)
                 orelse (ProtocolMagic == ?PROTOCOL_MAGIC_311)
                 orelse (ProtocolMagic == ?PROTOCOL_MAGIC_131)
            of
                true ->
                    wrap(Fixed,
                         #mqtt_frame_connect{
                           proto_ver   = ProtoVersion,
                           will_retain = bool(WillRetain),
                           will_qos    = WillQos,
                           will_flag   = bool(WillFlag),
                           clean_sess  = bool(CleanSession),
                           keep_alive  = KeepAlive,
                           client_id   = ClientId,
                           will_topic  = WillTopic,
                           will_msg    = WillMsg,
                           username    = UserName,
                           password    = PasssWord}, Rest);
               false ->
                    {error, protocol_header_corrupt}
            end;
        {?CONNACK, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<_:8, ReturnCode:8/big>> = FrameBin,
            wrap(Fixed, #mqtt_frame_connack{return_code=ReturnCode}, <<>>, Rest);
        {?PUBLISH, <<FrameBin:Length/binary, Rest/binary>>} ->
            {TopicName, Rest1} = parse_utf(FrameBin),
            {MessageId, Payload} = case Qos of
                                       0 -> {undefined, Rest1};
                                       _ -> <<M:16/big, R/binary>> = Rest1,
                                            {M, R}
                                   end,
            wrap(Fixed, #mqtt_frame_publish {topic_name = TopicName,
                                             message_id = MessageId },
                 Payload, Rest);
        {?PUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<MessageId:16/big>> = FrameBin,
            wrap(Fixed, #mqtt_frame_publish{message_id = MessageId}, Rest);
        {?PUBREC, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<MessageId:16/big>> = FrameBin,
            wrap(Fixed, #mqtt_frame_publish{message_id = MessageId}, Rest);
        {?PUBREL, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<MessageId:16/big>> = FrameBin,
            wrap(Fixed, #mqtt_frame_publish { message_id = MessageId }, Rest);
        {?PUBCOMP, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<MessageId:16/big>> = FrameBin,
            wrap(Fixed, #mqtt_frame_publish { message_id = MessageId }, Rest);
        {Subs, <<FrameBin:Length/binary, Rest/binary>>}
          when Subs =:= ?SUBSCRIBE orelse Subs =:= ?UNSUBSCRIBE ->
            1 = Qos,
            <<MessageId:16/big, Rest1/binary>> = FrameBin,
            Topics = parse_topics(Subs, Rest1, []),
            wrap(Fixed, #mqtt_frame_subscribe { message_id  = MessageId,
                                                topic_table = Topics }, Rest);
        {?SUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<MessageId:16/big, Rest1/binary>> = FrameBin,
            QoSTable = parse_acks(Rest1, []),
            wrap(Fixed, #mqtt_frame_suback { message_id  = MessageId,
                                             qos_table = QoSTable }, Rest);
        {?UNSUBACK, <<FrameBin:Length/binary, Rest/binary>>} ->
            <<MessageId:16/big>> = FrameBin,
            wrap(Fixed, #mqtt_frame_suback { message_id  = MessageId,
                                                qos_table = [] }, Rest);
        {Minimal, Rest}
          when Minimal =:= ?DISCONNECT orelse Minimal =:= ?PINGREQ orelse Minimal =:= ?PINGRESP ->
            Length = 0,
            wrap(Fixed, Rest);
        {_, TooShortBin} ->
            {more, fun(BinMore) ->
                           parse_frame(<<TooShortBin/binary, BinMore/binary>>,
                                       Fixed, Length)
                   end}
     end.

parse_topics(_, <<>>, Topics) ->
    Topics;
parse_topics(?SUBSCRIBE = Sub, Bin, Topics) ->
    {Name, <<_:6, QoS:2, Rest/binary>>} = parse_utf(Bin),
    parse_topics(Sub, Rest, [#mqtt_topic { name = Name, qos = QoS } | Topics]);
parse_topics(?UNSUBSCRIBE = Sub, Bin, Topics) ->
    {Name, <<Rest/binary>>} = parse_utf(Bin),
    parse_topics(Sub, Rest, [#mqtt_topic { name = Name } | Topics]).

parse_acks(<<>>, Acks) ->
    Acks;
parse_acks(<<_:6, QoS:2, Rest/binary>>, Acks) ->
    parse_acks(Rest, [QoS | Acks]).


wrap(Fixed, Variable, Payload, Rest) ->
    {ok, #mqtt_frame { variable = Variable, fixed = Fixed, payload = Payload }, Rest}.
wrap(Fixed, Variable, Rest) ->
    {ok, #mqtt_frame { variable = Variable, fixed = Fixed }, Rest}.
wrap(Fixed, Rest) ->
    {ok, #mqtt_frame { fixed = Fixed }, Rest}.

parse_utf(Bin, 0) ->
    {undefined, Bin};
parse_utf(Bin, _) ->
    parse_utf(Bin).

parse_utf(<<Len:16/big, Str:Len/binary, Rest/binary>>) ->
    {binary_to_list(Str), Rest}.

parse_msg(Bin, 0) ->
    {undefined, Bin};
parse_msg(<<Len:16/big, Msg:Len/binary, Rest/binary>>, _) ->
    {Msg, Rest}.

parse_client_id(<<>>) -> {missing, <<>>};
parse_client_id(<<0,0>>) -> {empty, <<>>};
parse_client_id(Bin) -> parse_utf(Bin).

bool(0) -> false;
bool(1) -> true.

%% serialisation

serialise(#mqtt_frame{ fixed    = Fixed,
                       variable = Variable,
                       payload  = Payload }) ->
    serialise_variable(Fixed, Variable, serialise_payload(Payload)).

serialise_payload(undefined)           -> <<>>;
serialise_payload(B) when is_binary(B) -> B.

serialise_variable(#mqtt_frame_fixed   { type        = ?CONNACK } = Fixed,
                   #mqtt_frame_connack { return_code = ReturnCode },
                   <<>> = PayloadBin) ->
    VariableBin = <<?RESERVED:8, ReturnCode:8>>,
    serialise_fixed(Fixed, VariableBin, PayloadBin);

serialise_variable(#mqtt_frame_fixed  { type       = SubAck } = Fixed,
                   #mqtt_frame_suback { message_id = MessageId,
                                        qos_table  = Qos },
                   <<>> = _PayloadBin)
  when SubAck =:= ?SUBACK orelse SubAck =:= ?UNSUBACK ->
    VariableBin = <<MessageId:16/big>>,
    QosBin = << <<?RESERVED:6, Q:2>> || Q <- Qos >>,
    serialise_fixed(Fixed, VariableBin, QosBin);

serialise_variable(#mqtt_frame_fixed   { type       = ?PUBLISH,
                                         qos        = Qos } = Fixed,
                   #mqtt_frame_publish { topic_name = TopicName,
                                         message_id = MessageId },
                   PayloadBin) ->
    TopicBin = serialise_utf(TopicName),
    MessageIdBin = case Qos of
                       0 -> <<>>;
                       1 -> <<MessageId:16/big>>;
                       2 -> <<MessageId:16/big>>
                   end,
    serialise_fixed(Fixed, <<TopicBin/binary, MessageIdBin/binary>>, PayloadBin);

serialise_variable(#mqtt_frame_fixed   { type       = ?PUBACK } = Fixed,
                   #mqtt_frame_publish { message_id = MessageId },
                   PayloadBin) ->
    MessageIdBin = <<MessageId:16/big>>,
    serialise_fixed(Fixed, MessageIdBin, PayloadBin);

serialise_variable(#mqtt_frame_fixed { type = ?PUBREC } = Fixed,
                   #mqtt_frame_publish{ message_id = MsgId},
                   PayloadBin) ->
    serialise_fixed(Fixed, <<MsgId:16/big>>, PayloadBin);

serialise_variable(#mqtt_frame_fixed { type = ?PUBREL } = Fixed,
                   #mqtt_frame_publish{ message_id = MsgId},
                   PayloadBin) ->
    serialise_fixed(Fixed, <<MsgId:16/big>>, PayloadBin);

serialise_variable(#mqtt_frame_fixed { type = ?PUBCOMP } = Fixed,
                   #mqtt_frame_publish{ message_id = MsgId},
                   PayloadBin) ->
    serialise_fixed(Fixed, <<MsgId:16/big>>, PayloadBin);

serialise_variable(#mqtt_frame_fixed { type = ?SUBSCRIBE } = Fixed,
                   #mqtt_frame_subscribe { message_id = MessageId,
                                           topic_table = TopicTable },
                   _) ->
    MessageIdBin = <<MessageId:16/big>>,

    F = fun(#mqtt_topic{name=Topic, qos=Qos}, BinList) ->
                [serialise_utf(Topic), <<Qos:8/integer>> | BinList]
        end,
    PayloadBin = list_to_binary(lists:foldl(F, [], TopicTable)),
    serialise_fixed(Fixed, MessageIdBin, PayloadBin);

serialise_variable(#mqtt_frame_fixed {type = ?UNSUBSCRIBE } = Fixed,
                   #mqtt_frame_subscribe { message_id = MessageId,
                                           topic_table = Topics },
                   _) ->
    MessageIdBin = <<MessageId:16/big>>,
	PayloadBin = list_to_binary([serialise_utf(T) || T <- Topics]),
    serialise_variable(Fixed, MessageIdBin, PayloadBin);

serialise_variable(#mqtt_frame_fixed { type = ?CONNECT } = Fixed,
                   #mqtt_frame_connect{ proto_ver = ProtoVer,
                                        username = Username,
                                        password = Password,
                                        will_retain = WillRetain,
                                        will_qos = WillQos,
                                        will_flag = WillFlag,
                                        clean_sess = CleanSess,
                                        keep_alive = KeepAlive,
                                        client_id = ClientId,
                                        will_topic = WillTopic,
                                        will_msg = WillMsg},
                   <<>>) ->
    ProtoName = list_to_binary(proto_name(ProtoVer)),
    UsernameFlag = case Username of
                       undefined -> false;
                       U when is_binary(U) or is_list(U) -> true
                   end,

    PasswordFlag = case Password of
                       undefined -> false;
                       P when is_binary(P) or is_list(P) -> true
                   end,

    Bin = <<(byte_size(ProtoName)):16/big-unsigned-integer,
            ProtoName/binary,
            ProtoVer:8/unsigned-integer,
            (opt(UsernameFlag)):1/integer,
            (opt(PasswordFlag)):1/integer,
            (opt(WillRetain)):1/integer,
            (opt(WillQos)):2/integer,
            (opt(WillFlag)):1/integer,
            (opt(CleanSess)):1/integer,
            0:1,
            KeepAlive:16/big-unsigned-integer>>,

    Payloads1 = [serialise_utf(ClientId)],
    Payloads2 = case WillFlag of
                    true -> [serialise_utf(WillTopic) | Payloads1];
                    _ -> Payloads1
                end,
    Payloads3 = case WillFlag of
                    true -> [serialise_utf(WillMsg) | Payloads2];
                    _ -> Payloads2
                end,
    Payloads4 = case UsernameFlag of
                    true -> [serialise_utf(Username) | Payloads3];
                    _ -> Payloads3
                end,
    Payloads5 = case PasswordFlag of
                    true -> [serialise_utf(Password) | Payloads4];
                    _ -> Payloads4
                end,

    PayloadBin = list_to_binary(lists:reverse(Payloads5)),
    serialise_fixed(Fixed, Bin, PayloadBin);

serialise_variable(#mqtt_frame_fixed {} = Fixed,
                   undefined,
                   <<>> = _PayloadBin) ->
    serialise_fixed(Fixed, <<>>, <<>>).

serialise_fixed(#mqtt_frame_fixed{ type   = Type,
                                   dup    = Dup,
                                   qos    = Qos,
                                   retain = Retain }, VariableBin, PayloadBin)
  when is_integer(Type) andalso ?CONNECT =< Type andalso Type =< ?DISCONNECT ->
    Len = size(VariableBin) + size(PayloadBin),
    true = (Len =< ?MAX_LEN),
    LenBin = serialise_len(Len),
    <<Type:4, (opt(Dup)):1, (opt(Qos)):2, (opt(Retain)):1,
      LenBin/binary, VariableBin/binary, PayloadBin/binary>>.

serialise_utf(String) ->
    StringBin = unicode:characters_to_binary(String),
    Len = size(StringBin),
    true = (Len =< 16#ffff),
    <<Len:16/big, StringBin/binary>>.

serialise_len(N) when N =< ?LOWBITS ->
    <<0:1, N:7>>;
serialise_len(N) ->
    <<1:1, (N rem ?HIGHBIT):7, (serialise_len(N div ?HIGHBIT))/binary>>.

opt(undefined)            -> ?RESERVED;
opt(false)                -> 0;
opt(true)                 -> 1;
opt(X) when is_integer(X) -> X.

proto_name(3) -> ?PROTOCOL_MAGIC_31;
proto_name(4) -> ?PROTOCOL_MAGIC_311;
proto_name(131) -> ?PROTOCOL_MAGIC_31;
proto_name(141) -> ?PROTOCOL_MAGIC_311;
proto_name(_) -> error.

