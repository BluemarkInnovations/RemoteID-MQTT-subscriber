from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AlertAck(_message.Message):
    __slots__ = ("alert_id", "reason", "alert_ack_status")
    class AlertAckStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ALERT_ACK_STATUS_UNSPECIFIED: _ClassVar[AlertAck.AlertAckStatus]
        ALERT_ACK_STATUS_ACCEPTED: _ClassVar[AlertAck.AlertAckStatus]
        ALERT_ACK_STATUS_REJECTED: _ClassVar[AlertAck.AlertAckStatus]
        ALERT_ACK_STATUS_CANCELLED: _ClassVar[AlertAck.AlertAckStatus]
    ALERT_ACK_STATUS_UNSPECIFIED: AlertAck.AlertAckStatus
    ALERT_ACK_STATUS_ACCEPTED: AlertAck.AlertAckStatus
    ALERT_ACK_STATUS_REJECTED: AlertAck.AlertAckStatus
    ALERT_ACK_STATUS_CANCELLED: AlertAck.AlertAckStatus
    ALERT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ALERT_ACK_STATUS_FIELD_NUMBER: _ClassVar[int]
    alert_id: str
    reason: _containers.RepeatedScalarFieldContainer[str]
    alert_ack_status: AlertAck.AlertAckStatus
    def __init__(self, alert_id: _Optional[str] = ..., reason: _Optional[_Iterable[str]] = ..., alert_ack_status: _Optional[_Union[AlertAck.AlertAckStatus, str]] = ...) -> None: ...
