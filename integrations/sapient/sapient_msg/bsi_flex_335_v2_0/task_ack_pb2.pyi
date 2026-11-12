from sapient_msg.bsi_flex_335_v2_0 import associated_file_pb2 as _associated_file_pb2
from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TaskAck(_message.Message):
    __slots__ = ("task_id", "task_status", "associated_file", "reason")
    class TaskStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TASK_STATUS_UNSPECIFIED: _ClassVar[TaskAck.TaskStatus]
        TASK_STATUS_ACCEPTED: _ClassVar[TaskAck.TaskStatus]
        TASK_STATUS_REJECTED: _ClassVar[TaskAck.TaskStatus]
        TASK_STATUS_COMPLETED: _ClassVar[TaskAck.TaskStatus]
        TASK_STATUS_FAILED: _ClassVar[TaskAck.TaskStatus]
    TASK_STATUS_UNSPECIFIED: TaskAck.TaskStatus
    TASK_STATUS_ACCEPTED: TaskAck.TaskStatus
    TASK_STATUS_REJECTED: TaskAck.TaskStatus
    TASK_STATUS_COMPLETED: TaskAck.TaskStatus
    TASK_STATUS_FAILED: TaskAck.TaskStatus
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_STATUS_FIELD_NUMBER: _ClassVar[int]
    ASSOCIATED_FILE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    task_status: TaskAck.TaskStatus
    associated_file: _associated_file_pb2.AssociatedFile
    reason: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, task_id: _Optional[str] = ..., task_status: _Optional[_Union[TaskAck.TaskStatus, str]] = ..., associated_file: _Optional[_Union[_associated_file_pb2.AssociatedFile, _Mapping]] = ..., reason: _Optional[_Iterable[str]] = ...) -> None: ...
