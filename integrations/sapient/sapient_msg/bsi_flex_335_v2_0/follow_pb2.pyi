from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class FollowObject(_message.Message):
    __slots__ = ("follow_object_id",)
    FOLLOW_OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    follow_object_id: str
    def __init__(self, follow_object_id: _Optional[str] = ...) -> None: ...
