from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class AssociatedFile(_message.Message):
    __slots__ = ("type", "url")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    type: str
    url: str
    def __init__(self, type: _Optional[str] = ..., url: _Optional[str] = ...) -> None: ...
