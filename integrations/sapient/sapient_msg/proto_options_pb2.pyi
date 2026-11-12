from google.protobuf import descriptor_pb2 as _descriptor_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor
FILE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
file_options: _descriptor.FieldDescriptor
ENUM_OPTIONS_FIELD_NUMBER: _ClassVar[int]
enum_options: _descriptor.FieldDescriptor
ONEOF_OPTIONS_FIELD_NUMBER: _ClassVar[int]
oneof_options: _descriptor.FieldDescriptor
FIELD_OPTIONS_FIELD_NUMBER: _ClassVar[int]
field_options: _descriptor.FieldDescriptor
MESSAGE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
message_options: _descriptor.FieldDescriptor

class FileOptions(_message.Message):
    __slots__ = ("standard_version",)
    STANDARD_VERSION_FIELD_NUMBER: _ClassVar[int]
    standard_version: str
    def __init__(self, standard_version: _Optional[str] = ...) -> None: ...

class ValidationOptions(_message.Message):
    __slots__ = ("is_mandatory", "is_ulid", "is_uuid", "xml_name", "xml_is_attribute", "xml_ignore", "xml_singly_nested", "xml_is_parent_value", "enum_name", "is_proto_time", "xml_tentative", "is_time")
    IS_MANDATORY_FIELD_NUMBER: _ClassVar[int]
    IS_ULID_FIELD_NUMBER: _ClassVar[int]
    IS_UUID_FIELD_NUMBER: _ClassVar[int]
    XML_NAME_FIELD_NUMBER: _ClassVar[int]
    XML_IS_ATTRIBUTE_FIELD_NUMBER: _ClassVar[int]
    XML_IGNORE_FIELD_NUMBER: _ClassVar[int]
    XML_SINGLY_NESTED_FIELD_NUMBER: _ClassVar[int]
    XML_IS_PARENT_VALUE_FIELD_NUMBER: _ClassVar[int]
    ENUM_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_PROTO_TIME_FIELD_NUMBER: _ClassVar[int]
    XML_TENTATIVE_FIELD_NUMBER: _ClassVar[int]
    IS_TIME_FIELD_NUMBER: _ClassVar[int]
    is_mandatory: bool
    is_ulid: bool
    is_uuid: bool
    xml_name: str
    xml_is_attribute: bool
    xml_ignore: bool
    xml_singly_nested: str
    xml_is_parent_value: bool
    enum_name: str
    is_proto_time: bool
    xml_tentative: bool
    is_time: bool
    def __init__(self, is_mandatory: bool = ..., is_ulid: bool = ..., is_uuid: bool = ..., xml_name: _Optional[str] = ..., xml_is_attribute: bool = ..., xml_ignore: bool = ..., xml_singly_nested: _Optional[str] = ..., xml_is_parent_value: bool = ..., enum_name: _Optional[str] = ..., is_proto_time: bool = ..., xml_tentative: bool = ..., is_time: bool = ...) -> None: ...

class MessageOptions(_message.Message):
    __slots__ = ("xml_message_name", "is_sapient_message")
    XML_MESSAGE_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_SAPIENT_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    xml_message_name: str
    is_sapient_message: bool
    def __init__(self, xml_message_name: _Optional[str] = ..., is_sapient_message: bool = ...) -> None: ...
