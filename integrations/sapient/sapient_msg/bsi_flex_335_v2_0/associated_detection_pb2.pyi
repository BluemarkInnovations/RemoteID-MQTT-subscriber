from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AssociationRelation(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ASSOCIATION_RELATION_UNSPECIFIED: _ClassVar[AssociationRelation]
    ASSOCIATION_RELATION_NO_RELATION: _ClassVar[AssociationRelation]
    ASSOCIATION_RELATION_PARENT: _ClassVar[AssociationRelation]
    ASSOCIATION_RELATION_CHILD: _ClassVar[AssociationRelation]
    ASSOCIATION_RELATION_SIBLING: _ClassVar[AssociationRelation]
ASSOCIATION_RELATION_UNSPECIFIED: AssociationRelation
ASSOCIATION_RELATION_NO_RELATION: AssociationRelation
ASSOCIATION_RELATION_PARENT: AssociationRelation
ASSOCIATION_RELATION_CHILD: AssociationRelation
ASSOCIATION_RELATION_SIBLING: AssociationRelation

class AssociatedDetection(_message.Message):
    __slots__ = ("timestamp", "node_id", "object_id", "association_type")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    ASSOCIATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    timestamp: _timestamp_pb2.Timestamp
    node_id: str
    object_id: str
    association_type: AssociationRelation
    def __init__(self, timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., node_id: _Optional[str] = ..., object_id: _Optional[str] = ..., association_type: _Optional[_Union[AssociationRelation, str]] = ...) -> None: ...
