from sapient_msg.bsi_flex_335_v2_0 import associated_detection_pb2 as _associated_detection_pb2
from sapient_msg.bsi_flex_335_v2_0 import associated_file_pb2 as _associated_file_pb2
from sapient_msg.bsi_flex_335_v2_0 import location_pb2 as _location_pb2
from sapient_msg.bsi_flex_335_v2_0 import range_bearing_pb2 as _range_bearing_pb2
from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Alert(_message.Message):
    __slots__ = ("alert_id", "alert_type", "status", "description", "range_bearing", "location", "region_id", "priority", "ranking", "confidence", "associated_file", "associated_detection", "additional_information")
    class AlertType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ALERT_TYPE_UNSPECIFIED: _ClassVar[Alert.AlertType]
        ALERT_TYPE_INFORMATION: _ClassVar[Alert.AlertType]
        ALERT_TYPE_WARNING: _ClassVar[Alert.AlertType]
        ALERT_TYPE_CRITICAL: _ClassVar[Alert.AlertType]
        ALERT_TYPE_ERROR: _ClassVar[Alert.AlertType]
        ALERT_TYPE_FATAL: _ClassVar[Alert.AlertType]
        ALERT_TYPE_MODE_CHANGE: _ClassVar[Alert.AlertType]
    ALERT_TYPE_UNSPECIFIED: Alert.AlertType
    ALERT_TYPE_INFORMATION: Alert.AlertType
    ALERT_TYPE_WARNING: Alert.AlertType
    ALERT_TYPE_CRITICAL: Alert.AlertType
    ALERT_TYPE_ERROR: Alert.AlertType
    ALERT_TYPE_FATAL: Alert.AlertType
    ALERT_TYPE_MODE_CHANGE: Alert.AlertType
    class AlertStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ALERT_STATUS_UNSPECIFIED: _ClassVar[Alert.AlertStatus]
        ALERT_STATUS_ACTIVE: _ClassVar[Alert.AlertStatus]
        ALERT_STATUS_ACKNOWLEDGE: _ClassVar[Alert.AlertStatus]
        ALERT_STATUS_REJECT: _ClassVar[Alert.AlertStatus]
        ALERT_STATUS_IGNORE: _ClassVar[Alert.AlertStatus]
        ALERT_STATUS_CLEAR: _ClassVar[Alert.AlertStatus]
    ALERT_STATUS_UNSPECIFIED: Alert.AlertStatus
    ALERT_STATUS_ACTIVE: Alert.AlertStatus
    ALERT_STATUS_ACKNOWLEDGE: Alert.AlertStatus
    ALERT_STATUS_REJECT: Alert.AlertStatus
    ALERT_STATUS_IGNORE: Alert.AlertStatus
    ALERT_STATUS_CLEAR: Alert.AlertStatus
    class DiscretePriority(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DISCRETE_PRIORITY_UNSPECIFIED: _ClassVar[Alert.DiscretePriority]
        DISCRETE_PRIORITY_LOW: _ClassVar[Alert.DiscretePriority]
        DISCRETE_PRIORITY_MEDIUM: _ClassVar[Alert.DiscretePriority]
        DISCRETE_PRIORITY_HIGH: _ClassVar[Alert.DiscretePriority]
    DISCRETE_PRIORITY_UNSPECIFIED: Alert.DiscretePriority
    DISCRETE_PRIORITY_LOW: Alert.DiscretePriority
    DISCRETE_PRIORITY_MEDIUM: Alert.DiscretePriority
    DISCRETE_PRIORITY_HIGH: Alert.DiscretePriority
    ALERT_ID_FIELD_NUMBER: _ClassVar[int]
    ALERT_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    RANGE_BEARING_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    REGION_ID_FIELD_NUMBER: _ClassVar[int]
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    RANKING_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    ASSOCIATED_FILE_FIELD_NUMBER: _ClassVar[int]
    ASSOCIATED_DETECTION_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_INFORMATION_FIELD_NUMBER: _ClassVar[int]
    alert_id: str
    alert_type: Alert.AlertType
    status: Alert.AlertStatus
    description: str
    range_bearing: _range_bearing_pb2.RangeBearing
    location: _location_pb2.Location
    region_id: str
    priority: Alert.DiscretePriority
    ranking: float
    confidence: float
    associated_file: _containers.RepeatedCompositeFieldContainer[_associated_file_pb2.AssociatedFile]
    associated_detection: _containers.RepeatedCompositeFieldContainer[_associated_detection_pb2.AssociatedDetection]
    additional_information: str
    def __init__(self, alert_id: _Optional[str] = ..., alert_type: _Optional[_Union[Alert.AlertType, str]] = ..., status: _Optional[_Union[Alert.AlertStatus, str]] = ..., description: _Optional[str] = ..., range_bearing: _Optional[_Union[_range_bearing_pb2.RangeBearing, _Mapping]] = ..., location: _Optional[_Union[_location_pb2.Location, _Mapping]] = ..., region_id: _Optional[str] = ..., priority: _Optional[_Union[Alert.DiscretePriority, str]] = ..., ranking: _Optional[float] = ..., confidence: _Optional[float] = ..., associated_file: _Optional[_Iterable[_Union[_associated_file_pb2.AssociatedFile, _Mapping]]] = ..., associated_detection: _Optional[_Iterable[_Union[_associated_detection_pb2.AssociatedDetection, _Mapping]]] = ..., additional_information: _Optional[str] = ...) -> None: ...
