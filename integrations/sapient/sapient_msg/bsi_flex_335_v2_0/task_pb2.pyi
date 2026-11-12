from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sapient_msg.bsi_flex_335_v2_0 import location_pb2 as _location_pb2
from sapient_msg.bsi_flex_335_v2_0 import range_bearing_pb2 as _range_bearing_pb2
from sapient_msg.bsi_flex_335_v2_0 import follow_pb2 as _follow_pb2
from sapient_msg.bsi_flex_335_v2_0 import registration_pb2 as _registration_pb2
from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Task(_message.Message):
    __slots__ = ("task_id", "task_name", "task_description", "task_start_time", "task_end_time", "control", "region", "command")
    class Control(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        CONTROL_UNSPECIFIED: _ClassVar[Task.Control]
        CONTROL_START: _ClassVar[Task.Control]
        CONTROL_STOP: _ClassVar[Task.Control]
        CONTROL_PAUSE: _ClassVar[Task.Control]
    CONTROL_UNSPECIFIED: Task.Control
    CONTROL_START: Task.Control
    CONTROL_STOP: Task.Control
    CONTROL_PAUSE: Task.Control
    class DiscreteThreshold(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DISCRETE_THRESHOLD_UNSPECIFIED: _ClassVar[Task.DiscreteThreshold]
        DISCRETE_THRESHOLD_LOW: _ClassVar[Task.DiscreteThreshold]
        DISCRETE_THRESHOLD_MEDIUM: _ClassVar[Task.DiscreteThreshold]
        DISCRETE_THRESHOLD_HIGH: _ClassVar[Task.DiscreteThreshold]
    DISCRETE_THRESHOLD_UNSPECIFIED: Task.DiscreteThreshold
    DISCRETE_THRESHOLD_LOW: Task.DiscreteThreshold
    DISCRETE_THRESHOLD_MEDIUM: Task.DiscreteThreshold
    DISCRETE_THRESHOLD_HIGH: Task.DiscreteThreshold
    class RegionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        REGION_TYPE_UNSPECIFIED: _ClassVar[Task.RegionType]
        REGION_TYPE_AREA_OF_INTEREST: _ClassVar[Task.RegionType]
        REGION_TYPE_IGNORE: _ClassVar[Task.RegionType]
        REGION_TYPE_BOUNDARY: _ClassVar[Task.RegionType]
        REGION_TYPE_MOBILE_NODE_NO_GO_AREA: _ClassVar[Task.RegionType]
        REGION_TYPE_MOBILE_NODE_GO_AREA: _ClassVar[Task.RegionType]
    REGION_TYPE_UNSPECIFIED: Task.RegionType
    REGION_TYPE_AREA_OF_INTEREST: Task.RegionType
    REGION_TYPE_IGNORE: Task.RegionType
    REGION_TYPE_BOUNDARY: Task.RegionType
    REGION_TYPE_MOBILE_NODE_NO_GO_AREA: Task.RegionType
    REGION_TYPE_MOBILE_NODE_GO_AREA: Task.RegionType
    class Command(_message.Message):
        __slots__ = ("request", "detection_threshold", "detection_report_rate", "classification_threshold", "mode_change", "look_at", "move_to", "patrol", "follow", "command_parameter")
        REQUEST_FIELD_NUMBER: _ClassVar[int]
        DETECTION_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
        DETECTION_REPORT_RATE_FIELD_NUMBER: _ClassVar[int]
        CLASSIFICATION_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
        MODE_CHANGE_FIELD_NUMBER: _ClassVar[int]
        LOOK_AT_FIELD_NUMBER: _ClassVar[int]
        MOVE_TO_FIELD_NUMBER: _ClassVar[int]
        PATROL_FIELD_NUMBER: _ClassVar[int]
        FOLLOW_FIELD_NUMBER: _ClassVar[int]
        COMMAND_PARAMETER_FIELD_NUMBER: _ClassVar[int]
        request: str
        detection_threshold: Task.DiscreteThreshold
        detection_report_rate: Task.DiscreteThreshold
        classification_threshold: Task.DiscreteThreshold
        mode_change: str
        look_at: _range_bearing_pb2.LocationOrRangeBearing
        move_to: _location_pb2.LocationList
        patrol: _location_pb2.LocationList
        follow: _follow_pb2.FollowObject
        command_parameter: str
        def __init__(self, request: _Optional[str] = ..., detection_threshold: _Optional[_Union[Task.DiscreteThreshold, str]] = ..., detection_report_rate: _Optional[_Union[Task.DiscreteThreshold, str]] = ..., classification_threshold: _Optional[_Union[Task.DiscreteThreshold, str]] = ..., mode_change: _Optional[str] = ..., look_at: _Optional[_Union[_range_bearing_pb2.LocationOrRangeBearing, _Mapping]] = ..., move_to: _Optional[_Union[_location_pb2.LocationList, _Mapping]] = ..., patrol: _Optional[_Union[_location_pb2.LocationList, _Mapping]] = ..., follow: _Optional[_Union[_follow_pb2.FollowObject, _Mapping]] = ..., command_parameter: _Optional[str] = ...) -> None: ...
    class Region(_message.Message):
        __slots__ = ("type", "region_id", "region_name", "region_area", "class_filter", "behaviour_filter")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        REGION_ID_FIELD_NUMBER: _ClassVar[int]
        REGION_NAME_FIELD_NUMBER: _ClassVar[int]
        REGION_AREA_FIELD_NUMBER: _ClassVar[int]
        CLASS_FILTER_FIELD_NUMBER: _ClassVar[int]
        BEHAVIOUR_FILTER_FIELD_NUMBER: _ClassVar[int]
        type: Task.RegionType
        region_id: str
        region_name: str
        region_area: _range_bearing_pb2.LocationOrRangeBearing
        class_filter: _containers.RepeatedCompositeFieldContainer[Task.ClassFilter]
        behaviour_filter: _containers.RepeatedCompositeFieldContainer[Task.BehaviourFilter]
        def __init__(self, type: _Optional[_Union[Task.RegionType, str]] = ..., region_id: _Optional[str] = ..., region_name: _Optional[str] = ..., region_area: _Optional[_Union[_range_bearing_pb2.LocationOrRangeBearing, _Mapping]] = ..., class_filter: _Optional[_Iterable[_Union[Task.ClassFilter, _Mapping]]] = ..., behaviour_filter: _Optional[_Iterable[_Union[Task.BehaviourFilter, _Mapping]]] = ...) -> None: ...
    class ClassFilter(_message.Message):
        __slots__ = ("parameter", "type", "sub_class_filter", "priority")
        PARAMETER_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        SUB_CLASS_FILTER_FIELD_NUMBER: _ClassVar[int]
        PRIORITY_FIELD_NUMBER: _ClassVar[int]
        parameter: Task.Parameter
        type: str
        sub_class_filter: _containers.RepeatedCompositeFieldContainer[Task.SubClassFilter]
        priority: Task.DiscreteThreshold
        def __init__(self, parameter: _Optional[_Union[Task.Parameter, _Mapping]] = ..., type: _Optional[str] = ..., sub_class_filter: _Optional[_Iterable[_Union[Task.SubClassFilter, _Mapping]]] = ..., priority: _Optional[_Union[Task.DiscreteThreshold, str]] = ...) -> None: ...
    class SubClassFilter(_message.Message):
        __slots__ = ("parameter", "type", "sub_class_filter", "priority")
        PARAMETER_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        SUB_CLASS_FILTER_FIELD_NUMBER: _ClassVar[int]
        PRIORITY_FIELD_NUMBER: _ClassVar[int]
        parameter: Task.Parameter
        type: str
        sub_class_filter: _containers.RepeatedCompositeFieldContainer[Task.SubClassFilter]
        priority: Task.DiscreteThreshold
        def __init__(self, parameter: _Optional[_Union[Task.Parameter, _Mapping]] = ..., type: _Optional[str] = ..., sub_class_filter: _Optional[_Iterable[_Union[Task.SubClassFilter, _Mapping]]] = ..., priority: _Optional[_Union[Task.DiscreteThreshold, str]] = ...) -> None: ...
    class BehaviourFilter(_message.Message):
        __slots__ = ("parameter", "type", "priority")
        PARAMETER_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        PRIORITY_FIELD_NUMBER: _ClassVar[int]
        parameter: Task.Parameter
        type: str
        priority: Task.DiscreteThreshold
        def __init__(self, parameter: _Optional[_Union[Task.Parameter, _Mapping]] = ..., type: _Optional[str] = ..., priority: _Optional[_Union[Task.DiscreteThreshold, str]] = ...) -> None: ...
    class Parameter(_message.Message):
        __slots__ = ("name", "operator", "value")
        NAME_FIELD_NUMBER: _ClassVar[int]
        OPERATOR_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        name: str
        operator: _registration_pb2.Operator
        value: float
        def __init__(self, name: _Optional[str] = ..., operator: _Optional[_Union[_registration_pb2.Operator, str]] = ..., value: _Optional[float] = ...) -> None: ...
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_NAME_FIELD_NUMBER: _ClassVar[int]
    TASK_DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TASK_START_TIME_FIELD_NUMBER: _ClassVar[int]
    TASK_END_TIME_FIELD_NUMBER: _ClassVar[int]
    CONTROL_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    task_id: str
    task_name: str
    task_description: str
    task_start_time: _timestamp_pb2.Timestamp
    task_end_time: _timestamp_pb2.Timestamp
    control: Task.Control
    region: _containers.RepeatedCompositeFieldContainer[Task.Region]
    command: Task.Command
    def __init__(self, task_id: _Optional[str] = ..., task_name: _Optional[str] = ..., task_description: _Optional[str] = ..., task_start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., task_end_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., control: _Optional[_Union[Task.Control, str]] = ..., region: _Optional[_Iterable[_Union[Task.Region, _Mapping]]] = ..., command: _Optional[_Union[Task.Command, _Mapping]] = ...) -> None: ...
