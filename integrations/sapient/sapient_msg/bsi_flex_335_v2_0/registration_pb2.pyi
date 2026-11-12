from sapient_msg.bsi_flex_335_v2_0 import location_pb2 as _location_pb2
from sapient_msg.bsi_flex_335_v2_0 import range_bearing_pb2 as _range_bearing_pb2
from sapient_msg.bsi_flex_335_v2_0 import velocity_pb2 as _velocity_pb2
from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Operator(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OPERATOR_UNSPECIFIED: _ClassVar[Operator]
    OPERATOR_ALL: _ClassVar[Operator]
    OPERATOR_GREATER_THAN: _ClassVar[Operator]
    OPERATOR_LESS_THAN: _ClassVar[Operator]
    OPERATOR_EQUAL: _ClassVar[Operator]
OPERATOR_UNSPECIFIED: Operator
OPERATOR_ALL: Operator
OPERATOR_GREATER_THAN: Operator
OPERATOR_LESS_THAN: Operator
OPERATOR_EQUAL: Operator

class Registration(_message.Message):
    __slots__ = ("node_definition", "icd_version", "name", "short_name", "capabilities", "status_definition", "mode_definition", "dependent_nodes", "reporting_region", "config_data")
    class NodeType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        NODE_TYPE_UNSPECIFIED: _ClassVar[Registration.NodeType]
        NODE_TYPE_OTHER: _ClassVar[Registration.NodeType]
        NODE_TYPE_RADAR: _ClassVar[Registration.NodeType]
        NODE_TYPE_LIDAR: _ClassVar[Registration.NodeType]
        NODE_TYPE_CAMERA: _ClassVar[Registration.NodeType]
        NODE_TYPE_SEISMIC: _ClassVar[Registration.NodeType]
        NODE_TYPE_ACOUSTIC: _ClassVar[Registration.NodeType]
        NODE_TYPE_PROXIMITY_SENSOR: _ClassVar[Registration.NodeType]
        NODE_TYPE_PASSIVE_RF: _ClassVar[Registration.NodeType]
        NODE_TYPE_HUMAN: _ClassVar[Registration.NodeType]
        NODE_TYPE_CHEMICAL: _ClassVar[Registration.NodeType]
        NODE_TYPE_BIOLOGICAL: _ClassVar[Registration.NodeType]
        NODE_TYPE_RADIATION: _ClassVar[Registration.NodeType]
        NODE_TYPE_KINETIC: _ClassVar[Registration.NodeType]
        NODE_TYPE_JAMMER: _ClassVar[Registration.NodeType]
        NODE_TYPE_CYBER: _ClassVar[Registration.NodeType]
        NODE_TYPE_LDEW: _ClassVar[Registration.NodeType]
        NODE_TYPE_RFDEW: _ClassVar[Registration.NodeType]
        NODE_TYPE_MOBILE_NODE: _ClassVar[Registration.NodeType]
        NODE_TYPE_POINTABLE_NODE: _ClassVar[Registration.NodeType]
        NODE_TYPE_FUSION_NODE: _ClassVar[Registration.NodeType]
    NODE_TYPE_UNSPECIFIED: Registration.NodeType
    NODE_TYPE_OTHER: Registration.NodeType
    NODE_TYPE_RADAR: Registration.NodeType
    NODE_TYPE_LIDAR: Registration.NodeType
    NODE_TYPE_CAMERA: Registration.NodeType
    NODE_TYPE_SEISMIC: Registration.NodeType
    NODE_TYPE_ACOUSTIC: Registration.NodeType
    NODE_TYPE_PROXIMITY_SENSOR: Registration.NodeType
    NODE_TYPE_PASSIVE_RF: Registration.NodeType
    NODE_TYPE_HUMAN: Registration.NodeType
    NODE_TYPE_CHEMICAL: Registration.NodeType
    NODE_TYPE_BIOLOGICAL: Registration.NodeType
    NODE_TYPE_RADIATION: Registration.NodeType
    NODE_TYPE_KINETIC: Registration.NodeType
    NODE_TYPE_JAMMER: Registration.NodeType
    NODE_TYPE_CYBER: Registration.NodeType
    NODE_TYPE_LDEW: Registration.NodeType
    NODE_TYPE_RFDEW: Registration.NodeType
    NODE_TYPE_MOBILE_NODE: Registration.NodeType
    NODE_TYPE_POINTABLE_NODE: Registration.NodeType
    NODE_TYPE_FUSION_NODE: Registration.NodeType
    class TimeUnits(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TIME_UNITS_UNSPECIFIED: _ClassVar[Registration.TimeUnits]
        TIME_UNITS_NANOSECONDS: _ClassVar[Registration.TimeUnits]
        TIME_UNITS_MICROSECONDS: _ClassVar[Registration.TimeUnits]
        TIME_UNITS_MILLISECONDS: _ClassVar[Registration.TimeUnits]
        TIME_UNITS_SECONDS: _ClassVar[Registration.TimeUnits]
        TIME_UNITS_MINUTES: _ClassVar[Registration.TimeUnits]
        TIME_UNITS_HOURS: _ClassVar[Registration.TimeUnits]
        TIME_UNITS_DAYS: _ClassVar[Registration.TimeUnits]
    TIME_UNITS_UNSPECIFIED: Registration.TimeUnits
    TIME_UNITS_NANOSECONDS: Registration.TimeUnits
    TIME_UNITS_MICROSECONDS: Registration.TimeUnits
    TIME_UNITS_MILLISECONDS: Registration.TimeUnits
    TIME_UNITS_SECONDS: Registration.TimeUnits
    TIME_UNITS_MINUTES: Registration.TimeUnits
    TIME_UNITS_HOURS: Registration.TimeUnits
    TIME_UNITS_DAYS: Registration.TimeUnits
    class StatusReportCategory(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATUS_REPORT_CATEGORY_UNSPECIFIED: _ClassVar[Registration.StatusReportCategory]
        STATUS_REPORT_CATEGORY_SENSOR: _ClassVar[Registration.StatusReportCategory]
        STATUS_REPORT_CATEGORY_POWER: _ClassVar[Registration.StatusReportCategory]
        STATUS_REPORT_CATEGORY_MODE: _ClassVar[Registration.StatusReportCategory]
        STATUS_REPORT_CATEGORY_STATUS: _ClassVar[Registration.StatusReportCategory]
    STATUS_REPORT_CATEGORY_UNSPECIFIED: Registration.StatusReportCategory
    STATUS_REPORT_CATEGORY_SENSOR: Registration.StatusReportCategory
    STATUS_REPORT_CATEGORY_POWER: Registration.StatusReportCategory
    STATUS_REPORT_CATEGORY_MODE: Registration.StatusReportCategory
    STATUS_REPORT_CATEGORY_STATUS: Registration.StatusReportCategory
    class ModeType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        MODE_TYPE_UNSPECIFIED: _ClassVar[Registration.ModeType]
        MODE_TYPE_PERMANENT: _ClassVar[Registration.ModeType]
        MODE_TYPE_TEMPORARY: _ClassVar[Registration.ModeType]
        MODE_TYPE_DEFAULT: _ClassVar[Registration.ModeType]
    MODE_TYPE_UNSPECIFIED: Registration.ModeType
    MODE_TYPE_PERMANENT: Registration.ModeType
    MODE_TYPE_TEMPORARY: Registration.ModeType
    MODE_TYPE_DEFAULT: Registration.ModeType
    class ScanType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SCAN_TYPE_UNSPECIFIED: _ClassVar[Registration.ScanType]
        SCAN_TYPE_FIXED: _ClassVar[Registration.ScanType]
        SCAN_TYPE_SCANNING: _ClassVar[Registration.ScanType]
        SCAN_TYPE_STEERABLE: _ClassVar[Registration.ScanType]
    SCAN_TYPE_UNSPECIFIED: Registration.ScanType
    SCAN_TYPE_FIXED: Registration.ScanType
    SCAN_TYPE_SCANNING: Registration.ScanType
    SCAN_TYPE_STEERABLE: Registration.ScanType
    class TrackingType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        TRACKING_TYPE_UNSPECIFIED: _ClassVar[Registration.TrackingType]
        TRACKING_TYPE_NONE: _ClassVar[Registration.TrackingType]
        TRACKING_TYPE_TRACKLET: _ClassVar[Registration.TrackingType]
        TRACKING_TYPE_TRACK: _ClassVar[Registration.TrackingType]
        TRACKING_TYPE_TRACK_WITH_RE_ID: _ClassVar[Registration.TrackingType]
    TRACKING_TYPE_UNSPECIFIED: Registration.TrackingType
    TRACKING_TYPE_NONE: Registration.TrackingType
    TRACKING_TYPE_TRACKLET: Registration.TrackingType
    TRACKING_TYPE_TRACK: Registration.TrackingType
    TRACKING_TYPE_TRACK_WITH_RE_ID: Registration.TrackingType
    class DetectionReportCategory(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DETECTION_REPORT_CATEGORY_UNSPECIFIED: _ClassVar[Registration.DetectionReportCategory]
        DETECTION_REPORT_CATEGORY_DETECTION: _ClassVar[Registration.DetectionReportCategory]
        DETECTION_REPORT_CATEGORY_TRACK: _ClassVar[Registration.DetectionReportCategory]
        DETECTION_REPORT_CATEGORY_OBJECT: _ClassVar[Registration.DetectionReportCategory]
        DETECTION_REPORT_CATEGORY_SIGNAL: _ClassVar[Registration.DetectionReportCategory]
    DETECTION_REPORT_CATEGORY_UNSPECIFIED: Registration.DetectionReportCategory
    DETECTION_REPORT_CATEGORY_DETECTION: Registration.DetectionReportCategory
    DETECTION_REPORT_CATEGORY_TRACK: Registration.DetectionReportCategory
    DETECTION_REPORT_CATEGORY_OBJECT: Registration.DetectionReportCategory
    DETECTION_REPORT_CATEGORY_SIGNAL: Registration.DetectionReportCategory
    class ConfidenceDefinition(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        CONFIDENCE_DEFINITION_UNSPECIFIED: _ClassVar[Registration.ConfidenceDefinition]
        CONFIDENCE_DEFINITION_SINGLE_CLASS: _ClassVar[Registration.ConfidenceDefinition]
        CONFIDENCE_DEFINITION_MULTI_CLASS: _ClassVar[Registration.ConfidenceDefinition]
    CONFIDENCE_DEFINITION_UNSPECIFIED: Registration.ConfidenceDefinition
    CONFIDENCE_DEFINITION_SINGLE_CLASS: Registration.ConfidenceDefinition
    CONFIDENCE_DEFINITION_MULTI_CLASS: Registration.ConfidenceDefinition
    class CommandType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        COMMAND_TYPE_UNSPECIFIED: _ClassVar[Registration.CommandType]
        COMMAND_TYPE_REQUEST: _ClassVar[Registration.CommandType]
        COMMAND_TYPE_DETECTION_THRESHOLD: _ClassVar[Registration.CommandType]
        COMMAND_TYPE_DETECTION_REPORT_RATE: _ClassVar[Registration.CommandType]
        COMMAND_TYPE_CLASSIFICATION_THRESHOLD: _ClassVar[Registration.CommandType]
        COMMAND_TYPE_MODE_CHANGE: _ClassVar[Registration.CommandType]
        COMMAND_TYPE_LOOK_AT: _ClassVar[Registration.CommandType]
        COMMAND_TYPE_MOVE_TO: _ClassVar[Registration.CommandType]
        COMMAND_TYPE_PATROL: _ClassVar[Registration.CommandType]
        COMMAND_TYPE_FOLLOW: _ClassVar[Registration.CommandType]
    COMMAND_TYPE_UNSPECIFIED: Registration.CommandType
    COMMAND_TYPE_REQUEST: Registration.CommandType
    COMMAND_TYPE_DETECTION_THRESHOLD: Registration.CommandType
    COMMAND_TYPE_DETECTION_REPORT_RATE: Registration.CommandType
    COMMAND_TYPE_CLASSIFICATION_THRESHOLD: Registration.CommandType
    COMMAND_TYPE_MODE_CHANGE: Registration.CommandType
    COMMAND_TYPE_LOOK_AT: Registration.CommandType
    COMMAND_TYPE_MOVE_TO: Registration.CommandType
    COMMAND_TYPE_PATROL: Registration.CommandType
    COMMAND_TYPE_FOLLOW: Registration.CommandType
    class RegionType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        REGION_TYPE_UNSPECIFIED: _ClassVar[Registration.RegionType]
        REGION_TYPE_AREA_OF_INTEREST: _ClassVar[Registration.RegionType]
        REGION_TYPE_IGNORE: _ClassVar[Registration.RegionType]
        REGION_TYPE_BOUNDARY: _ClassVar[Registration.RegionType]
        REGION_TYPE_MOBILE_NODE_NO_GO_AREA: _ClassVar[Registration.RegionType]
        REGION_TYPE_MOBILE_NODE_GO_AREA: _ClassVar[Registration.RegionType]
    REGION_TYPE_UNSPECIFIED: Registration.RegionType
    REGION_TYPE_AREA_OF_INTEREST: Registration.RegionType
    REGION_TYPE_IGNORE: Registration.RegionType
    REGION_TYPE_BOUNDARY: Registration.RegionType
    REGION_TYPE_MOBILE_NODE_NO_GO_AREA: Registration.RegionType
    REGION_TYPE_MOBILE_NODE_GO_AREA: Registration.RegionType
    class NodeDefinition(_message.Message):
        __slots__ = ("node_type", "node_sub_type")
        NODE_TYPE_FIELD_NUMBER: _ClassVar[int]
        NODE_SUB_TYPE_FIELD_NUMBER: _ClassVar[int]
        node_type: Registration.NodeType
        node_sub_type: _containers.RepeatedScalarFieldContainer[str]
        def __init__(self, node_type: _Optional[_Union[Registration.NodeType, str]] = ..., node_sub_type: _Optional[_Iterable[str]] = ...) -> None: ...
    class Capability(_message.Message):
        __slots__ = ("category", "type", "value", "units")
        CATEGORY_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        category: str
        type: str
        value: str
        units: str
        def __init__(self, category: _Optional[str] = ..., type: _Optional[str] = ..., value: _Optional[str] = ..., units: _Optional[str] = ...) -> None: ...
    class StatusDefinition(_message.Message):
        __slots__ = ("status_interval", "location_definition", "coverage_definition", "obscuration_definition", "status_report", "field_of_view_definition")
        STATUS_INTERVAL_FIELD_NUMBER: _ClassVar[int]
        LOCATION_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        COVERAGE_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        OBSCURATION_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        STATUS_REPORT_FIELD_NUMBER: _ClassVar[int]
        FIELD_OF_VIEW_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        status_interval: Registration.Duration
        location_definition: Registration.LocationType
        coverage_definition: Registration.LocationType
        obscuration_definition: Registration.LocationType
        status_report: _containers.RepeatedCompositeFieldContainer[Registration.StatusReport]
        field_of_view_definition: Registration.LocationType
        def __init__(self, status_interval: _Optional[_Union[Registration.Duration, _Mapping]] = ..., location_definition: _Optional[_Union[Registration.LocationType, _Mapping]] = ..., coverage_definition: _Optional[_Union[Registration.LocationType, _Mapping]] = ..., obscuration_definition: _Optional[_Union[Registration.LocationType, _Mapping]] = ..., status_report: _Optional[_Iterable[_Union[Registration.StatusReport, _Mapping]]] = ..., field_of_view_definition: _Optional[_Union[Registration.LocationType, _Mapping]] = ...) -> None: ...
    class Duration(_message.Message):
        __slots__ = ("units", "value")
        UNITS_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        units: Registration.TimeUnits
        value: float
        def __init__(self, units: _Optional[_Union[Registration.TimeUnits, str]] = ..., value: _Optional[float] = ...) -> None: ...
    class ModeParameter(_message.Message):
        __slots__ = ("type", "value")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        type: str
        value: str
        def __init__(self, type: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class LocationType(_message.Message):
        __slots__ = ("location_units", "range_bearing_units", "location_datum", "range_bearing_datum", "zone")
        LOCATION_UNITS_FIELD_NUMBER: _ClassVar[int]
        RANGE_BEARING_UNITS_FIELD_NUMBER: _ClassVar[int]
        LOCATION_DATUM_FIELD_NUMBER: _ClassVar[int]
        RANGE_BEARING_DATUM_FIELD_NUMBER: _ClassVar[int]
        ZONE_FIELD_NUMBER: _ClassVar[int]
        location_units: _location_pb2.LocationCoordinateSystem
        range_bearing_units: _range_bearing_pb2.RangeBearingCoordinateSystem
        location_datum: _location_pb2.LocationDatum
        range_bearing_datum: _range_bearing_pb2.RangeBearingDatum
        zone: str
        def __init__(self, location_units: _Optional[_Union[_location_pb2.LocationCoordinateSystem, str]] = ..., range_bearing_units: _Optional[_Union[_range_bearing_pb2.RangeBearingCoordinateSystem, str]] = ..., location_datum: _Optional[_Union[_location_pb2.LocationDatum, str]] = ..., range_bearing_datum: _Optional[_Union[_range_bearing_pb2.RangeBearingDatum, str]] = ..., zone: _Optional[str] = ...) -> None: ...
    class VelocityType(_message.Message):
        __slots__ = ("enu_velocity_units", "location_datum", "range_bearing_datum", "zone")
        ENU_VELOCITY_UNITS_FIELD_NUMBER: _ClassVar[int]
        LOCATION_DATUM_FIELD_NUMBER: _ClassVar[int]
        RANGE_BEARING_DATUM_FIELD_NUMBER: _ClassVar[int]
        ZONE_FIELD_NUMBER: _ClassVar[int]
        enu_velocity_units: _velocity_pb2.ENUVelocityUnits
        location_datum: _location_pb2.LocationDatum
        range_bearing_datum: _range_bearing_pb2.RangeBearingDatum
        zone: str
        def __init__(self, enu_velocity_units: _Optional[_Union[_velocity_pb2.ENUVelocityUnits, _Mapping]] = ..., location_datum: _Optional[_Union[_location_pb2.LocationDatum, str]] = ..., range_bearing_datum: _Optional[_Union[_range_bearing_pb2.RangeBearingDatum, str]] = ..., zone: _Optional[str] = ...) -> None: ...
    class StatusReport(_message.Message):
        __slots__ = ("category", "type", "units", "on_change")
        CATEGORY_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        ON_CHANGE_FIELD_NUMBER: _ClassVar[int]
        category: Registration.StatusReportCategory
        type: str
        units: str
        on_change: bool
        def __init__(self, category: _Optional[_Union[Registration.StatusReportCategory, str]] = ..., type: _Optional[str] = ..., units: _Optional[str] = ..., on_change: bool = ...) -> None: ...
    class ModeDefinition(_message.Message):
        __slots__ = ("mode_name", "mode_type", "mode_description", "settle_time", "maximum_latency", "scan_type", "tracking_type", "duration", "mode_parameter", "detection_definition", "task")
        MODE_NAME_FIELD_NUMBER: _ClassVar[int]
        MODE_TYPE_FIELD_NUMBER: _ClassVar[int]
        MODE_DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
        SETTLE_TIME_FIELD_NUMBER: _ClassVar[int]
        MAXIMUM_LATENCY_FIELD_NUMBER: _ClassVar[int]
        SCAN_TYPE_FIELD_NUMBER: _ClassVar[int]
        TRACKING_TYPE_FIELD_NUMBER: _ClassVar[int]
        DURATION_FIELD_NUMBER: _ClassVar[int]
        MODE_PARAMETER_FIELD_NUMBER: _ClassVar[int]
        DETECTION_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        TASK_FIELD_NUMBER: _ClassVar[int]
        mode_name: str
        mode_type: Registration.ModeType
        mode_description: str
        settle_time: Registration.Duration
        maximum_latency: Registration.Duration
        scan_type: Registration.ScanType
        tracking_type: Registration.TrackingType
        duration: Registration.Duration
        mode_parameter: _containers.RepeatedCompositeFieldContainer[Registration.ModeParameter]
        detection_definition: _containers.RepeatedCompositeFieldContainer[Registration.DetectionDefinition]
        task: Registration.TaskDefinition
        def __init__(self, mode_name: _Optional[str] = ..., mode_type: _Optional[_Union[Registration.ModeType, str]] = ..., mode_description: _Optional[str] = ..., settle_time: _Optional[_Union[Registration.Duration, _Mapping]] = ..., maximum_latency: _Optional[_Union[Registration.Duration, _Mapping]] = ..., scan_type: _Optional[_Union[Registration.ScanType, str]] = ..., tracking_type: _Optional[_Union[Registration.TrackingType, str]] = ..., duration: _Optional[_Union[Registration.Duration, _Mapping]] = ..., mode_parameter: _Optional[_Iterable[_Union[Registration.ModeParameter, _Mapping]]] = ..., detection_definition: _Optional[_Iterable[_Union[Registration.DetectionDefinition, _Mapping]]] = ..., task: _Optional[_Union[Registration.TaskDefinition, _Mapping]] = ...) -> None: ...
    class DetectionDefinition(_message.Message):
        __slots__ = ("location_type", "detection_performance", "detection_report", "detection_class_definition", "behaviour_definition", "velocity_type", "geometric_error")
        LOCATION_TYPE_FIELD_NUMBER: _ClassVar[int]
        DETECTION_PERFORMANCE_FIELD_NUMBER: _ClassVar[int]
        DETECTION_REPORT_FIELD_NUMBER: _ClassVar[int]
        DETECTION_CLASS_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        BEHAVIOUR_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        VELOCITY_TYPE_FIELD_NUMBER: _ClassVar[int]
        GEOMETRIC_ERROR_FIELD_NUMBER: _ClassVar[int]
        location_type: Registration.LocationType
        detection_performance: _containers.RepeatedCompositeFieldContainer[Registration.PerformanceValue]
        detection_report: _containers.RepeatedCompositeFieldContainer[Registration.DetectionReport]
        detection_class_definition: _containers.RepeatedCompositeFieldContainer[Registration.DetectionClassDefinition]
        behaviour_definition: _containers.RepeatedCompositeFieldContainer[Registration.BehaviourDefinition]
        velocity_type: Registration.VelocityType
        geometric_error: Registration.GeometricError
        def __init__(self, location_type: _Optional[_Union[Registration.LocationType, _Mapping]] = ..., detection_performance: _Optional[_Iterable[_Union[Registration.PerformanceValue, _Mapping]]] = ..., detection_report: _Optional[_Iterable[_Union[Registration.DetectionReport, _Mapping]]] = ..., detection_class_definition: _Optional[_Iterable[_Union[Registration.DetectionClassDefinition, _Mapping]]] = ..., behaviour_definition: _Optional[_Iterable[_Union[Registration.BehaviourDefinition, _Mapping]]] = ..., velocity_type: _Optional[_Union[Registration.VelocityType, _Mapping]] = ..., geometric_error: _Optional[_Union[Registration.GeometricError, _Mapping]] = ...) -> None: ...
    class GeometricError(_message.Message):
        __slots__ = ("type", "units", "variation_type", "performance_value")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        VARIATION_TYPE_FIELD_NUMBER: _ClassVar[int]
        PERFORMANCE_VALUE_FIELD_NUMBER: _ClassVar[int]
        type: str
        units: str
        variation_type: str
        performance_value: _containers.RepeatedCompositeFieldContainer[Registration.PerformanceValue]
        def __init__(self, type: _Optional[str] = ..., units: _Optional[str] = ..., variation_type: _Optional[str] = ..., performance_value: _Optional[_Iterable[_Union[Registration.PerformanceValue, _Mapping]]] = ...) -> None: ...
    class PerformanceValue(_message.Message):
        __slots__ = ("type", "units", "unit_value", "variation_type")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        UNIT_VALUE_FIELD_NUMBER: _ClassVar[int]
        VARIATION_TYPE_FIELD_NUMBER: _ClassVar[int]
        type: str
        units: str
        unit_value: str
        variation_type: str
        def __init__(self, type: _Optional[str] = ..., units: _Optional[str] = ..., unit_value: _Optional[str] = ..., variation_type: _Optional[str] = ...) -> None: ...
    class DetectionReport(_message.Message):
        __slots__ = ("category", "type", "units", "on_change")
        CATEGORY_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        ON_CHANGE_FIELD_NUMBER: _ClassVar[int]
        category: Registration.DetectionReportCategory
        type: str
        units: str
        on_change: bool
        def __init__(self, category: _Optional[_Union[Registration.DetectionReportCategory, str]] = ..., type: _Optional[str] = ..., units: _Optional[str] = ..., on_change: bool = ...) -> None: ...
    class DetectionClassDefinition(_message.Message):
        __slots__ = ("confidence_definition", "class_performance", "class_definition", "taxonomy_dock_definition")
        CONFIDENCE_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        CLASS_PERFORMANCE_FIELD_NUMBER: _ClassVar[int]
        CLASS_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        TAXONOMY_DOCK_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        confidence_definition: Registration.ConfidenceDefinition
        class_performance: _containers.RepeatedCompositeFieldContainer[Registration.PerformanceValue]
        class_definition: _containers.RepeatedCompositeFieldContainer[Registration.ClassDefinition]
        taxonomy_dock_definition: _containers.RepeatedCompositeFieldContainer[Registration.TaxonomyDockDefinition]
        def __init__(self, confidence_definition: _Optional[_Union[Registration.ConfidenceDefinition, str]] = ..., class_performance: _Optional[_Iterable[_Union[Registration.PerformanceValue, _Mapping]]] = ..., class_definition: _Optional[_Iterable[_Union[Registration.ClassDefinition, _Mapping]]] = ..., taxonomy_dock_definition: _Optional[_Iterable[_Union[Registration.TaxonomyDockDefinition, _Mapping]]] = ...) -> None: ...
    class ClassDefinition(_message.Message):
        __slots__ = ("type", "units", "sub_class")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        SUB_CLASS_FIELD_NUMBER: _ClassVar[int]
        type: str
        units: str
        sub_class: _containers.RepeatedCompositeFieldContainer[Registration.SubClass]
        def __init__(self, type: _Optional[str] = ..., units: _Optional[str] = ..., sub_class: _Optional[_Iterable[_Union[Registration.SubClass, _Mapping]]] = ...) -> None: ...
    class SubClass(_message.Message):
        __slots__ = ("type", "units", "level", "sub_class")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        LEVEL_FIELD_NUMBER: _ClassVar[int]
        SUB_CLASS_FIELD_NUMBER: _ClassVar[int]
        type: str
        units: str
        level: int
        sub_class: _containers.RepeatedCompositeFieldContainer[Registration.SubClass]
        def __init__(self, type: _Optional[str] = ..., units: _Optional[str] = ..., level: _Optional[int] = ..., sub_class: _Optional[_Iterable[_Union[Registration.SubClass, _Mapping]]] = ...) -> None: ...
    class BehaviourDefinition(_message.Message):
        __slots__ = ("type", "units")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        type: str
        units: str
        def __init__(self, type: _Optional[str] = ..., units: _Optional[str] = ...) -> None: ...
    class TaskDefinition(_message.Message):
        __slots__ = ("concurrent_tasks", "region_definition", "command")
        CONCURRENT_TASKS_FIELD_NUMBER: _ClassVar[int]
        REGION_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        COMMAND_FIELD_NUMBER: _ClassVar[int]
        concurrent_tasks: int
        region_definition: Registration.RegionDefinition
        command: _containers.RepeatedCompositeFieldContainer[Registration.Command]
        def __init__(self, concurrent_tasks: _Optional[int] = ..., region_definition: _Optional[_Union[Registration.RegionDefinition, _Mapping]] = ..., command: _Optional[_Iterable[_Union[Registration.Command, _Mapping]]] = ...) -> None: ...
    class ClassFilterDefinition(_message.Message):
        __slots__ = ("filter_parameter", "sub_class_definition", "type")
        FILTER_PARAMETER_FIELD_NUMBER: _ClassVar[int]
        SUB_CLASS_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        filter_parameter: _containers.RepeatedCompositeFieldContainer[Registration.FilterParameter]
        sub_class_definition: _containers.RepeatedCompositeFieldContainer[Registration.SubClassFilterDefinition]
        type: str
        def __init__(self, filter_parameter: _Optional[_Iterable[_Union[Registration.FilterParameter, _Mapping]]] = ..., sub_class_definition: _Optional[_Iterable[_Union[Registration.SubClassFilterDefinition, _Mapping]]] = ..., type: _Optional[str] = ...) -> None: ...
    class SubClassFilterDefinition(_message.Message):
        __slots__ = ("filter_parameter", "level", "type", "sub_class_definition")
        FILTER_PARAMETER_FIELD_NUMBER: _ClassVar[int]
        LEVEL_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        SUB_CLASS_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        filter_parameter: _containers.RepeatedCompositeFieldContainer[Registration.FilterParameter]
        level: int
        type: str
        sub_class_definition: _containers.RepeatedCompositeFieldContainer[Registration.SubClassFilterDefinition]
        def __init__(self, filter_parameter: _Optional[_Iterable[_Union[Registration.FilterParameter, _Mapping]]] = ..., level: _Optional[int] = ..., type: _Optional[str] = ..., sub_class_definition: _Optional[_Iterable[_Union[Registration.SubClassFilterDefinition, _Mapping]]] = ...) -> None: ...
    class FilterParameter(_message.Message):
        __slots__ = ("parameter", "operators")
        PARAMETER_FIELD_NUMBER: _ClassVar[int]
        OPERATORS_FIELD_NUMBER: _ClassVar[int]
        parameter: str
        operators: _containers.RepeatedScalarFieldContainer[Operator]
        def __init__(self, parameter: _Optional[str] = ..., operators: _Optional[_Iterable[_Union[Operator, str]]] = ...) -> None: ...
    class BehaviourFilterDefinition(_message.Message):
        __slots__ = ("filter_parameter", "type")
        FILTER_PARAMETER_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        filter_parameter: _containers.RepeatedCompositeFieldContainer[Registration.FilterParameter]
        type: str
        def __init__(self, filter_parameter: _Optional[_Iterable[_Union[Registration.FilterParameter, _Mapping]]] = ..., type: _Optional[str] = ...) -> None: ...
    class Command(_message.Message):
        __slots__ = ("units", "completion_time", "type")
        UNITS_FIELD_NUMBER: _ClassVar[int]
        COMPLETION_TIME_FIELD_NUMBER: _ClassVar[int]
        TYPE_FIELD_NUMBER: _ClassVar[int]
        units: str
        completion_time: Registration.Duration
        type: Registration.CommandType
        def __init__(self, units: _Optional[str] = ..., completion_time: _Optional[_Union[Registration.Duration, _Mapping]] = ..., type: _Optional[_Union[Registration.CommandType, str]] = ...) -> None: ...
    class RegionDefinition(_message.Message):
        __slots__ = ("region_type", "settle_time", "region_area", "class_filter_definition", "behaviour_filter_definition")
        REGION_TYPE_FIELD_NUMBER: _ClassVar[int]
        SETTLE_TIME_FIELD_NUMBER: _ClassVar[int]
        REGION_AREA_FIELD_NUMBER: _ClassVar[int]
        CLASS_FILTER_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        BEHAVIOUR_FILTER_DEFINITION_FIELD_NUMBER: _ClassVar[int]
        region_type: _containers.RepeatedScalarFieldContainer[Registration.RegionType]
        settle_time: Registration.Duration
        region_area: _containers.RepeatedCompositeFieldContainer[Registration.LocationType]
        class_filter_definition: _containers.RepeatedCompositeFieldContainer[Registration.ClassFilterDefinition]
        behaviour_filter_definition: _containers.RepeatedCompositeFieldContainer[Registration.BehaviourFilterDefinition]
        def __init__(self, region_type: _Optional[_Iterable[_Union[Registration.RegionType, str]]] = ..., settle_time: _Optional[_Union[Registration.Duration, _Mapping]] = ..., region_area: _Optional[_Iterable[_Union[Registration.LocationType, _Mapping]]] = ..., class_filter_definition: _Optional[_Iterable[_Union[Registration.ClassFilterDefinition, _Mapping]]] = ..., behaviour_filter_definition: _Optional[_Iterable[_Union[Registration.BehaviourFilterDefinition, _Mapping]]] = ...) -> None: ...
    class TaxonomyDockDefinition(_message.Message):
        __slots__ = ("Dock_class_namespace", "Dock_class", "Extension_subclass")
        DOCK_CLASS_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
        DOCK_CLASS_FIELD_NUMBER: _ClassVar[int]
        EXTENSION_SUBCLASS_FIELD_NUMBER: _ClassVar[int]
        Dock_class_namespace: str
        Dock_class: str
        Extension_subclass: _containers.RepeatedCompositeFieldContainer[Registration.ExtensionSubclass]
        def __init__(self, Dock_class_namespace: _Optional[str] = ..., Dock_class: _Optional[str] = ..., Extension_subclass: _Optional[_Iterable[_Union[Registration.ExtensionSubclass, _Mapping]]] = ...) -> None: ...
    class ExtensionSubclass(_message.Message):
        __slots__ = ("Subclass_namespace", "Subclass_name", "units")
        SUBCLASS_NAMESPACE_FIELD_NUMBER: _ClassVar[int]
        SUBCLASS_NAME_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        Subclass_namespace: str
        Subclass_name: str
        units: str
        def __init__(self, Subclass_namespace: _Optional[str] = ..., Subclass_name: _Optional[str] = ..., units: _Optional[str] = ...) -> None: ...
    class ConfigurationData(_message.Message):
        __slots__ = ("manufacturer", "model", "serial_number", "hardware_version", "software_version", "sub_components")
        MANUFACTURER_FIELD_NUMBER: _ClassVar[int]
        MODEL_FIELD_NUMBER: _ClassVar[int]
        SERIAL_NUMBER_FIELD_NUMBER: _ClassVar[int]
        HARDWARE_VERSION_FIELD_NUMBER: _ClassVar[int]
        SOFTWARE_VERSION_FIELD_NUMBER: _ClassVar[int]
        SUB_COMPONENTS_FIELD_NUMBER: _ClassVar[int]
        manufacturer: str
        model: str
        serial_number: str
        hardware_version: str
        software_version: str
        sub_components: _containers.RepeatedCompositeFieldContainer[Registration.ConfigurationData]
        def __init__(self, manufacturer: _Optional[str] = ..., model: _Optional[str] = ..., serial_number: _Optional[str] = ..., hardware_version: _Optional[str] = ..., software_version: _Optional[str] = ..., sub_components: _Optional[_Iterable[_Union[Registration.ConfigurationData, _Mapping]]] = ...) -> None: ...
    NODE_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    ICD_VERSION_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SHORT_NAME_FIELD_NUMBER: _ClassVar[int]
    CAPABILITIES_FIELD_NUMBER: _ClassVar[int]
    STATUS_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    MODE_DEFINITION_FIELD_NUMBER: _ClassVar[int]
    DEPENDENT_NODES_FIELD_NUMBER: _ClassVar[int]
    REPORTING_REGION_FIELD_NUMBER: _ClassVar[int]
    CONFIG_DATA_FIELD_NUMBER: _ClassVar[int]
    node_definition: _containers.RepeatedCompositeFieldContainer[Registration.NodeDefinition]
    icd_version: str
    name: str
    short_name: str
    capabilities: _containers.RepeatedCompositeFieldContainer[Registration.Capability]
    status_definition: Registration.StatusDefinition
    mode_definition: _containers.RepeatedCompositeFieldContainer[Registration.ModeDefinition]
    dependent_nodes: _containers.RepeatedScalarFieldContainer[str]
    reporting_region: _containers.RepeatedCompositeFieldContainer[_range_bearing_pb2.LocationOrRangeBearing]
    config_data: _containers.RepeatedCompositeFieldContainer[Registration.ConfigurationData]
    def __init__(self, node_definition: _Optional[_Iterable[_Union[Registration.NodeDefinition, _Mapping]]] = ..., icd_version: _Optional[str] = ..., name: _Optional[str] = ..., short_name: _Optional[str] = ..., capabilities: _Optional[_Iterable[_Union[Registration.Capability, _Mapping]]] = ..., status_definition: _Optional[_Union[Registration.StatusDefinition, _Mapping]] = ..., mode_definition: _Optional[_Iterable[_Union[Registration.ModeDefinition, _Mapping]]] = ..., dependent_nodes: _Optional[_Iterable[str]] = ..., reporting_region: _Optional[_Iterable[_Union[_range_bearing_pb2.LocationOrRangeBearing, _Mapping]]] = ..., config_data: _Optional[_Iterable[_Union[Registration.ConfigurationData, _Mapping]]] = ...) -> None: ...
