from sapient_msg.bsi_flex_335_v2_0 import location_pb2 as _location_pb2
from sapient_msg.bsi_flex_335_v2_0 import range_bearing_pb2 as _range_bearing_pb2
from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StatusReport(_message.Message):
    __slots__ = ("report_id", "system", "info", "active_task_id", "mode", "power", "node_location", "field_of_view", "obscuration", "status", "coverage")
    class System(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SYSTEM_UNSPECIFIED: _ClassVar[StatusReport.System]
        SYSTEM_OK: _ClassVar[StatusReport.System]
        SYSTEM_WARNING: _ClassVar[StatusReport.System]
        SYSTEM_ERROR: _ClassVar[StatusReport.System]
        SYSTEM_GOODBYE: _ClassVar[StatusReport.System]
    SYSTEM_UNSPECIFIED: StatusReport.System
    SYSTEM_OK: StatusReport.System
    SYSTEM_WARNING: StatusReport.System
    SYSTEM_ERROR: StatusReport.System
    SYSTEM_GOODBYE: StatusReport.System
    class PowerSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        POWERSOURCE_UNSPECIFIED: _ClassVar[StatusReport.PowerSource]
        POWERSOURCE_OTHER: _ClassVar[StatusReport.PowerSource]
        POWERSOURCE_MAINS: _ClassVar[StatusReport.PowerSource]
        POWERSOURCE_INTERNAL_BATTERY: _ClassVar[StatusReport.PowerSource]
        POWERSOURCE_EXTERNAL_BATTERY: _ClassVar[StatusReport.PowerSource]
        POWERSOURCE_GENERATOR: _ClassVar[StatusReport.PowerSource]
        POWERSOURCE_SOLAR_PV: _ClassVar[StatusReport.PowerSource]
        POWERSOURCE_WIND_TURBINE: _ClassVar[StatusReport.PowerSource]
        POWERSOURCE_FUEL_CELL: _ClassVar[StatusReport.PowerSource]
    POWERSOURCE_UNSPECIFIED: StatusReport.PowerSource
    POWERSOURCE_OTHER: StatusReport.PowerSource
    POWERSOURCE_MAINS: StatusReport.PowerSource
    POWERSOURCE_INTERNAL_BATTERY: StatusReport.PowerSource
    POWERSOURCE_EXTERNAL_BATTERY: StatusReport.PowerSource
    POWERSOURCE_GENERATOR: StatusReport.PowerSource
    POWERSOURCE_SOLAR_PV: StatusReport.PowerSource
    POWERSOURCE_WIND_TURBINE: StatusReport.PowerSource
    POWERSOURCE_FUEL_CELL: StatusReport.PowerSource
    class PowerStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        POWERSTATUS_UNSPECIFIED: _ClassVar[StatusReport.PowerStatus]
        POWERSTATUS_OK: _ClassVar[StatusReport.PowerStatus]
        POWERSTATUS_FAULT: _ClassVar[StatusReport.PowerStatus]
    POWERSTATUS_UNSPECIFIED: StatusReport.PowerStatus
    POWERSTATUS_OK: StatusReport.PowerStatus
    POWERSTATUS_FAULT: StatusReport.PowerStatus
    class Info(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        INFO_UNSPECIFIED: _ClassVar[StatusReport.Info]
        INFO_NEW: _ClassVar[StatusReport.Info]
        INFO_UNCHANGED: _ClassVar[StatusReport.Info]
    INFO_UNSPECIFIED: StatusReport.Info
    INFO_NEW: StatusReport.Info
    INFO_UNCHANGED: StatusReport.Info
    class StatusType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATUS_TYPE_UNSPECIFIED: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_INTERNAL_FAULT: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_EXTERNAL_FAULT: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_ILLUMINATION: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_WEATHER: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_CLUTTER: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_EXPOSURE: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_MOTION_SENSITIVITY: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_PTZ_STATUS: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_PD: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_FAR: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_NOT_DETECTING: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_PLATFORM: _ClassVar[StatusReport.StatusType]
        STATUS_TYPE_OTHER: _ClassVar[StatusReport.StatusType]
    STATUS_TYPE_UNSPECIFIED: StatusReport.StatusType
    STATUS_TYPE_INTERNAL_FAULT: StatusReport.StatusType
    STATUS_TYPE_EXTERNAL_FAULT: StatusReport.StatusType
    STATUS_TYPE_ILLUMINATION: StatusReport.StatusType
    STATUS_TYPE_WEATHER: StatusReport.StatusType
    STATUS_TYPE_CLUTTER: StatusReport.StatusType
    STATUS_TYPE_EXPOSURE: StatusReport.StatusType
    STATUS_TYPE_MOTION_SENSITIVITY: StatusReport.StatusType
    STATUS_TYPE_PTZ_STATUS: StatusReport.StatusType
    STATUS_TYPE_PD: StatusReport.StatusType
    STATUS_TYPE_FAR: StatusReport.StatusType
    STATUS_TYPE_NOT_DETECTING: StatusReport.StatusType
    STATUS_TYPE_PLATFORM: StatusReport.StatusType
    STATUS_TYPE_OTHER: StatusReport.StatusType
    class StatusLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        STATUS_LEVEL_UNSPECIFIED: _ClassVar[StatusReport.StatusLevel]
        STATUS_LEVEL_INFORMATION_STATUS: _ClassVar[StatusReport.StatusLevel]
        STATUS_LEVEL_WARNING_STATUS: _ClassVar[StatusReport.StatusLevel]
        STATUS_LEVEL_ERROR_STATUS: _ClassVar[StatusReport.StatusLevel]
    STATUS_LEVEL_UNSPECIFIED: StatusReport.StatusLevel
    STATUS_LEVEL_INFORMATION_STATUS: StatusReport.StatusLevel
    STATUS_LEVEL_WARNING_STATUS: StatusReport.StatusLevel
    STATUS_LEVEL_ERROR_STATUS: StatusReport.StatusLevel
    class Power(_message.Message):
        __slots__ = ("level", "source", "status")
        LEVEL_FIELD_NUMBER: _ClassVar[int]
        SOURCE_FIELD_NUMBER: _ClassVar[int]
        STATUS_FIELD_NUMBER: _ClassVar[int]
        level: int
        source: StatusReport.PowerSource
        status: StatusReport.PowerStatus
        def __init__(self, level: _Optional[int] = ..., source: _Optional[_Union[StatusReport.PowerSource, str]] = ..., status: _Optional[_Union[StatusReport.PowerStatus, str]] = ...) -> None: ...
    class Status(_message.Message):
        __slots__ = ("status_level", "status_value", "status_type")
        STATUS_LEVEL_FIELD_NUMBER: _ClassVar[int]
        STATUS_VALUE_FIELD_NUMBER: _ClassVar[int]
        STATUS_TYPE_FIELD_NUMBER: _ClassVar[int]
        status_level: StatusReport.StatusLevel
        status_value: str
        status_type: StatusReport.StatusType
        def __init__(self, status_level: _Optional[_Union[StatusReport.StatusLevel, str]] = ..., status_value: _Optional[str] = ..., status_type: _Optional[_Union[StatusReport.StatusType, str]] = ...) -> None: ...
    REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_FIELD_NUMBER: _ClassVar[int]
    INFO_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_TASK_ID_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    POWER_FIELD_NUMBER: _ClassVar[int]
    NODE_LOCATION_FIELD_NUMBER: _ClassVar[int]
    FIELD_OF_VIEW_FIELD_NUMBER: _ClassVar[int]
    OBSCURATION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    COVERAGE_FIELD_NUMBER: _ClassVar[int]
    report_id: str
    system: StatusReport.System
    info: StatusReport.Info
    active_task_id: str
    mode: str
    power: StatusReport.Power
    node_location: _location_pb2.Location
    field_of_view: _range_bearing_pb2.LocationOrRangeBearing
    obscuration: _containers.RepeatedCompositeFieldContainer[_range_bearing_pb2.LocationOrRangeBearing]
    status: _containers.RepeatedCompositeFieldContainer[StatusReport.Status]
    coverage: _containers.RepeatedCompositeFieldContainer[_range_bearing_pb2.LocationOrRangeBearing]
    def __init__(self, report_id: _Optional[str] = ..., system: _Optional[_Union[StatusReport.System, str]] = ..., info: _Optional[_Union[StatusReport.Info, str]] = ..., active_task_id: _Optional[str] = ..., mode: _Optional[str] = ..., power: _Optional[_Union[StatusReport.Power, _Mapping]] = ..., node_location: _Optional[_Union[_location_pb2.Location, _Mapping]] = ..., field_of_view: _Optional[_Union[_range_bearing_pb2.LocationOrRangeBearing, _Mapping]] = ..., obscuration: _Optional[_Iterable[_Union[_range_bearing_pb2.LocationOrRangeBearing, _Mapping]]] = ..., status: _Optional[_Iterable[_Union[StatusReport.Status, _Mapping]]] = ..., coverage: _Optional[_Iterable[_Union[_range_bearing_pb2.LocationOrRangeBearing, _Mapping]]] = ...) -> None: ...
