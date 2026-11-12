from sapient_msg.bsi_flex_335_v2_0 import location_pb2 as _location_pb2
from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RangeBearingCoordinateSystem(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RANGE_BEARING_COORDINATE_SYSTEM_UNSPECIFIED: _ClassVar[RangeBearingCoordinateSystem]
    RANGE_BEARING_COORDINATE_SYSTEM_DEGREES_M: _ClassVar[RangeBearingCoordinateSystem]
    RANGE_BEARING_COORDINATE_SYSTEM_RADIANS_M: _ClassVar[RangeBearingCoordinateSystem]
    RANGE_BEARING_COORDINATE_SYSTEM_DEGREES_KM: _ClassVar[RangeBearingCoordinateSystem]
    RANGE_BEARING_COORDINATE_SYSTEM_RADIANS_KM: _ClassVar[RangeBearingCoordinateSystem]

class RangeBearingDatum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RANGE_BEARING_DATUM_UNSPECIFIED: _ClassVar[RangeBearingDatum]
    RANGE_BEARING_DATUM_TRUE: _ClassVar[RangeBearingDatum]
    RANGE_BEARING_DATUM_MAGNETIC: _ClassVar[RangeBearingDatum]
    RANGE_BEARING_DATUM_GRID: _ClassVar[RangeBearingDatum]
    RANGE_BEARING_DATUM_PLATFORM: _ClassVar[RangeBearingDatum]
RANGE_BEARING_COORDINATE_SYSTEM_UNSPECIFIED: RangeBearingCoordinateSystem
RANGE_BEARING_COORDINATE_SYSTEM_DEGREES_M: RangeBearingCoordinateSystem
RANGE_BEARING_COORDINATE_SYSTEM_RADIANS_M: RangeBearingCoordinateSystem
RANGE_BEARING_COORDINATE_SYSTEM_DEGREES_KM: RangeBearingCoordinateSystem
RANGE_BEARING_COORDINATE_SYSTEM_RADIANS_KM: RangeBearingCoordinateSystem
RANGE_BEARING_DATUM_UNSPECIFIED: RangeBearingDatum
RANGE_BEARING_DATUM_TRUE: RangeBearingDatum
RANGE_BEARING_DATUM_MAGNETIC: RangeBearingDatum
RANGE_BEARING_DATUM_GRID: RangeBearingDatum
RANGE_BEARING_DATUM_PLATFORM: RangeBearingDatum

class RangeBearing(_message.Message):
    __slots__ = ("elevation", "azimuth", "range", "elevation_error", "azimuth_error", "range_error", "coordinate_system", "datum")
    ELEVATION_FIELD_NUMBER: _ClassVar[int]
    AZIMUTH_FIELD_NUMBER: _ClassVar[int]
    RANGE_FIELD_NUMBER: _ClassVar[int]
    ELEVATION_ERROR_FIELD_NUMBER: _ClassVar[int]
    AZIMUTH_ERROR_FIELD_NUMBER: _ClassVar[int]
    RANGE_ERROR_FIELD_NUMBER: _ClassVar[int]
    COORDINATE_SYSTEM_FIELD_NUMBER: _ClassVar[int]
    DATUM_FIELD_NUMBER: _ClassVar[int]
    elevation: float
    azimuth: float
    range: float
    elevation_error: float
    azimuth_error: float
    range_error: float
    coordinate_system: RangeBearingCoordinateSystem
    datum: RangeBearingDatum
    def __init__(self, elevation: _Optional[float] = ..., azimuth: _Optional[float] = ..., range: _Optional[float] = ..., elevation_error: _Optional[float] = ..., azimuth_error: _Optional[float] = ..., range_error: _Optional[float] = ..., coordinate_system: _Optional[_Union[RangeBearingCoordinateSystem, str]] = ..., datum: _Optional[_Union[RangeBearingDatum, str]] = ...) -> None: ...

class RangeBearingCone(_message.Message):
    __slots__ = ("elevation", "azimuth", "range", "horizontal_extent", "vertical_extent", "horizontal_extent_error", "vertical_extent_error", "elevation_error", "azimuth_error", "range_error", "coordinate_system", "datum")
    ELEVATION_FIELD_NUMBER: _ClassVar[int]
    AZIMUTH_FIELD_NUMBER: _ClassVar[int]
    RANGE_FIELD_NUMBER: _ClassVar[int]
    HORIZONTAL_EXTENT_FIELD_NUMBER: _ClassVar[int]
    VERTICAL_EXTENT_FIELD_NUMBER: _ClassVar[int]
    HORIZONTAL_EXTENT_ERROR_FIELD_NUMBER: _ClassVar[int]
    VERTICAL_EXTENT_ERROR_FIELD_NUMBER: _ClassVar[int]
    ELEVATION_ERROR_FIELD_NUMBER: _ClassVar[int]
    AZIMUTH_ERROR_FIELD_NUMBER: _ClassVar[int]
    RANGE_ERROR_FIELD_NUMBER: _ClassVar[int]
    COORDINATE_SYSTEM_FIELD_NUMBER: _ClassVar[int]
    DATUM_FIELD_NUMBER: _ClassVar[int]
    elevation: float
    azimuth: float
    range: float
    horizontal_extent: float
    vertical_extent: float
    horizontal_extent_error: float
    vertical_extent_error: float
    elevation_error: float
    azimuth_error: float
    range_error: float
    coordinate_system: RangeBearingCoordinateSystem
    datum: RangeBearingDatum
    def __init__(self, elevation: _Optional[float] = ..., azimuth: _Optional[float] = ..., range: _Optional[float] = ..., horizontal_extent: _Optional[float] = ..., vertical_extent: _Optional[float] = ..., horizontal_extent_error: _Optional[float] = ..., vertical_extent_error: _Optional[float] = ..., elevation_error: _Optional[float] = ..., azimuth_error: _Optional[float] = ..., range_error: _Optional[float] = ..., coordinate_system: _Optional[_Union[RangeBearingCoordinateSystem, str]] = ..., datum: _Optional[_Union[RangeBearingDatum, str]] = ...) -> None: ...

class LocationOrRangeBearing(_message.Message):
    __slots__ = ("range_bearing", "location_list")
    RANGE_BEARING_FIELD_NUMBER: _ClassVar[int]
    LOCATION_LIST_FIELD_NUMBER: _ClassVar[int]
    range_bearing: RangeBearingCone
    location_list: _location_pb2.LocationList
    def __init__(self, range_bearing: _Optional[_Union[RangeBearingCone, _Mapping]] = ..., location_list: _Optional[_Union[_location_pb2.LocationList, _Mapping]] = ...) -> None: ...
