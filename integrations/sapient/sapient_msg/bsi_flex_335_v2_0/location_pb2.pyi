from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LocationCoordinateSystem(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LOCATION_COORDINATE_SYSTEM_UNSPECIFIED: _ClassVar[LocationCoordinateSystem]
    LOCATION_COORDINATE_SYSTEM_LAT_LNG_DEG_M: _ClassVar[LocationCoordinateSystem]
    LOCATION_COORDINATE_SYSTEM_LAT_LNG_RAD_M: _ClassVar[LocationCoordinateSystem]
    LOCATION_COORDINATE_SYSTEM_UTM_M: _ClassVar[LocationCoordinateSystem]

class LocationDatum(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LOCATION_DATUM_UNSPECIFIED: _ClassVar[LocationDatum]
    LOCATION_DATUM_WGS84_E: _ClassVar[LocationDatum]
    LOCATION_DATUM_WGS84_G: _ClassVar[LocationDatum]
LOCATION_COORDINATE_SYSTEM_UNSPECIFIED: LocationCoordinateSystem
LOCATION_COORDINATE_SYSTEM_LAT_LNG_DEG_M: LocationCoordinateSystem
LOCATION_COORDINATE_SYSTEM_LAT_LNG_RAD_M: LocationCoordinateSystem
LOCATION_COORDINATE_SYSTEM_UTM_M: LocationCoordinateSystem
LOCATION_DATUM_UNSPECIFIED: LocationDatum
LOCATION_DATUM_WGS84_E: LocationDatum
LOCATION_DATUM_WGS84_G: LocationDatum

class LocationList(_message.Message):
    __slots__ = ("locations",)
    LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    locations: _containers.RepeatedCompositeFieldContainer[Location]
    def __init__(self, locations: _Optional[_Iterable[_Union[Location, _Mapping]]] = ...) -> None: ...

class Location(_message.Message):
    __slots__ = ("x", "y", "z", "x_error", "y_error", "z_error", "coordinate_system", "datum", "utm_zone")
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    Z_FIELD_NUMBER: _ClassVar[int]
    X_ERROR_FIELD_NUMBER: _ClassVar[int]
    Y_ERROR_FIELD_NUMBER: _ClassVar[int]
    Z_ERROR_FIELD_NUMBER: _ClassVar[int]
    COORDINATE_SYSTEM_FIELD_NUMBER: _ClassVar[int]
    DATUM_FIELD_NUMBER: _ClassVar[int]
    UTM_ZONE_FIELD_NUMBER: _ClassVar[int]
    x: float
    y: float
    z: float
    x_error: float
    y_error: float
    z_error: float
    coordinate_system: LocationCoordinateSystem
    datum: LocationDatum
    utm_zone: str
    def __init__(self, x: _Optional[float] = ..., y: _Optional[float] = ..., z: _Optional[float] = ..., x_error: _Optional[float] = ..., y_error: _Optional[float] = ..., z_error: _Optional[float] = ..., coordinate_system: _Optional[_Union[LocationCoordinateSystem, str]] = ..., datum: _Optional[_Union[LocationDatum, str]] = ..., utm_zone: _Optional[str] = ...) -> None: ...
