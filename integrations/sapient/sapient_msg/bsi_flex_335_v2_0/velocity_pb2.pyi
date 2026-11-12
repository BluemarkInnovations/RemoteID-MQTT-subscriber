from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SpeedUnits(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SPEED_UNITS_UNSPECIFIED: _ClassVar[SpeedUnits]
    SPEED_UNITS_MS: _ClassVar[SpeedUnits]
    SPEED_UNITS_KPH: _ClassVar[SpeedUnits]
SPEED_UNITS_UNSPECIFIED: SpeedUnits
SPEED_UNITS_MS: SpeedUnits
SPEED_UNITS_KPH: SpeedUnits

class ENUVelocity(_message.Message):
    __slots__ = ("east_rate", "north_rate", "up_rate", "east_rate_error", "north_rate_error", "up_rate_error")
    EAST_RATE_FIELD_NUMBER: _ClassVar[int]
    NORTH_RATE_FIELD_NUMBER: _ClassVar[int]
    UP_RATE_FIELD_NUMBER: _ClassVar[int]
    EAST_RATE_ERROR_FIELD_NUMBER: _ClassVar[int]
    NORTH_RATE_ERROR_FIELD_NUMBER: _ClassVar[int]
    UP_RATE_ERROR_FIELD_NUMBER: _ClassVar[int]
    east_rate: float
    north_rate: float
    up_rate: float
    east_rate_error: float
    north_rate_error: float
    up_rate_error: float
    def __init__(self, east_rate: _Optional[float] = ..., north_rate: _Optional[float] = ..., up_rate: _Optional[float] = ..., east_rate_error: _Optional[float] = ..., north_rate_error: _Optional[float] = ..., up_rate_error: _Optional[float] = ...) -> None: ...

class ENUVelocityUnits(_message.Message):
    __slots__ = ("east_north_rate_units", "up_rate_units")
    EAST_NORTH_RATE_UNITS_FIELD_NUMBER: _ClassVar[int]
    UP_RATE_UNITS_FIELD_NUMBER: _ClassVar[int]
    east_north_rate_units: SpeedUnits
    up_rate_units: SpeedUnits
    def __init__(self, east_north_rate_units: _Optional[_Union[SpeedUnits, str]] = ..., up_rate_units: _Optional[_Union[SpeedUnits, str]] = ...) -> None: ...
