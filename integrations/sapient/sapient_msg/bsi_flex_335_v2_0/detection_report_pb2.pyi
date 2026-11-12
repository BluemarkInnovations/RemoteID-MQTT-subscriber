from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sapient_msg.bsi_flex_335_v2_0 import associated_file_pb2 as _associated_file_pb2
from sapient_msg.bsi_flex_335_v2_0 import associated_detection_pb2 as _associated_detection_pb2
from sapient_msg.bsi_flex_335_v2_0 import location_pb2 as _location_pb2
from sapient_msg.bsi_flex_335_v2_0 import range_bearing_pb2 as _range_bearing_pb2
from sapient_msg.bsi_flex_335_v2_0 import velocity_pb2 as _velocity_pb2
from sapient_msg import proto_options_pb2 as _proto_options_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DetectionReport(_message.Message):
    __slots__ = ("report_id", "object_id", "task_id", "state", "range_bearing", "location", "detection_confidence", "track_info", "prediction_location", "object_info", "classification", "behaviour", "associated_file", "signal", "associated_detection", "derived_detection", "enu_velocity", "colour", "id")
    class PredictedLocation(_message.Message):
        __slots__ = ("range_bearing", "location", "predicted_timestamp")
        RANGE_BEARING_FIELD_NUMBER: _ClassVar[int]
        LOCATION_FIELD_NUMBER: _ClassVar[int]
        PREDICTED_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
        range_bearing: _range_bearing_pb2.RangeBearing
        location: _location_pb2.Location
        predicted_timestamp: _timestamp_pb2.Timestamp
        def __init__(self, range_bearing: _Optional[_Union[_range_bearing_pb2.RangeBearing, _Mapping]] = ..., location: _Optional[_Union[_location_pb2.Location, _Mapping]] = ..., predicted_timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
    class TrackObjectInfo(_message.Message):
        __slots__ = ("type", "value", "error")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        ERROR_FIELD_NUMBER: _ClassVar[int]
        type: str
        value: str
        error: float
        def __init__(self, type: _Optional[str] = ..., value: _Optional[str] = ..., error: _Optional[float] = ...) -> None: ...
    class DetectionReportClassification(_message.Message):
        __slots__ = ("type", "confidence", "sub_class")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
        SUB_CLASS_FIELD_NUMBER: _ClassVar[int]
        type: str
        confidence: float
        sub_class: _containers.RepeatedCompositeFieldContainer[DetectionReport.SubClass]
        def __init__(self, type: _Optional[str] = ..., confidence: _Optional[float] = ..., sub_class: _Optional[_Iterable[_Union[DetectionReport.SubClass, _Mapping]]] = ...) -> None: ...
    class SubClass(_message.Message):
        __slots__ = ("type", "confidence", "level", "sub_class")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
        LEVEL_FIELD_NUMBER: _ClassVar[int]
        SUB_CLASS_FIELD_NUMBER: _ClassVar[int]
        type: str
        confidence: float
        level: int
        sub_class: _containers.RepeatedCompositeFieldContainer[DetectionReport.SubClass]
        def __init__(self, type: _Optional[str] = ..., confidence: _Optional[float] = ..., level: _Optional[int] = ..., sub_class: _Optional[_Iterable[_Union[DetectionReport.SubClass, _Mapping]]] = ...) -> None: ...
    class Behaviour(_message.Message):
        __slots__ = ("type", "confidence")
        TYPE_FIELD_NUMBER: _ClassVar[int]
        CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
        type: str
        confidence: float
        def __init__(self, type: _Optional[str] = ..., confidence: _Optional[float] = ...) -> None: ...
    class Signal(_message.Message):
        __slots__ = ("amplitude", "start_frequency", "centre_frequency", "stop_frequency", "pulse_duration")
        AMPLITUDE_FIELD_NUMBER: _ClassVar[int]
        START_FREQUENCY_FIELD_NUMBER: _ClassVar[int]
        CENTRE_FREQUENCY_FIELD_NUMBER: _ClassVar[int]
        STOP_FREQUENCY_FIELD_NUMBER: _ClassVar[int]
        PULSE_DURATION_FIELD_NUMBER: _ClassVar[int]
        amplitude: float
        start_frequency: float
        centre_frequency: float
        stop_frequency: float
        pulse_duration: float
        def __init__(self, amplitude: _Optional[float] = ..., start_frequency: _Optional[float] = ..., centre_frequency: _Optional[float] = ..., stop_frequency: _Optional[float] = ..., pulse_duration: _Optional[float] = ...) -> None: ...
    class DerivedDetection(_message.Message):
        __slots__ = ("timestamp", "node_id", "object_id")
        TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
        NODE_ID_FIELD_NUMBER: _ClassVar[int]
        OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
        timestamp: _timestamp_pb2.Timestamp
        node_id: str
        object_id: str
        def __init__(self, timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., node_id: _Optional[str] = ..., object_id: _Optional[str] = ...) -> None: ...
    REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    RANGE_BEARING_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    DETECTION_CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    TRACK_INFO_FIELD_NUMBER: _ClassVar[int]
    PREDICTION_LOCATION_FIELD_NUMBER: _ClassVar[int]
    OBJECT_INFO_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    BEHAVIOUR_FIELD_NUMBER: _ClassVar[int]
    ASSOCIATED_FILE_FIELD_NUMBER: _ClassVar[int]
    SIGNAL_FIELD_NUMBER: _ClassVar[int]
    ASSOCIATED_DETECTION_FIELD_NUMBER: _ClassVar[int]
    DERIVED_DETECTION_FIELD_NUMBER: _ClassVar[int]
    ENU_VELOCITY_FIELD_NUMBER: _ClassVar[int]
    COLOUR_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    report_id: str
    object_id: str
    task_id: str
    state: str
    range_bearing: _range_bearing_pb2.RangeBearing
    location: _location_pb2.Location
    detection_confidence: float
    track_info: _containers.RepeatedCompositeFieldContainer[DetectionReport.TrackObjectInfo]
    prediction_location: DetectionReport.PredictedLocation
    object_info: _containers.RepeatedCompositeFieldContainer[DetectionReport.TrackObjectInfo]
    classification: _containers.RepeatedCompositeFieldContainer[DetectionReport.DetectionReportClassification]
    behaviour: _containers.RepeatedCompositeFieldContainer[DetectionReport.Behaviour]
    associated_file: _containers.RepeatedCompositeFieldContainer[_associated_file_pb2.AssociatedFile]
    signal: _containers.RepeatedCompositeFieldContainer[DetectionReport.Signal]
    associated_detection: _containers.RepeatedCompositeFieldContainer[_associated_detection_pb2.AssociatedDetection]
    derived_detection: _containers.RepeatedCompositeFieldContainer[DetectionReport.DerivedDetection]
    enu_velocity: _velocity_pb2.ENUVelocity
    colour: str
    id: str
    def __init__(self, report_id: _Optional[str] = ..., object_id: _Optional[str] = ..., task_id: _Optional[str] = ..., state: _Optional[str] = ..., range_bearing: _Optional[_Union[_range_bearing_pb2.RangeBearing, _Mapping]] = ..., location: _Optional[_Union[_location_pb2.Location, _Mapping]] = ..., detection_confidence: _Optional[float] = ..., track_info: _Optional[_Iterable[_Union[DetectionReport.TrackObjectInfo, _Mapping]]] = ..., prediction_location: _Optional[_Union[DetectionReport.PredictedLocation, _Mapping]] = ..., object_info: _Optional[_Iterable[_Union[DetectionReport.TrackObjectInfo, _Mapping]]] = ..., classification: _Optional[_Iterable[_Union[DetectionReport.DetectionReportClassification, _Mapping]]] = ..., behaviour: _Optional[_Iterable[_Union[DetectionReport.Behaviour, _Mapping]]] = ..., associated_file: _Optional[_Iterable[_Union[_associated_file_pb2.AssociatedFile, _Mapping]]] = ..., signal: _Optional[_Iterable[_Union[DetectionReport.Signal, _Mapping]]] = ..., associated_detection: _Optional[_Iterable[_Union[_associated_detection_pb2.AssociatedDetection, _Mapping]]] = ..., derived_detection: _Optional[_Iterable[_Union[DetectionReport.DerivedDetection, _Mapping]]] = ..., enu_velocity: _Optional[_Union[_velocity_pb2.ENUVelocity, _Mapping]] = ..., colour: _Optional[str] = ..., id: _Optional[str] = ...) -> None: ...
