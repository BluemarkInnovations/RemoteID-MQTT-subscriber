from google.protobuf import timestamp_pb2 as _timestamp_pb2
from sapient_msg.bsi_flex_335_v2_0 import alert_pb2 as _alert_pb2
from sapient_msg.bsi_flex_335_v2_0 import alert_ack_pb2 as _alert_ack_pb2
from sapient_msg.bsi_flex_335_v2_0 import detection_report_pb2 as _detection_report_pb2
from sapient_msg.bsi_flex_335_v2_0 import error_pb2 as _error_pb2
from sapient_msg import proto_options_pb2 as _proto_options_pb2
from sapient_msg.bsi_flex_335_v2_0 import registration_pb2 as _registration_pb2
from sapient_msg.bsi_flex_335_v2_0 import registration_ack_pb2 as _registration_ack_pb2
from sapient_msg.bsi_flex_335_v2_0 import status_report_pb2 as _status_report_pb2
from sapient_msg.bsi_flex_335_v2_0 import task_pb2 as _task_pb2
from sapient_msg.bsi_flex_335_v2_0 import task_ack_pb2 as _task_ack_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SapientMessage(_message.Message):
    __slots__ = ("timestamp", "node_id", "destination_id", "registration", "registration_ack", "status_report", "detection_report", "task", "task_ack", "alert", "alert_ack", "error", "additional_information")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    DESTINATION_ID_FIELD_NUMBER: _ClassVar[int]
    REGISTRATION_FIELD_NUMBER: _ClassVar[int]
    REGISTRATION_ACK_FIELD_NUMBER: _ClassVar[int]
    STATUS_REPORT_FIELD_NUMBER: _ClassVar[int]
    DETECTION_REPORT_FIELD_NUMBER: _ClassVar[int]
    TASK_FIELD_NUMBER: _ClassVar[int]
    TASK_ACK_FIELD_NUMBER: _ClassVar[int]
    ALERT_FIELD_NUMBER: _ClassVar[int]
    ALERT_ACK_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ADDITIONAL_INFORMATION_FIELD_NUMBER: _ClassVar[int]
    timestamp: _timestamp_pb2.Timestamp
    node_id: str
    destination_id: str
    registration: _registration_pb2.Registration
    registration_ack: _registration_ack_pb2.RegistrationAck
    status_report: _status_report_pb2.StatusReport
    detection_report: _detection_report_pb2.DetectionReport
    task: _task_pb2.Task
    task_ack: _task_ack_pb2.TaskAck
    alert: _alert_pb2.Alert
    alert_ack: _alert_ack_pb2.AlertAck
    error: _error_pb2.Error
    additional_information: str
    def __init__(self, timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., node_id: _Optional[str] = ..., destination_id: _Optional[str] = ..., registration: _Optional[_Union[_registration_pb2.Registration, _Mapping]] = ..., registration_ack: _Optional[_Union[_registration_ack_pb2.RegistrationAck, _Mapping]] = ..., status_report: _Optional[_Union[_status_report_pb2.StatusReport, _Mapping]] = ..., detection_report: _Optional[_Union[_detection_report_pb2.DetectionReport, _Mapping]] = ..., task: _Optional[_Union[_task_pb2.Task, _Mapping]] = ..., task_ack: _Optional[_Union[_task_ack_pb2.TaskAck, _Mapping]] = ..., alert: _Optional[_Union[_alert_pb2.Alert, _Mapping]] = ..., alert_ack: _Optional[_Union[_alert_ack_pb2.AlertAck, _Mapping]] = ..., error: _Optional[_Union[_error_pb2.Error, _Mapping]] = ..., additional_information: _Optional[str] = ...) -> None: ...
