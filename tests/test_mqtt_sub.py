import base64
import csv
import json
import lzma

import pytest

import config
import mqtt_sub


class DummyClient:
    def __init__(self):
        self.on_message = None
        self.subscribed_topics = []

    def subscribe(self, topic):
        self.subscribed_topics.append(topic)


class DummyMsg:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload


@pytest.fixture
def dummy_client(monkeypatch):
    monkeypatch.setattr(config, 'print_messages', True, raising=False)
    monkeypatch.delattr(config, 'log_path', raising=False)
    monkeypatch.delattr(mqtt_sub, 'filename_rid', raising=False)
    monkeypatch.delattr(mqtt_sub, 'filename_aircraft', raising=False)
    client = DummyClient()
    mqtt_sub.subscribe(client)
    return client


def _send(client, obj_or_str, topic='dronescout/test'):
    if isinstance(obj_or_str, (dict, list)):
        payload = json.dumps(obj_or_str).encode('utf-8')
    else:
        payload = obj_or_str
    client.on_message(client, None, DummyMsg(topic, payload))


def _data_message(golden, **overrides):
    data = {
        'UASdata': base64.b64encode(golden.payload()).decode('ascii'),
        'raw': base64.b64encode(b'\x01\x02').decode('ascii'),
        'sensor ID': 'DS-TEST-1',
        'RSSI': -60,
        'channel': 6,
        'timestamp': 1700000000000,
        'MAC address': 'AA:BB:CC:DD:EE:FF',
        'type': 'WLAN Beacon',
    }
    data.update(overrides)
    return {'protocol': 1.0, 'data': data}


def _aircraft_message(**overrides):
    aircraft = {
        'raw': base64.b64encode(b'\x01\x02\x03').decode('ascii'),
        'sensor ID': 'DS-TEST-1',
        'timestamp': 1700000000000,
        'frequency MHz': 1090.0,
        'type': 'ADSB',
        'address': '4A1B2C',
        'address type': 0,
        'flight': 'KLM123',
        'emergency status': 0,
        'flight state': 1,
        'category': 7,
        'altitude': 3500,
        'altitude type': 1,
        'speed': 120.0,
        'track': 270.0,
        'vertical rate': 0.0,
        'message count': 42,
        'latitude': 52.3,
        'longitude': 4.9,
        'SBS': 'MSG,3,,,4A1B2C,,,,,,,,,,,,,,,0,0,0,0;',
    }
    aircraft.update(overrides)
    return {'protocol': 1.0, 'aircraft': aircraft}


def _status_message(sensor_id='DS-TEST-1', **overrides):
    status = {
        'sensor ID': sensor_id,
        'timestamp': 1700000000000,
        'firmware version': '20240927-1205',
        'model': 'ds230',
        'status': 'OK',
    }
    status.update(overrides)
    return {'protocol': 1.0, 'status': status}


def _location_message(**overrides):
    location = {
        'sensor ID': 'DS-TEST-1',
        'timestamp': 1700000000000,
        'latitude': 52.379189,
        'longitude': 4.899431,
        'altitude MSL': 12.3,
        'satellites': 11,
        'HDOP': 0.9,
    }
    location.update(overrides)
    return {'protocol': 1.0, 'location': location}


def _network_message(**overrides):
    network = {
        'sensor ID': 'DS-TEST-1',
        'timestamp': 1700000000000,
        'PLMN': '20404',
        'operator': 'KPN',
        'band': 20,
        'access technology': 'LTE',
        'RSSI': -80,
        'BER': 1,
    }
    network.update(overrides)
    return {'protocol': 1.0, 'mobile network': network}


def test_subscribe_registers_callback_and_topic(dummy_client):
    assert callable(dummy_client.on_message)
    assert dummy_client.subscribed_topics == [config.topic]


def test_data_message_decodes_open_drone_id(dummy_client, golden, capsys):
    _send(dummy_client, _data_message(golden))
    out = capsys.readouterr().out
    assert "Helicopter (or Multirotor)" in out  # BasicID0 UAType
    assert "Airborne" in out  # Location status


def test_data_message_writes_csv_when_log_path_set(dummy_client, golden, tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'log_path', str(tmp_path), raising=False)
    _send(dummy_client, _data_message(golden))

    csv_files = list(tmp_path.glob('*_remoteID_log.csv'))
    assert len(csv_files) == 1
    with open(csv_files[0], newline='', encoding='utf-8') as f:
        rows = list(csv.reader(f))
    assert len(rows) == 2  # header + one data row
    assert 'DS-TEST-1' in rows[1]


def test_aircraft_message_prints_and_does_not_crash(dummy_client, capsys):
    _send(dummy_client, _aircraft_message())
    out = capsys.readouterr().out
    assert "4A1B2C" in out
    assert "rotorcraft" in out


def test_status_message_prints_expected_fields(dummy_client, capsys):
    _send(dummy_client, _status_message())
    out = capsys.readouterr().out
    assert "status message" in out
    assert "ds230" in out


def test_location_message_prints_hdop_and_satellites(dummy_client, capsys):
    _send(dummy_client, _location_message())
    out = capsys.readouterr().out
    assert "location message" in out
    assert "11" in out  # satellites
    assert "0.9" in out  # HDOP


def test_mobile_network_message_prints_expected_fields(dummy_client, capsys):
    _send(dummy_client, _network_message())
    out = capsys.readouterr().out
    assert "mobile network message" in out
    assert "KPN" in out


def test_concatenated_json_objects_are_both_processed(dummy_client, capsys):
    raw = (json.dumps(_status_message(sensor_id='SENSOR-A')) +
           json.dumps(_status_message(sensor_id='SENSOR-B'))).encode('utf-8')
    _send(dummy_client, raw)
    out = capsys.readouterr().out
    assert "SENSOR-A" in out
    assert "SENSOR-B" in out


def test_lzma_compressed_payload_is_decompressed(dummy_client, capsys):
    raw = lzma.compress(json.dumps(_status_message()).encode('utf-8'))
    _send(dummy_client, raw)
    out = capsys.readouterr().out
    assert "status message" in out


def test_trailing_null_byte_is_stripped(dummy_client, capsys):
    raw = json.dumps(_status_message()).encode('utf-8') + b'\x00'
    _send(dummy_client, raw)
    out = capsys.readouterr().out
    assert "status message" in out


def test_malformed_payload_does_not_raise(dummy_client):
    _send(dummy_client, b'this is not json{{{')  # must not propagate an exception


def test_empty_payload_does_not_raise(dummy_client):
    _send(dummy_client, b'')
