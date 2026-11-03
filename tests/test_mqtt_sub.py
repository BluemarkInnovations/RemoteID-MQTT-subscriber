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
    monkeypatch.delattr(mqtt_sub, 'filename_wlan_stats', raising=False)
    monkeypatch.delattr(mqtt_sub, 'filename_status', raising=False)
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


def _wlan_stats_message(**overrides):
    stats = {
        'sensor ID': 'DS-TEST-1',
        'timestamp': 1700000300000,
        'interface': 'wlan1',
        'interval s': 300,
        'start timestamp': 1700000000000,
        'radio state': 'ok',
        'last packet s ago': 0,
        'summary': {
            'busiest channel': 6,
            'busiest channel utilization %': 37.5,
            'channel 6 utilization %': 37.5,
            'channel 6 weakest signal dBm': -88,
            'channel 6 noise rise dB': 4.2,
            'congestion': 'medium',
        },
        'channels': [
            {'channel': 1, 'frequency MHz': 2412, 'dwell ms': 20000, 'packets': 812, 'packets total': 9120,
             'bad FCS packets': 31, 'strong packets': 40, 'networks': 7, 'utilization %': 12.1},
            {'channel': 6, 'frequency MHz': 2437, 'dwell ms': 150000, 'packets': 15200, 'packets total': 140000,
             'bad FCS packets': 420, 'strong packets': 900, 'networks': 14, 'weakest signal dBm': -88,
             'median signal dBm': -71, 'utilization %': 37.5, 'noise dBm': -90.8, 'noise min dBm': -93,
             'noise max dBm': -84, 'noise rise dB': 4.2},
        ],
        'hardware': {'IGI avg': 32.5, 'IGI max': 40, 'OFDM false alarms per s': 120.3,
                     'CCK false alarms per s': 15.0, 'CRC errors per s': 3.1, 'PHY error rate %': 35.3,
                     'OFDM false alarm ratio %': 97.6, 'samples': 60},
        'LTE': {'band': 40, 'RSSI dBm': -65, 'adjacent band': True},
    }
    stats.update(overrides)
    return {'protocol': 1.0, 'wlan statistics': stats}


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


def test_aircraft_message_prints_rssi(dummy_client, capsys):
    _send(dummy_client, _aircraft_message(RSSI=-82.5))
    out = capsys.readouterr().out
    assert "-82.5" in out


def test_aircraft_message_without_rssi_prints_na(dummy_client, capsys):
    _send(dummy_client, _aircraft_message())  # auto gain: no RSSI field
    out = capsys.readouterr().out
    assert "RSSI [dBm]........... N/A" in out


def test_aircraft_message_writes_rssi_to_csv(dummy_client, tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'log_path', str(tmp_path), raising=False)
    _send(dummy_client, _aircraft_message(RSSI=-82.5))
    _send(dummy_client, _aircraft_message())

    csv_files = list(tmp_path.glob('*_aircraft_log.csv'))
    with open(csv_files[0], newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    assert [row['RSSI'] for row in rows] == ['-82.5', '']


def test_status_message_prints_expected_fields(dummy_client, capsys):
    _send(dummy_client, _status_message())
    out = capsys.readouterr().out
    assert "status message" in out
    assert "ds230" in out


def test_status_message_prints_degraded_receivers(dummy_client, capsys):
    _send(dummy_client, _status_message(**{
        'status': 'degraded',
        'status details': 'UAT 978 SDR missing: no SDR found; WLAN wlan2 stalled',
        'WLAN radios': {'wlan1': 'ok', 'wlan2': 'stalled'},
        'SDR receivers': {'1090': 'ok', '978': 'missing'},
    }))
    out = capsys.readouterr().out
    assert "degraded" in out
    assert "UAT 978 SDR missing: no SDR found" in out
    assert "wlan2: stalled" in out
    assert "978 MHz: missing" in out


def test_status_message_without_receivers_prints_no_receiver_lines(dummy_client, capsys):
    _send(dummy_client, _status_message())
    out = capsys.readouterr().out
    assert "WLAN radios" not in out
    assert "SDR receivers" not in out
    assert "status details" not in out


def test_status_message_writes_csv_when_log_path_set(dummy_client, tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'log_path', str(tmp_path), raising=False)
    _send(dummy_client, _status_message())
    _send(dummy_client, _status_message(**{
        'status': 'degraded',
        'status details': 'WLAN wlan2 stalled',
        'WLAN radios': {'wlan1': 'ok', 'wlan2': 'stalled'},
        'SDR receivers': {'1090': 'ok'},
    }))

    csv_files = list(tmp_path.glob('*_status_log.csv'))
    assert len(csv_files) == 1
    with open(csv_files[0], newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    assert rows[0]['WLAN radios'] == ''
    assert rows[1]['status'] == 'degraded'
    assert rows[1]['status details'] == 'WLAN wlan2 stalled'
    assert json.loads(rows[1]['WLAN radios']) == {'wlan1': 'ok', 'wlan2': 'stalled'}
    assert json.loads(rows[1]['SDR receivers']) == {'1090': 'ok'}


def test_wlan_stats_message_prints_summary_and_channels(dummy_client, capsys):
    _send(dummy_client, _wlan_stats_message())
    out = capsys.readouterr().out
    assert "WLAN statistics message" in out
    assert "wlan1" in out
    assert "medium" in out  # congestion
    assert "2437" in out  # channel 6 row
    assert "120.3" in out  # OFDM false alarms
    assert "35.3" in out  # PHY error rate
    assert "97.6" in out  # OFDM false alarm ratio
    assert "adjacent to 2.4 GHz" in out


def test_wlan_stats_message_without_optional_fields_does_not_crash(dummy_client, capsys):
    msg = _wlan_stats_message(summary={}, channels=[])
    del msg['wlan statistics']['hardware']
    del msg['wlan statistics']['LTE']
    _send(dummy_client, msg)
    out = capsys.readouterr().out
    assert "WLAN statistics message" in out
    assert "N/A" in out


def test_wlan_stats_message_writes_one_csv_row_per_channel(dummy_client, tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'log_path', str(tmp_path), raising=False)
    _send(dummy_client, _wlan_stats_message())

    csv_files = list(tmp_path.glob('*_wlan_stats_log.csv'))
    assert len(csv_files) == 1
    with open(csv_files[0], newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    assert [row['channel'] for row in rows] == ['1', '6']
    assert rows[1]['noise rise dB'] == '4.2'
    assert rows[0]['congestion'] == 'medium'  # summary repeated on each row
    assert rows[0]['weakest signal dBm'] == ''  # left out by the sensor -> empty cell
    assert rows[0]['OFDM false alarm ratio %'] == '97.6'


def test_wlan_stats_message_lte_serving_and_neighbour_cells(dummy_client, tmp_path, monkeypatch, capsys):
    # modem camps on a sub-1 GHz cell, the band 7 layer of the mast only shows up as a neighbour
    lte = {'band': 20, 'RSSI dBm': -51, 'adjacent band': False, 'duplex': 'FDD', 'EARFCN': 6300, 'PCI': 123,
           'downlink MHz': 806, 'bandwidth MHz': 10, 'MHz from WLAN band': 1589, 'RSRP dBm': -95, 'RSRQ dB': -10,
           'downlink RSSI dBm': -65, 'neighbour cells': 4, 'adjacent band neighbour cells': 2,
           'strongest adjacent band neighbour': {'band': 7, 'EARFCN': 3350, 'PCI': 302, 'downlink MHz': 2680,
                                                 'MHz from WLAN band': 196.5, 'RSRP dBm': -59}}
    hardware = {'IGI avg': 37, 'IGI max': 42, 'IGI min': 34, 'samples': 5}
    monkeypatch.setattr(config, 'log_path', str(tmp_path), raising=False)
    _send(dummy_client, _wlan_stats_message(LTE=lte, hardware=hardware))
    out = capsys.readouterr().out
    assert "IGI min / avg / max.. 34 / 37 / 42" in out
    assert "EARFCN 6300" in out
    assert "2 in an adjacent band" in out
    assert "band 7 EARFCN 3350" in out

    csv_files = list(tmp_path.glob('*_wlan_stats_log.csv'))
    with open(csv_files[0], newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    assert rows[0]['IGI min'] == '34'
    assert rows[0]['LTE band'] == '20'
    assert rows[0]['LTE downlink RSSI dBm'] == '-65'
    assert rows[0]['LTE adjacent neighbour band'] == '7'
    assert rows[0]['LTE adjacent neighbour RSRP dBm'] == '-59'


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
