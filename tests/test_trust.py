import base64
import json

import pytest

import config
import mqtt_sub
from modules import trust


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


def _ble_adv_raw(lat_e7=465000000):
    # Bluetooth advertising data holding one Location/Vector message,
    # as received on the air interface.
    loc = bytes([0x12, 0x20, 0, 0, 0]) + lat_e7.to_bytes(4, "little", signed=True)
    loc += (85000000).to_bytes(4, "little", signed=True) + bytes(12)
    return bytes([30, 0x16, 0xFA, 0xFF, 0x0D, 1]) + loc


@pytest.fixture
def trust_client(monkeypatch):
    pytest.importorskip("remoteid")
    monkeypatch.setattr(config, 'print_messages', True, raising=False)
    monkeypatch.setattr(config, 'trust_verdicts', True, raising=False)
    monkeypatch.delattr(config, 'log_path', raising=False)
    monkeypatch.delattr(mqtt_sub, 'filename_rid', raising=False)
    monkeypatch.delattr(mqtt_sub, 'filename_aircraft', raising=False)
    monkeypatch.setattr(trust, 'tracker', None)  # fresh tracker per test
    client = DummyClient()
    mqtt_sub.subscribe(client)
    return client


def _send(client, obj):
    payload = json.dumps(obj).encode('utf-8')
    client.on_message(client, None, DummyMsg('dronescout/test', payload))


def _data_message(golden, mac, raw):
    data = {
        'UASdata': base64.b64encode(golden.payload()).decode('ascii'),
        'raw': base64.b64encode(raw).decode('ascii'),
        'sensor ID': 'DS-TEST-1',
        'RSSI': -60,
        'channel': 6,
        'timestamp': 1700000000000,
        'MAC address': mac,
        'type': 'BT4 Legacy',
    }
    return {'protocol': 1.0, 'data': data}


def test_trust_verdict_printed_for_raw_payload(trust_client, golden, capsys):
    _send(trust_client, _data_message(golden, 'AA:BB:CC:DD:EE:01', _ble_adv_raw()))
    out = capsys.readouterr().out
    assert "trust verdict" in out
    assert "plausible" in out


def test_no_verdict_without_raw_field(trust_client, golden, capsys):
    msg = _data_message(golden, 'AA:BB:CC:DD:EE:02', _ble_adv_raw())
    del msg['data']['raw']
    _send(trust_client, msg)
    out = capsys.readouterr().out
    assert "trust verdict" not in out


def test_garbage_raw_does_not_raise_and_prints_nothing(trust_client, golden, capsys):
    _send(trust_client, _data_message(golden, 'AA:BB:CC:DD:EE:03', b'\x01\x02'))
    out = capsys.readouterr().out
    assert "trust verdict" not in out


def test_verdict_is_none_when_remoteid_not_installed(golden, monkeypatch):
    monkeypatch.setattr(trust, 'remoteid', None)
    data = _data_message(golden, 'AA:BB:CC:DD:EE:04', _ble_adv_raw())['data']
    assert trust.verdict(data) is None
