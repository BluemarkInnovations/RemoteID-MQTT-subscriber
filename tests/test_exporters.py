import base64
import json
import threading

import pytest

import config
import mqtt_sub
from modules import exporters


class Recorder(exporters.Exporter):
    name = "recorder"

    def __init__(self):
        self.events = []
        self.closed = False

    def on_remote_id(self, msg):
        self.events.append(('remote_id', msg))

    def on_aircraft(self, msg):
        self.events.append(('aircraft', msg))

    def on_status(self, status_json):
        self.events.append(('status', status_json))

    def on_location(self, location_json):
        self.events.append(('location', location_json))

    def close(self):
        self.closed = True


class Failing(exporters.Exporter):
    name = "failing"

    def on_aircraft(self, msg):
        raise RuntimeError("broken output")


def test_publish_without_exporters_does_nothing():
    exporters.publish('on_aircraft', object())


def test_unknown_event_raises():
    with pytest.raises(ValueError):
        exporters.publish('on_something', object())


def test_base_exporter_ignores_all_events():
    exporters.register(exporters.Exporter(), threaded=False)
    for event in exporters.EVENTS:
        exporters.publish(event, object())


def test_synchronous_exporter_gets_every_event():
    rec = exporters.register(Recorder(), threaded=False)
    exporters.publish('on_remote_id', 1)
    exporters.publish('on_aircraft', 2)
    exporters.publish('on_status', 3)
    exporters.publish('on_location', 4)
    assert rec.events == [('remote_id', 1), ('aircraft', 2), ('status', 3), ('location', 4)]
    assert exporters.registered() == [rec]


def test_threaded_exporter_runs_in_its_own_thread():
    seen = []

    class ThreadCheck(exporters.Exporter):
        name = "thread-check"

        def on_aircraft(self, msg):
            seen.append((msg, threading.current_thread().name))

    exporters.register(ThreadCheck())
    for i in range(50):
        exporters.publish('on_aircraft', i)
    exporters.flush()
    assert [m for m, _ in seen] == list(range(50))       # in order
    assert all(name == "export-thread-check" for _, name in seen)


def test_failing_exporter_does_not_stop_others(capsys):
    exporters.register(Failing(), threaded=False)
    rec = exporters.register(Recorder(), threaded=False)
    exporters.publish('on_aircraft', 'a')
    assert rec.events == [('aircraft', 'a')]
    assert "exporter failing: error in on_aircraft" in capsys.readouterr().out


def test_failing_threaded_exporter_keeps_running(capsys):
    calls = []

    class FailsOnce(exporters.Exporter):
        name = "fails-once"

        def on_aircraft(self, msg):
            calls.append(msg)
            if msg == 0:
                raise RuntimeError("first one fails")

    exporters.register(FailsOnce())
    exporters.publish('on_aircraft', 0)
    exporters.publish('on_aircraft', 1)
    exporters.flush()
    assert calls == [0, 1]


def test_full_queue_drops_and_counts(capsys):
    release = threading.Event()
    started = threading.Event()

    class Slow(exporters.Exporter):
        name = "slow"

        def on_aircraft(self, msg):
            started.set()
            release.wait(5)

    exporters.register(Slow(), queue_size=2)
    exporters.publish('on_aircraft', 0)      # taken by the thread, which then blocks
    assert started.wait(5)
    for i in range(1, 6):                    # 2 fit in the queue, 3 are dropped
        exporters.publish('on_aircraft', i)
    assert exporters.dropped() == {"slow": 3}
    assert "exporter slow: queue full" in capsys.readouterr().out
    release.set()
    exporters.flush()


def test_shutdown_closes_and_forgets_exporters():
    sync = exporters.register(Recorder(), threaded=False)
    threaded = exporters.register(Recorder())
    exporters.shutdown()
    assert sync.closed and threaded.closed
    assert exporters.registered() == []


# --- mqtt_sub hands decoded messages to the exporters -----------------------

class DummyClient:
    def subscribe(self, topic):
        pass


class DummyMsg:
    def __init__(self, payload):
        self.topic = 'dronescout/test'
        self.payload = payload


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(config, 'print_messages', False, raising=False)
    monkeypatch.delattr(config, 'log_path', raising=False)
    c = DummyClient()
    mqtt_sub.subscribe(c)
    return c


def _send(client, *objs):
    payload = ''.join(json.dumps(o) for o in objs).encode('utf-8')
    client.on_message(client, None, DummyMsg(payload))


def _data(golden):
    return {'protocol': 1.0, 'data': {
        'UASdata': base64.b64encode(golden.payload()).decode('ascii'),
        'sensor ID': 'DS-TEST-1', 'RSSI': -60, 'channel': 6, 'timestamp': 1700000000000,
        'MAC address': 'AA:BB:CC:DD:EE:FF', 'type': 'WLAN Beacon'}}


def _aircraft():
    return {'protocol': 1.0, 'aircraft': {
        'raw': base64.b64encode(b'\x8d\x48\x49\xeb\x00').decode('ascii'),
        'sensor ID': 'DS-TEST-1', 'timestamp': 1700000000000, 'address': '4849EB',
        'flight state': 1, 'SBS': 'MSG,1,x;MSG,3,y\n'}}


def test_on_message_publishes_decoded_remote_id(client, golden):
    rec = exporters.register(Recorder(), threaded=False)
    _send(client, _data(golden))
    [(kind, msg)] = rec.events
    assert kind == 'remote_id'
    assert msg.sensor_id == 'DS-TEST-1'
    assert msg.uas.location.latitude == pytest.approx(golden.latitude)


def test_on_message_publishes_decoded_aircraft(client):
    rec = exporters.register(Recorder(), threaded=False)
    _send(client, _aircraft())
    [(kind, msg)] = rec.events
    assert kind == 'aircraft'
    assert msg.raw == b'\x8d\x48\x49\xeb'
    assert msg.sbs_lines == ['MSG,1,x', 'MSG,3,y']
    assert msg.get('flight state') == 1


def test_on_message_publishes_status_and_location(client):
    rec = exporters.register(Recorder(), threaded=False)
    status = {'sensor ID': 'DS-TEST-1', 'timestamp': 1700000000000, 'status': 'OK'}
    location = {'sensor ID': 'DS-TEST-1', 'timestamp': 1700000000000, 'latitude': 52.0, 'longitude': 6.0}
    _send(client, {'protocol': 1.0, 'status': status}, {'protocol': 1.0, 'location': location})
    assert rec.events == [('status', status), ('location', location)]


def test_concatenated_messages_all_reach_exporter(client, golden):
    rec = exporters.register(Recorder(), threaded=False)
    _send(client, _data(golden), _aircraft(), _data(golden))
    assert [k for k, _ in rec.events] == ['remote_id', 'aircraft', 'remote_id']


def test_broken_uasdata_is_not_published(client):
    rec = exporters.register(Recorder(), threaded=False)
    _send(client, {'protocol': 1.0, 'data': {'UASdata': base64.b64encode(b'short').decode('ascii'),
                                             'timestamp': 1700000000000}})
    assert rec.events == []


def test_exporter_still_called_when_csv_logging_fails(client, golden, monkeypatch, tmp_path):
    # log_path points to a missing folder: the CSV write fails, the export must still happen
    monkeypatch.setattr(config, 'log_path', str(tmp_path / 'missing'), raising=False)
    monkeypatch.delattr(mqtt_sub, 'filename_rid', raising=False)
    monkeypatch.delattr(mqtt_sub, 'filename_aircraft', raising=False)
    rec = exporters.register(Recorder(), threaded=False)
    _send(client, _data(golden), _aircraft())
    assert [k for k, _ in rec.events] == ['remote_id', 'aircraft']


def test_setup_exporters_registers_sbs_only_when_configured(monkeypatch):
    monkeypatch.delattr(config, 'sbs_server_ip_address', raising=False)
    monkeypatch.delattr(config, 'sbs_server_port', raising=False)
    mqtt_sub.setup_exporters(threaded=False, start_sbs_listener=False)
    assert exporters.registered() == []

    monkeypatch.setattr(config, 'sbs_server_ip_address', '127.0.0.1', raising=False)
    monkeypatch.setattr(config, 'sbs_server_port', 30003, raising=False)
    mqtt_sub.setup_exporters(threaded=False, start_sbs_listener=False)
    assert [e.name for e in exporters.registered()] == ['sbs']
