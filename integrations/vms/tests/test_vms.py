import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from tests.conftest import _no_exporters_left_over   # noqa: F401 (autouse fixture)
from modules import test_objects
from integrations.vms import vms_alarms, vms_export

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from milestone_mock import Mock   # noqa: E402

CENTER = (52.22, 6.89)
T0 = 1_760_000_000.0


class FakeClock:
    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t


# --- alarm logic ------------------------------------------------------------------------------

def test_drone_detected_update_lost():
    clock = FakeClock(100.0)
    alarms = vms_alarms.Alarms(lost_s=30, update_s=10, clock=clock)
    events = alarms.drone(test_objects.drone(T0, CENTER))
    assert [e['event'] for e in events] == ['drone_detected']
    e = events[0]
    assert e['id'] == test_objects.TEST + '-DRONE' and e['sensor'] == test_objects.TEST_SENSOR
    assert e['height'] == '120' and e['pilot_lat'] == '52.220000' and e['operator'] == test_objects.TEST + '-OPER'
    clock.t = 105.0
    assert alarms.drone(test_objects.drone(T0 + 5, CENTER)) == []
    clock.t = 110.0
    assert [e['event'] for e in alarms.drone(test_objects.drone(T0 + 10, CENTER))] == ['drone_update']
    clock.t = 139.0
    assert alarms.tick() == []
    clock.t = 140.0
    lost = alarms.tick()
    assert [e['event'] for e in lost] == ['drone_lost'] and lost[0]['duration'] == '10'
    assert alarms.active == 0
    assert [e['event'] for e in alarms.drone(test_objects.drone(T0, CENTER))] == ['drone_detected']


def test_aircraft_alarms_are_off_by_default():
    assert vms_alarms.Alarms().aircraft_message(test_objects.aircraft(T0, CENTER)) == []
    events = vms_alarms.Alarms(aircraft=True).aircraft_message(test_objects.aircraft(T0, CENTER))
    assert events[0]['event'] == 'aircraft_detected' and events[0]['callsign'] == test_objects.TEST


def test_sensor_status_changes():
    alarms = vms_alarms.Alarms()
    assert alarms.status({'sensor ID': 'ds1', 'status': 'normal'}) == []
    events = alarms.status({'sensor ID': 'ds1', 'status': 'degraded', 'status details': 'wlan1 stalled'})
    assert events[0]['event'] == 'sensor_degraded' and events[0]['details'] == 'wlan1 stalled'
    assert alarms.status({'sensor ID': 'ds1', 'status': 'degraded'}) == []
    assert alarms.status({'sensor ID': 'ds1', 'status': 'normal'})[0]['event'] == 'sensor_normal'
    assert alarms.status({'sensor ID': 'ds2', 'status': 'degraded'})[0]['event'] == 'sensor_degraded'
    assert alarms.status({'sensor ID': 'ds3', 'status': 'unknown'}) == []


def test_render_leaves_unknown_fields_empty():
    assert vms_alarms.render('{event} {id} {nope}', {'event': 'x', 'id': 'y'}) == 'x y '


# --- Milestone -----------------------------------------------------------------------------------

@pytest.fixture
def milestone():
    m = Mock(port=0, host='127.0.0.1', quiet=True)
    yield m
    m.close()


def wait_for(cond, timeout=2.0):
    import time
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.01)
    return False


@pytest.mark.parametrize('protocol', ['tcp', 'udp'])
def test_milestone_generic_events(milestone, protocol):
    backend = vms_export.MilestoneGenericEvents('127.0.0.1', milestone.port, protocol)
    exp = vms_export.VMSExporter([backend], start=False)
    exp.on_remote_id(test_objects.drone(T0, CENTER))
    exp.on_status({'sensor ID': 'ds1', 'status': 'degraded', 'status details': 'wlan1 stalled'})
    exp.drain()
    assert wait_for(lambda: len(milestone.messages) == 2)
    (p1, drone), (p2, sensor) = milestone.messages
    assert p1 == p2 == protocol
    assert drone.startswith('DRONESCOUT drone_detected id=DRONESCOUT-TEST-DRONE sensor=DRONESCOUT-TEST-SENSOR lat=')
    assert sensor == 'DRONESCOUT sensor_degraded id=ds1 sensor=ds1 wlan1 stalled'     # no empty key= pairs
    assert exp.sent == {'milestone': 2}
    exp.close()


def test_unreachable_vms_is_counted_not_raised():
    logs = []
    backend = vms_export.MilestoneGenericEvents('127.0.0.1', 9, 'tcp', timeout_s=1)
    exp = vms_export.VMSExporter([backend], start=False, log=logs.append)
    exp.on_remote_id(test_objects.drone(T0, CENTER))
    exp.drain()
    assert exp.failed == {'milestone': 1} and 'not sent' in logs[0]
    exp.close()


# --- Nx Witness (mock of the REST API as observed on Nx Witness 6.1.2) ---------------------------------

class NxMock(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(('127.0.0.1', 0), NxHandler)
        self.tokens = set()
        self.logins = 0
        self.events = []
        self.url = 'http://127.0.0.1:%d' % self.server_address[1]


class NxHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _reply(self, code, body=None):
        data = json.dumps(body).encode() if body is not None else b''
        self.send_response(code)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if self.path == '/rest/v3/login/sessions':
            if body == {'username': 'dronescout', 'password': 'pw'}:
                self.server.logins += 1
                token = 'vms-token-%d' % self.server.logins
                self.server.tokens.add(token)
                return self._reply(200, {'token': token, 'username': 'dronescout', 'expiresInS': 2592000})
            return self._reply(401, {'error': '2', 'errorString': 'Wrong password'})
        if self.path == '/rest/v4/events/generic':
            auth = self.headers.get('Authorization', '')
            if not auth.startswith('Bearer ') or auth[7:] not in self.server.tokens:
                return self._reply(401)
            if body.get('state', 'instant') not in ('instant', 'started', 'stopped'):
                return self._reply(422, {'error': '2', 'errorString': 'Invalid parameter `state`: '})
            self.server.events.append(body)
            return self._reply(200)
        self._reply(404)


@pytest.fixture
def nx():
    srv = NxMock()
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield srv
    srv.shutdown()
    srv.server_close()


def test_nx_prolonged_drone_event(nx):
    clock = FakeClock(0.0)
    backend = vms_export.NxWitnessGenericEvents(nx.url, 'dronescout', 'pw', camera_ids=['cam-1'])
    exp = vms_export.VMSExporter([backend], vms_alarms.Alarms(lost_s=30, clock=clock), start=False)
    exp.on_remote_id(test_objects.drone(T0, CENTER))
    clock.t = 31.0
    exp.tick()
    exp.drain()
    assert [(e['caption'], e['state']) for e in nx.events] == [
        ('Drone detected: DRONESCOUT-TEST-DRONE', 'started'), ('Drone gone: DRONESCOUT-TEST-DRONE', 'stopped')]
    assert nx.events[0]['source'] == 'DroneScout' and nx.events[0]['deviceIds'] == ['cam-1']
    assert 'seen by sensor DRONESCOUT-TEST-SENSOR' in nx.events[0]['description']
    assert nx.logins == 1
    exp.close()


def test_nx_logs_in_again_when_the_session_expires(nx):
    backend = vms_export.NxWitnessGenericEvents(nx.url, 'dronescout', 'pw')
    backend.send({'event': 'sensor_degraded', 'sensor': 'ds1'})
    nx.tokens.clear()                                  # server restarted / session expired
    backend.send({'event': 'sensor_normal', 'sensor': 'ds1'})
    assert nx.logins == 2 and len(nx.events) == 2 and nx.events[1]['state'] == 'instant'


def test_nx_wrong_password(nx):
    exp = vms_export.VMSExporter([vms_export.NxWitnessGenericEvents(nx.url, 'dronescout', 'wrong')],
                                 start=False, log=lambda *a: None)
    exp.on_remote_id(test_objects.drone(T0, CENTER))
    exp.drain()
    assert exp.failed == {'nx': 1} and nx.events == []
    exp.close()


def test_gateway_builds_backends_from_vms_config(monkeypatch, nx, milestone):
    from integrations.vms import vms_config, vms_gateway
    monkeypatch.setattr(vms_config, 'vms_milestone_host', '127.0.0.1', raising=False)
    monkeypatch.setattr(vms_config, 'vms_milestone_port', milestone.port)
    monkeypatch.setattr(vms_config, 'vms_nx_url', nx.url, raising=False)
    monkeypatch.setattr(vms_config, 'vms_nx_username', 'dronescout', raising=False)
    monkeypatch.setattr(vms_config, 'vms_nx_password', 'pw', raising=False)
    exp = vms_gateway.make_exporter(vms_config, start=False)
    try:
        assert [b.name for b in exp.backends] == ['milestone', 'nx'] and exp.alarms.lost_s == 30
        exp.on_remote_id(test_objects.drone(T0, CENTER))
        exp.drain()
        assert wait_for(lambda: len(milestone.messages) == 1) and len(nx.events) == 1
    finally:
        exp.close()


def test_no_backend_configured():
    with pytest.raises(ValueError):
        vms_export.VMSExporter([], start=False)
