import base64
import json
import math
import sys
import threading
import urllib.request
from pathlib import Path

import pytest

from tests.conftest import _no_exporters_left_over   # noqa: F401 (autouse fixture)
from modules import decode, test_objects
from integrations.rest import rest_export
from modules import json_events

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from example_receiver import Receiver   # noqa: E402

CENTER = (52.22, 6.89)
T0 = 1_760_000_000.0
TOKEN = 'secret-token'


class FakeClock:
    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t


@pytest.fixture
def receiver():
    srv = Receiver(('127.0.0.1', 0), rest_export.auth_headers('bearer', token=TOKEN), quiet=True)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    srv.url = 'http://127.0.0.1:%d/dronescout/events' % srv.server_address[1]
    yield srv
    srv.shutdown()
    srv.server_close()


def exporter(url, headers=None, **kw):
    kw.setdefault('min_interval_s', 0)
    kw.setdefault('log', lambda *a: None)
    return rest_export.RESTExporter(url, headers=headers if headers is not None
                                    else rest_export.auth_headers('bearer', token=TOKEN), start=False, **kw)


def all_values(d):
    for v in d.values():
        if isinstance(v, dict):
            yield from all_values(v)
        else:
            yield v


# --- auth headers ------------------------------------------------------------------------

def test_auth_headers():
    assert rest_export.auth_headers(None) == {}
    assert rest_export.auth_headers('bearer', token='abc') == {'Authorization': 'Bearer abc'}
    basic = rest_export.auth_headers('basic', username='user', password='p:w')
    assert base64.b64decode(basic['Authorization'].split()[1]) == b'user:p:w'
    assert rest_export.auth_headers('header', header_name='X-API-Key', header_value='k') == {'X-API-Key': 'k'}


@pytest.mark.parametrize('kw', [dict(auth='bearer'), dict(auth='basic', username='u'),
                                dict(auth='header', header_value='v'), dict(auth='digest')])
def test_auth_headers_incomplete(kw):
    with pytest.raises(ValueError):
        rest_export.auth_headers(**kw)


def test_url_must_be_http():
    with pytest.raises(ValueError):
        rest_export.RESTExporter('ftp://example.org/', start=False)


# --- mapping -----------------------------------------------------------------------------

def test_drone_event():
    e = json_events.drone_event(test_objects.drone(T0, CENTER))
    assert e['type'] == 'drone'
    assert e['id'] == test_objects.TEST + '-DRONE'
    assert e['sensor_id'] == test_objects.TEST_SENSOR
    assert e['time'].endswith('Z') and e['time'].startswith('2025-10-09T')
    assert e['height_m'] == 120.0 and e['height_reference'] == 'ground'
    assert e['horizontal_accuracy_m'] == 10.0
    assert e['operator']['id'] == test_objects.TEST + '-OPER'
    assert e['self_id'] == 'TEST DATA'
    assert math.hypot(e['latitude'] - CENTER[0], e['longitude'] - CENTER[1]) < 0.01
    json.dumps(e, allow_nan=False)                      # valid JSON: no NaN anywhere


def test_drone_event_leaves_out_unknown_values():
    msg = test_objects.drone(T0, CENTER)
    loc = msg.uas.location
    loc.altitude_baro = loc.speed_vertical = loc.direction = float('nan')
    msg.uas.system.valid = False
    msg.uas.location.valid = False
    e = json_events.drone_event(msg)
    assert 'latitude' not in e and 'altitude_pressure_m' not in e
    assert 'operator' in e and 'latitude' not in e['operator']     # operator ID only
    assert None not in all_values(e) and '' not in all_values(e)
    json.dumps(e, allow_nan=False)


def test_aircraft_event():
    e = json_events.aircraft_event(test_objects.aircraft(T0, CENTER))
    assert e['id'] == 'ADSB-' + test_objects.TEST_AIRCRAFT_ADDRESS
    assert e['callsign'] == test_objects.TEST
    assert e['altitude_m'] == 900 and e['speed_mps'] == 60 and e['on_ground'] is False
    assert e['emergency'] is False


def test_aircraft_without_position():
    msg = decode.decode_aircraft({'sensor ID': 's1', 'timestamp': 1, 'type': 'ADSB', 'address': 'abc123',
                                  'latitude': 0, 'longitude': 0, 'raw': ''})
    e = json_events.aircraft_event(msg)
    assert e['address'] == 'ABC123' and 'latitude' not in e and 'longitude' not in e


def test_status_and_location_events():
    location, status = test_objects.sensor(T0, CENTER)
    s = json_events.status_event(status)
    assert s == {'type': 'sensor_status', 'sensor_id': test_objects.TEST_SENSOR, 'time': s['time'],
                 'status': 'normal', 'model': 'TEST DATA', 'firmware_version': 'test'}
    loc = json_events.location_event(location)
    assert loc['type'] == 'sensor_location' and loc['altitude_msl_m'] == 10


# --- exporter against the example receiver ------------------------------------------------

def feed(exp, t=T0):
    location, status = test_objects.sensor(t, CENTER)
    exp.on_location(location)
    exp.on_status(status)
    exp.on_remote_id(test_objects.drone(t, CENTER))
    exp.on_aircraft(test_objects.aircraft(t, CENTER))


def test_post_with_bearer_token(receiver):
    exp = exporter(receiver.url, source='test-gw')
    feed(exp)
    assert exp.flush() == 4
    assert exp.last_status == 204 and exp.requests == 1
    body = receiver.posts[0]
    assert body['schema_version'] == json_events.SCHEMA_VERSION and body['source'] == 'test-gw'
    assert [e['type'] for e in body['events']] == ['sensor_location', 'sensor_status', 'drone', 'aircraft']
    exp.close()


@pytest.mark.parametrize('auth', ['basic', 'header'])
def test_post_with_basic_and_api_key(auth):
    headers = rest_export.auth_headers(auth, username='u', password='pw', header_name='X-API-Key', header_value='k')
    srv = Receiver(('127.0.0.1', 0), headers, quiet=True)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        exp = exporter('http://127.0.0.1:%d/dronescout/events' % srv.server_address[1], headers=headers)
        feed(exp)
        assert exp.flush() == 4 and len(srv.events) == 4
        exp.close()
    finally:
        srv.shutdown()
        srv.server_close()


@pytest.mark.parametrize('headers', [{}, {'Authorization': 'Bearer wrong'}])
def test_wrong_credentials_are_dropped_not_retried(receiver, headers):
    logs = []
    exp = exporter(receiver.url, headers=headers, log=logs.append)
    feed(exp)
    assert exp.flush() == 0
    assert exp.last_status == 401 and exp.dropped == 4 and receiver.rejected == 1
    assert 'check the credentials' in logs[0]
    assert exp.flush() == 0 and exp.requests == 1            # nothing left to retry
    exp.close()


def test_receiver_rejects_bad_requests(receiver):
    def post(body, path='/dronescout/events'):
        req = urllib.request.Request(receiver.url.rsplit('/dronescout', 1)[0] + path, data=body, method='POST',
                                     headers={'Authorization': 'Bearer ' + TOKEN})
        try:
            return urllib.request.urlopen(req, timeout=5).status
        except urllib.error.HTTPError as e:
            return e.code
    assert post(b'not json') == 400
    assert post(b'{"events": 1}') == 400
    assert post(b'{"events": []}', path='/other') == 404
    assert post(b'{"events": []}') == 204


def test_server_errors_are_retried_with_backoff(receiver):
    clock = FakeClock(100.0)
    exp = exporter(receiver.url, clock=clock)
    receiver.status = 503
    feed(exp)
    assert exp.flush() == 0 and exp.dropped == 0 and len(exp._queue) == 4
    assert exp._next_attempt == 101.0
    assert exp.flush() == 0 and exp.requests == 1             # backing off
    clock.t = 101.0
    exp.flush()
    assert exp._next_attempt == 103.0                         # 2 s
    receiver.status = None
    clock.t = 103.0
    feed(exp)
    assert exp.flush() == 8
    assert [e['type'] for e in receiver.events][:4] == ['sensor_location', 'sensor_status', 'drone', 'aircraft']
    assert exp._backoff_s == 0
    exp.close()


def test_unreachable_endpoint_keeps_newest_events():
    exp = exporter('http://127.0.0.1:9/dronescout/events', max_queue=6, timeout_s=1, clock=FakeClock())
    feed(exp)
    feed(exp, T0 + 1)
    assert exp.dropped == 2 and len(exp._queue) == 6
    assert exp.flush() == 0 and exp.failed_requests == 1 and len(exp._queue) == 6
    exp.close()


def test_batches_and_rate_limit(receiver):
    clock = FakeClock(0.0)
    exp = exporter(receiver.url, max_batch=3, min_interval_s=1.0, clock=clock)
    for i in range(5):
        clock.t = i * 0.3                                     # 0, 0.3, 0.6, 0.9, 1.2 s
        exp.on_remote_id(test_objects.drone(T0 + i, CENTER))
    assert exp.rate_limited == 3                              # 0.3, 0.6 and 0.9 s
    for i in range(4):
        exp.on_status({'sensor ID': 's%d' % i, 'status': 'normal'})   # status: no rate limit
    assert exp.flush() == 6 and exp.requests == 2
    assert [len(p['events']) for p in receiver.posts] == [3, 3]
    exp.close()


def test_background_thread_posts(receiver):
    exp = rest_export.RESTExporter(receiver.url, headers=rest_export.auth_headers('bearer', token=TOKEN),
                                   interval_s=0.05, min_interval_s=0, log=lambda *a: None)
    feed(exp)
    exp.close()                                               # sends what is left
    assert len(receiver.events) == 4


def test_gateway_builds_exporter_from_rest_config(monkeypatch, receiver):
    from integrations.rest import rest_config, rest_gateway
    monkeypatch.setattr(rest_config, 'rest_url', receiver.url)
    monkeypatch.setattr(rest_config, 'rest_token', TOKEN)
    monkeypatch.setattr(rest_config, 'rest_send_aircraft', False)
    monkeypatch.setattr(rest_config, 'rest_extra_headers', {'X-Site': 'test'}, raising=False)
    exp = rest_gateway.make_exporter(rest_config, start=False)
    try:
        assert exp.headers['Authorization'] == 'Bearer ' + TOKEN and exp.headers['X-Site'] == 'test'
        assert exp.send_aircraft is False
        feed(exp)
        assert exp.flush() == 3
    finally:
        exp.close()
