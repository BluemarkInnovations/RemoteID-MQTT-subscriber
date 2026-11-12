import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.conftest import make_rid_msg, _no_exporters_left_over, golden   # noqa: F401 (fixtures) (autouse fixture)
from modules import decode
from integrations.tak import tak_export

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from mock_tak_server import MockTakServer   # noqa: E402


@pytest.fixture(autouse=True)
def _no_presence_pause(monkeypatch):
    """The 1 s pause after the presence event is for FreeTAKServer; tests don't need it."""
    init = tak_export.TakClient.__init__

    def fast_init(self, *args, **kwargs):
        kwargs.setdefault('presence_settle_s', 0)
        init(self, *args, **kwargs)
    monkeypatch.setattr(tak_export.TakClient, '__init__', fast_init)


def wait_for(condition, timeout=5.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if condition():
            return True
        time.sleep(0.02)
    return False


def _aircraft(**overrides):
    a = {'raw': '', 'sensor ID': 'ds0948', 'timestamp': 1793712471226, 'frequency MHz': 1090,
         'type': 'ADSB', 'address': '4849EB', 'address type': 0, 'flight': 'PHTWK', 'category': 2,
         'altitude': 1402, 'speed': 64, 'track': 210, 'latitude': 52.16774116, 'longitude': 6.91573661}
    a.update(overrides)
    return decode.decode_aircraft(a)


def _logs():
    lines = []
    return lines, lines.append


# --- exporter (no network) -------------------------------------------------

class FakeClient:
    def __init__(self):
        self.sent = []
        self.closed = False

    def send(self, data):
        self.sent.append(data)
        return True

    def close(self):
        self.closed = True


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _uids(client):
    import xml.etree.ElementTree as ET
    return [ET.fromstring(d).get('uid') for d in client.sent]


def test_exporter_sends_drone_and_pilot(golden):
    c = FakeClient()
    exp = tak_export.TAKExporter(c)
    exp.on_remote_id(make_rid_msg({'timestamp': 1793712471000}, golden.payload()))
    assert len(c.sent) == 2
    assert _uids(c)[0].startswith('dronescout-rid-') and _uids(c)[1].startswith('dronescout-op-')


def test_exporter_rate_limit_per_object(golden):
    c, clock = FakeClient(), FakeClock()
    exp = tak_export.TAKExporter(c, min_interval_s=1.0, clock=clock)
    exp.on_aircraft(_aircraft())
    clock.t += 0.5
    exp.on_aircraft(_aircraft())                      # too soon: skipped
    exp.on_aircraft(_aircraft(address='4864EB'))      # another aircraft: sent
    clock.t += 0.6
    exp.on_aircraft(_aircraft())                      # 1.1 s after the first: sent
    assert _uids(c) == ['icao-4849EB', 'icao-4864EB', 'icao-4849EB']
    assert exp.rate_limited == 1


def test_exporter_options(golden):
    c = FakeClient()
    exp = tak_export.TAKExporter(c, send_aircraft=False, send_operator=False, send_sensors=False)
    exp.on_aircraft(_aircraft())
    exp.on_remote_id(make_rid_msg({'timestamp': 1793712471000}, golden.payload()))
    exp.on_status({'sensor ID': 'ds0948', 'status': 'OK'})
    exp.on_location({'sensor ID': 'ds0948', 'latitude': 52.0, 'longitude': 6.0})
    assert [u.split('-')[1] for u in _uids(c)] == ['rid']


def test_exporter_sensor_from_location_and_fixed_position():
    c = FakeClient()
    exp = tak_export.TAKExporter(c, sensor_positions={'fixed1': (52.0, 6.0, 10.0)})
    exp.on_status({'sensor ID': 'nogps', 'status': 'OK'})                  # no position yet: nothing
    exp.on_status({'sensor ID': 'fixed1', 'status': 'OK'})                 # fixed position from the config
    exp.on_location({'sensor ID': 'nogps', 'latitude': 0.0, 'longitude': 0.0})   # no fix: ignored
    exp.on_location({'sensor ID': 'nogps', 'latitude': 52.1, 'longitude': 6.1, 'altitude MSL': 12.0})
    exp.on_status({'sensor ID': 'nogps', 'status': 'degraded'})
    assert _uids(c) == ['dronescout-fixed1', 'dronescout-nogps', 'dronescout-nogps']


def test_exporter_close_closes_client():
    c = FakeClient()
    tak_export.TAKExporter(c).close()
    assert c.closed


@pytest.mark.parametrize("url", ['http://host:80', 'tls://host', 'host:8089', 'tcp://:8087'])
def test_bad_url_is_rejected(url):
    with pytest.raises(ValueError):
        tak_export.TakClient(url)


# --- TCP -------------------------------------------------------------------

@pytest.fixture
def tcp_server():
    srv = MockTakServer(port=0, use_tls=False, chatter_interval_s=0.05).start()
    yield srv
    srv.stop()


def test_tcp_events_arrive_and_server_traffic_is_read(tcp_server):
    logs, log = _logs()
    client = tak_export.TakClient('tcp://127.0.0.1:%d' % tcp_server.port, ping_interval_s=0, log=log)
    try:
        exp = tak_export.TAKExporter(client, min_interval_s=0)
        for _ in range(3):
            exp.on_aircraft(_aircraft())
            time.sleep(0.3)                 # the server keeps sending "other users" meanwhile
        assert wait_for(lambda: len(tcp_server.events) == 3)
        assert all(e.get('uid') == 'icao-4849EB' for _, e in tcp_server.events)
        assert client.sent == 3 and client.dropped == 0 and client.connects == 1
        # the gateway announced itself first, like a TAK client
        assert [e.get('uid') for _, e in tcp_server.contacts] == ['dronescout-gateway']
    finally:
        client.close()


def test_server_down_drops_without_raising():
    s = socket.socket(); s.bind(('127.0.0.1', 0)); port = s.getsockname()[1]; s.close()
    logs, log = _logs()
    client = tak_export.TakClient('tcp://127.0.0.1:%d' % port, ping_interval_s=0, log=log)
    try:
        assert client.send(b'<event/>') is False
        assert client.send(b'<event/>') is False          # within the backoff: no new attempt
        assert client.dropped == 2
        assert sum('failed' in line for line in logs) == 1
    finally:
        client.close()


def test_reconnects_after_server_drops_the_connection(tcp_server):
    logs, log = _logs()
    client = tak_export.TakClient('tcp://127.0.0.1:%d' % tcp_server.port, ping_interval_s=0, log=log)
    try:
        assert client.send(b'<event uid=\"a\"></event>')
        assert wait_for(lambda: len(tcp_server.events) == 1)
        tcp_server.disconnect_clients()
        assert wait_for(lambda: not client.connected, 3)    # noticed by the 1 s read loop
        assert wait_for(lambda: client.send(b'<event uid=\"b\"></event>'), 5)   # after the backoff
        assert wait_for(lambda: len(tcp_server.events) == 2)
        assert client.connects == 2
        assert any('connection lost' in line for line in logs)
    finally:
        client.close()


def test_presence_is_sent_on_every_connection(tcp_server):
    client = tak_export.TakClient('tcp://127.0.0.1:%d' % tcp_server.port, ping_interval_s=0,
                                  ping_uid='gw1', presence_callsign='GW one',
                                  presence_position=(52.1, 6.9, 12.0), log=lambda *a: None)
    try:
        assert client.send(b'<event uid=\"a\"></event>')
        assert wait_for(lambda: len(tcp_server.contacts) == 1)
        tcp_server.disconnect_clients()
        assert wait_for(lambda: not client.connected, 3)
        assert wait_for(lambda: client.send(b'<event uid=\"b\"></event>'), 5)
        assert wait_for(lambda: len(tcp_server.contacts) == 2)
        e = tcp_server.contacts[0][1]
        assert e.get('uid') == 'gw1' and e.get('type') == 'a-f-G-U-C'
        assert e.find('detail/contact').get('callsign') == 'GW one'
        assert e.find('point').get('lat') == '52.1'
    finally:
        client.close()


def test_no_presence_when_disabled(tcp_server):
    client = tak_export.TakClient('tcp://127.0.0.1:%d' % tcp_server.port, ping_interval_s=0,
                                  presence=False, log=lambda *a: None)
    try:
        assert client.send(b'<event uid=\"a\"></event>')
        assert wait_for(lambda: len(tcp_server.events) == 1)
        assert tcp_server.contacts == []
    finally:
        client.close()


def test_ping_when_idle(tcp_server):
    client = tak_export.TakClient('tcp://127.0.0.1:%d' % tcp_server.port, ping_interval_s=1, log=lambda *a: None)
    try:
        assert client.send(b'<event uid=\"first\"></event>')
        assert wait_for(lambda: any(e.get('type') == 't-x-c-t' for _, e in tcp_server.events), 4)
    finally:
        client.close()


# --- UDP -------------------------------------------------------------------

def test_udp():
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.bind(('127.0.0.1', 0))
    rx.settimeout(2)
    client = tak_export.TakClient('udp://127.0.0.1:%d' % rx.getsockname()[1], log=lambda *a: None)
    try:
        assert client.send(b'<event uid=\"udp\"></event>')
        assert rx.recv(1000) == b'<event uid=\"udp\"></event>'
    finally:
        client.close()
        rx.close()


# --- TLS with client certificates ------------------------------------------

@pytest.fixture(scope='module')
def certs(tmp_path_factory):
    if shutil.which('openssl') is None:
        pytest.skip("openssl not installed")
    out = tmp_path_factory.mktemp('certs')
    subprocess.run(['sh', str(ROOT / 'tools' / 'make_test_certs.sh'), str(out)], check=True,
                   stdout=subprocess.DEVNULL)
    other = tmp_path_factory.mktemp('other_ca')
    subprocess.run(['sh', str(ROOT / 'tools' / 'make_test_certs.sh'), str(other)], check=True,
                   stdout=subprocess.DEVNULL)
    return out, other


@pytest.fixture
def tls_server(certs):
    c, _ = certs
    srv = MockTakServer(port=0, certfile=str(c / 'server.pem'), keyfile=str(c / 'server.key'),
                        cafile=str(c / 'ca.pem'), chatter_interval_s=0.05).start()
    yield srv
    srv.stop()


def test_tls_with_client_certificate(certs, tls_server):
    c, _ = certs
    client = tak_export.TakClient('tls://localhost:%d' % tls_server.port, certfile=str(c / 'dronescout.pem'),
                                  keyfile=str(c / 'dronescout.key'), cafile=str(c / 'ca.pem'),
                                  ping_interval_s=0, log=lambda *a: None)
    try:
        exp = tak_export.TAKExporter(client)
        exp.on_aircraft(_aircraft())
        assert wait_for(lambda: len(tls_server.events) == 1)
        name, event = tls_server.events[0]
        assert name == 'dronescout'                  # the server saw our certificate
        assert event.get('uid') == 'icao-4849EB'
    finally:
        client.close()


def test_tls_without_client_certificate_is_refused(certs, tls_server):
    c, _ = certs
    client = tak_export.TakClient('tls://localhost:%d' % tls_server.port, cafile=str(c / 'ca.pem'),
                                  ping_interval_s=0, log=lambda *a: None)
    try:
        # TLS 1.3: the server's refusal can arrive just after the handshake
        client.send(b'<event uid=\"x\"></event>')
        time.sleep(0.5)
        client.send(b'<event uid=\"y\"></event>')
        assert tls_server.events == []
    finally:
        client.close()


def test_tls_server_from_another_ca_is_refused(certs, tls_server):
    c, other = certs
    logs, log = _logs()
    client = tak_export.TakClient('tls://localhost:%d' % tls_server.port, certfile=str(c / 'dronescout.pem'),
                                  keyfile=str(c / 'dronescout.key'), cafile=str(other / 'ca.pem'),
                                  ping_interval_s=0, log=log)
    try:
        assert client.send(b'<event uid=\"x\"></event>') is False
        assert any('CERTIFICATE_VERIFY_FAILED' in line for line in logs)
    finally:
        client.close()


def test_tls_hostname_check(certs):
    c, _ = certs
    # server on 127.0.0.2: that address is not in its certificate (localhost, 127.0.0.1)
    try:
        srv = MockTakServer(host='127.0.0.2', port=0, certfile=str(c / 'server.pem'), keyfile=str(c / 'server.key'),
                            cafile=str(c / 'ca.pem'), chatter_interval_s=0).start()
    except OSError:
        pytest.skip("cannot listen on 127.0.0.2 here")
    kwargs = dict(certfile=str(c / 'dronescout.pem'), keyfile=str(c / 'dronescout.key'),
                  cafile=str(c / 'ca.pem'), ping_interval_s=0, log=lambda *a: None)
    strict = tak_export.TakClient('tls://127.0.0.2:%d' % srv.port, **kwargs)
    relaxed = tak_export.TakClient('tls://127.0.0.2:%d' % srv.port, check_hostname=False, **kwargs)
    try:
        assert strict.send(b'<event uid="strict"></event>') is False
        assert relaxed.send(b'<event uid="relaxed"></event>') is True
        assert wait_for(lambda: [e.get('uid') for _, e in srv.events] == ['relaxed'])
    finally:
        strict.close()
        relaxed.close()
        srv.stop()


# --- gateway wiring --------------------------------------------------------

def test_gateway_builds_exporter_from_tak_config(monkeypatch, tcp_server):
    from integrations.tak import tak_config, tak_gateway
    monkeypatch.setattr(tak_config, 'tak_url', 'tcp://127.0.0.1:%d' % tcp_server.port)
    monkeypatch.setattr(tak_config, 'tak_type_drone', 'a-h-A-M-F-Q')
    monkeypatch.setattr(tak_config, 'tak_send_aircraft', False)
    monkeypatch.setattr(tak_config, 'tak_sensor_positions', {'ds1': (52.0, 6.0, 5.0)}, raising=False)
    exp = tak_gateway.make_exporter(tak_config)
    try:
        assert exp.settings['type_drone'] == 'a-h-A-M-F-Q'
        assert exp.send_aircraft is False
        assert exp.sensor_positions == {'ds1': (52.0, 6.0, 5.0)}
        assert exp.client.scheme == 'tcp'
    finally:
        exp.close()
