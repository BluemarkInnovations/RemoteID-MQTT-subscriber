import json
import socket
import sys
from pathlib import Path

import pytest

from tests.conftest import _no_exporters_left_over   # noqa: F401 (autouse fixture)
from modules import json_events, test_objects
from integrations.udp import udp_export

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from udp_listen import open_socket, summary   # noqa: E402

CENTER = (52.22, 6.89)
T0 = 1_760_000_000.0


class FakeClock:
    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t


@pytest.fixture
def listener():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('127.0.0.1', 0))
    s.settimeout(2)
    yield s
    s.close()


def receive(sock, n):
    return [json.loads(sock.recvfrom(65535)[0]) for _ in range(n)]


def exporter(dests, **kw):
    kw.setdefault('log', lambda *a: None)
    kw.setdefault('min_interval_s', 0)
    return udp_export.UDPExporter(dests, **kw)


def feed(exp, t=T0):
    location, status = test_objects.sensor(t, CENTER)
    exp.on_location(location)
    exp.on_status(status)
    exp.on_remote_id(test_objects.drone(t, CENTER))
    exp.on_aircraft(test_objects.aircraft(t, CENTER))


@pytest.mark.parametrize('host,kind', [('239.255.86.1', 'multicast'), ('255.255.255.255', 'broadcast'),
                                       ('192.168.1.255', 'broadcast'), ('192.168.1.20', 'unicast'),
                                       ('receiver.local', 'name')])
def test_destination_kind(host, kind):
    assert udp_export.destination_kind(host) == kind


def test_parse_destinations():
    assert udp_export.parse_destinations(['239.255.86.1:8700', ('10.0.0.1', '9')]) == \
        [('239.255.86.1', 8700), ('10.0.0.1', 9)]


def test_one_event_per_datagram(listener):
    exp = exporter([listener.getsockname()], source='gw-1')
    feed(exp)
    events = receive(listener, 4)
    assert [e['type'] for e in events] == ['sensor_location', 'sensor_status', 'drone', 'aircraft']
    for e in events:
        assert e['source'] == 'gw-1' and e['schema_version'] == json_events.SCHEMA_VERSION
    drone = events[2]
    assert drone == dict({'schema_version': 1, 'source': 'gw-1'},
                         **json.loads(json.dumps(json_events.drone_event(test_objects.drone(T0, CENTER)))))
    assert all(summary(e) for e in events)
    exp.close()


def test_socket_allows_broadcast(listener):
    exp = exporter([listener.getsockname()])
    assert exp.sock.getsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST) == 1
    exp.close()


def test_rate_limit_per_object(listener):
    clock = FakeClock()
    exp = exporter([listener.getsockname()], min_interval_s=1.0, clock=clock)
    for i in range(4):
        clock.t = i * 0.4                          # 0, 0.4, 0.8, 1.2 s
        exp.on_remote_id(test_objects.drone(T0 + i, CENTER))
    assert exp.sent == 2 and exp.rate_limited == 2
    exp.close()


def test_too_large_event_is_not_sent(listener):
    logs = []
    exp = exporter([listener.getsockname()], max_datagram=200, log=logs.append)
    exp.on_remote_id(test_objects.drone(T0, CENTER))
    assert exp.sent == 0 and exp.too_large == 1 and 'larger than 200' in logs[-1]
    exp.close()


def test_every_destination_gets_a_copy(listener):
    other = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    other.bind(('127.0.0.1', 0))
    other.settimeout(2)
    try:
        exp = exporter([listener.getsockname(), other.getsockname()], send_status=False, send_aircraft=False)
        feed(exp)
        assert receive(listener, 1)[0]['type'] == 'drone' and receive(other, 1)[0]['type'] == 'drone'
        assert exp.sent == 2
        exp.close()
    finally:
        other.close()


def test_multicast_loopback():
    group = '239.255.86.99'
    try:
        rx = open_socket(0, group, '127.0.0.1')
    except OSError as e:
        pytest.skip('no multicast on this host: %s' % e)
    rx.settimeout(2)
    try:
        exp = exporter([(group, rx.getsockname()[1])], multicast_interface='127.0.0.1',
                       send_status=False, send_aircraft=False)
        exp.on_remote_id(test_objects.drone(T0, CENTER))
        try:
            event = json.loads(rx.recvfrom(65535)[0])
        except socket.timeout:
            pytest.skip('multicast is not looped back on this host')
        assert event['type'] == 'drone'
        exp.close()
    finally:
        rx.close()


def test_gateway_builds_exporter_from_udp_config(monkeypatch, listener):
    from integrations.udp import udp_config, udp_gateway
    monkeypatch.setattr(udp_config, 'udp_destinations', ['127.0.0.1:%d' % listener.getsockname()[1]])
    monkeypatch.setattr(udp_config, 'udp_send_aircraft', False)
    exp = udp_gateway.make_exporter(udp_config)
    try:
        feed(exp)
        assert [e['type'] for e in receive(listener, 3)] == ['sensor_location', 'sensor_status', 'drone']
    finally:
        exp.close()
