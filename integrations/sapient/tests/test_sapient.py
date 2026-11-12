import re
import sys
import time
from pathlib import Path

import pytest

from tests.conftest import _no_exporters_left_over   # noqa: F401 (autouse fixture)
from modules import decode, test_objects
from integrations.sapient import sapient_export, sapient_map as smap
from sapient_msg import proto_options_pb2

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from mock_fusion_node import MockFusionNode   # noqa: E402

ULID_RE = re.compile('[0-7][0-9A-HJKMNP-TV-Z]{25}')
UUID4_RE = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}')
CENTER = (52.22, 6.89)


# --- a mandatory-field check driven by the (is_mandatory) options in the proto files ---------

def missing_mandatory(message, path=''):
    """Paths of mandatory fields / oneofs that are not set, recursively (like Apex validation)."""
    missing = []
    desc = message.DESCRIPTOR
    for oneof in desc.oneofs:
        if oneof.name.startswith('_'):           # proto3 'optional' synthetic oneof
            continue
        opts = oneof.GetOptions().Extensions[proto_options_pb2.oneof_options]
        if opts.is_mandatory and message.WhichOneof(oneof.name) is None:
            missing.append(path + oneof.name)
    for field in desc.fields:
        opts = field.GetOptions().Extensions[proto_options_pb2.field_options]
        name = path + field.name
        if field.label == field.LABEL_REPEATED:
            values = getattr(message, field.name)
            if opts.is_mandatory and len(values) == 0:
                missing.append(name)
            if field.message_type is not None and not field.message_type.GetOptions().map_entry:
                for i, v in enumerate(values):
                    missing += missing_mandatory(v, '%s[%d].' % (name, i))
            continue
        if field.has_presence:
            present = message.HasField(field.name)
        else:
            present = getattr(message, field.name) not in ('', 0, None)
        if opts.is_mandatory and not present:
            missing.append(name)
        if present and field.message_type is not None:
            missing += missing_mandatory(getattr(message, field.name), name + '.')
    return missing


def assert_valid(m):
    assert missing_mandatory(m) == [], m
    assert UUID4_RE.fullmatch(m.node_id)


# --- identifiers -----------------------------------------------------------------------------

def test_ulid_format_and_time_order():
    a = smap.new_ulid(1_700_000_000)
    b = smap.new_ulid(1_700_000_001)
    assert ULID_RE.fullmatch(a) and ULID_RE.fullmatch(b)
    assert a[:10] < b[:10]                     # the time part sorts
    assert smap.new_ulid() != smap.new_ulid()


def test_node_uuid_is_stable_v4_format():
    a = smap.node_uuid('ds0948')
    assert a == smap.node_uuid('ds0948')
    assert UUID4_RE.fullmatch(a)
    assert a != smap.node_uuid('ds0949')
    assert a != smap.node_uuid('ds0948', namespace='other-gateway')


# --- messages ----------------------------------------------------------------------------------

def test_registration_is_complete():
    m = smap.registration(smap.node_uuid('s1'), 's1', {'model': 'DS230', 'firmware version': '2.7'})
    assert_valid(m)
    r = m.registration
    assert r.icd_version == 'BSI Flex 335 v2.0'
    assert r.node_definition[0].node_type == smap.reg.Registration.NODE_TYPE_PASSIVE_RF
    assert r.config_data[0].serial_number == 's1'
    assert r.config_data[0].software_version == '2.7'
    classes = {c.type: [s.type for s in c.sub_class]
               for c in r.mode_definition[0].detection_definition[0].detection_class_definition[0].class_definition}
    assert classes == {'Air Vehicle': ['UAV', 'Fixed Wing', 'Rotary Wing', 'Lighter Than Air']}


def test_registration_classes_follow_settings():
    m = smap.registration('x', 's1', settings={'class_drone': ('Drone', None)})
    classes = [c.type for c in m.registration.mode_definition[0].detection_definition[0]
               .detection_class_definition[0].class_definition]
    assert 'Drone' in classes


@pytest.mark.parametrize('status, system', [
    ({'status': 'normal'}, 'SYSTEM_OK'),
    ({'status': 'degraded', 'status details': 'ADS-B 1090 SDR missing'}, 'SYSTEM_WARNING'),
    ({'status': 'something else'}, 'SYSTEM_ERROR'),
    (None, 'SYSTEM_WARNING'),
])
def test_status_report_system(status, system):
    m = smap.status_report(smap.node_uuid('s1'), status, (52.0, 6.0, 12.0))
    assert_valid(m)
    assert smap.st.StatusReport.System.Name(m.status_report.system) == system
    assert ULID_RE.fullmatch(m.status_report.report_id)
    assert m.status_report.node_location.y == 52.0
    assert len(m.status_report.coverage[0].location_list.locations) == 16


def test_status_report_details_and_goodbye():
    status = {'status': 'degraded', 'status details': 'WLAN wlan1 missing',
              'WLAN radios': {'wlan0': 'ok', 'wlan1': 'missing'}}
    r = smap.status_report('x', status, None).status_report
    texts = [s.status_value for s in r.status]
    assert any('WLAN wlan1 missing' in t for t in texts)
    assert 'WLAN radios wlan1: missing' in texts
    assert not r.HasField('node_location') and len(r.coverage) == 0
    g = smap.status_report('x', status, None, goodbye=True).status_report
    assert g.system == smap.st.StatusReport.SYSTEM_GOODBYE


def test_remote_id_detection():
    msg = test_objects.drone(1_794_480_000.0, CENTER)
    m = smap.remote_id_detection(smap.node_uuid('s1'), smap.new_ulid(), msg)
    assert_valid(m)
    d = m.detection_report
    assert d.location.datum == smap.WGS84_E and d.location.z == 165.0
    assert d.location.x_error == 10.0 and d.location.z_error == 10.0
    assert d.classification[0].type == 'Air Vehicle' and d.classification[0].sub_class[0].type == 'UAV'
    assert d.id == 'DRONESCOUT-TEST-DRONE'
    info = {i.type: i.value for i in d.object_info}
    assert info['serial number'] == 'DRONESCOUT-TEST-DRONE'
    assert info['operator latitude'] == '52.22'
    assert info['EU class'] == 'Class 1'
    assert d.signal[0].amplitude == -60 and d.signal[0].centre_frequency == 2437.0
    speed = (d.enu_velocity.east_rate ** 2 + d.enu_velocity.north_rate ** 2) ** 0.5
    assert speed == pytest.approx(msg.uas.location.speed_horizontal)


def test_remote_id_without_position_is_skipped():
    msg = test_objects.drone(0.0, CENTER)
    msg.uas.location.latitude = float('nan')
    assert smap.remote_id_detection('x', smap.new_ulid(), msg) is None


def test_bluetooth_signal_has_a_band():
    msg = test_objects.drone(0.0, CENTER)
    msg.channel = 0
    sig = smap.remote_id_detection('x', smap.new_ulid(), msg).detection_report.signal[0]
    assert (sig.start_frequency, sig.stop_frequency) == (2400.0, 2483.5)


def _aircraft(**overrides):
    a = {'raw': '', 'sensor ID': 'ds0948', 'timestamp': 1793712471226, 'frequency MHz': 1090, 'RSSI': -71.5,
         'type': 'ADSB', 'address': '4849EB', 'address type': 0, 'flight': 'PHTWK   ', 'category': 2,
         'altitude': 1402, 'altitude type': 1, 'speed': 64, 'track': 90, 'vertical rate': -2,
         'latitude': 52.16774116, 'longitude': 6.91573661}
    a.update(overrides)
    return decode.decode_aircraft(a)


def test_aircraft_detection():
    m = smap.aircraft_detection(smap.node_uuid('s1'), smap.new_ulid(), _aircraft())
    assert_valid(m)
    d = m.detection_report
    assert d.id == 'PHTWK'
    assert d.location.datum == smap.WGS84_G            # baro altitude: not the ellipsoid
    assert d.classification[0].sub_class[0].type == 'Fixed Wing'
    assert d.enu_velocity.east_rate == pytest.approx(64) and d.enu_velocity.north_rate == pytest.approx(0, abs=1e-9)
    assert d.enu_velocity.up_rate == -2
    info = {i.type: i.value for i in d.object_info}
    assert info['ICAO address'] == '4849EB' and info['callsign'] == 'PHTWK'
    assert d.signal[0].centre_frequency == 1090.0


@pytest.mark.parametrize('overrides, expect', [
    ({'altitude type': 2}, smap.WGS84_E),
    ({'altitude type': 3}, smap.WGS84_G),
])
def test_aircraft_altitude_datum(overrides, expect):
    assert smap.aircraft_detection('x', smap.new_ulid(), _aircraft(**overrides)).detection_report.location.datum == expect


@pytest.mark.parametrize('category, sub', [(7, 'Rotary Wing'), (14, 'UAV'), (10, 'Lighter Than Air'), (0, None)])
def test_aircraft_classes(category, sub):
    assert smap.aircraft_class(_aircraft(category=category)) == ('Air Vehicle', sub)


def test_aircraft_unknown_velocity_and_position():
    d = smap.aircraft_detection('x', smap.new_ulid(), _aircraft(speed=0, track=0, **{'vertical rate': 0}))
    assert not d.detection_report.HasField('enu_velocity')
    assert smap.aircraft_detection('x', smap.new_ulid(), _aircraft(latitude=0, longitude=0)) is None
    assert smap.aircraft_detection('x', smap.new_ulid(), _aircraft(latitude=None)) is None


def test_aircraft_keys():
    assert smap.aircraft_key(_aircraft()) == 'icao-4849EB'
    assert smap.aircraft_key(_aircraft(type='FLARM', **{'address type': 1})) == 'flarm-4849EB'


def test_lost_and_task_ack():
    last = smap.aircraft_detection('n', 'O' * 26, _aircraft())
    lost = smap.lost_detection('n', 'O' * 26, last)
    assert lost.detection_report.state == 'lost'
    assert lost.detection_report.report_id != last.detection_report.report_id
    assert_valid(smap.task_ack(smap.node_uuid('s'), smap.new_ulid()))


def test_framing_round_trip():
    m = smap.status_report(smap.node_uuid('s1'), {'status': 'normal'}, None)
    framed = smap.frame(m)
    assert int.from_bytes(framed[:4], 'little') == len(framed) - 4
    assert smap.parse(framed[4:]) == m


# --- exporter against the mock fusion node ------------------------------------------------------

def wait_for(condition, timeout=5.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if condition():
            return True
        time.sleep(0.02)
    return False


@pytest.fixture
def fusion_node():
    fn = MockFusionNode(port=0, quiet=True).start()
    yield fn
    fn.stop()


def _exporter(fn, **kwargs):
    kwargs.setdefault('log', lambda *a: None)
    return sapient_export.SAPIENTExporter('tcp://127.0.0.1:%d' % fn.port, **kwargs)


def test_exporter_registers_reports_and_loses_tracks(fusion_node):
    exp = _exporter(fusion_node, stale_s=0.5, min_interval_s=0, heartbeat_s=0.3)
    try:
        location, status = test_objects.sensor(time.time(), CENTER)
        exp.on_location(location)
        exp.on_status(status)
        for _ in range(3):
            exp.on_remote_id(test_objects.drone(time.time(), CENTER))
            exp.on_aircraft(test_objects.aircraft(time.time(), CENTER))
            time.sleep(0.15)
        assert wait_for(lambda: len([m for m in fusion_node.of_kind('detection_report')
                                     if m.detection_report.state == 'lost']) == 2)
        assert len(fusion_node.of_kind('registration')) == 1
        assert len(fusion_node.of_kind('status_report')) >= 2
        dets = fusion_node.of_kind('detection_report')
        objects = {m.detection_report.object_id for m in dets}
        assert len(objects) == 2                      # one object_id per track, also in the lost report
        assert fusion_node.problems == []
        for m in fusion_node.messages:
            assert missing_mandatory(m[1]) == []
    finally:
        exp.close()
    assert wait_for(lambda: fusion_node.of_kind('status_report')[-1].status_report.system
                    == smap.st.StatusReport.SYSTEM_GOODBYE)


def test_detections_are_paced_and_the_newest_wins(fusion_node):
    exp = _exporter(fusion_node, min_interval_s=0, detection_gap_s=0.1, start=False)
    try:
        exp.on_status({'sensor ID': 's1', 'status': 'normal'})
        node = exp.nodes['s1']
        while not node.registered:
            exp.pump()
            time.sleep(0.01)
        for i in range(5):                            # 5 updates of one object, queued at once
            exp.on_aircraft(_aircraft(**{'sensor ID': 's1', 'altitude': 1000 + i}))
        exp.on_aircraft(_aircraft(**{'sensor ID': 's1', 'address': 'ABCDEF'}))
        assert len(node.pending) == 2                 # one per object
        end = time.monotonic() + 0.5
        while time.monotonic() < end:
            exp.pump()
            time.sleep(0.01)
        dets = fusion_node.of_kind('detection_report')
        assert [d.detection_report.location.z for d in dets] == [1004, 1402]
        times = [d.timestamp.ToNanoseconds() / 1e9 for d in dets]
        assert times[1] - times[0] >= 0.1
        assert fusion_node.problems == []
    finally:
        exp.close()


def test_rate_limit_per_object(fusion_node):
    exp = _exporter(fusion_node, min_interval_s=10, start=False)
    try:
        for _ in range(3):
            exp.on_aircraft(_aircraft())
        exp.on_aircraft(_aircraft(address='ABCDEF'))
        assert exp.rate_limited == 2
        assert len(exp.nodes['ds0948'].pending) == 2
    finally:
        exp.close()


def test_task_is_rejected(fusion_node):
    fusion_node.send_task = True
    exp = _exporter(fusion_node)
    try:
        exp.on_status({'sensor ID': 's1', 'status': 'normal'})
        assert wait_for(lambda: len(fusion_node.of_kind('task_ack')) == 1)
        ack = fusion_node.of_kind('task_ack')[0].task_ack
        assert ack.task_status == smap.tack.TaskAck.TASK_STATUS_REJECTED and ack.reason
        assert exp.tasks_rejected == 1
    finally:
        exp.close()


def test_one_connection_per_sensor(fusion_node):
    exp = _exporter(fusion_node)
    try:
        exp.on_status({'sensor ID': 's1', 'status': 'normal'})
        exp.on_status({'sensor ID': 's2', 'status': 'degraded'})
        assert wait_for(lambda: len(fusion_node.registered) == 2)
        assert fusion_node.connections == 2
        assert set(fusion_node.registered) == {smap.node_uuid('s1'), smap.node_uuid('s2')}
        assert fusion_node.problems == []
    finally:
        exp.close()


def test_rejected_registration_backs_off(fusion_node):
    fusion_node.accept = False
    logs = []
    exp = _exporter(fusion_node, log=logs.append)
    try:
        exp.on_status({'sensor ID': 's1', 'status': 'normal'})
        assert wait_for(lambda: any('registration rejected' in line for line in logs))
        exp.on_aircraft(_aircraft(**{'sensor ID': 's1'}))
        time.sleep(0.3)
        assert fusion_node.of_kind('detection_report') == []
        assert exp.nodes['s1'].next_attempt - time.monotonic() > 30     # no hammering
    finally:
        exp.close()


def test_reconnects_and_registers_again(fusion_node):
    exp = _exporter(fusion_node)
    try:
        exp.on_status({'sensor ID': 's1', 'status': 'normal'})
        assert wait_for(lambda: len(fusion_node.of_kind('registration')) == 1)
        fusion_node.disconnect_clients()
        assert wait_for(lambda: len(fusion_node.of_kind('registration')) == 2, 6)
        regs = fusion_node.of_kind('registration')
        assert regs[0].node_id == regs[1].node_id
    finally:
        exp.close()


def test_fusion_node_down_drops_without_blocking():
    exp = sapient_export.SAPIENTExporter('tcp://127.0.0.1:1', log=lambda *a: None, start=False)
    try:
        exp.on_aircraft(_aircraft())
        t = time.monotonic()
        exp.pump()
        assert time.monotonic() - t < 2
        assert exp.dropped == 1
    finally:
        exp.close()


def test_bad_url():
    with pytest.raises(ValueError):
        sapient_export.SAPIENTExporter('udp://x:1', start=False)


def test_gateway_builds_exporter_from_config(monkeypatch):
    from integrations.sapient import sapient_config, sapient_gateway
    monkeypatch.setattr(sapient_config, 'sapient_url', 'tcp://127.0.0.1:5999')
    monkeypatch.setattr(sapient_config, 'sapient_send_aircraft', False)
    monkeypatch.setattr(sapient_config, 'sapient_class_drone', ('Drone', 'Multirotor'), raising=False)
    exp = sapient_gateway.make_exporter(sapient_config, start=False)
    try:
        assert exp.port == 5999 and exp.send_aircraft is False
        assert exp.settings['class_drone'] == ('Drone', 'Multirotor')
        assert exp.heartbeat_s == 10
    finally:
        exp.close()
