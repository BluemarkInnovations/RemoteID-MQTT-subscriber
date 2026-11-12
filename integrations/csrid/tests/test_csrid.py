import json
import math
import socket
import sys
import threading
import time
from pathlib import Path

import pytest

from tests.conftest import _no_exporters_left_over   # noqa: F401 (autouse fixture)
from modules import test_objects
from integrations.csrid import astm, coap, csrid_export
from integrations.csrid import csrid_cbor as cbor
from integrations.csrid import csrid_map as cm

SDSP_DIR = Path(__file__).resolve().parents[1] / 'sdsp'
sys.path.insert(0, str(SDSP_DIR))
from brid import decode_brid   # noqa: E402

CENTER = (52.22, 6.89)
KEY = bytes(range(32))
RID = 'DRONESCOUT-TEST-FINDER'


def wait_for(cond, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.02)
    return False


# --- CBOR: examples from RFC 8949 appendix A ---------------------------------------------------

@pytest.mark.parametrize('value, hexdata', [
    (0, '00'), (23, '17'), (24, '1818'), (100, '1864'), (1000, '1903e8'), (1000000, '1a000f4240'),
    (1000000000000, '1b000000e8d4a51000'), (-1, '20'), (-1000, '3903e7'), (1.1, 'fb3ff199999999999a'),
    (False, 'f4'), (True, 'f5'), (None, 'f6'), (b'', '40'), (b'\x01\x02\x03\x04', '4401020304'),
    ('a', '6161'), ('ü', '62c3bc'), ([1, [2, 3], [4, 5]], '8301820203820405'),
    ({'a': 1, 'b': [2, 3]}, 'a26161016162820203'),
    (cbor.Tag(0, '2013-03-21T20:04:00Z'), 'c074323031332d30332d32315432303a30343a30305a'),
])
def test_cbor_rfc8949_examples(value, hexdata):
    assert cbor.dumps(value).hex() == hexdata
    assert cbor.loads(bytes.fromhex(hexdata)) == value


def test_cbor_decodes_half_and_single_floats_and_rejects_bad_data():
    assert cbor.loads(bytes.fromhex('f93c00')) == 1.0
    assert cbor.loads(bytes.fromhex('f9c400')) == -4.0
    assert cbor.loads(bytes.fromhex('fa47c35000')) == 100000.0
    for bad in ('1a000f42', '6161ff', '5f', '9a7fffffff'):
        with pytest.raises(ValueError):
            cbor.loads(bytes.fromhex(bad))


# --- COSE_Mac0 --------------------------------------------------------------------------------

# made with pycose 1.1.0: Mac0Message(phdr={Algorithm: HMAC256}, uhdr={KID: b'f1'}, payload=b'hello')
PYCOSE_MAC0 = ('d18443a10105a1044266314568656c6c6f5820a40db77255d78c45239ff26158c8b5c7'
               '60fc4b36c3a058742c46bb7590b9189a')


def test_cose_mac0_matches_pycose_byte_for_byte():
    assert cbor.mac0(b'hello', KEY, b'f1').hex() == PYCOSE_MAC0
    assert cbor.mac0_verify(bytes.fromhex(PYCOSE_MAC0), KEY) == b'hello'
    assert cbor.mac0_parts(bytes.fromhex(PYCOSE_MAC0))[0] == b'f1'


def test_cose_mac0_rejects_wrong_key_and_changed_payload():
    data = cbor.mac0(b'hello', KEY, b'f1')
    with pytest.raises(ValueError, match='wrong MAC'):
        cbor.mac0_verify(data, bytes(32))
    with pytest.raises(ValueError, match='wrong MAC'):
        cbor.mac0_verify(data.replace(b'hello', b'jello'), KEY)


# --- CoAP -------------------------------------------------------------------------------------

def test_coap_encode_decode():
    msg = coap.Message(coap.CON, coap.POST, 0x1234, b'\xaa\xbb\xcc\xdd',
                       coap.path_options('csrid') + [(coap.OPT_CONTENT_FORMAT, coap.uint_option(97))], b'\x01')
    data = coap.encode(msg)
    #             ver 1 CON TKL 4, POST, mid, token, Uri-Path (11) "csrid", Content-Format (12) 97, payload
    assert data.hex() == '44021234aabbccdd' + 'b5' + b'csrid'.hex() + '1161' + 'ff01'
    back = coap.decode(data)
    assert (back.mtype, back.code, back.mid, back.token) == (coap.CON, coap.POST, 0x1234, b'\xaa\xbb\xcc\xdd')
    assert back.uri_path == 'csrid' and back.content_format == 97 and back.payload == b'\x01'
    long = coap.Message(coap.NON, coap.POST, 1, options=[(300, b'x' * 300)])
    assert coap.decode(coap.encode(long)).options == [(300, b'x' * 300)]
    for bad in (b'\x44\x02', b'\x48\x02\x00\x01', b'\x40\x02\x00\x01\xff'):
        with pytest.raises(ValueError):
            coap.decode(bad)


# --- ASTM F3411 encoder, checked with the SDSP's independent decoder -----------------------------

def test_astm_message_pack_of_the_test_drone():
    t = 1_760_000_000.0
    msg = test_objects.drone(t, CENTER)
    packs = astm.message_packs(msg.uas)
    assert len(packs) == 1 and packs[0][:3] == bytes([0xF2, 25, 5]) and len(packs[0]) == 3 + 5 * 25
    d = {m['type']: m for m in decode_brid(packs[0])}
    assert set(d) == {'basic_id', 'location', 'self_id', 'system', 'operator_id'}
    assert d['basic_id']['uas_id'] == 'DRONESCOUT-TEST-DRON'          # 20 characters on the air
    assert (d['basic_id']['id_type'], d['basic_id']['ua_type']) == (1, 2)
    loc, uas_loc = d['location'], msg.uas.location
    assert loc['status'] == 2 and loc['height_type'] == 1
    assert abs(loc['latitude'] - uas_loc.latitude) < 1e-7 and abs(loc['longitude'] - uas_loc.longitude) < 1e-7
    assert loc['altitude_baro'] == 120.0 and loc['altitude_geo'] == 165.0 and loc['height'] == 120.0
    assert abs(loc['direction'] - uas_loc.direction) <= 0.5
    assert abs(loc['speed'] - uas_loc.speed_horizontal) <= 0.125
    assert loc['vertical_speed'] == 0.0
    assert d['self_id']['text'] == 'TEST DATA'
    assert d['operator_id']['operator_id'] == 'DRONESCOUT-TEST-OPERATOR'[:20]
    assert (d['system']['operator_latitude'], d['system']['operator_longitude']) == CENTER


@pytest.mark.parametrize('speed, mult, raw', [(0.0, 0, 0), (10.0, 0, 40), (63.75, 0, 255), (64.5, 1, 1),
                                              (100.0, 1, 48), (300.0, 1, 254), (math.nan, 1, 255)])
def test_astm_speed_encoding(speed, mult, raw):
    assert astm._speed_horizontal(speed) == (mult, raw)


def test_astm_unknown_values():
    assert astm._direction(math.nan) == (1, 181)
    assert astm._altitude(math.nan) == 0 and astm._altitude(-1000.0) == 0 and astm._altitude(0.0) == 2000
    assert astm._speed_vertical(math.nan) == 126 and astm._speed_vertical(-3.0) == -6


# --- CS-RID messages ---------------------------------------------------------------------------

def test_brid_forwarding_message():
    t = 1_760_000_000.25
    pack = astm.message_packs(test_objects.drone(t, CENTER).uas)[0]
    obj = cm.brid_forwarding(RID, '02:00:00:00:00:99', t, (52.1, 6.8, 30.0), cm.RADIO_UHF, '02:00:00:00:00:01', pack)
    data = cm.encode(obj, KEY, RID)
    assert cbor.mac0_parts(data)[0] == RID.encode()
    mtype, m = cm.decode(data, KEY)
    assert mtype == cm.BRID_FORWARDING
    assert cbor.loads(cbor.mac0_parts(data)[1])['application'] == 'DRIP-CSRID'
    assert m['id'] == RID and m['message_type'] == 1 and m['radio_type'] == 7
    assert m['gps'] == [52.1, 6.8, 30.0] and m['broadcast_message'] == pack
    assert m['mac_address'] == bytes.fromhex('020000000099') and m['broadcast_mac_address'] == bytes.fromhex('020000000001')
    assert m['timestamp'] == cbor.Tag(0, '2025-10-09T08:53:20.250Z') and cm.parse_tdate(m['timestamp']) == t
    with pytest.raises(ValueError):
        cm.decode(data, bytes(32))


def test_parse_rejects_objects_with_none_or_several_messages():
    reg = cm.finder_registration(RID, None, 0, (1, 2, 3))
    loc = cm.finder_location(RID, None, 0, (1, 2, 3))
    with pytest.raises(ValueError):
        cm.parse(dict(reg, location_update=loc['location_update']))
    with pytest.raises(ValueError):
        cm.parse({'application': 'DRIP-CSRID', 'version': 1})


def test_radio_type():
    assert cm.radio_type('WLAN Beacon', 6) == cm.RADIO_UHF
    assert cm.radio_type('WLAN NaN', 149) == cm.RADIO_SHF
    assert cm.radio_type('BT5 long range', 37) == cm.RADIO_UHF


# --- Finder against a fake SDSP (standard library, our own CoAP / CBOR) ---------------------------

class FakeSDSP:
    def __init__(self, key=KEY, proxy_status=cm.PROXY_FORWARD, update_interval=300):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('127.0.0.1', 0))
        self.sock.settimeout(0.1)
        self.port = self.sock.getsockname()[1]
        self.key = key
        self.proxy_status = proxy_status
        self.update_interval = update_interval
        self.received = []                       # (coap message, type, members)
        self.answer_unauthorized = False
        self.heartbeat_next = False
        self.ignore_registrations = 0
        self._stop = False
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def types(self):
        return [t for _, t, _ in self.received]

    def _loop(self):
        while not self._stop:
            try:
                data, addr = self.sock.recvfrom(4096)
            except OSError:
                continue
            req = coap.decode(data)
            mtype, m = cm.decode(req.payload, self.key)
            if mtype == cm.FINDER_REGISTRATION and self.ignore_registrations:
                self.ignore_registrations -= 1
                self.received.append((req, mtype, m))
                continue
            self.received.append((req, mtype, m))
            payload, code = b'', coap.CHANGED
            if self.answer_unauthorized:
                self.answer_unauthorized = False
                code = coap.UNAUTHORIZED
            elif mtype == cm.FINDER_REGISTRATION:
                payload = cm.encode(cm.sdsp_response(None, 'fake-sdsp', m['id'], self.proxy_status,
                                                     self.update_interval), self.key, 'fake-sdsp')
            elif self.heartbeat_next:
                self.heartbeat_next = False
                payload = cm.encode(cm.sdsp_heartbeat(None, 'fake-sdsp', time.time()), self.key, 'fake-sdsp')
            rtype = coap.ACK if req.mtype == coap.CON else coap.NON
            resp = coap.Message(rtype, code, req.mid if rtype == coap.ACK else 7, req.token,
                                [(coap.OPT_CONTENT_FORMAT, coap.uint_option(coap.CF_COSE_MAC0))] if payload else [],
                                payload)
            self.sock.sendto(coap.encode(resp), addr)

    def close(self):
        self._stop = True
        self.thread.join()
        self.sock.close()


@pytest.fixture
def fake_sdsp():
    s = FakeSDSP()
    yield s
    s.close()


def finder(port, **kw):
    kw.setdefault('position', (52.21, 6.88, 30.0))
    kw.setdefault('log', lambda *a: None)
    return csrid_export.CSRIDFinder('coap://127.0.0.1:%d/csrid' % port, RID, key=KEY.hex(),
                                    finder_mac='02:00:00:00:00:99', **kw)


def test_finder_registers_forwards_and_gets_a_heartbeat(fake_sdsp):
    f = finder(fake_sdsp.port, min_interval_s=1.0)
    try:
        assert wait_for(lambda: f.registered)
        assert f.sdsp_id == 'fake-sdsp' and f.update_interval_s == 300
        reg_req, _, reg = fake_sdsp.received[0]
        assert reg_req.mtype == coap.CON and reg_req.uri_path == 'csrid' and reg_req.content_format == 97
        assert reg['gps'] == [52.21, 6.88, 30.0]
        f.on_location({'sensor ID': test_objects.TEST_SENSOR, 'latitude': 52.2, 'longitude': 6.9, 'altitude MSL': 12})
        fake_sdsp.heartbeat_next = True
        t = time.time()
        f.on_remote_id(test_objects.drone(t, CENTER))
        f.on_remote_id(test_objects.drone(t + 0.1, CENTER))          # within min_interval_s: not forwarded
        assert wait_for(lambda: f.heartbeats == 1)
        req, mtype, m = fake_sdsp.received[1]
        assert mtype == cm.BRID_FORWARDING and req.mtype == coap.NON
        assert m['gps'] == [52.2, 6.9, 12.0]                          # position of the receiving sensor
        assert m['broadcast_mac_address'] == bytes.fromhex('020000000001')
        assert decode_brid(m['broadcast_message'])[0]['uas_id'] == 'DRONESCOUT-TEST-DRON'
        assert f.dropped['rate'] == 1 and fake_sdsp.types() == [cm.FINDER_REGISTRATION, cm.BRID_FORWARDING]
    finally:
        f.close()


def test_finder_registers_again_after_401(fake_sdsp):
    f = finder(fake_sdsp.port, min_interval_s=0)
    try:
        assert wait_for(lambda: f.registered)
        fake_sdsp.answer_unauthorized = True                         # e.g. the SDSP restarted
        f.on_remote_id(test_objects.drone(time.time(), CENTER))
        assert wait_for(lambda: fake_sdsp.types().count(cm.FINDER_REGISTRATION) == 2 and f.registered)
    finally:
        f.close()


def test_finder_retransmits_the_registration(fake_sdsp):
    fake_sdsp.ignore_registrations = 1
    f = finder(fake_sdsp.port)
    try:
        assert wait_for(lambda: f.registered, timeout=5)               # second try after ACK_TIMEOUT (2 s)
        regs = [r for r, t, _ in fake_sdsp.received if t == cm.FINDER_REGISTRATION]
        assert len(regs) == 2 and regs[0].mid == regs[1].mid             # a retransmission: same message ID
    finally:
        f.close()


def test_suspended_finder_forwards_nothing_but_sends_location_updates():
    sdsp = FakeSDSP(proxy_status=cm.PROXY_REVERSE, update_interval=1)
    f = finder(sdsp.port)
    try:
        assert wait_for(lambda: f.registered)
        f.on_remote_id(test_objects.drone(time.time(), CENTER))
        assert f.dropped['suspended'] == 1
        assert wait_for(lambda: cm.FINDER_LOCATION in sdsp.types(), timeout=4)
        assert cm.BRID_FORWARDING not in sdsp.types()
    finally:
        f.close()
        sdsp.close()


def test_no_forwarding_before_registration():
    f = csrid_export.CSRIDFinder('coap://127.0.0.1:9/csrid', RID, key=KEY.hex(), start=False, log=lambda *a: None)
    f.on_remote_id(test_objects.drone(time.time(), CENTER))
    assert f.dropped['not registered'] == 1
    f.close()


def test_settings_are_checked():
    with pytest.raises(ValueError, match='coaps'):
        csrid_export.CSRIDFinder('coaps://sdsp/csrid', RID, start=False)
    with pytest.raises(ValueError, match='16 bytes'):
        csrid_export.CSRIDFinder('coap://sdsp/csrid', RID, key='0011', start=False)
    assert csrid_export.parse_sdsp_url('coap://sdsp.example') == ('sdsp.example', 5683, 'csrid')


def test_gateway_builds_the_finder_from_csrid_config(monkeypatch):
    from integrations.csrid import csrid_config, csrid_gateway
    monkeypatch.setattr(csrid_config, 'csrid_key_hex', KEY.hex())
    monkeypatch.setattr(csrid_config, 'csrid_position', (52.0, 6.0, 1.0), raising=False)
    f = csrid_gateway.make_exporter(csrid_config, start=False)
    assert (f.host, f.port, f.path, f.rid, f.key) == ('127.0.0.1', 5683, 'csrid', 'dronescout-finder-1', KEY)
    assert 'not registered' in f.stats()
    f.close()


# --- Finder against the reference SDSP (cbor2, pycose, aiocoap: independent implementations) ------

@pytest.fixture
def reference_sdsp(tmp_path):
    pytest.importorskip('aiocoap')
    pytest.importorskip('cbor2')
    pytest.importorskip('pycose')
    import asyncio
    import aiocoap
    import aiocoap.resource
    import sdsp as sdsp_mod
    finders = tmp_path / 'finders.json'
    finders.write_text(json.dumps({RID: {'key': KEY.hex()}, 'suspended': {'key': KEY.hex(), 'proxy_status': 'reverse'}}))
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    server = sdsp_mod.SDSP('test-sdsp', sdsp_mod.load_finders(str(finders)), update_interval=120, quiet=True)
    loop = asyncio.new_event_loop()
    site = aiocoap.resource.Site()
    site.add_resource(['csrid'], sdsp_mod.CSRIDResource(server))
    ctx = loop.run_until_complete(aiocoap.Context.create_server_context(site, bind=('127.0.0.1', port)))
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    server.port = port
    yield server
    asyncio.run_coroutine_threadsafe(ctx.shutdown(), loop).result(5)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(5)
    loop.close()


def test_finder_with_the_reference_sdsp(reference_sdsp):
    f = finder(reference_sdsp.port, min_interval_s=0)
    try:
        assert wait_for(lambda: f.registered)
        assert f.sdsp_id == 'test-sdsp' and f.update_interval_s == 120 and f.proxy_status == cm.PROXY_FORWARD
        f.on_location({'sensor ID': test_objects.TEST_SENSOR, 'latitude': 52.2, 'longitude': 6.9, 'altitude MSL': 12})
        t = time.time()
        for i in range(3):
            f.on_remote_id(test_objects.drone(t + i, CENTER))
        assert wait_for(lambda: reference_sdsp.snapshot('stats')['received'] == 4)
        assert reference_sdsp.snapshot('stats')['rejected'] == 0
        ua = reference_sdsp.snapshot('uas')['02:00:00:00:00:01']
        assert ua['messages'] == 3 and ua['radio_type'] == 'UHF'
        assert ua['basic_id']['1']['uas_id'] == 'DRONESCOUT-TEST-DRON'
        assert ua['self_id']['text'] == 'TEST DATA' and ua['location']['height'] == 120.0
        assert ua['finders'][RID]['gps'] == [52.2, 6.9, 12.0]
        assert reference_sdsp.snapshot('finders')[RID]['gps'] == [52.2, 6.9, 12.0]      # the Finder map (4.2)
    finally:
        f.close()


def test_reference_sdsp_rejects_wrong_key_and_suspends(reference_sdsp):
    bad = csrid_export.CSRIDFinder('coap://127.0.0.1:%d/csrid' % reference_sdsp.port, RID, key=bytes(32).hex(),
                                   position=(52.0, 6.0, 0.0), log=lambda *a: None, registration_retry_s=60)
    suspended = csrid_export.CSRIDFinder('coap://127.0.0.1:%d/csrid' % reference_sdsp.port, 'suspended',
                                         key=KEY.hex(), position=(52.0, 6.0, 0.0), log=lambda *a: None)
    try:
        assert wait_for(lambda: suspended.registered and reference_sdsp.snapshot('stats')['rejected'] >= 1)
        assert not bad.registered and suspended.proxy_status == cm.PROXY_REVERSE
        suspended.on_remote_id(test_objects.drone(time.time(), CENTER))
        assert suspended.dropped['suspended'] == 1
    finally:
        bad.close()
        suspended.close()
