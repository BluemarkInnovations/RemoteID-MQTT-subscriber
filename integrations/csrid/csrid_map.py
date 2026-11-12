# (c) Bluemark Innovations BV
# MIT license
"""CS-RID messages (draft-moskowitz-drip-crowd-sourced-rid-16, section 6 / 7) as CBOR.

The draft is an expired, individual Internet-Draft with open issues; where it is unclear
or inconsistent, this module makes the choices listed in the README ("Interpretation of
the draft"). The most important:

- One CSRID_Object per message: {"application": "DRIP-CSRID", "version": 1, <label>: {...}}
  with exactly one message member (the CDDL lists all members as required).
- The text labels of Figure 4 (the draft moves to uint keys later).
- MAC addresses and the B-RID message as plain byte strings: the draft's tag 37 is the
  CBOR tag for a 16-byte UUID, which decoders reject for a 6-byte MAC address.
- tdate (tag 0, RFC 3339 text) timestamps, as in the CDDL.
- The CS-RID MAC: the whole CBOR object as payload of a COSE_Mac0 (HMAC 256/256) with a
  key per Finder (csrid_cbor.mac0).
"""

import datetime
import math

from integrations.csrid import csrid_cbor as cbor

APPLICATION = 'DRIP-CSRID'
VERSION = 1

# Figure 1
RESERVED, BRID_FORWARDING, FINDER_REGISTRATION, SDSP_RESPONSE, FINDER_LOCATION, SDSP_HEARTBEAT = 0, 1, 2, 3, 4, 5

# Figure 4 labels of the message members
LABELS = {
    RESERVED: 'message_info',
    BRID_FORWARDING: 'proxy_message-type',
    FINDER_REGISTRATION: 'finder_registration',
    SDSP_RESPONSE: 'sdsp_response',
    FINDER_LOCATION: 'location_update',
    SDSP_HEARTBEAT: 'sdsp_heartbeat',
}
TYPES = {v: k for k, v in LABELS.items()}
TYPE_NAMES = {RESERVED: 'info', BRID_FORWARDING: 'B-RID forwarding', FINDER_REGISTRATION: 'Finder registration',
              SDSP_RESPONSE: 'SDSP response', FINDER_LOCATION: 'Finder location', SDSP_HEARTBEAT: 'SDSP heartbeat'}

# radio_types: frequency bands. 2.4 GHz Bluetooth / Wi-Fi is UHF (0.3-3 GHz), 5 GHz Wi-Fi SHF.
RADIO_UHF, RADIO_SHF = 7, 8

# proxy_status_types
PROXY_FORWARD, PROXY_REVERSE, PROXY_BI = 0, 1, 2
PROXY_STATUS_NAMES = {PROXY_FORWARD: 'forward', PROXY_REVERSE: 'reverse', PROXY_BI: 'bi-directional'}

DEFAULT_UPDATE_INTERVAL_S = 300          # section 6.5: 5 minutes without an SDSP response


def proxying(proxy_status):
    """Whether the Finder forwards B-RID messages. The draft has no "suspend" value: "reverse"
    (SDSP to Finder only) is taken as "suspend proxying, only report the location"."""
    return proxy_status != PROXY_REVERSE


def tdate(t):
    """Unix time (s) -> tdate: tag 0 with RFC 3339 UTC text, millisecond resolution."""
    dt = datetime.datetime.fromtimestamp(t, datetime.timezone.utc)
    return cbor.Tag(0, dt.strftime('%Y-%m-%dT%H:%M:%S.') + '%03dZ' % (dt.microsecond // 1000))


def parse_tdate(value):
    """tdate (tag 0 text, or tag 1 epoch number) -> Unix time (s)."""
    if isinstance(value, cbor.Tag) and value.tag == 1 and isinstance(value.value, (int, float)):
        return float(value.value)
    if isinstance(value, cbor.Tag) and value.tag == 0:
        value = value.value
    if not isinstance(value, str):
        raise ValueError("timestamp is not a tdate: %r" % (value,))
    return datetime.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def gps(lat, lon, alt):
    """gps-coordinates [lat, long, alt] as floats; altitude in m above mean sea level."""
    return [float(lat), float(lon), float(0.0 if alt is None or math.isnan(alt) else alt)]


def mac_bytes(mac):
    """'aa:bb:cc:dd:ee:ff' / 'aabbccddeeff' / bytes -> 6 bytes (zeros when unknown)."""
    if isinstance(mac, (bytes, bytearray)):
        return bytes(mac)
    try:
        b = bytes.fromhex(str(mac or '').replace(':', '').replace('-', ''))
    except ValueError:
        b = b''
    return b if len(b) == 6 else bytes(6)


def radio_type(msg_type, channel=None):
    """DroneScout "type" + channel -> radio_types band: Wi-Fi on a 5 GHz channel is SHF,
    everything else (Bluetooth, 2.4 GHz Wi-Fi) UHF."""
    if 'wlan' in str(msg_type or '').lower() or 'wifi' in str(msg_type or '').lower():
        try:
            if int(channel) > 14:
                return RADIO_SHF
        except (TypeError, ValueError):
            pass
    return RADIO_UHF


def csrid_object(message_type, members):
    body = {'message_type': message_type}
    body.update(members)
    return {'application': APPLICATION, 'version': VERSION, LABELS[message_type]: body}


def brid_forwarding(finder_rid, finder_mac, t, position, radio, brid_mac, brid_message):
    return csrid_object(BRID_FORWARDING, {
        'mac_address': mac_bytes(finder_mac), 'id': finder_rid, 'timestamp': tdate(t),
        'gps': gps(*position), 'radio_type': radio,
        'broadcast_mac_address': mac_bytes(brid_mac), 'broadcast_message': bytes(brid_message)})


def finder_registration(finder_rid, finder_mac, t, position):
    return csrid_object(FINDER_REGISTRATION, {
        'mac_address': mac_bytes(finder_mac), 'id': finder_rid, 'timestamp': tdate(t), 'gps': gps(*position)})


def finder_location(finder_rid, finder_mac, t, position):
    return csrid_object(FINDER_LOCATION, {
        'mac_address': mac_bytes(finder_mac), 'id': finder_rid, 'timestamp': tdate(t), 'gps': gps(*position)})


def sdsp_response(sdsp_mac, sdsp_id, finder_rid, proxy_status=PROXY_FORWARD,
                  update_interval=DEFAULT_UPDATE_INTERVAL_S):
    return csrid_object(SDSP_RESPONSE, {
        'mac_address': mac_bytes(sdsp_mac), 'sdsp_id': sdsp_id, 'id': finder_rid,
        'proxy_status_type': proxy_status, 'update_interval': int(update_interval)})


def sdsp_heartbeat(sdsp_mac, sdsp_id, t):
    return csrid_object(SDSP_HEARTBEAT, {'mac_address': mac_bytes(sdsp_mac), 'sdsp_id': sdsp_id, 'timestamp': tdate(t)})


def parse(obj):
    """CSRID_Object (decoded CBOR) -> (message type, members). ValueError when it isn't one."""
    if not isinstance(obj, dict) or obj.get('application') != APPLICATION:
        raise ValueError("not a DRIP-CSRID object")
    found = [(TYPES[k], v) for k, v in obj.items() if k in TYPES]
    if len(found) != 1 or not isinstance(found[0][1], dict):
        raise ValueError("expected exactly one CS-RID message in the object, found %d" % len(found))
    mtype, members = found[0]
    if members.get('message_type', mtype) != mtype:
        raise ValueError("message_type %r doesn't match the label %s" % (members.get('message_type'), LABELS[mtype]))
    return mtype, members


def encode(obj, key=None, kid=None):
    """CSRID_Object -> bytes: COSE_Mac0 when there is a key, else bare CBOR."""
    payload = cbor.dumps(obj)
    if key is None:
        return payload
    return cbor.mac0(payload, key, kid.encode('utf-8') if isinstance(kid, str) else kid)


def decode(data, key=None):
    """bytes -> (message type, members). With a key the data must be a COSE_Mac0 with a valid MAC."""
    if key is not None:
        data = cbor.mac0_verify(data, key)
    return parse(cbor.loads(data))
