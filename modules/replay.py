# (c) Bluemark Innovations BV
# MIT license
"""Turn the subscriber's CSV logs back into DroneScout MQTT messages, for replay and load tests.

RemoteID rows become "data" messages with the UASdata block re-encoded (the inverse of
decode.decode_uas_data), aircraft rows "aircraft" messages, status rows "status" messages.
Decoding a replayed message gives the values of the CSV row: the replay of a real log
is an end-to-end test of the outputs with real data.

Not reconstructed: the serial number lookup ("extra"), the SBS lines of aircraft and the
WLAN radio / SDR details of status messages.
"""

import base64
import csv
import heapq
import math
import os

from modules import decode

NAN = float('nan')


def _f(v, unknown):
    """CSV value -> float; '' / NaN -> the sensor's 'unknown' value."""
    if v == '' or v is None:
        return unknown
    v = float(v)
    return unknown if math.isnan(v) else v


def _i(v, default=0):
    if v == '' or v is None:
        return default
    v = float(v)
    return default if math.isnan(v) else int(v)


def _ascii(text, size):
    return str(text).encode('ascii', 'replace')[:size].ljust(size, b'\x00')


def _id_bytes(text, id_type, size):
    """Basic ID column -> bytes: text for serial / CAA IDs, else hex (as log_remote_id writes it)."""
    if id_type in (1, 2):
        return _ascii(text, size)
    try:
        return bytes.fromhex(str(text))[:size].ljust(size, b'\x00')
    except ValueError:
        return _ascii(text, size)


def uas_data(row):
    """A RemoteID CSV row (dict) -> UASdata bytes (914)."""
    p = bytearray(decode.UASDATA_MIN_LENGTH)
    valid = bytearray(22)
    for n, offset in enumerate(decode.BASIC_ID_OFFSETS):
        if row['BasicID_%d' % n] != '':
            id_type = _i(row['IDType_%d' % n])
            decode._BASIC_ID.pack_into(p, offset, _i(row['UAType_%d' % n]), id_type,
                                       _id_bytes(row['BasicID_%d' % n], id_type, 21))
            valid[n] = 1
    if row['Status'] != '':
        decode._LOCATION.pack_into(
            p, decode.LOCATION_OFFSET, _i(row['Status']), _f(row['Direction'], 361.0),
            _f(row['SpeedHorizontal'], 255.0), _f(row['SpeedVertical'], 63.0), _f(row['Latitude'], 0.0),
            _f(row['Longitude'], 0.0), _f(row['AltitudeBaro'], -1000.0), _f(row['AltitudeGeo'], -1000.0),
            _i(row['HeightType']), _f(row['Height'], -1000.0), _i(row['HorizAccuracy']), _i(row['VertAccuracy']),
            _i(row['BaroAccuracy']), _i(row['SpeedAccuracy']), _i(row['TSAccuracy']),
            _f(row['TimeStamp Location'], 0.0))
        valid[2] = 1
    for page in range(decode.AUTH_PAGES):
        if row.get('AuthPage_%d_DataPage' % page, '') == '':
            continue
        data = bytes.fromhex(str(row['AuthPage_%d_Data' % page]))
        if page == 0:
            decode._AUTH_PAGE.pack_into(p, decode.AUTH_OFFSET, _i(row['AuthPage_0_DataPage']),
                                        _i(row['AuthPage_0_AuthType']), _i(row['AuthPage_0_LastPageIndex']),
                                        _i(row['AuthPage_0_Length']), _i(row['AuthPage_0_Timestamp']),
                                        data[:23].ljust(23, b'\x00'))
        else:
            decode._AUTH_PAGE.pack_into(p, decode.AUTH_OFFSET + decode.AUTH_PAGE_SIZE * page,
                                        _i(row['AuthPage_%d_DataPage' % page]),
                                        _i(row['AuthPage_%d_AuthType' % page]), 0, 0, 0, data[:23].ljust(23, b'\x00'))
        valid[3 + page] = 1
    if row['Desc'] != '' or _i(row['DescType']) != 0:
        decode._SELF_ID.pack_into(p, decode.SELFID_OFFSET, _i(row['DescType']), _ascii(row['Desc'], 23))
        valid[19] = 1
    if row['OperatorLocationType'] != '':
        decode._SYSTEM.pack_into(
            p, decode.SYSTEM_OFFSET, _i(row['OperatorLocationType']), _i(row['ClassificationType']),
            _f(row['OperatorLatitude'], 0.0), _f(row['OperatorLongitude'], 0.0), _i(row['AreaCount']),
            _i(row['AreaRadius']), _f(row['AreaCeiling'], -1000.0), _f(row['AreaFloor'], -1000.0),
            _i(row['CategoryEU']), _i(row['ClassEU']), _f(row['OperatorAltitudeGeo'], -1000.0),
            _i(row['TimeStamp System']))
        valid[20] = 1
    if row['OperatorIdType'] != '':
        decode._OPERATOR_ID.pack_into(p, decode.OPERATORID_OFFSET, _i(row['OperatorIdType']),
                                      _ascii(row['OperatorId'], 20))
        valid[21] = 1
    p[decode.VALID_OFFSET:decode.VALID_OFFSET + 22] = valid
    return bytes(p)


def remote_id_message(row):
    data = {'sensor ID': row['sensor ID'], 'RSSI': row['RSSI'], 'channel': _i(row['channel']),
            'timestamp': _i(row['timestamp']), 'MAC address': row['MAC address'], 'type': row['type'],
            'UASdata': base64.b64encode(uas_data(row)).decode('ascii')}
    if row.get('raw data'):
        # + a terminator: the logs hold the frame without it, the decoder / CSV writer remove one
        data['raw'] = base64.b64encode(bytes.fromhex(str(row['raw data'])) + b'\x00').decode('ascii')
    return {'protocol': 1.0, 'data': data}


_AIRCRAFT_INT = ('timestamp', 'frequency MHz', 'address type', 'emergency status', 'flight state', 'category',
                 'altitude', 'altitude type', 'speed', 'track', 'vertical rate', 'message count')


def aircraft_message(row):
    a = {}
    for k, v in row.items():
        if k in ('time (of timestamp)', 'raw data') or (v == '' and k != 'flight') or (isinstance(v, float) and math.isnan(v)):
            continue
        a[k] = int(v) if k in _AIRCRAFT_INT and isinstance(v, float) and v.is_integer() else v
    if row.get('raw data'):
        a['raw'] = base64.b64encode(bytes.fromhex(str(row['raw data'])) + b'\x00').decode('ascii')
    return {'protocol': 1.0, 'aircraft': a}


def status_message(row):
    s = {k: (int(v) if k == 'timestamp' else v) for k, v in row.items()
         if v != '' and not (isinstance(v, float) and math.isnan(v)) and not k.startswith('time (')}
    return {'protocol': 1.0, 'status': s}


def section(msg):
    """The data / aircraft / status object of a message."""
    return next(v for k, v in msg.items() if k != 'protocol')


def retime(msg, timestamp_ms):
    """Set the message time (e.g. to now, so receivers don't treat replayed data as stale)."""
    section(msg)['timestamp'] = int(timestamp_ms)
    return msg


def kind_of(path):
    name = os.path.basename(path)
    for kind in ('remoteID', 'aircraft', 'status'):
        if name.endswith('_%s_log.csv' % kind):
            return kind
    return None


_BUILD = {'remoteID': remote_id_message, 'aircraft': aircraft_message, 'status': status_message}


def read(path):
    """(timestamp ms, MQTT message dict) for every row of one CSV log."""
    build = _BUILD[kind_of(path)]
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f, quoting=csv.QUOTE_NONNUMERIC)
        header = next(reader)
        seen = set()
        for i, name in enumerate(header):           # remoteID logs have "type" twice: the 2nd is the SN lookup's
            if name in seen:
                header[i] = 'extra ' + name
            seen.add(header[i])
        for values in reader:
            row = dict(zip(header, values))
            try:
                msg = build(row)
            except (ValueError, KeyError, TypeError):
                continue
            ts = section(msg).get('timestamp')
            if ts:
                yield int(ts), msg


def messages(paths, align=True):
    """All rows of several logs merged in time order. align: shift every file to start at the
    same moment (logs from different days played at once, e.g. a RemoteID and an ADS-B log)."""
    streams = []
    for n, path in enumerate(paths):
        if kind_of(path) is None:
            raise ValueError("%s: not a remoteID / aircraft / status CSV log" % path)
        it = read(path)
        first = next(it, None)
        if first is None:
            continue
        offset = first[0] if align else 0

        def shifted(first=first, it=it, offset=offset, n=n):
            yield first[0] - offset, n, first[1]
            for ts, msg in it:
                yield ts - offset, n, msg
        streams.append(shifted())
    for ts, _, msg in heapq.merge(*streams, key=lambda x: (x[0], x[1])):
        yield ts, msg
