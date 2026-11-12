# (c) Bluemark Innovations BV
# MIT license
"""Minimal CBOR (RFC 8949) and COSE_Mac0 (RFC 9052, HMAC 256/256) for the CS-RID Finder.

Standard library only, so the gateway runs on the sensor without extra packages. Enough
for the CS-RID messages: unsigned / negative integers, byte and text strings, arrays,
maps, tags, float64, true / false / null. Not a general CBOR library: no indefinite
lengths, no half floats on output (the decoder reads them).

The reference SDSP (sdsp/) uses the independent cbor2 and pycose libraries instead, so
the tests and the Docker setup check this code against them.
"""

import hashlib
import hmac
import struct


class Tag:
    """A CBOR tag (major type 6) around a value."""
    __slots__ = ('tag', 'value')

    def __init__(self, tag, value):
        self.tag = tag
        self.value = value

    def __eq__(self, other):
        return isinstance(other, Tag) and (self.tag, self.value) == (other.tag, other.value)

    def __repr__(self):
        return 'Tag(%d, %r)' % (self.tag, self.value)


def _head(major, n):
    if n < 24:
        return bytes([(major << 5) | n])
    if n < 0x100:
        return bytes([(major << 5) | 24, n])
    if n < 0x10000:
        return bytes([(major << 5) | 25]) + struct.pack('>H', n)
    if n < 0x100000000:
        return bytes([(major << 5) | 26]) + struct.pack('>I', n)
    return bytes([(major << 5) | 27]) + struct.pack('>Q', n)


def dumps(value):
    """Python value -> CBOR bytes. Maps keep their insertion order."""
    if value is False:
        return b'\xf4'
    if value is True:
        return b'\xf5'
    if value is None:
        return b'\xf6'
    if isinstance(value, int):
        if value >= 0:
            return _head(0, value)
        return _head(1, -1 - value)
    if isinstance(value, float):
        return b'\xfb' + struct.pack('>d', value)
    if isinstance(value, (bytes, bytearray)):
        return _head(2, len(value)) + bytes(value)
    if isinstance(value, str):
        data = value.encode('utf-8')
        return _head(3, len(data)) + data
    if isinstance(value, (list, tuple)):
        return _head(4, len(value)) + b''.join(dumps(v) for v in value)
    if isinstance(value, dict):
        return _head(5, len(value)) + b''.join(dumps(k) + dumps(v) for k, v in value.items())
    if isinstance(value, Tag):
        return _head(6, value.tag) + dumps(value.value)
    raise TypeError("can't encode %r as CBOR" % type(value))


class _Reader:
    def __init__(self, data):
        self.data = bytes(data)
        self.pos = 0

    def take(self, n):
        if self.pos + n > len(self.data):
            raise ValueError("CBOR data truncated")
        out = self.data[self.pos:self.pos + n]
        self.pos += n
        return out

    def item(self, depth=0):
        if depth > 32:
            raise ValueError("CBOR nested too deeply")
        ib = self.take(1)[0]
        major, info = ib >> 5, ib & 0x1f
        if major == 7:
            if info == 20:
                return False
            if info == 21:
                return True
            if info in (22, 23):
                return None
            if info == 25:
                return _half(struct.unpack('>H', self.take(2))[0])
            if info == 26:
                return struct.unpack('>f', self.take(4))[0]
            if info == 27:
                return struct.unpack('>d', self.take(8))[0]
            raise ValueError("unsupported CBOR simple value %d" % info)
        if info < 24:
            n = info
        elif info == 24:
            n = self.take(1)[0]
        elif info == 25:
            n = struct.unpack('>H', self.take(2))[0]
        elif info == 26:
            n = struct.unpack('>I', self.take(4))[0]
        elif info == 27:
            n = struct.unpack('>Q', self.take(8))[0]
        else:
            raise ValueError("indefinite length CBOR not supported")
        if major == 0:
            return n
        if major == 1:
            return -1 - n
        if major == 2:
            return self.take(n)
        if major == 3:
            return self.take(n).decode('utf-8')
        if major == 4:
            if n > len(self.data):
                raise ValueError("CBOR array too long")
            return [self.item(depth + 1) for _ in range(n)]
        if major == 5:
            if n > len(self.data):
                raise ValueError("CBOR map too long")
            out = {}
            for _ in range(n):
                k = self.item(depth + 1)
                if isinstance(k, (list, dict)):
                    raise ValueError("unsupported CBOR map key")
                out[k] = self.item(depth + 1)
            return out
        return Tag(n, self.item(depth + 1))


def _half(h):
    sign = -1.0 if h & 0x8000 else 1.0
    exp, frac = (h >> 10) & 0x1f, h & 0x3ff
    if exp == 0:
        return sign * frac * 2.0 ** -24
    if exp == 31:
        return sign * float('inf') if frac == 0 else float('nan')
    return sign * (1 + frac / 1024.0) * 2.0 ** (exp - 15)


def loads(data):
    """CBOR bytes -> Python value (tags as Tag). Raises ValueError on bad or trailing data."""
    r = _Reader(data)
    try:
        value = r.item()
    except (UnicodeDecodeError, struct.error) as e:
        raise ValueError("bad CBOR: %s" % e)
    if r.pos != len(r.data):
        raise ValueError("%d bytes after the CBOR item" % (len(r.data) - r.pos))
    return value


# --- COSE_Mac0 --------------------------------------------------------------------------------

COSE_MAC0_TAG = 17
ALG_HMAC_256_256 = 5          # COSE algorithm "HMAC 256/256"
HDR_ALG = 1
HDR_KID = 4


def _mac_structure(protected, payload, external_aad=b''):
    return dumps(['MAC0', protected, external_aad, payload])


def mac0(payload, key, kid=None):
    """COSE_Mac0 (tagged) with HMAC-SHA256 over payload (bytes); kid (bytes) in the
    unprotected header tells the receiver which key to use."""
    protected = dumps({HDR_ALG: ALG_HMAC_256_256})
    tag = hmac.new(key, _mac_structure(protected, payload), hashlib.sha256).digest()
    unprotected = {HDR_KID: kid} if kid is not None else {}
    return dumps(Tag(COSE_MAC0_TAG, [protected, unprotected, payload, tag]))


def mac0_parts(data):
    """COSE_Mac0 bytes -> (kid or None, payload, (protected, tag)) without checking the MAC."""
    obj = loads(data)
    if isinstance(obj, Tag):
        if obj.tag != COSE_MAC0_TAG:
            raise ValueError("CBOR tag %d, not COSE_Mac0" % obj.tag)
        obj = obj.value
    if not (isinstance(obj, list) and len(obj) == 4 and isinstance(obj[0], bytes)
            and isinstance(obj[1], dict) and isinstance(obj[2], bytes) and isinstance(obj[3], bytes)):
        raise ValueError("not a COSE_Mac0 structure")
    protected, unprotected, payload, tag = obj
    return unprotected.get(HDR_KID), payload, (protected, tag)


def mac0_verify(data, key):
    """COSE_Mac0 bytes -> payload; ValueError when the MAC or the algorithm is wrong."""
    _, payload, (protected, tag) = mac0_parts(data)
    headers = loads(protected) if protected else {}
    if headers.get(HDR_ALG) != ALG_HMAC_256_256:
        raise ValueError("COSE algorithm %r, expected HMAC 256/256" % headers.get(HDR_ALG))
    expected = hmac.new(key, _mac_structure(protected, payload), hashlib.sha256).digest()
    if not hmac.compare_digest(expected, tag):
        raise ValueError("wrong MAC")
    return payload
