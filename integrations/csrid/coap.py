# (c) Bluemark Innovations BV
# MIT license
"""Minimal CoAP (RFC 7252) message encoding for the CS-RID Finder: header, token,
options (delta encoded) and payload. Standard library only; the reference SDSP uses
the independent aiocoap library.
"""

import struct

VERSION = 1
CON, NON, ACK, RST = 0, 1, 2, 3
TYPE_NAMES = {CON: 'CON', NON: 'NON', ACK: 'ACK', RST: 'RST'}

EMPTY = 0
POST = 2
CREATED = 0x41          # 2.01
CHANGED = 0x44          # 2.04
CONTENT = 0x45          # 2.05
BAD_REQUEST = 0x80      # 4.00
UNAUTHORIZED = 0x81     # 4.01
FORBIDDEN = 0x83        # 4.03
NOT_FOUND = 0x84        # 4.04

OPT_URI_HOST = 3
OPT_URI_PATH = 11
OPT_CONTENT_FORMAT = 12

CF_CBOR = 60            # application/cbor
CF_COSE_MAC0 = 97       # application/cose; cose-type="cose-mac0"

# RFC 7252 section 4.8 transmission parameters
ACK_TIMEOUT = 2.0
MAX_RETRANSMIT = 4


def code_text(code):
    return '%d.%02d' % (code >> 5, code & 0x1f)


def is_success(code):
    return code >> 5 == 2


class Message:
    __slots__ = ('mtype', 'code', 'mid', 'token', 'options', 'payload')

    def __init__(self, mtype, code, mid, token=b'', options=(), payload=b''):
        self.mtype = mtype
        self.code = code
        self.mid = mid
        self.token = bytes(token)
        self.options = list(options)        # [(number, bytes)]
        self.payload = bytes(payload)

    def option(self, number):
        for n, v in self.options:
            if n == number:
                return v
        return None

    @property
    def content_format(self):
        v = self.option(OPT_CONTENT_FORMAT)
        return None if v is None else int.from_bytes(v, 'big')

    @property
    def uri_path(self):
        return '/'.join(v.decode('utf-8', 'replace') for n, v in self.options if n == OPT_URI_PATH)

    def __repr__(self):
        return 'Message(%s %s mid=%d token=%s, %d bytes)' % (
            TYPE_NAMES[self.mtype], code_text(self.code), self.mid, self.token.hex(), len(self.payload))


def uint_option(value):
    return value.to_bytes((value.bit_length() + 7) // 8, 'big') if value else b''


def path_options(path):
    return [(OPT_URI_PATH, p.encode('utf-8')) for p in path.strip('/').split('/') if p]


def _ext(n):
    if n < 13:
        return n, b''
    if n < 269:
        return 13, bytes([n - 13])
    return 14, struct.pack('>H', n - 269)


def encode(msg):
    if len(msg.token) > 8:
        raise ValueError("CoAP token longer than 8 bytes")
    out = bytearray([(VERSION << 6) | (msg.mtype << 4) | len(msg.token), msg.code])
    out += struct.pack('>H', msg.mid) + msg.token
    last = 0
    for number, value in sorted(msg.options, key=lambda o: o[0]):
        d, dext = _ext(number - last)
        l, lext = _ext(len(value))
        out.append((d << 4) | l)
        out += dext + lext + value
        last = number
    if msg.payload:
        out.append(0xFF)
        out += msg.payload
    return bytes(out)


def decode(data):
    """bytes -> Message; ValueError when malformed."""
    try:
        return _decode(bytes(data))
    except (IndexError, struct.error):
        raise ValueError("CoAP message truncated")


def _decode(data):
    if len(data) < 4:
        raise ValueError("CoAP message shorter than 4 bytes")
    b0, code, mid = data[0], data[1], struct.unpack('>H', data[2:4])[0]
    if b0 >> 6 != VERSION:
        raise ValueError("CoAP version %d" % (b0 >> 6))
    tkl = b0 & 0x0f
    if tkl > 8:
        raise ValueError("CoAP token length %d" % tkl)
    pos = 4 + tkl
    if pos > len(data):
        raise ValueError("CoAP message truncated")
    token = data[4:pos]
    options = []
    number = 0
    payload = b''
    while pos < len(data):
        b = data[pos]
        pos += 1
        if b == 0xFF:
            payload = data[pos:]
            if not payload:
                raise ValueError("CoAP payload marker without payload")
            break
        values = []
        for nib in (b >> 4, b & 0x0f):
            if nib == 13:
                values.append(data[pos] + 13)
                pos += 1
            elif nib == 14:
                values.append(struct.unpack('>H', data[pos:pos + 2])[0] + 269)
                pos += 2
            elif nib == 15:
                raise ValueError("CoAP option nibble 15")
            else:
                values.append(nib)
        number += values[0]
        if pos + values[1] > len(data):
            raise ValueError("CoAP option truncated")
        options.append((number, data[pos:pos + values[1]]))
        pos += values[1]
    return Message(b0 >> 4 & 3, code, mid, token, options, payload)
