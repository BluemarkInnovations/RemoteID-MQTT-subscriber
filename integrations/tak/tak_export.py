# (c) Bluemark Innovations BV
# MIT license
"""Send CoT to a TAK Server (or TAK clients via multicast) like an ordinary TAK client.

    tls://host:8089   TAK Server streaming port, client certificate (normal setup)
    tcp://host:8087   TAK Server without TLS (lab only)
    udp://239.2.3.1:6969  SA multicast (mesh, no server)

While the connection is down, updates are dropped and counted: they would be
stale by the time the connection is back. The client reconnects with backoff.
"""

import socket
import ssl
import threading
import time
from urllib.parse import urlparse

from integrations.tak import cot
from modules import exporters


class TakClient:
    def __init__(self, url, certfile=None, keyfile=None, key_password=None, cafile=None,
                 check_hostname=True, ping_uid='dronescout-gateway', ping_interval_s=60.0,
                 presence=True, presence_callsign='DroneScout gateway', presence_position=None,
                 presence_settle_s=1.0,
                 connect_timeout_s=10.0, multicast_ttl=1, log=print):
        u = urlparse(url)
        if u.scheme not in ('tls', 'tcp', 'udp') or not u.hostname or not u.port:
            raise ValueError("TAK url must be tls://host:port, tcp://host:port or udp://host:port, not %r" % url)
        self.scheme, self.host, self.port = u.scheme, u.hostname, u.port
        self.ping_uid = ping_uid
        self.presence = presence                      # announce the gateway on every connection
        self.presence_callsign = presence_callsign
        self.presence_position = presence_position
        # pause after the presence event: FreeTAKServer reads the first 1024 bytes of a
        # connection as the client's identity and fails when the next event is in them
        self.presence_settle_s = presence_settle_s
        self.ping_interval_s = ping_interval_s
        self.connect_timeout_s = connect_timeout_s
        self.multicast_ttl = multicast_ttl
        self.log = log
        self.sent = 0
        self.dropped = 0
        self.connects = 0
        self._sock = None
        self._lock = threading.Lock()
        self._backoff_s = 1.0
        self._next_attempt = 0.0
        self._last_send = 0.0
        self._closed = False
        self._ssl_context = None
        if self.scheme == 'tls':
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2
            if cafile:
                ctx.load_verify_locations(cafile)
            else:
                ctx.load_default_certs()
            ctx.check_hostname = bool(check_hostname)
            if certfile:
                ctx.load_cert_chain(certfile, keyfile, key_password)
            self._ssl_context = ctx
        self._keepalive = None
        if self.scheme != 'udp':
            self._keepalive = threading.Thread(target=self._keepalive_loop, name='tak-keepalive', daemon=True)
            self._keepalive.start()

    @property
    def connected(self):
        return self._sock is not None

    # --- connection ----------------------------------------------------------

    def _connect(self):
        if self.scheme == 'udp':
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.multicast_ttl)
            s.connect((self.host, self.port))
            return s
        raw = socket.create_connection((self.host, self.port), timeout=self.connect_timeout_s)
        try:
            if self._ssl_context is not None:
                s = self._ssl_context.wrap_socket(raw, server_hostname=self.host)
            else:
                s = raw
            s.settimeout(self.connect_timeout_s)   # sends must not block the exporter forever
        except Exception:
            raw.close()
            raise
        return s

    def _ensure_connected(self):
        if self._sock is not None:
            return True
        now = time.monotonic()
        if self._closed or now < self._next_attempt:
            return False
        try:
            self._sock = self._connect()
        except (OSError, ssl.SSLError) as e:
            self.log("TAK: connecting to %s://%s:%d failed: %s (retry in %.0f s)"
                     % (self.scheme, self.host, self.port, e, self._backoff_s))
            self._next_attempt = now + self._backoff_s
            self._backoff_s = min(self._backoff_s * 2, 60.0)
            return False
        if self.presence and self.scheme != 'udp':
            try:
                self._sock.sendall(cot.presence_event(self.ping_uid, self.presence_callsign,
                                                      self.presence_position))
            except (OSError, ssl.SSLError) as e:
                self._drop_connection(e)
                return False
            if self.presence_settle_s:
                time.sleep(self.presence_settle_s)
        self.connects += 1
        self._backoff_s = 1.0
        self.log("TAK: connected to %s://%s:%d" % (self.scheme, self.host, self.port))
        return True

    def _drop_connection(self, reason):
        s, self._sock = self._sock, None
        if s is not None:
            self.log("TAK: connection lost: %s" % reason)
            try:
                s.close()
            except OSError:
                pass
        self._next_attempt = time.monotonic() + self._backoff_s

    def _drain_locked(self):
        """Read and discard what the server sends (other users' positions, ping replies),
        without blocking. Without this the server's send buffer for us fills up. Also
        notices a connection the server closed. Call with self._lock held."""
        s = self._sock
        if s is None or self.scheme == 'udp':
            return
        try:
            s.setblocking(False)
            while True:
                try:
                    data = s.recv(65536)
                except (ssl.SSLWantReadError, ssl.SSLWantWriteError, BlockingIOError):
                    return
                if not data:
                    self._drop_connection("closed by the server")
                    return
        except (OSError, ssl.SSLError) as e:
            self._drop_connection(e)
        finally:
            if self._sock is s:
                s.settimeout(self.connect_timeout_s)

    # --- sending ---------------------------------------------------------------

    def send(self, data):
        """Send one CoT event (bytes). Returns False when it was dropped."""
        with self._lock:
            self._drain_locked()
            if not self._ensure_connected():
                self.dropped += 1
                return False
            try:
                if self.scheme == 'udp':
                    self._sock.send(data)
                else:
                    self._sock.sendall(data)
            except (OSError, ssl.SSLError) as e:
                self._drop_connection(e)
                self.dropped += 1
                return False
            self.sent += 1
            self._last_send = time.monotonic()
            return True

    def _keepalive_loop(self):
        """Every second: read what the server sent; ping when nothing was sent for a while."""
        while not self._closed:
            time.sleep(1.0)
            with self._lock:
                self._drain_locked()
            if (self.ping_interval_s and self._sock is not None
                    and time.monotonic() - self._last_send >= self.ping_interval_s):
                self.send(cot.ping_event(self.ping_uid))

    def close(self):
        self._closed = True
        with self._lock:
            s, self._sock = self._sock, None
        if s is not None:
            try:
                s.close()
            except OSError:
                pass


class TAKExporter(exporters.Exporter):
    """Exporter: decoded messages -> CoT -> TAK."""
    name = "tak"

    def __init__(self, client, settings=None, min_interval_s=1.0, send_aircraft=True,
                 send_operator=True, send_sensors=True, sensor_positions=None, clock=time.monotonic):
        self.client = client
        self.settings = dict(settings or {})
        self.min_interval_s = min_interval_s
        self.send_aircraft = send_aircraft
        self.send_operator = send_operator
        self.send_sensors = send_sensors
        self.sensor_positions = dict(sensor_positions or {})   # sensor ID -> (lat, lon[, alt]); fixed ones
        self.sensor_status = {}
        self.rate_limited = 0
        self._last_sent = {}
        self._clock = clock

    def _send(self, uid, build):
        """Send build() (CoT bytes, or None: nothing to send) unless uid was sent within
        min_interval_s. The rate limit is checked first: most updates are dropped by it, and
        building the XML is the expensive part."""
        now = self._clock()
        last = self._last_sent.get(uid)
        if last is not None and now - last < self.min_interval_s:
            self.rate_limited += 1
            return
        data = build()
        if data is None:
            return
        self._last_sent[uid] = now
        if len(self._last_sent) > 10000:      # forget objects not seen for a while
            cutoff = now - 600
            self._last_sent = {k: v for k, v in self._last_sent.items() if v >= cutoff}
        self.client.send(data)

    def on_remote_id(self, msg):
        self._send(cot.drone_uid(msg), lambda: cot.drone_event(msg, self.settings))
        if self.send_operator:
            self._send('dronescout-op-' + cot.drone_id(msg), lambda: cot.operator_event(msg, self.settings))

    def on_aircraft(self, msg):
        if self.send_aircraft:
            self._send(cot.aircraft_uid(msg), lambda: cot.aircraft_event(msg, self.settings))

    def _send_sensor(self, sensor_id):
        position = self.sensor_positions.get(sensor_id)
        event = cot.sensor_event(sensor_id, position, self.sensor_status.get(sensor_id), self.settings)
        if event is not None:
            self.client.send(event)       # rare, no rate limit

    def on_status(self, status_json):
        sensor_id = status_json.get('sensor ID')
        self.sensor_status[sensor_id] = status_json
        if self.send_sensors:
            self._send_sensor(sensor_id)

    def on_location(self, location_json):
        sensor_id = location_json.get('sensor ID')
        try:
            lat, lon = float(location_json.get('latitude')), float(location_json.get('longitude'))
        except (TypeError, ValueError):
            return
        if lat == 0.0 and lon == 0.0:
            return
        self.sensor_positions[sensor_id] = (lat, lon, location_json.get('altitude MSL'))
        if self.send_sensors:
            self._send_sensor(sensor_id)

    def close(self):
        self.client.close()
