# (c) Bluemark Innovations BV
# MIT license
"""SAPIENT exporter: every DroneScout sensor becomes a SAPIENT edge node (ASM).

Each sensor gets its own TCP connection to the fusion node / DMM / middleware,
as SAPIENT ties a connection to one node_id:

    connect -> Registration -> (RegistrationAck) -> StatusReport every N s
            -> DetectionReport per track update -> final report with state "lost"

One "pump" thread does all socket work. The exporter callbacks (run by the
exporter thread) only update the track table and the per-node queue of pending
detections, so a slow or unreachable fusion node never blocks MQTT.

Pending detections are kept per object: a newer update of the same object
replaces the queued one. They go out in arrival order, at most one per
detection_gap_s per node (the Dstl Apex middleware rejects detections less
than 80 ms apart on one connection).
"""

import collections
import select
import socket
import ssl
import threading
import time
from urllib.parse import urlparse

from modules import decode
from modules import exporters
from integrations.sapient import sapient_map as smap


class Track:
    __slots__ = ('object_id', 'last_seen', 'last_queued', 'last_report')

    def __init__(self, object_id, now):
        self.object_id = object_id
        self.last_seen = now
        self.last_queued = None
        self.last_report = None


class Node:
    """One DroneScout sensor = one SAPIENT node = one TCP connection."""

    def __init__(self, sensor_id, node_id):
        self.sensor_id = sensor_id
        self.node_id = node_id
        self.sock = None
        self.rx = b''
        self.registered = False        # RegistrationAck accepted (or ack timeout passed)
        self.registration_sent_at = None
        self.next_attempt = 0.0
        self.backoff_s = 1.0
        self.status_json = None
        self.position = None           # (lat, lon[, alt MSL])
        self.status_changed = True
        self.last_heartbeat = None
        self.last_detection = 0.0
        self.pending = collections.OrderedDict()    # key -> SapientMessage (detection)
        self.tracks = {}                             # object key -> Track
        self.sent = 0
        self.dropped = 0


class SAPIENTExporter(exporters.Exporter):
    name = "sapient"

    def __init__(self, url, settings=None, stale_s=10.0, min_interval_s=1.0, detection_gap_s=0.1,
                 heartbeat_s=None, ack_timeout_s=10.0, send_aircraft=True, sensor_positions=None,
                 node_namespace='dronescout', certfile=None, keyfile=None, cafile=None,
                 check_hostname=True, connect_timeout_s=5.0, max_pending=500, log=print,
                 clock=time.monotonic, start=True):
        u = urlparse(url)
        if u.scheme not in ('tcp', 'tls') or not u.hostname or not u.port:
            raise ValueError("SAPIENT url must be tcp://host:port or tls://host:port, not %r" % url)
        self.scheme, self.host, self.port = u.scheme, u.hostname, u.port
        self.settings = dict(settings or {})
        if heartbeat_s is not None:
            self.settings['heartbeat_s'] = heartbeat_s
        self.heartbeat_s = smap._settings(self.settings)['heartbeat_s']
        self.stale_s = stale_s
        self.min_interval_s = min_interval_s
        self.detection_gap_s = detection_gap_s
        self.ack_timeout_s = ack_timeout_s
        self.send_aircraft = send_aircraft
        self.fixed_positions = dict(sensor_positions or {})
        self.node_namespace = node_namespace
        self.connect_timeout_s = connect_timeout_s
        self.max_pending = max_pending
        self.log = log
        self._clock = clock
        self.nodes = {}                 # sensor ID -> Node
        self.rate_limited = 0
        self.tasks_rejected = 0
        self._lock = threading.Lock()
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
                ctx.load_cert_chain(certfile, keyfile)
            self._ssl_context = ctx
        self._thread = None
        if start:
            self._thread = threading.Thread(target=self._pump_loop, name='sapient-pump', daemon=True)
            self._thread.start()

    # --- statistics ---------------------------------------------------------------

    @property
    def sent(self):
        return sum(n.sent for n in self.nodes.values())

    @property
    def dropped(self):
        return sum(n.dropped for n in self.nodes.values())

    # --- exporter callbacks (exporter thread) -------------------------------------------

    def _node(self, sensor_id):
        sensor_id = str(sensor_id)
        node = self.nodes.get(sensor_id)
        if node is None:
            node = Node(sensor_id, smap.node_uuid(sensor_id, self.node_namespace))
            if sensor_id in self.fixed_positions:
                node.position = tuple(self.fixed_positions[sensor_id])
            self.nodes[sensor_id] = node
            self.log("SAPIENT: sensor %s is node %s" % (sensor_id, node.node_id))
        return node

    def _queue_detection(self, sensor_id, key, build):
        """build(node_id, object_id) -> SapientMessage or None."""
        if sensor_id is None:
            return
        now = self._clock()
        with self._lock:
            node = self._node(sensor_id)
            track = node.tracks.get(key)
            if track is None:
                track = node.tracks[key] = Track(smap.new_ulid(), now)
            track.last_seen = now
            if track.last_queued is not None and now - track.last_queued < self.min_interval_s:
                self.rate_limited += 1
                return
            message = build(node.node_id, track.object_id)
            if message is None:
                return
            track.last_queued = now
            track.last_report = message
            if key not in node.pending and len(node.pending) >= self.max_pending:
                node.pending.popitem(last=False)
                node.dropped += 1
            node.pending[key] = message

    def on_remote_id(self, msg):
        key = 'rid-' + decode.drone_id(msg)
        self._queue_detection(msg.sensor_id, key,
                              lambda node_id, object_id: smap.remote_id_detection(node_id, object_id, msg,
                                                                                  settings=self.settings))

    def on_aircraft(self, msg):
        if not self.send_aircraft:
            return
        self._queue_detection(msg.sensor_id, smap.aircraft_key(msg),
                              lambda node_id, object_id: smap.aircraft_detection(node_id, object_id, msg,
                                                                                 settings=self.settings))

    def on_status(self, status_json):
        sensor_id = status_json.get('sensor ID')
        if sensor_id is None:
            return
        with self._lock:
            node = self._node(sensor_id)
            old = node.status_json
            node.status_json = status_json
            changed = old is None or any(old.get(k) != status_json.get(k)
                                         for k in ('status', 'status details', 'WLAN radios', 'SDR receivers'))
            node.status_changed = node.status_changed or changed

    def on_location(self, location_json):
        sensor_id = location_json.get('sensor ID')
        try:
            lat, lon = float(location_json.get('latitude')), float(location_json.get('longitude'))
        except (TypeError, ValueError):
            return
        if sensor_id is None or (lat == 0.0 and lon == 0.0):
            return
        with self._lock:
            node = self._node(sensor_id)
            if str(sensor_id) in self.fixed_positions:
                return
            old = node.position
            node.position = (lat, lon, location_json.get('altitude MSL'))
            if old is None:
                node.status_changed = True

    # --- connection handling (pump thread) ---------------------------------------------

    def _connect(self, node):
        raw = socket.create_connection((self.host, self.port), timeout=self.connect_timeout_s)
        try:
            s = self._ssl_context.wrap_socket(raw, server_hostname=self.host) if self._ssl_context else raw
            s.settimeout(self.connect_timeout_s)
        except Exception:
            raw.close()
            raise
        return s

    def _send(self, node, message):
        try:
            node.sock.sendall(smap.frame(message))
            node.sent += 1
            return True
        except (OSError, ssl.SSLError) as e:
            self._drop(node, e)
            return False

    def _drop(self, node, reason):
        if node.sock is not None:
            self.log("SAPIENT: node %s: connection lost: %s" % (node.sensor_id, reason))
            try:
                node.sock.close()
            except OSError:
                pass
        node.sock = None
        node.rx = b''
        node.registered = False
        node.registration_sent_at = None
        node.next_attempt = self._clock() + node.backoff_s
        node.backoff_s = min(node.backoff_s * 2, 60.0)

    def _ensure_connected(self, node, now):
        if node.sock is not None:
            return True
        if now < node.next_attempt:
            return False
        try:
            node.sock = self._connect(node)
        except (OSError, ssl.SSLError) as e:
            self.log("SAPIENT: node %s: connecting to %s:%d failed: %s (retry in %.0f s)"
                     % (node.sensor_id, self.host, self.port, e, node.backoff_s))
            node.next_attempt = now + node.backoff_s
            node.backoff_s = min(node.backoff_s * 2, 60.0)
            return False
        self.log("SAPIENT: node %s connected to %s://%s:%d" % (node.sensor_id, self.scheme, self.host, self.port))
        if self._send(node, smap.registration(node.node_id, node.sensor_id, node.status_json, self.settings)):
            node.registration_sent_at = now
            node.status_changed = True
        return node.sock is not None

    def _receive(self, node):
        """Read what the fusion node sent, without blocking."""
        s = node.sock
        try:
            while True:
                r, _, _ = select.select([s], [], [], 0)
                if not r and not (isinstance(s, ssl.SSLSocket) and s.pending()):
                    break
                data = s.recv(65536)
                if not data:
                    self._drop(node, "closed by the fusion node")
                    return
                node.rx += data
        except (OSError, ssl.SSLError) as e:
            self._drop(node, e)
            return
        while len(node.rx) >= 4:
            length = int.from_bytes(node.rx[:4], 'little')
            if length > 16 * 1024 * 1024:
                self._drop(node, "invalid message length %d" % length)
                return
            if len(node.rx) < 4 + length:
                break
            data, node.rx = node.rx[4:4 + length], node.rx[4 + length:]
            try:
                self._handle(node, smap.parse(data))
            except Exception as e:                   # DecodeError and bad content
                self.log("SAPIENT: node %s: can't parse a message from the fusion node: %s" % (node.sensor_id, e))
            if node.sock is None:
                return

    def _handle(self, node, message):
        kind = message.WhichOneof('content')
        if kind == 'registration_ack':
            ack = message.registration_ack
            if ack.acceptance:
                if not node.registered:
                    self.log("SAPIENT: node %s registered" % node.sensor_id)
                node.registered = True
                node.backoff_s = 1.0
            else:
                self.log("SAPIENT: node %s: registration rejected: %s"
                         % (node.sensor_id, '; '.join(ack.ack_response_reason) or 'no reason given'))
                self._drop(node, "registration rejected")
                node.next_attempt = self._clock() + 60.0
        elif kind == 'task':
            if message.HasField('destination_id') and message.destination_id not in ('', node.node_id):
                return
            self.tasks_rejected += 1
            self.log("SAPIENT: node %s: task %s (%s) rejected: tasking is not supported"
                     % (node.sensor_id, message.task.task_id, message.task.task_name))
            self._send(node, smap.task_ack(node.node_id, message.task.task_id))
        elif kind == 'error':
            self.log("SAPIENT: node %s: error from the fusion node: %s"
                     % (node.sensor_id, '; '.join(message.error.error_message)))

    def _expire_tracks(self, node, now):
        for key, track in list(node.tracks.items()):
            if now - track.last_seen < self.stale_s:
                continue
            del node.tracks[key]
            node.pending.pop(key, None)
            if track.last_report is not None:
                node.pending[('lost', key)] = smap.lost_detection(node.node_id, track.object_id,
                                                                 track.last_report)

    def pump(self):
        """One round of socket work for every node. Called every 20 ms by the pump thread
        (and directly by tests that pass start=False)."""
        now = self._clock()
        with self._lock:
            nodes = list(self.nodes.values())
        for node in nodes:
            with self._lock:
                self._expire_tracks(node, now)
            if not self._ensure_connected(node, now):
                with self._lock:              # nobody to send to: the updates would be stale anyway
                    if node.pending:
                        node.dropped += len(node.pending)
                        node.pending.clear()
                continue
            self._receive(node)
            if node.sock is None:
                continue
            if not node.registered:
                if node.registration_sent_at is not None and now - node.registration_sent_at >= self.ack_timeout_s:
                    self.log("SAPIENT: node %s: no RegistrationAck within %.0f s, sending anyway"
                             % (node.sensor_id, self.ack_timeout_s))
                    node.registered = True
                else:
                    continue
            if (node.status_changed or node.last_heartbeat is None
                    or now - node.last_heartbeat >= self.heartbeat_s):
                with self._lock:
                    status = smap.status_report(node.node_id, node.status_json, node.position,
                                                info_new=node.status_changed, settings=self.settings)
                    node.status_changed = False
                if self._send(node, status):
                    node.last_heartbeat = now
                else:
                    continue
            if now - node.last_detection >= self.detection_gap_s:
                with self._lock:
                    item = node.pending.popitem(last=False) if node.pending else None
                if item is not None:
                    message = item[1]
                    message.timestamp.CopyFrom(smap._ts(time.time()))     # the send time
                    if self._send(node, message):
                        node.last_detection = now

    def _pump_loop(self):
        while not self._closed:
            try:
                self.pump()
            except Exception as e:           # never let the thread die
                self.log("SAPIENT: pump error: %r" % e)
            time.sleep(0.02)

    def close(self):
        self._closed = True
        if self._thread is not None:
            self._thread.join(timeout=2)
        for node in list(self.nodes.values()):
            if node.sock is not None:
                self._send(node, smap.status_report(node.node_id, node.status_json, node.position,
                                                    goodbye=True, settings=self.settings))
                try:
                    node.sock.close()
                except OSError:
                    pass
                node.sock = None
