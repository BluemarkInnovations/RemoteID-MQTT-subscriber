# (c) Bluemark Innovations BV
# MIT license
"""VMS exporter: drone alarms (vms_alarms.py) as generic events in video management systems.

Backends:
- MilestoneGenericEvents: Milestone XProtect generic events, one text message per event
  over TCP (connect, send, close) or UDP to a generic event data source (default port 1234).
  An XProtect "generic event" matches on the text, a rule then starts recording, moves a
  PTZ camera, raises an alarm, ...
- NxWitnessGenericEvents: Nx Witness / Nx Meta / DW Spectrum / Hanwha Wisenet WAVE
  generic events (REST API v4, bearer token from a login session). drone_detected starts
  a prolonged event and drone_lost stops it, so the VMS sees one event with a duration.

Events are sent from one worker thread with a queue, so a slow or unreachable VMS
never delays the others or the MQTT reception.
"""

import json
import re
import queue
import socket
import ssl
import threading
import urllib.error
import urllib.request

from modules import exporters
from integrations.vms import vms_alarms

DEFAULT_MILESTONE_TEMPLATE = ('DRONESCOUT {event} id={id} sensor={sensor} lat={lat} lon={lon} height={height} '
                              'pilot_lat={pilot_lat} pilot_lon={pilot_lon} duration={duration} {details}')
DEFAULT_CAPTION = {
    'drone_detected': 'Drone detected: {id}',
    'drone_update': 'Drone still present: {id}',
    'drone_lost': 'Drone gone: {id}',
    'aircraft_detected': 'Aircraft detected: {id} {callsign}',
    'aircraft_lost': 'Aircraft gone: {id} {callsign}',
    'sensor_degraded': 'DroneScout sensor {sensor} degraded',
    'sensor_normal': 'DroneScout sensor {sensor} normal again',
}
DEFAULT_DESCRIPTION = ('{event} {id} seen by sensor {sensor} at {lat}, {lon}, height {height} m, speed {speed} m/s. '
                       'Pilot: {pilot_lat}, {pilot_lon}. Operator ID: {operator}. {details}')


class MilestoneGenericEvents:
    """Text messages to a Milestone XProtect generic event data source."""

    name = 'milestone'

    def __init__(self, host, port=1234, protocol='tcp', template=DEFAULT_MILESTONE_TEMPLATE, separator='\r\n',
                 encoding='utf-8', timeout_s=5.0, read_echo=True):
        if protocol not in ('tcp', 'udp'):
            raise ValueError("Milestone protocol must be 'tcp' or 'udp'")
        self.address = (host, int(port))
        self.protocol = protocol
        self.template = template
        self.separator = separator
        self.encoding = encoding
        self.timeout_s = timeout_s
        self.read_echo = read_echo

    def text(self, event):
        # 'key=' pairs without a value are left out, and no double / trailing spaces
        return ' '.join(w for w in vms_alarms.render(self.template, event).split() if not re.fullmatch(r'\w+=', w))

    def send(self, event):
        data = (self.text(event) + self.separator).encode(self.encoding, 'replace')
        if self.protocol == 'udp':
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.sendto(data, self.address)
            return
        with socket.create_connection(self.address, timeout=self.timeout_s) as s:
            s.sendall(data)
            if self.read_echo:              # the data source echoes / acknowledges; wait for it, ignore content
                try:
                    s.recv(4096)
                except socket.timeout:
                    pass

    def close(self):
        pass


class NxWitnessGenericEvents:
    """Generic events to an Nx Witness (6.x) server: POST /rest/v4/events/generic."""

    name = 'nx'

    def __init__(self, url, username, password, source='DroneScout', camera_ids=(), caption=None,
                 description=DEFAULT_DESCRIPTION, cafile=None, verify_tls=True, timeout_s=10.0, token=None):
        if not url.startswith('https://') and not url.startswith('http://'):
            raise ValueError("Nx url must start with https://")
        self.url = url.rstrip('/')
        self.username = username
        self.password = password
        self.source = source
        self.camera_ids = list(camera_ids)
        self.caption = dict(DEFAULT_CAPTION, **(caption or {}))
        self.description = description
        self.timeout_s = timeout_s
        self._token = token                      # e.g. a long-lived token made in the Nx web admin
        self._login_with_password = token is None
        self._context = None
        if url.startswith('https://'):
            self._context = ssl.create_default_context(cafile=cafile)
            if not verify_tls:
                self._context.check_hostname = False
                self._context.verify_mode = ssl.CERT_NONE

    def _request(self, method, path, body, token=None):
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        if token:
            headers['Authorization'] = 'Bearer ' + token
        req = urllib.request.Request(self.url + path, data=json.dumps(body).encode(), headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=self.timeout_s, context=self._context) as resp:
            data = resp.read()
        return json.loads(data) if data else None

    def login(self):
        reply = self._request('POST', '/rest/v3/login/sessions', {'username': self.username, 'password': self.password})
        self._token = reply['token']

    def body(self, event):
        state = {'drone_detected': 'started', 'drone_lost': 'stopped',
                 'aircraft_detected': 'started', 'aircraft_lost': 'stopped'}.get(event['event'], 'instant')
        caption = self.caption.get(event['event'], '{event} {id}')
        body = {'source': self.source,
                'caption': ' '.join(vms_alarms.render(caption, event).split()),
                'description': ' '.join(vms_alarms.render(self.description, event).split()),
                'state': state}
        if self.camera_ids:
            body['deviceIds'] = self.camera_ids
        return body

    def send(self, event):
        if self._token is None:
            self.login()
        try:
            self._request('POST', '/rest/v4/events/generic', self.body(event), self._token)
        except urllib.error.HTTPError as e:
            if e.code != 401 or not self._login_with_password:
                raise
            self.login()                         # session expired or server restarted
            self._request('POST', '/rest/v4/events/generic', self.body(event), self._token)

    def close(self):
        pass


class VMSExporter(exporters.Exporter):
    name = "vms"

    def __init__(self, backends, alarms=None, max_queue=1000, log=print, start=True):
        if not backends:
            raise ValueError("no VMS configured (vms_milestone_host / vms_nx_url)")
        self.backends = list(backends)
        self.alarms = alarms or vms_alarms.Alarms()
        self.log = log
        self.sent = {b.name: 0 for b in self.backends}
        self.failed = {b.name: 0 for b in self.backends}
        self.dropped = 0
        self._lock = threading.Lock()            # alarms are updated from the exporter and tick threads
        self._queue = queue.Queue(maxsize=max_queue)
        self._closed = False
        self._stop = threading.Event()
        self._threads = []
        if start:
            self._threads = [threading.Thread(target=self._worker, name='vms-send', daemon=True),
                             threading.Thread(target=self._ticker, name='vms-tick', daemon=True)]
            for t in self._threads:
                t.start()

    def _emit(self, events):
        for event in events:
            try:
                self._queue.put_nowait(event)
            except queue.Full:
                self.dropped += 1

    def on_remote_id(self, msg):
        with self._lock:
            events = self.alarms.drone(msg)
        self._emit(events)

    def on_aircraft(self, msg):
        with self._lock:
            events = self.alarms.aircraft_message(msg)
        self._emit(events)

    def on_status(self, status_json):
        with self._lock:
            events = self.alarms.status(status_json)
        self._emit(events)

    def tick(self):
        with self._lock:
            events = self.alarms.tick()
        self._emit(events)

    def deliver(self, event):
        """Send one event to every backend (worker thread, and tests)."""
        for b in self.backends:
            try:
                b.send(event)
                self.sent[b.name] += 1
            except Exception as e:
                self.failed[b.name] += 1
                n = self.failed[b.name]
                if n in (1, 10, 100) or n % 1000 == 0:
                    self.log("VMS: %s: %s not sent: %s (%d failures)" % (b.name, event['event'], e, n))

    def drain(self):
        """Deliver everything queued (tests, close)."""
        while True:
            try:
                event = self._queue.get_nowait()
            except queue.Empty:
                return
            self.deliver(event)

    def _worker(self):
        while not self._closed:
            try:
                event = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            self.deliver(event)

    def _ticker(self):
        while not self._stop.wait(1.0):
            try:
                self.tick()
            except Exception as e:
                self.log("VMS: error: %r" % e)

    def close(self):
        self._closed = True
        self._stop.set()
        for t in self._threads:
            t.join(timeout=2)
        self.drain()
        for b in self.backends:
            b.close()
