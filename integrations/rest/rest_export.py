# (c) Bluemark Innovations BV
# MIT license
"""REST exporter: decoded DroneScout messages -> JSON, POSTed to an HTTP(S) endpoint.

Events are collected for interval_s (default 1 s) and sent as one POST:

    POST <url>
    Content-Type: application/json
    Authorization: Bearer <token>            (or Basic, or an API-key header)

    {"schema_version": 1, "source": "dronescout-gateway", "sent": "...Z",
     "events": [{"type": "drone", ...}, {"type": "aircraft", ...}, ...]}

Delivery: a batch that fails with a network error, HTTP 429 or 5xx is kept and
retried with the next one (with backoff), up to max_queue events; the oldest
events are dropped first. 4xx (except 429) means the request itself is wrong
(401/403: check the credentials): the batch is dropped and the error logged.
Only the Python standard library is used.
"""

import base64
import json
import ssl
import threading
import time
import urllib.error
import urllib.request
from collections import deque

from modules import decode
from modules import exporters
from modules import json_events


def auth_headers(auth=None, token=None, username=None, password=None, header_name=None, header_value=None):
    """HTTP headers for the configured authentication.

    auth: None, 'bearer' (token), 'basic' (username/password) or 'header' (header_name/header_value)."""
    if not auth:
        return {}
    if auth == 'bearer':
        if not token:
            raise ValueError("REST auth 'bearer' needs a token")
        return {'Authorization': 'Bearer ' + token}
    if auth == 'basic':
        if username is None or password is None:
            raise ValueError("REST auth 'basic' needs a username and a password")
        return {'Authorization': 'Basic ' + base64.b64encode(('%s:%s' % (username, password)).encode()).decode()}
    if auth == 'header':
        if not header_name or header_value is None:
            raise ValueError("REST auth 'header' needs a header name and value")
        return {header_name: header_value}
    raise ValueError("REST auth must be None, 'bearer', 'basic' or 'header', not %r" % auth)


class RESTExporter(exporters.Exporter):
    name = "rest"

    def __init__(self, url, headers=None, source='dronescout-gateway', interval_s=1.0, min_interval_s=1.0,
                 send_drones=True, send_aircraft=True, send_status=True, timeout_s=10.0, max_queue=10000,
                 max_batch=500, cafile=None, verify_tls=True, log=print, clock=time.monotonic, start=True):
        if not (url.startswith('http://') or url.startswith('https://')):
            raise ValueError("REST url must start with http:// or https://, not %r" % url)
        self.url = url
        self.headers = {'Content-Type': 'application/json', 'User-Agent': 'dronescout-gateway'}
        self.headers.update(headers or {})
        self.source = source
        self.interval_s = interval_s
        self.min_interval_s = min_interval_s
        self.send_drones = send_drones
        self.send_aircraft = send_aircraft
        self.send_status = send_status
        self.timeout_s = timeout_s
        self.max_batch = max_batch
        self.log = log
        self._clock = clock
        self._context = None
        if url.startswith('https://'):
            self._context = ssl.create_default_context(cafile=cafile)
            if not verify_tls:
                self._context.check_hostname = False
                self._context.verify_mode = ssl.CERT_NONE
        self._queue = deque(maxlen=max_queue)
        self._lock = threading.Lock()
        self._last_queued = {}
        self._backoff_s = 0.0
        self._next_attempt = 0.0
        self.posted_events = 0
        self.requests = 0
        self.failed_requests = 0
        self.dropped = 0
        self.rate_limited = 0
        self.last_status = None
        self._closed = False
        self._thread = None
        if start:
            self._thread = threading.Thread(target=self._loop, name='rest-post', daemon=True)
            self._thread.start()

    # --- exporter callbacks --------------------------------------------------------------

    def _queue_event(self, key, event):
        now = self._clock()
        with self._lock:
            if key is not None:
                last = self._last_queued.get(key)
                if last is not None and now - last < self.min_interval_s:
                    self.rate_limited += 1
                    return
                self._last_queued[key] = now
                if len(self._last_queued) > 20000:
                    cutoff = now - 600
                    self._last_queued = {k: v for k, v in self._last_queued.items() if v >= cutoff}
            if len(self._queue) == self._queue.maxlen:
                self.dropped += 1
            self._queue.append(event)

    def on_remote_id(self, msg):
        if self.send_drones:
            self._queue_event(('drone', msg.sensor_id, decode.drone_id(msg)), json_events.drone_event(msg))

    def on_aircraft(self, msg):
        if self.send_aircraft:
            self._queue_event(('aircraft', msg.sensor_id, msg.get('type'), msg.get('address')),
                              json_events.aircraft_event(msg))

    def on_status(self, status_json):
        if self.send_status:
            self._queue_event(None, json_events.status_event(status_json))

    def on_location(self, location_json):
        if self.send_status:
            self._queue_event(None, json_events.location_event(location_json))

    # --- sending --------------------------------------------------------------------------

    def _post(self, body):
        req = urllib.request.Request(self.url, data=body, headers=self.headers, method='POST')
        with urllib.request.urlopen(req, timeout=self.timeout_s, context=self._context) as resp:
            resp.read()
            return resp.status

    def flush(self):
        """POST what is queued (at most max_batch events per request). Returns the number sent."""
        now = self._clock()
        if now < self._next_attempt:
            return 0
        sent = 0
        while True:
            with self._lock:
                if not self._queue:
                    return sent
                batch = [self._queue.popleft() for _ in range(min(self.max_batch, len(self._queue)))]
            body = json.dumps(json_events.envelope(batch, self.source, time.time()), separators=(',', ':')).encode()
            self.requests += 1
            retry, reason = False, None
            try:
                self.last_status = self._post(body)
            except urllib.error.HTTPError as e:
                self.last_status = e.code
                retry = e.code == 429 or e.code >= 500
                reason = 'HTTP %d %s' % (e.code, e.reason)
                if e.code in (401, 403):
                    reason += ' (check the credentials in rest_config.py)'
            except (urllib.error.URLError, OSError, ValueError) as e:
                self.last_status = None
                retry, reason = True, str(getattr(e, 'reason', e))
            if reason is None:
                self.posted_events += len(batch)
                sent += len(batch)
                self._backoff_s = 0.0
                continue
            self.failed_requests += 1
            if retry:
                with self._lock:                  # put back in front, the oldest drop first when full
                    room = self._queue.maxlen - len(self._queue)
                    keep = batch[-room:] if room < len(batch) else batch
                    self.dropped += len(batch) - len(keep)
                    self._queue.extendleft(reversed(keep))
                self._backoff_s = min(max(self._backoff_s * 2, 1.0), 60.0)
                self._next_attempt = now + self._backoff_s
                self.log("REST: POST to %s failed: %s (retry in %.0f s, %d events waiting)"
                         % (self.url, reason, self._backoff_s, len(self._queue)))
            else:
                self.dropped += len(batch)
                self.log("REST: POST to %s rejected: %s, %d events dropped" % (self.url, reason, len(batch)))
            return sent

    def _loop(self):
        while not self._closed:
            start = time.monotonic()
            try:
                self.flush()
            except Exception as e:                # never let the thread die
                self.log("REST: error: %r" % e)
            time.sleep(max(0.0, self.interval_s - (time.monotonic() - start)))

    def close(self):
        self._closed = True
        if self._thread is not None:
            self._thread.join(timeout=self.interval_s + self.timeout_s)
        self._next_attempt = 0.0
        try:
            self.flush()
        except Exception:
            pass
