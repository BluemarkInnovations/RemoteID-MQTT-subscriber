# (c) Bluemark Innovations BV
# MIT license
"""Exporter hook: every decoded message goes to every registered output.

An exporter subclasses Exporter and overrides the methods it needs:

    class MyExporter(exporters.Exporter):
        name = "my output"
        def on_remote_id(self, msg):   # decode.RemoteIDMessage
            ...
        def on_aircraft(self, msg):    # decode.AircraftMessage
            ...

    exporters.register(MyExporter())

By default each exporter runs in its own thread with its own queue, so a slow
or unreachable receiver (TAK server, SAPIENT fusion node, ...) never stalls MQTT
reception or the other outputs. When the queue is full the message is dropped
and counted.
"""

import queue
import threading
import time
import traceback


class Exporter:
    name = "exporter"

    def on_remote_id(self, msg):
        """RemoteID "data" message, decode.RemoteIDMessage."""

    def on_aircraft(self, msg):
        """ADS-B / UAT / ADS-L "aircraft" message, decode.AircraftMessage."""

    def on_status(self, status_json):
        """Sensor "status" message (dict as received)."""

    def on_location(self, location_json):
        """Sensor "location" message (dict as received)."""

    def close(self):
        """Called once at shutdown."""


EVENTS = ('on_remote_id', 'on_aircraft', 'on_status', 'on_location')

_STOP = object()


class _Runner:
    """Calls one exporter, directly or from its own thread."""

    def __init__(self, exporter, threaded, queue_size):
        self.exporter = exporter
        self.threaded = threaded
        self.dropped = 0
        self._last_drop_report = 0.0
        self.queue = None
        self.thread = None
        if threaded:
            self.queue = queue.Queue(maxsize=queue_size)
            self.thread = threading.Thread(target=self._loop, name="export-" + exporter.name, daemon=True)
            self.thread.start()

    def _call(self, event, msg):
        try:
            getattr(self.exporter, event)(msg)
        except Exception:
            print("exporter %s: error in %s" % (self.exporter.name, event))
            traceback.print_exc()

    def _loop(self):
        while True:
            item = self.queue.get()
            try:
                if item is _STOP:
                    return
                self._call(*item)
            finally:
                self.queue.task_done()

    def submit(self, event, msg):
        if not self.threaded:
            self._call(event, msg)
            return
        try:
            self.queue.put_nowait((event, msg))
        except queue.Full:
            self.dropped += 1
            now = time.monotonic()
            if now - self._last_drop_report > 10:
                self._last_drop_report = now
                print("exporter %s: queue full, %d messages dropped" % (self.exporter.name, self.dropped))


_runners = []
_lock = threading.Lock()


def register(exporter, threaded=True, queue_size=1000):
    """Add an output. Returns the exporter."""
    with _lock:
        _runners.append(_Runner(exporter, threaded, queue_size))
    return exporter


def registered():
    return [r.exporter for r in _runners]


def publish(event, msg):
    """Hand a decoded message to every exporter. event is one of EVENTS."""
    if event not in EVENTS:
        raise ValueError("unknown exporter event %r" % event)
    for runner in list(_runners):
        runner.submit(event, msg)


def flush():
    """Wait until every threaded exporter has handled what is queued (tests, shutdown)."""
    for runner in list(_runners):
        if runner.threaded:
            runner.queue.join()


def dropped():
    """Dropped messages per exporter name."""
    return {r.exporter.name: r.dropped for r in _runners}


def shutdown():
    """Stop the threads, call close() and forget all exporters."""
    with _lock:
        runners = list(_runners)
        _runners.clear()
    for runner in runners:
        if runner.threaded:
            runner.queue.put(_STOP)
            runner.thread.join(timeout=5)
        try:
            runner.exporter.close()
        except Exception:
            traceback.print_exc()
