#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Load test of the gateways: replay CSV logs at N x speed through mqtt_sub's message handling
(JSON parsing, decoding) into every output at once, in one process, and measure per output:
messages handled, queue drops, hand-off latency (publish -> handler start) and CPU time.

One process with all outputs is the worst case: in production each gateway is its own process.
The outputs send to the Docker test servers (start them first, see integrations/README.md);
an output whose server isn't running still has to cope (reconnects, errors).

    python3 tools/load_test.py --speed 10 --copies 3 --duration 120
    python3 tools/load_test.py --outputs asterix,rest --speed 50 logs/<remoteID log>.csv logs/<aircraft log>.csv

The replayed messages get the current time as timestamp, so the receivers don't drop them as old.
Sensors in the logs that send no location get a manual position (config.sensor_locations).
"""

import argparse
import base64
import glob
import json
import os
import resource
import sys
import threading
import time
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import config                                        # noqa: E402
import mqtt_sub                                      # noqa: E402
from modules import exporters, replay                # noqa: E402

TEST_KEY = '5ad1c1b1d5a0e1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f80910'   # csrid/docker/finders.json

# settings per output, pointing to the Docker test servers on localhost
SETTINGS = {
    'tak': dict(tak_url='tcp://127.0.0.1:8087', tak_check_hostname=False),
    'sapient': dict(sapient_url='tcp://127.0.0.1:5020'),
    'asterix': dict(asterix_destinations=['127.0.0.1:8600'], asterix_categories=[21, 23, 62, 65, 129],
                    asterix_gateway_sic=100),
    'rest': dict(rest_url='http://127.0.0.1:8080/dronescout/events', rest_auth='bearer', rest_token='change-me'),
    'udp': dict(udp_destinations=['127.0.0.1:8700']),
    'gdl90': dict(gdl90_destinations=['127.0.0.1:4000']),
    'flarm': dict(flarm_tcp_port=None, flarm_udp_destinations=['127.0.0.1:10110']),
    'vms': dict(vms_milestone_host='127.0.0.1', vms_milestone_port=1234, vms_nx_url='https://127.0.0.1:7001',
                vms_nx_username='admin', vms_nx_password='DroneScout-Test-1', vms_nx_verify_tls=False),
    'csrid': dict(csrid_sdsp_url='coap://127.0.0.1:5683/csrid', csrid_finder_id='dronescout-finder-1',
                  csrid_key_hex=TEST_KEY),
}


def make(name, position):
    cfg = types.SimpleNamespace(**SETTINGS[name])
    if name == 'flarm':
        cfg.flarm_position = position
    module = __import__('integrations.%s.%s_gateway' % (name, name), fromlist=['make_exporter'])
    return module.make_exporter(cfg)


class Meter:
    """Per exporter: hand-off latencies and CPU time of its handlers."""

    def __init__(self, exporter, published):
        self.exporter = exporter
        self.published = published
        self.latencies = []
        self.cpu = 0.0
        self.handled = 0
        for event in exporters.EVENTS:
            setattr(exporter, event, self._wrap(getattr(exporter, event)))

    def _wrap(self, handler):
        def measured(msg):
            start = time.perf_counter()
            t_pub = self.published.get(id(msg))
            if t_pub is not None:
                self.latencies.append(start - t_pub)
            c0 = time.thread_time()
            try:
                return handler(msg)
            finally:
                self.cpu += time.thread_time() - c0
                self.handled += 1
        return measured


def percentile(values, p):
    if not values:
        return float('nan')
    values = sorted(values)
    return values[min(len(values) - 1, int(p / 100.0 * len(values)))]


def copies_of(msg, n):
    """n variants of a message: other sensor ID, MAC address, drone serial / aircraft address."""
    out = [msg]
    for c in range(1, n):
        m = json.loads(json.dumps(msg))
        s = replay.section(m)
        s['sensor ID'] = '%s-copy%d' % (s.get('sensor ID'), c)
        if 'data' in m:
            mac = s.get('MAC address') or '00:00:00:00:00:00'
            s['MAC address'] = mac[:-2] + '%02X' % ((int(mac[-2:], 16) + c) & 0xff)
            raw = bytearray(base64.b64decode(s['UASdata']))
            for off in (8, 40):                       # last character of both basic IDs
                end = raw.find(b'\x00', off, off + 21)
                end = off + 20 if end < 0 else end
                if end > off:
                    raw[end - 1] = ord('A') + c % 26
            s['UASdata'] = base64.b64encode(bytes(raw)).decode('ascii')
        elif 'aircraft' in m and s.get('address'):
            s['address'] = '%06X' % ((int(s['address'], 16) + 0x1000 * c) & 0xFFFFFF)
        out.append(m)
    return out


def default_logs():
    logs = sorted(glob.glob(os.path.join(ROOT, 'logs', '*_log.csv')), key=os.path.getsize, reverse=True)
    rid = [p for p in logs if replay.kind_of(p) == 'remoteID'][:1]
    adsb = [p for p in logs if replay.kind_of(p) == 'aircraft'][:1]
    return rid + adsb


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('logs', nargs='*', help='CSV logs (default: the largest RemoteID and aircraft log in logs/)')
    p.add_argument('--speed', type=float, default=10)
    p.add_argument('--copies', type=int, default=1, help='replay every message N times as other drones / sensors')
    p.add_argument('--duration', type=float, default=120, help='stop after this many seconds (wall clock)')
    p.add_argument('--outputs', default=','.join(SETTINGS))
    p.add_argument('--queue-size', type=int, default=1000)
    a = p.parse_args()

    logs = a.logs or default_logs()
    config.print_messages = False
    if hasattr(config, 'log_path'):
        del config.log_path
    stream = replay.messages(logs)
    first_ts, first = next(stream)
    sensor = replay.section(first).get('sensor ID')
    position = (52.21852, 6.97486, 30.0)
    config.sensor_locations = {sensor: position}
    config.sensor_locations.update({'%s-copy%d' % (sensor, c): position for c in range(1, a.copies)})
    mqtt_sub._sensor_locations = None
    SETTINGS['asterix']['asterix_sic_table'] = {k: n + 1 for n, k in enumerate(config.sensor_locations)}

    published = {}
    meters = {}
    for name in a.outputs.split(','):
        exp = make(name, position)
        meters[name] = Meter(exp, published)
        exporters.register(exp, queue_size=a.queue_size)

    original_publish = exporters.publish

    def publish(event, msg):
        published[id(msg)] = time.perf_counter()
        original_publish(event, msg)
    exporters.publish = publish
    mqtt_sub.exporters.publish = publish

    class Client:
        def subscribe(self, topic):
            pass
    client = Client()
    mqtt_sub.subscribe(client)

    class Msg:
        topic = 'replay'

        def __init__(self, payload):
            self.payload = payload

    print("load test: %s at %gx, %d copies, outputs %s, %d s" % (
        ', '.join(os.path.basename(x) for x in logs), a.speed, a.copies, a.outputs, a.duration))
    usage0 = resource.getrusage(resource.RUSAGE_SELF)
    wall0 = time.perf_counter()
    end = wall0 + a.duration
    sent = 0
    behind = []
    feed_cpu = 0.0
    def all_messages():
        yield first_ts, first
        yield from stream
    for ts, msg in all_messages():
        due = wall0 + (ts - first_ts) / 1000.0 / a.speed
        now = time.perf_counter()
        if due > end or now > end:
            break
        if due > now:
            time.sleep(due - now)
        else:
            behind.append(now - due)
        c0 = time.thread_time()
        now_ms = int(time.time() * 1000)
        for m in copies_of(msg, a.copies):
            replay.retime(m, now_ms)
            client.on_message(client, None, Msg(json.dumps(m).encode()))
            sent += 1
        feed_cpu += time.thread_time() - c0
        if len(published) > 200000:
            cutoff = time.perf_counter() - 60
            for k in [k for k, t in published.items() if t < cutoff]:
                del published[k]
    wall = time.perf_counter() - wall0
    exporters.flush()
    drained = time.perf_counter() - wall0 - wall
    usage1 = resource.getrusage(resource.RUSAGE_SELF)
    cpu = (usage1.ru_utime - usage0.ru_utime) + (usage1.ru_stime - usage0.ru_stime)
    dropped = exporters.dropped()

    print("\n%d messages in %.0f s (%.0f/s), replayed %.0f s of log; queues drained %.1f s after the end"
          % (sent, wall, sent / wall, wall * a.speed, drained))
    print("process CPU %.1f s = %.0f%% of one core; mqtt_sub parsing + decoding %.1f s; feeder behind schedule: "
          "p99 %.0f ms, max %.0f ms" % (cpu, 100 * cpu / wall, feed_cpu, 1000 * percentile(behind, 99),
                                         1000 * max(behind or [0])))
    print("\n%-8s %9s %8s %10s %10s %10s %9s" % ('output', 'handled', 'dropped', 'p50 ms', 'p99 ms', 'max ms', 'CPU s'))
    for name, m in meters.items():
        print("%-8s %9d %8d %10.2f %10.2f %10.1f %9.2f" % (
            name, m.handled, dropped.get(m.exporter.name, 0), 1000 * percentile(m.latencies, 50),
            1000 * percentile(m.latencies, 99), 1000 * max(m.latencies or [0]), m.cpu))
    for name, m in meters.items():
        e = m.exporter
        stats = {k: getattr(e, k) for k in ('sent', 'errors', 'failed', 'dropped', 'sent_lines', 'out_of_range')
                 if isinstance(getattr(e, k, None), (int, dict))}
        if hasattr(e, 'stats') and callable(e.stats):
            stats = e.stats()
        print("  %s: %s" % (name, stats))
    done = threading.Event()
    threading.Thread(target=lambda: (exporters.shutdown(), done.set()), daemon=True).start()
    done.wait(30)


if __name__ == '__main__':
    main()
