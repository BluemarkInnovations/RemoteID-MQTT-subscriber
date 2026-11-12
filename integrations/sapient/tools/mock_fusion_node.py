#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Minimal stand-in for a SAPIENT fusion node / DMM, to test sapient_gateway.py.

- accepts TCP connections from edge nodes (4-byte little-endian length + SapientMessage),
- answers a Registration with a RegistrationAck (accepted, or rejected with --reject),
- optionally sends a Task after the registration (--task) and expects a TaskAck,
- checks a few things a real fusion node / Apex would complain about (node_id format,
  ULIDs, node_id matching the registration, detections less than 80 ms apart),
- prints every message (--quiet: only the summary).

It does not fuse, store or forward anything. For testing only. For a full check of
the BSI Flex 335 v2.0 rules use the Dstl Apex middleware (integrations/sapient/docker).

    python3 integrations/sapient/tools/mock_fusion_node.py --port 5020
"""

import argparse
import os
import re
import socket
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, '..'))                  # sapient_msg
sys.path.insert(0, os.path.join(HERE, '..', '..', '..'))      # repository root

from integrations.sapient import sapient_map as smap          # noqa: E402

ULID_RE = re.compile('[0-7][0-9A-HJKMNP-TV-Z]{25}')
UUID4_RE = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}')


class MockFusionNode:
    def __init__(self, host='127.0.0.1', port=5020, accept=True, send_task=False, quiet=False,
                 min_detection_gap_s=0.08):
        self.accept = accept
        self.send_task = send_task
        self.quiet = quiet
        self.min_detection_gap_s = min_detection_gap_s
        self.messages = []          # (connection number, SapientMessage)
        self.problems = []          # strings
        self.registered = {}        # node_id -> registration
        self.connections = 0
        self._lock = threading.Lock()
        self._conns = []
        self._stop = False
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.listen(16)
        self.sock.settimeout(0.2)
        self.port = self.sock.getsockname()[1]

    def start(self):
        threading.Thread(target=self._accept_loop, daemon=True).start()
        return self

    def stop(self):
        self._stop = True
        self.disconnect_clients()
        self.sock.close()

    def disconnect_clients(self):
        with self._lock:
            conns, self._conns = list(self._conns), []
        for c in conns:
            try:
                c.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            c.close()

    def of_kind(self, kind):
        return [m for _, m in self.messages if m.WhichOneof('content') == kind]

    def _problem(self, text):
        self.problems.append(text)
        print("mock FN: PROBLEM: %s" % text)

    def _accept_loop(self):
        while not self._stop:
            try:
                conn, addr = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self._lock:
                self.connections += 1
                number = self.connections
                self._conns.append(conn)
            threading.Thread(target=self._serve, args=(conn, number), daemon=True).start()

    def _send(self, conn, message):
        try:
            conn.sendall(smap.frame(message))
        except OSError:
            pass

    def _serve(self, conn, number):
        buf = b''
        node_id = None
        last_detection = None
        while not self._stop:
            try:
                data = conn.recv(65536)
            except OSError:
                break
            if not data:
                break
            buf += data
            while len(buf) >= 4:
                length = int.from_bytes(buf[:4], 'little')
                if len(buf) < 4 + length:
                    break
                raw, buf = buf[4:4 + length], buf[4 + length:]
                try:
                    m = smap.parse(raw)
                except Exception as e:
                    self._problem("connection %d: undecodable message: %s" % (number, e))
                    continue
                self.messages.append((number, m))
                kind = m.WhichOneof('content')
                if not self.quiet:
                    print("mock FN: #%d %s from %s" % (number, kind, m.node_id))
                if not UUID4_RE.fullmatch(m.node_id):
                    self._problem("node_id %r is not a lower case UUID v4" % m.node_id)
                if kind == 'registration':
                    node_id = m.node_id
                    self.registered[node_id] = m.registration
                    ack = smap.envelope(node_id)
                    ack.registration_ack.acceptance = self.accept
                    if not self.accept:
                        ack.registration_ack.ack_response_reason.append('rejected by the mock fusion node')
                    self._send(conn, ack)
                    if self.accept and self.send_task:
                        task = smap.envelope('00000000-0000-4000-8000-000000000001')
                        task.destination_id = node_id
                        task.task.task_id = smap.new_ulid()
                        task.task.task_name = 'test task'
                        task.task.control = 1          # CONTROL_START
                        self._send(conn, task)
                    continue
                if node_id is None:
                    self._problem("connection %d: %s before the registration" % (number, kind))
                elif m.node_id != node_id:
                    self._problem("connection %d: node_id %s, registered as %s" % (number, m.node_id, node_id))
                if kind == 'detection_report':
                    d = m.detection_report
                    for name in ('report_id', 'object_id'):
                        if not ULID_RE.fullmatch(getattr(d, name)):
                            self._problem("%s %r is not a ULID" % (name, getattr(d, name)))
                    t = m.timestamp.ToNanoseconds() / 1e9
                    if last_detection is not None and 0 <= t - last_detection < self.min_detection_gap_s:
                        self._problem("detections %.3f s apart on one connection" % (t - last_detection))
                    last_detection = t
                    if not self.quiet:
                        print("mock FN:     object %s %s at %.6f, %.6f%s" % (
                            d.object_id, d.id, d.location.y, d.location.x,
                            (' state ' + d.state) if d.state else ''))
                elif kind == 'status_report' and not ULID_RE.fullmatch(m.status_report.report_id):
                    self._problem("status report_id is not a ULID")
        with self._lock:
            if conn in self._conns:
                self._conns.remove(conn)
        conn.close()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=5020)
    p.add_argument('--reject', action='store_true', help='reject every registration')
    p.add_argument('--task', action='store_true', help='send a task after each registration')
    p.add_argument('--quiet', action='store_true')
    args = p.parse_args()
    fn = MockFusionNode(args.host, args.port, accept=not args.reject, send_task=args.task,
                        quiet=args.quiet).start()
    print("mock fusion node on %s:%d (Ctrl-C to stop)" % (args.host, fn.port))
    try:
        while True:
            time.sleep(10)
            kinds = {}
            for _, m in fn.messages:
                kinds[m.WhichOneof('content')] = kinds.get(m.WhichOneof('content'), 0) + 1
            print("mock FN: %d connections, %s, %d problems" % (fn.connections, kinds, len(fn.problems)))
    except KeyboardInterrupt:
        fn.stop()


if __name__ == '__main__':
    main()
