#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Mock of a Milestone XProtect generic event data source (XProtect itself only runs
on Windows): accepts TCP and UDP on one port, splits the bytes on the separator,
prints every message and echoes the bytes back ("Echo all bytes"), like the built-in
"Compatible" data source.

    python3 integrations/vms/tools/milestone_mock.py --port 1234
"""

import argparse
import signal
import socketserver
import sys
import threading


class Mock:
    def __init__(self, port=1234, host='0.0.0.0', separator=b'\r\n', echo=True, quiet=False):
        self.separator = separator
        self.echo = echo
        self.quiet = quiet
        self.messages = []                    # (protocol, text)
        self.lock = threading.Lock()
        mock = self

        class TCPHandler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(10)
                buf = b''
                try:
                    while True:
                        data = self.request.recv(4096)
                        if not data:
                            break
                        if mock.echo:
                            self.request.sendall(data)
                        buf += data
                        *parts, buf = buf.split(mock.separator)
                        for p in parts:
                            mock.add('tcp', p)
                except OSError:
                    pass
                if buf:
                    mock.add('tcp', buf)

        class UDPHandler(socketserver.BaseRequestHandler):
            def handle(self):
                for p in self.request[0].split(mock.separator):
                    if p:
                        mock.add('udp', p)

        class TCPServer(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        self.tcp = TCPServer((host, port), TCPHandler)
        self.port = self.tcp.server_address[1]
        self.udp = socketserver.UDPServer((host, self.port), UDPHandler)
        for srv in (self.tcp, self.udp):
            threading.Thread(target=srv.serve_forever, daemon=True).start()

    def add(self, protocol, data):
        text = data.decode('utf-8', 'replace')
        with self.lock:
            self.messages.append((protocol, text))
        if not self.quiet:
            print('%s generic event: %s' % (protocol.upper(), text))

    def close(self):
        for srv in (self.tcp, self.udp):
            srv.shutdown()
            srv.server_close()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--port', type=int, default=1234)
    p.add_argument('--host', default='0.0.0.0')
    a = p.parse_args()
    sys.stdout.reconfigure(line_buffering=True)
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    m = Mock(a.port, a.host)
    print('Milestone generic event mock on TCP and UDP port %d' % m.port)
    try:
        threading.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    print('%d messages' % len(m.messages))


if __name__ == '__main__':
    main()
