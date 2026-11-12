#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
"""Example REST endpoint for rest_gateway.py: checks the authentication, parses the
JSON and prints one line per event. Standard library only and no imports from this
repository, so it can be copied as is: a starting point for your own receiver, not a
production server.

    python3 integrations/rest/tools/example_receiver.py --auth bearer --token change-me
    python3 integrations/rest/tools/example_receiver.py --auth basic --username dronescout --password change-me
    python3 integrations/rest/tools/example_receiver.py --auth header --header-name X-API-Key --header-value change-me
    # HTTPS (e.g. with the certificate from integrations/tak/tools/make_test_certs.sh):
    python3 integrations/rest/tools/example_receiver.py --cert server.pem --key server.key ...

Responses: 204 accepted, 400 bad JSON, 401 missing/wrong credentials, 404 wrong path.
"""

import argparse
import base64
import hmac
import json
import signal
import ssl
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_BODY = 10 * 1024 * 1024


def expected_headers(auth, token=None, username=None, password=None, header_name=None, header_value=None):
    """The header(s) a request must carry: {} (no authentication), bearer, basic or an API-key header."""
    if auth in (None, 'none'):
        return {}
    if auth == 'bearer' and token:
        return {'Authorization': 'Bearer ' + token}
    if auth == 'basic' and username is not None and password is not None:
        return {'Authorization': 'Basic ' + base64.b64encode(('%s:%s' % (username, password)).encode()).decode()}
    if auth == 'header' and header_name and header_value is not None:
        return {header_name: header_value}
    raise ValueError("--auth %s: the token, username/password or header value is missing" % auth)


def summary(event):
    t = event.get('type')
    pos = ''
    if 'latitude' in event:
        pos = ' %.6f %.6f' % (event['latitude'], event['longitude'])
    if t == 'drone':
        return 'drone %s via %s%s height %s m' % (event.get('id'), event.get('sensor_id'), pos, event.get('height_m'))
    if t == 'aircraft':
        return 'aircraft %s %s via %s%s alt %s m' % (event.get('id'), event.get('callsign', ''),
                                                     event.get('sensor_id'), pos, event.get('altitude_m'))
    if t == 'sensor_status':
        return 'sensor %s status %s' % (event.get('sensor_id'), event.get('status'))
    if t == 'sensor_location':
        return 'sensor %s location%s' % (event.get('sensor_id'), pos)
    return '%s event' % t


class Receiver(ThreadingHTTPServer):
    """HTTP server that keeps the received POST bodies (for tests) and the counts."""

    daemon_threads = True

    def __init__(self, address, expected_headers, path='/dronescout/events', quiet=False, status=None):
        super().__init__(address, Handler)
        self.expected_headers = expected_headers      # {header: value}, {} = no authentication
        self.path = path
        self.quiet = quiet
        self.status = status                          # force a response code (tests)
        self.posts = []
        self.events = []
        self.rejected = 0
        self.lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    server_version = 'dronescout-example-receiver'

    def log_message(self, fmt, *args):                # no access log per request
        pass

    def _reply(self, code, text=None):
        body = (json.dumps({'error': text}) + '\n').encode() if text else b''
        self.send_response(code)
        if code == 401:
            self.send_header('WWW-Authenticate', 'Bearer' if 'Authorization' not in self.server.expected_headers
                             or self.server.expected_headers['Authorization'].startswith('Bearer') else 'Basic')
        if body:
            self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self):
        for name, value in self.server.expected_headers.items():
            got = self.headers.get(name)
            # constant-time comparison, so the secret can't be guessed from response times
            if got is None or not hmac.compare_digest(got.encode(), value.encode()):
                return False
        return True

    def do_POST(self):
        srv = self.server
        if self.path.split('?')[0] != srv.path:
            return self._reply(404, 'unknown path')
        if not self._authorized():
            with srv.lock:
                srv.rejected += 1
            if not srv.quiet:
                print('401 from %s: missing or wrong credentials' % self.client_address[0])
            return self._reply(401, 'missing or wrong credentials')
        length = int(self.headers.get('Content-Length') or 0)
        if length <= 0 or length > MAX_BODY:
            return self._reply(400, 'body missing or too large')
        try:
            body = json.loads(self.rfile.read(length))
            events = body['events']
            if not isinstance(events, list):
                raise ValueError('events is not a list')
        except (ValueError, KeyError, TypeError) as e:
            return self._reply(400, 'bad JSON: %s' % e)
        if srv.status:
            return self._reply(srv.status, 'forced status')
        with srv.lock:
            srv.posts.append(body)
            srv.events.extend(events)
        if not srv.quiet:
            print('POST from %s (%s), %d events' % (body.get('source'), self.client_address[0], len(events)))
            for e in events:
                print('  ' + summary(e))
        self._reply(204)

    def do_GET(self):
        self._reply(405, 'use POST')


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--bind', default='127.0.0.1')
    p.add_argument('--port', type=int, default=8080)
    p.add_argument('--path', default='/dronescout/events')
    p.add_argument('--auth', choices=['none', 'bearer', 'basic', 'header'], default='bearer')
    p.add_argument('--token')
    p.add_argument('--username')
    p.add_argument('--password')
    p.add_argument('--header-name', default='X-API-Key')
    p.add_argument('--header-value')
    p.add_argument('--cert', help='server certificate (PEM): serve HTTPS')
    p.add_argument('--key', help='key of the server certificate')
    p.add_argument('--quiet', action='store_true')
    a = p.parse_args()
    sys.stdout.reconfigure(line_buffering=True)       # also when redirected to a file / docker logs
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    try:
        expected = expected_headers(a.auth, token=a.token, username=a.username, password=a.password,
                                    header_name=a.header_name, header_value=a.header_value)
    except ValueError as e:
        p.error(str(e))
    srv = Receiver((a.bind, a.port), expected, a.path, a.quiet)
    scheme = 'http'
    if a.cert:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(a.cert, a.key)
        srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
        scheme = 'https'
    print('listening on %s://%s:%d%s, authentication: %s' % (scheme, a.bind, a.port, a.path, a.auth))
    try:
        srv.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
    print('%d POSTs, %d events, %d rejected (401)' % (len(srv.posts), len(srv.events), srv.rejected))


if __name__ == '__main__':
    main()
