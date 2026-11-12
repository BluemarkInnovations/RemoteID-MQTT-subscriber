# TAK (ATAK, WinTAK, iTAK, TAK Server)

`tak_gateway.py` sends the DroneScout detections as [Cursor-on-Target](https://www.mitre.org/sites/default/files/pdf/09_4937.pdf)
(CoT) to a TAK Server or directly to TAK clients. It connects like an ordinary TAK
client, so no plugin or server configuration is needed. Python standard library only.

```
python3 integrations/tak/tak_gateway.py [--config my_tak_config.py] [--mqtt-config my_config.py]
```

## What is sent

| Object | CoT uid | CoT type (default, `tak_config.py`) | Notes |
|---|---|---|---|
| RemoteID drone | `dronescout-rid-<ID>` | `a-u-A-M-F-Q` unknown UAV | ID: serial number, else another basic ID, else the MAC address; RemoteID details in the remarks |
| its pilot | `dronescout-op-<ID>` | `a-u-G` | only when the drone sends the operator position (`tak_send_operator`) |
| ADS-B / UAT aircraft | `icao-<address>` for ICAO addresses | from the emitter category: fixed wing, rotorcraft, lighter than air, UAV | so the same aircraft from another ADS-B feed lands on the same marker |
| ADS-L / FLARM / OGN and non-ICAO addresses | `dronescout-<technology>-<address>` | `tak_type_aircraft` | |
| DroneScout sensor | `dronescout-<sensor ID>` | `a-f-G-E-S` friendly ground sensor | status (normal / degraded and why) in the remarks; position from GPS or `tak_sensor_positions` |
| the gateway | `tak_gateway_uid` | `a-f-G-U-C` with `takv` / `contact` | presence: sent once on every connection, see below |

Each marker is updated at most once per second per object (`tak_min_interval_s`) and
disappears when it isn't updated for 10 s (`tak_stale_s`; sensors 15 minutes).
Positions are WGS-84 (`hae`): the RemoteID geodetic altitude; for aircraft the barometric
altitude, which is not a WGS-84 height but the best estimate TAK gets.

## Settings (`tak_config.py`)

- `tak_url`:
  - `tls://host:8089`: TAK Server, normal setup, client certificate;
  - `tcp://host:8087`: TAK Server without TLS (lab only);
  - `udp://239.2.3.1:6969`: SA multicast to TAK clients on the local network, no server.
- Client certificate: ask the TAK Server admin for one (`makeCert.sh client dronescout`) in
  the right group. TAK Server hands out `.p12` files; `tak_config.py` shows the `openssl`
  commands to convert them to PEM. Set `tak_check_hostname = False` when the server is
  addressed by an IP address that isn't in its certificate.
- What to send (aircraft, pilots, sensors), the CoT types, and positions of sensors without GPS.

While the TAK Server is unreachable, updates are dropped (a TAK map only shows the current
picture) and the gateway reconnects by itself. When nothing was sent for
`tak_ping_interval_s`, a ping keeps the connection open.

### Presence (FreeTAKServer)

On every connection the gateway first announces itself like a TAK client does (a
`a-f-G-U-C` event with `takv`, `contact` and `__group`), then waits 1 s. FreeTAKServer
takes the first bytes of a connection as the client's identity and closes the connection
otherwise; TAK Server shows the gateway as a contact. `tak_gateway_callsign` and
`tak_gateway_position` set its name and where it is shown. Only turn it off
(`tak_presence = False`) for a receiver that can't handle it.

## Testing

### Mock server (no Docker)

```
integrations/tak/tools/make_test_certs.sh test_certs
python3 integrations/tak/tools/mock_tak_server.py --certs test_certs          # TLS on 8089 (--tcp: plain TCP)
```

Set in `tak_config.py`: `tak_url = 'tls://localhost:8089'`, `tak_client_cert = 'test_certs/dronescout.pem'`,
`tak_client_key = 'test_certs/dronescout.key'`, `tak_ca_cert = 'test_certs/ca.pem'`.
The mock server prints every event it receives. These certificates are for testing only.

### FreeTAKServer in Docker

```
docker compose -f integrations/tak/docker/compose.yaml up -d --build
python3 integrations/tak/tools/send_test_events.py tcp://127.0.0.1:8087 --verify
docker compose -f integrations/tak/docker/compose.yaml down        # add -v to drop the data volume
```

FreeTAKServer 2.2.1 from PyPI, with OpenTelemetry pinned to 1.24.0 (newer versions crash
FTS 2.2.1 at start-up). TLS on 8089 uses the certificates FTS generates in its data
volume; test it with `--no-hostname-check`.

### A real TAK Server

Before sending real detections to a TAK Server (yours or a customer's), send made-up
objects only: a drone with its pilot flying a circle, an aircraft and a sensor, all with
`DRONESCOUT-TEST` in the callsign and `TEST DATA` in the remarks. `--verify` connects a
second time as another TAK user and checks that the server passes the events on (what an
ATAK user would see):

```
python3 integrations/tak/tools/send_test_events.py tls://takserver:8089 --cert dronescout.pem --key dronescout.key --ca truststore-root.pem --verify
python3 integrations/tak/tools/send_test_events.py tls://localhost:8089 --certs test_certs --verify    # with the mock server
python3 integrations/tak/tools/send_test_events.py tcp://host:8087 --dry-run                          # print the CoT only
```

A tak.gov TAK Server differs from FreeTAKServer in places (groups, certificate
enrolment): always repeat the test there. With `--verify` the second connection must be
allowed to see the gateway's group.

### Unit tests

```
python3 -m pytest -q integrations/tak
```

## Files

| File | |
|---|---|
| `tak_gateway.py` | start this |
| `tak_config.py` | settings |
| `tak_export.py` | `TakClient` (TCP / TLS / UDP, reconnect, presence, ping) and `TAKExporter` (rate limit, what to send) |
| `cot.py` | CoT events from decoded messages (pure functions) |
| `tools/send_test_events.py` | made-up objects to a TAK Server, `--verify` |
| `tools/mock_tak_server.py` | prints what it receives, used by the tests |
| `tools/make_test_certs.sh` | CA, server and client certificate (also as `.p12`) for testing |
| `docker/` | FreeTAKServer 2.2.1 test server |
