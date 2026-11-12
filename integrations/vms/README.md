# VMS (Milestone XProtect, Nx Witness)

`vms_gateway.py` turns detections into a few alarms that a video management system can act
on, start recording, move a PTZ camera, show cameras or alert an operator:

| Event | When |
|---|---|
| `drone_detected` | a drone is seen (again after `vms_lost_s`) |
| `drone_update` | while the drone is present, every `vms_update_s` (off by default) |
| `drone_lost` | the drone hasn't been seen for `vms_lost_s` (30 s); with its duration |
| `sensor_degraded` / `sensor_normal` | a DroneScout sensor's status changed (a receiver stopped working, and back) |
| `aircraft_detected` / `aircraft_lost` | the same for aircraft; off by default (`vms_aircraft`) |

Each event carries the drone ID, the sensor, position, height, speed, the pilot position
and operator ID when the drone sends them. Python standard library only.

```
python3 integrations/vms/vms_gateway.py [--config my_vms_config.py] [--mqtt-config my_config.py]
```

Events are sent from a queue in their own thread: an unreachable VMS is retried with the
next event, never delays the MQTT reception, and failures are counted and logged.

## Milestone XProtect: generic events

One text message per event, over TCP (connect, send, close) or UDP, to a generic event data
source:

```
DRONESCOUT drone_detected id=1596F35WXYZ sensor=ds02261000000948 lat=52.218520 lon=6.974860 height=120 pilot_lat=52.218100 pilot_lon=6.974200 duration=0
DRONESCOUT sensor_degraded id=ds02261000000948 sensor=ds02261000000948 WLAN radio wlan1 stalled
```

`key=` pairs without a value are left out. The text comes from `vms_milestone_template`.

In the XProtect Management Client:

1. Tools > Options > Generic Events: enable the "Compatible" data source (port 1234,
   TCP and UDP) and add the gateway's IP address to the allowed addresses.
2. Rules and Events > Generic Events: a new generic event with expression, e.g.
   `"DRONESCOUT" AND "drone_detected"` (expression type "Search"), priority, data source "Compatible".
3. Rules: on the generic event, start recording / go to a PTZ preset / trigger an alarm.
   The ID and positions are in the event text, which shows in the alarm and event lists.

XProtect only runs on Windows, so the test setup has a mock of the data source
(`tools/milestone_mock.py`) that prints what XProtect would receive. **Not yet tested
against a real XProtect**: check the expression matching and the allowed-addresses setting
there first.

## Nx Witness, Nx Meta, DW Spectrum, Wisenet WAVE: generic events

REST API of version 6.x: `POST /rest/v4/events/generic` with a bearer token from a login
session (`POST /rest/v3/login/sessions`; the gateway logs in again when the session has
expired). The older `/api/createEvent` is disabled by default in 6.1 ("Disabled insecure
deprecated API").

```json
{"source": "DroneScout", "caption": "Drone detected: 1596F35WXYZ",
 "description": "drone_detected 1596F35WXYZ seen by sensor ds02261000000948 at 52.218520, 6.974860, height 120 m, ...",
 "state": "started", "deviceIds": ["<camera id>"]}
```

- `drone_detected` starts a **prolonged** event (`state: started`), `drone_lost` stops it,
  so a rule can record or show cameras for as long as the drone is there. The other events
  are instant.
- `vms_nx_camera_ids`: cameras linked to the events (IDs from the web admin; the server
  rejects IDs of cameras that don't exist).
- A local user with the "Advanced Viewer" role is enough. Use a dedicated user, not the admin.
- Nx servers have a self-signed certificate: set `vms_nx_ca_cert` to it, or
  `vms_nx_verify_tls = False` on a trusted network.

The default rules of a new Nx system show generic events as a desktop notification and
write them to the event log. For more: Event Rules > "Generic Event", source contains
"DroneScout", caption contains "Drone detected" > action, e.g. "Bookmark" or "Show on Alarm Layout".

Tested against the real Nx Witness 6.1.2.42921 server in Docker (below).

## Testing

```
docker compose -f integrations/vms/docker/compose.yaml up -d
integrations/vms/tools/nx_setup.sh            # first time: sets the admin password (new systems accept admin/admin)
python3 integrations/vms/tools/send_test_events.py --milestone 127.0.0.1:1234 \
        --nx https://127.0.0.1:7001 --password DroneScout-Test-1 --insecure --verify-nx
docker compose -f integrations/vms/docker/compose.yaml logs milestone-mock
docker compose -f integrations/vms/docker/compose.yaml down            # add -v to reset Nx Witness
```

- `nxwitness`: the real Nx Witness media server, community image
  [ptr727/nxwitness](https://github.com/ptr727/NxWitness) pinned to 6.1.2.42921 (Network Optix
  only offers Docker experimentally). Web admin on https://127.0.0.1:7001. Generic events need
  no licence. The server contacts Network Optix's cloud on start-up (`nxvms.com`).
- `milestone-mock`: TCP and UDP port 1234, prints every message and echoes the bytes.

`send_test_events.py` runs a short alarm sequence with made-up data: sensor
`DRONESCOUT-TEST-SENSOR` degraded, `DRONESCOUT-TEST-DRONE` detected, still present, lost, the
sensor normal again. `--verify-nx` reads the Nx event log afterwards. Against a customer's
VMS: tell the operators first, the test events show up as real alarms.

Unit tests (with mocks of both, the Nx mock follows the behaviour of the real 6.1 server):

```
python3 -m pytest -q integrations/vms
```

## Files

| File | |
|---|---|
| `vms_gateway.py` | start this |
| `vms_config.py` | settings |
| `vms_alarms.py` | detected / update / lost / sensor status logic (no I/O) |
| `vms_export.py` | Milestone and Nx backends, queue and worker thread |
| `tools/send_test_events.py` | made-up alarm sequence, `--verify-nx` |
| `tools/nx_setup.sh` | first-time setup of the Nx test server |
| `tools/milestone_mock.py` | XProtect generic event data source mock |
| `docker/` | Nx Witness server and the Milestone mock |
