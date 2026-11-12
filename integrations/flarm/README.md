# FLARM NMEA (XCSoar, LK8000, SeeYou Navigator, ...)

`flarm_gateway.py` serves drones and aircraft as FLARM traffic on the FLARM NMEA data
port, the format gliding and GA software reads from a FLARM device: XCSoar, LK8000,
SeeYou Navigator, SkyDemon, Oudie, ... Drones show up as FLARM aircraft type "UAV".
Python standard library only.

```
python3 integrations/flarm/flarm_gateway.py [--config my_flarm_config.py] [--mqtt-config my_config.py]
```

Typical setups: a display at a gliding club or airfield (launch point, tower, clubhouse
screen with XCSoar or LK8000), or a ground station that shows nearby drones to a
flight director. **Traffic information only**: no FLARM collision warnings
(`AlarmLevel` is always 0) and nothing is transmitted to aircraft.

## What is sent

FLARM traffic is relative to the own position: the DroneScout sensor
(`flarm_position_sensor`, from its GPS) or a fixed position (`flarm_position`). Every second:

```
$GPRMC,...           own position (standing still), like the GPS sentences of a FLARM
$GPGGA,...
$PFLAU,2,0,2,1,0,,0,,,*..          2 targets, TX 0 (DroneScout doesn't transmit), GPS 3D fix
$PFLAA,0,546,-7,120,0,FF5561,45,,16,0.0,D*..   drone: 546 m north, 7 m west, 120 m up, UAV
$PFLAA,0,2557,8297,890,1,4840D6,90,,60,0.0,9*..  aircraft (ICAO address), jet
```

Over TCP (the gateway is a TCP server on port 2000, like a FLARM Wi-Fi bridge: set up the
software as "TCP client" to the gateway's address) and/or as UDP datagrams to
`flarm_udp_destinations` (one datagram per second).

| `$PFLAA` field | Drone | Aircraft |
|---|---|---|
| RelativeNorth / East | from the positions (m) | |
| RelativeVertical | RemoteID height above ground − `flarm_height_agl_m` when the drone sends height above ground; else pressure altitude, else geodetic altitude, − the own altitude MSL | altitude − own altitude MSL |
| IDType / ID | 0 (random) / `FF` + hash: the same address as the SBS and GDL90 outputs | 1 (ICAO) for ICAO addresses, 2 for FLARM addresses, else 0 |
| Track, GroundSpeed, ClimbRate | from RemoteID | from the aircraft message |
| AcftType | D (UAV) | from the emitter category: 8 light, 9 medium / heavy / high performance, 3 rotorcraft, 1 glider, B balloon, 4 skydiver, D UAV, F obstacle; 0 when unknown (also "ultralight / hang glider / paraglider", which can be powered or not) |

Targets are sent while they are updated (`flarm_stale_s`, 5 s) and within
`flarm_max_range_m` (50 km) of the own position. Without an own position nothing but
`$PFLAU` (GPS 0) is sent and a warning is printed once.

## Settings (`flarm_config.py`)

The TCP port / bind address, UDP destinations, own position (sensor or fixed) and antenna
height, what to send, range and stale time. Bind the TCP server to the address of the
network the displays are on (`flarm_tcp_bind`) to limit who can connect; there is no
authentication. At most 10 TCP clients; a client that stops reading is dropped.

## Testing

### Docker receiver with an independent parser

The test receiver parses with [pynmea2](https://github.com/Knio/pynmea2) 1.19.0 (MIT): it
checks every checksum and decodes `$GPRMC` / `$GPGGA` itself. pynmea2 knows `$PFLAU` /
`$PFLAA` only as proprietary sentences; their fields are then checked against the FLARM
data port specification (`docker/nmea_receiver.py`).

```
# UDP
docker compose -f integrations/flarm/docker/compose.yaml up -d --build receiver-udp
python3 integrations/flarm/tools/send_test_events.py --no-tcp --udp 127.0.0.1:10110 --duration 10
docker compose -f integrations/flarm/docker/compose.yaml logs -f receiver-udp

# TCP: the container connects to port 2000 on the Docker host, like XCSoar does
python3 integrations/flarm/tools/send_test_events.py --tcp-bind 0.0.0.0 --duration 60 &
docker compose -f integrations/flarm/docker/compose.yaml --profile tcp up -d --build receiver-tcp
docker compose -f integrations/flarm/docker/compose.yaml --profile tcp down
```

### In XCSoar / LK8000

`send_test_events.py --tcp-bind 0.0.0.0` serves a test drone flying a circle and a test
aircraft around the test sensor position (`--center lat,lon`). In XCSoar: Config > Devices >
"TCP client", the gateway's IP address, port 2000, driver "FLARM". The map then shows the
traffic around the own position; the FLARM radar shows the drone as UAV.

### Unit tests

```
python3 -m pytest -q integrations/flarm
```

## Files

| File | |
|---|---|
| `flarm_gateway.py` | start this |
| `flarm_config.py` | settings |
| `flarm_export.py` | own position, targets, TCP server and UDP |
| `flarm_map.py` | NMEA sentences (pure functions) |
| `tools/send_test_events.py` | made-up traffic over TCP / UDP |
| `docker/` | receivers with the pynmea2 parser |

Specification: FLARM "Data Port Specification" (`$PFLAU`, `$PFLAA`, aircraft types);
NMEA 0183 (`$GPRMC`, `$GPGGA`).
