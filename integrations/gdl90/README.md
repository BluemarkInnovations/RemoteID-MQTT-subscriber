# GDL90 (cockpit apps / EFBs)

`gdl90_gateway.py` sends drones and aircraft as GDL90 traffic over UDP, the protocol ADS-B
receivers such as Stratux use to feed cockpit apps (EFBs): ForeFlight, Garmin Pilot,
SkyDemon, Avare, FlyQ, ... The drones then appear on the moving map like ADS-B traffic,
as UAVs. Python standard library only.

```
python3 integrations/gdl90/gdl90_gateway.py [--config my_gdl90_config.py] [--mqtt-config my_config.py]
```

Typical setups: a tablet in the briefing room or tower of an airfield, or a helicopter
landing site / HEMS base, on the same Wi-Fi network as the gateway. **This is traffic
information, not a certified collision avoidance system**, and UDP over Wi-Fi can lose
packets.

## What is sent

Every second, each message as its own UDP datagram (like Stratux) to every destination
in `gdl90_destinations` (normally the Wi-Fi network's broadcast address, port 4000):

| Message | |
|---|---|
| Heartbeat (ID 0) | UTC time from the gateway, "GPS position valid" only with an ownship |
| ForeFlight ID (0x65) | device name (`gdl90_device_name`), how ForeFlight lists the source |
| Ownship (10) + geometric altitude (11) | only when `gdl90_ownship` / `gdl90_ownship_sensor` is set, see below |
| Traffic report (20) | every drone / aircraft that sent an update in the last second |

### Drones

| GDL90 field | Value |
|---|---|
| address | `FF` + 16-bit hash of the drone ID: **the same as the SBS output** (`decode.pseudo_icao`), so a drone has one address in Virtual Radar Server and in the EFB |
| address type | 1 (ADS-B self-assigned) |
| callsign | first 4 + last 4 characters of the ID (the SBS rule): manufacturer code + end of the serial number |
| emitter category | 14 (UAV) |
| altitude | RemoteID pressure altitude; without it the geodetic altitude (an approximation) |
| NACp | the RemoteID horizontal accuracy (same encoding as ADS-B NACp) |
| NIC | RemoteID has none: the NACp value (`gdl90_drone_nic`), since some EFBs hide traffic with NIC 0 |
| airborne | unless the RemoteID status is "ground"; status "emergency" sets the emergency code |

SBS only sends drones that have a serial number; GDL90 also sends the others, under the
fallback ID (another basic ID, else the MAC address).

### Aircraft (ADS-B, UAT, ADS-L)

Address and address type (ICAO via ADS-B / TIS-B; others as self-assigned), callsign,
emitter category (the sensor uses the same DO-260 numbering as GDL90), altitude
(barometric, or GNSS height as an approximation), speed, track, vertical rate, on ground,
emergency. NIC / NACp are 0: the sensor doesn't pass them on.

### Ownship

An EFB treats the ownship report as the position of its own aircraft. Only set
`gdl90_ownship` (a fixed position) or `gdl90_ownship_sensor` (the GPS position of a
sensor) for a **display on the ground**. When the Wi-Fi network is (also) used in a
cockpit, leave it off: the EFB then uses its own GPS. Without ownship, some EFBs only
show traffic after they have their own GPS position.

## Settings (`gdl90_config.py`)

`gdl90_destinations` (`host:port`; broadcast `192.168.10.255:4000`, or one tablet),
`gdl90_send_drones`, `gdl90_send_aircraft`, the ownship, the device name and the drone NIC.
Use the subnet's broadcast address; `255.255.255.255` only leaves through the default
interface. Unicast to one tablet is more reliable over Wi-Fi than broadcast.

## Testing

### Docker receiver with an independent decoder

The test receiver decodes with [etdey/gdl90](https://github.com/etdey/gdl90) (MIT, pinned
to commit `a026ec7`), not with our code, so it checks the encoder: CRC, field layout and
values.

```
docker compose -f integrations/gdl90/docker/compose.yaml up -d --build
python3 integrations/gdl90/tools/send_test_events.py 127.0.0.1:4000 --ownship --duration 10
docker compose -f integrations/gdl90/docker/compose.yaml logs -f receiver
docker compose -f integrations/gdl90/docker/compose.yaml down
```

Expected: no bad CRCs; the test drone `FF5561` `DRONRONE` at 400 ft, emitter 14; the test
aircraft `FFFFF1` at 2950 ft. etdey/gdl90 reports the ForeFlight ID message as "not decoded":
it uses message ID 101 (0x65) for a SkyRadar message instead.

Docker doesn't forward broadcast; test broadcast on the local network with
`tools/gdl90_dump.py` (our own decoder).

### In an EFB

`send_test_events.py <broadcast address>:4000` sends a test drone flying a circle and a test
aircraft (positions around `--center`); the tablet must be on the same network. In
ForeFlight the device shows up as "DroneSct" under Devices. Tell other pilots on the network
first: they see the test traffic too.

### Unit tests

Including the CRC and traffic report examples from the GDL90 specification, byte for byte:

```
python3 -m pytest -q integrations/gdl90
```

## Files

| File | |
|---|---|
| `gdl90_gateway.py` | start this |
| `gdl90_config.py` | settings |
| `gdl90_export.py` | once per second heartbeat, ID, ownship and traffic, UDP |
| `gdl90_map.py` | GDL90 framing (CRC, escaping) and messages (pure functions), decoder for tests |
| `tools/send_test_events.py` | made-up traffic |
| `tools/gdl90_dump.py` | receiver with our own decoder |
| `docker/` | receiver with the etdey/gdl90 decoder |

Specification: GDL 90 Data Interface Specification, 560-1058-00 Rev A (FAA public ICD);
ForeFlight GDL 90 extended specification (ID message).
