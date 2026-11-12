# ASTERIX

`asterix_gateway.py` sends the DroneScout detections as EUROCONTROL
[ASTERIX](https://www.eurocontrol.int/asterix) over UDP (unicast and/or multicast), the
format of air traffic management and military air-picture systems.

```
pip3 install -r integrations/asterix/requirements.txt        # libasterix, Python 3.10 or newer
python3 integrations/asterix/asterix_gateway.py [--config my_asterix_config.py] [--mqtt-config my_config.py]
```

## Categories

| Category | Edition | Contents | Encoded with |
|---|---|---|---|
| CAT021 | 2.7 | ADS-B target reports: aircraft received by the ADS-B 1090 and UAT 978 add-ons | libasterix |
| CAT023 | 1.3 | ground station and service status: every DroneScout sensor, with the 1090 and 978 receivers as services (heartbeat) | libasterix |
| CAT129 | 1.2 | UAS identification reports: RemoteID drones | own encoder (`cat129_record`) |
| CAT062 | 1.21 | system tracks: drones (and optionally all aircraft, also ADS-L / FLARM / OGN) as one air picture | libasterix |
| CAT065 | 1.6 | SDPS service status: the gateway as track source of CAT062 (heartbeat) | libasterix |

Default: `asterix_categories = [21, 23, 129]`, i.e. the reports of each sensor. Add 62 and 65
for a consumer that wants system tracks. Leave `asterix_cat062_aircraft = False` when CAT021
is sent too, or ADS-B aircraft arrive twice.

Every `asterix_interval_s` (1 s) one datagram per category is sent (more when it doesn't fit
in `asterix_max_datagram`, 1400 bytes), with the newest report of every object since the
previous one. CAT023 and CAT065 are also sent every `asterix_heartbeat_s` (10 s) without
traffic, so the consumer knows the sensors are alive.

## SAC / SIC

ASTERIX identifies a data source by System Area Code (SAC) and System Identification Code
(SIC). These are allocated by the operator of the consumer, so **ask the customer**:

- `asterix_sac`: one SAC for the installation.
- `asterix_sic_table`: a SIC per DroneScout sensor ID (CAT021, CAT023, CAT129). A sensor that
  isn't in the table is not sent (a warning is printed once): no guessed SICs.
- `asterix_gateway_sic`: the SIC of the gateway, as source of CAT062 and CAT065.

## Mapping notes

### CAT021

- Mandatory items 010, 040, 080, 090 are always sent. I021/090 (quality indicators) is 0
  ("unknown"): the sensor doesn't pass on NUCp / NIC / NACp.
- 040: address type ICAO → ATP 0, others → 3; GBS set when the aircraft reports it is on the ground.
- Position 131 (high resolution), times 073 (receipt) and 077 (transmit), altitude 145
  (flight level, barometric) or 140 (GNSS height), ground vector 160, vertical rate 155 / 157,
  callsign 170, emitter category 020 (DO-260 categories mapped to the ASTERIX values), RSSI 132.
- Track number 161: 12 bits, per sensor.

### CAT129 (UAS identification)

No library or Wireshark dissector knows CAT129, so it is encoded here directly from the
EUROCONTROL specification, Part 29, edition 1.2 (`cat129p29ed12_0.pdf`), and tested
byte for byte against it. `tools/asterix_dump.py` decodes it.

- **I129/040 holds only 12 characters**, while an ANSI/CTA-2063 serial number has up to 20:
  it carries the last 12 characters (right-aligned, space padded). The full ID is in the
  Special Purpose field as `ID=<full id>` (`asterix_cat129_special_purpose`).
- **I129/050 (country of the manufacturer ID)** is mandatory, but RemoteID doesn't tell:
  `asterix_cat129_country`, default `ZZ` (ISO 3166 user-assigned, "unknown").
- I129/070 is the time of transmission. I129/080 position, LSB 180/2^30 degrees.
- I129/090 AMSL: the barometric altitude, else the geodetic altitude as an approximation.
  I129/100 AGL: the RemoteID height when its reference is the ground.
- I129/110: CEP50, derived from the RemoteID 95 % accuracy (× 0.481); 0 when unknown or above 255 m.
- **I129/120 (risk levels) is left out**: its value 0 means ARC-1, there is no "unknown".
- I129/185 and 220: horizontal velocity (east, north) and vertical velocity, 0.01 m/s.
- Inconsistencies in the specification, resolved as follows: I129/120 is described as
  "three-octet" but is one octet in the UAP; the FRN of I129/220 differs between the item
  description and the UAP. The UAP is followed (FRN 15 = I129/220).

### CAT062 and CAT065

- Drones: 010, 015, 040, 070, 080 (with track begin `TSB` on the first report and end `TSE`
  after `asterix_stale_s`), 105 position, 130 / 136 altitude, 185 velocity, 220 rate of climb,
  380 (aircraft address, identification, emitter category).
- CAT065: SDPS status of the gateway (items 000, 010, 015, 030, 040).

## Testing

### Receiver in Docker

There is no common open-source ASTERIX "server" to test against (consumers are ATM / C2
systems), so the test setup is a receiver: it decodes every record with libasterix (CAT129
with the own decoder), prints it and writes a pcap for Wireshark.

```
docker compose -f integrations/asterix/docker/compose.yaml up -d --build
python3 integrations/asterix/tools/send_test_events.py 127.0.0.1:8600 --all-categories --duration 20
docker compose -f integrations/asterix/docker/compose.yaml logs -f receiver
docker compose -f integrations/asterix/docker/compose.yaml cp receiver:/captures/asterix.pcap .
docker compose -f integrations/asterix/docker/compose.yaml down
```

Docker's port publishing doesn't forward multicast; the `multicast` profile runs a second
receiver on the host network that joins a group (default `239.255.86.0:8601`):

```
ASTERIX_GROUP=239.255.86.0 docker compose -f integrations/asterix/docker/compose.yaml --profile multicast up -d --build
python3 integrations/asterix/tools/send_test_events.py 239.255.86.0:8601 --duration 10
```

### Without Docker

```
python3 integrations/asterix/tools/asterix_dump.py --port 8600 [--group 239.1.2.3] [--pcap out.pcap]
python3 integrations/asterix/tools/send_test_events.py 127.0.0.1:8600 --all-categories --verify   # sends and decodes itself
```

In Wireshark: Decode As → UDP port 8600 → ASTERIX. Wireshark decodes CAT021/023/062/065;
it shows CAT129 records without their items.

`send_test_events.py` only sends made-up objects (`DRONESCOUT-TEST`, SAC 0, SIC 1). Before
sending to a customer's system, agree on SAC / SIC and the categories, and test with the
consumer: decoders differ in which editions they accept.

### Replay of recorded logs

```
python3 integrations/asterix/tools/replay_check.py logs/*_remoteID_log.csv logs/*_aircraft_log.csv
```

Replays the subscriber's CSV logs through the encoder (CAT021, CAT062, CAT129), decodes every
record again (libasterix; CAT129 with our decoder) and compares address, callsign, position,
altitude, speed and track with the CSV, within each item's resolution. Result: all
logs (61,499 rows): 41,507 CAT021, 61,434 CAT062 and 18,662 CAT129 records, every value within the
resolution of its item. Rows that are not encoded on purpose: ADS-L / FLARM in CAT021, and
positions without a fix.

### Unit tests

```
python3 -m pytest -q integrations/asterix          # skipped without libasterix
```

## Files

| File | |
|---|---|
| `asterix_gateway.py` | start this |
| `asterix_config.py` | settings |
| `asterix_export.py` | batching per category, track numbers, heartbeats, UDP |
| `asterix_map.py` | ASTERIX records from decoded messages, the CAT129 encoder and decoder |
| `tools/asterix_dump.py` | receiver / decoder with pcap output |
| `tools/send_test_events.py` | made-up objects as ASTERIX, `--verify` |
| `tools/replay_check.py` | recorded logs through the encoder, compared with the CSV |
| `docker/` | the receiver in Docker |
