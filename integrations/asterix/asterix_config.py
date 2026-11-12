#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings for asterix_gateway.py (the MQTT broker settings are in config.py in the repository root)

# Where to send: "host:port", unicast and/or multicast (one datagram is sent to each)
asterix_destinations = ['127.0.0.1:8600']
#asterix_destinations = ['192.168.1.20:8600', '239.1.2.3:8600']

# Multicast only: TTL and the local interface address to send from
asterix_multicast_ttl = 1
#asterix_multicast_interface = '192.168.1.10'

# System Area Code of the installation (allocated per country / by the customer: ask them)
asterix_sac = 0

# System Identification Code per DroneScout sensor (CAT021, CAT023, CAT129).
# A sensor that is not in this table is not sent (a warning is printed once).
asterix_sic_table = {
#    'ds02261000000948': 1,
}

# SIC of the gateway itself, as track source for CAT062 / CAT065
asterix_gateway_sic = 100

# Categories to send:
#   21  ADS-B target reports (ADS-B 1090, UAT)       23  sensor / service status (heartbeat)
#   129 UAS identification reports (RemoteID)        62  system tracks (drones, optionally aircraft)
#   65  SDPS status of the gateway (with 62)
asterix_categories = [21, 23, 129]
#asterix_categories = [21, 23, 129, 62, 65]

# CAT062 also carries aircraft (all technologies, also ADS-L / FLARM / OGN). Leave False
# when CAT021 is sent as well, or the consumer gets every ADS-B aircraft twice.
asterix_cat062_aircraft = False

# Send interval (one datagram per category per interval) and heartbeat interval in seconds
asterix_interval_s = 1.0
asterix_heartbeat_s = 10

# CAT062: a track that isn't updated within this time gets its final report (TSE)
asterix_stale_s = 10

# CAT129: country of the UAS manufacturer ID (mandatory item I129/050). RemoteID doesn't tell;
# 'ZZ' = unknown. Optionally the destination (DAC, DIC), and whether the full RemoteID is
# also sent in the Special Purpose field (I129/040 only holds 12 characters).
asterix_cat129_country = 'ZZ'
#asterix_cat129_destination = (0, 0)
asterix_cat129_special_purpose = True

# Operational range announced in CAT023 (NM), None: leave out
asterix_range_nm = None

# Service identification (I062/015, I065/015)
asterix_service_id = 1

# Largest datagram (bytes)
asterix_max_datagram = 1400
