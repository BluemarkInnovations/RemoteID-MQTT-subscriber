#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings for udp_gateway.py (the MQTT broker settings are in config.py in the repository root)

# Where to send, "host:port"; every event goes to each destination as one JSON datagram.
#   239.255.86.1:8700     multicast (recommended): receivers join the group, switches with IGMP
#                         snooping only forward it to them. 239.255.x.x is for local use.
#   192.168.1.255:8700    broadcast to every host of the subnet (use your subnet's broadcast
#                         address; 255.255.255.255 only leaves through the default interface).
#                         Doesn't pass routers / VLANs; over Wi-Fi slow and lossy.
#   192.168.1.20:8700     unicast to one receiver: the most reliable, also over Wi-Fi
udp_destinations = ['239.255.86.1:8700']
#udp_destinations = ['192.168.1.255:8700']
#udp_destinations = ['192.168.1.20:8700', '192.168.1.21:8700']

# Multicast only: hops (1 = local network only) and the address of the interface to send from
# (default: the interface of the default route)
udp_multicast_ttl = 1
#udp_multicast_interface = '192.168.1.10'

# "source" in every datagram, to tell several gateways apart
udp_source = 'dronescout-gateway'

# What to send
udp_send_drones = True
udp_send_aircraft = True         # ADS-B, UAT, ADS-L, FLARM, OGN
udp_send_status = True           # sensor status and location messages

# At most one datagram per drone / aircraft per this many seconds (0: every message)
udp_min_interval_s = 1.0

# Largest datagram in bytes; larger events are not sent (IP fragments often get lost)
udp_max_datagram = 1400
