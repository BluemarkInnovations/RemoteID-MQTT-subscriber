#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings for flarm_gateway.py (the MQTT broker settings are in config.py in the repository root)

# TCP server for XCSoar / LK8000 / SeeYou Navigator ("TCP client" device, port 2000, like a FLARM
# Wi-Fi bridge). None: no TCP server. Bind to one interface address to limit who can connect.
flarm_tcp_port = 2000
flarm_tcp_bind = '0.0.0.0'

# UDP: the same sentences as datagrams, "host:port" (e.g. XCSoar "UDP port" device, OpenCPN: 10110)
flarm_udp_destinations = []
#flarm_udp_destinations = ['192.168.10.255:10110']

# Own position: FLARM traffic is relative to it, and it is sent as $GPRMC / $GPGGA (the display
# then shows the traffic around this point). Normally the DroneScout sensor itself:
#flarm_position_sensor = 'ds02261000000948'      # the GPS position of this sensor
#flarm_position = (52.21852, 6.97486, 30.0)      # or: latitude, longitude, altitude MSL (m)
# Height of the sensor antenna above the ground (m): relative height of drones that send their
# height above ground
flarm_height_agl_m = 0.0

# What to send
flarm_send_drones = True        # FLARM aircraft type D (UAV)
flarm_send_aircraft = True      # ADS-B, UAT, ADS-L

# Only traffic within this distance (m) of the own position
flarm_max_range_m = 50000

# A target disappears when it isn't updated within this time (s)
flarm_stale_s = 5
