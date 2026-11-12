#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings for gdl90_gateway.py (the MQTT broker settings are in config.py in the repository root)

# Where to send, "host:port". EFBs (ForeFlight, Garmin Pilot, SkyDemon, ...) listen on UDP port 4000.
#   192.168.10.255:4000   broadcast to every tablet on the Wi-Fi network (use your subnet's broadcast
#                         address; 255.255.255.255 only leaves through the default interface)
#   192.168.10.20:4000    one tablet (more reliable over Wi-Fi than broadcast)
gdl90_destinations = ['255.255.255.255:4000']

# What to send
gdl90_send_drones = True
gdl90_send_aircraft = True        # ADS-B, UAT, ADS-L received by the sensors

# Ownship: the position the EFB takes as "own aircraft". Only for a display on the ground (a
# tablet in the briefing room or the tower): an EFB in an aircraft must use its own GPS, so leave
# this off when the network is also used in a cockpit.
#gdl90_ownship = (52.21852, 6.97486, 30.0)       # latitude, longitude, altitude MSL (m)
#gdl90_ownship_sensor = 'ds02261000000948'       # or: the GPS position of this DroneScout sensor

# Name of the device as shown in the EFB (ForeFlight ID message): 8 and 16 characters
gdl90_device_name = 'DroneSct'
gdl90_device_long_name = 'DroneScout'

# NIC of drone traffic. RemoteID has no NIC; None = the same value as the NACp (from the RemoteID
# horizontal accuracy), so EFBs that hide traffic with NIC 0 still show drones.
gdl90_drone_nic = None
