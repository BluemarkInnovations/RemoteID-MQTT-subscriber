#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license
# settings for sapient_gateway.py (the MQTT broker settings are in config.py in the repository root)

# Fusion node, DMM or SAPIENT middleware (e.g. Dstl Apex) that edge nodes connect to.
#   tcp://host:port   normal SAPIENT (Apex: the port of a "Child" connection, BSI Flex 335 v2.0)
#   tls://host:port   when the fusion node offers TLS
sapient_url = 'tcp://127.0.0.1:5020'

# TLS only (tls://): client certificate, key and the CA of the fusion node
#sapient_client_cert = './certs/dronescout.pem'
#sapient_client_key = './certs/dronescout.key'
#sapient_ca_cert = './certs/ca.pem'
#sapient_check_hostname = True

# Every DroneScout sensor is registered as its own SAPIENT node. Its node_id is derived from
# the sensor ID and this namespace, so it stays the same after a restart. Change the namespace
# only if two gateways would otherwise register the same sensor.
sapient_node_namespace = 'dronescout'

# Registration details
sapient_name = 'DroneScout'
sapient_manufacturer = 'BlueMark Innovations'
sapient_model = 'DroneScout'

# Heartbeat (StatusReport) interval, also announced in the registration
sapient_heartbeat_s = 10

# Radius of the coverage polygon in the StatusReport (0: don't send coverage)
sapient_coverage_m = 5000

# What to send
sapient_send_aircraft = True     # ADS-B, UAT, ADS-L, FLARM, OGN; False: RemoteID drones only

# At most one detection per object per this many seconds
sapient_min_interval_s = 1.0

# At least this much time between two detections on one node's connection. The Dstl Apex
# middleware rejects detections less than 80 ms apart; 0 for fusion nodes without that rule.
sapient_detection_gap_s = 0.1

# A track that isn't updated within this time gets a final detection with state "lost"
sapient_stale_s = 10

# Wait this long for a RegistrationAck before sending anyway
sapient_ack_timeout_s = 10

# Classification (class, sub-class). Check the names against the taxonomy the fusion node
# uses (BSI Flex 335 v2.0 section 7, or its run-time taxonomy) and change them here.
#sapient_class_drone = ('Air Vehicle', 'UAV')
#sapient_class_fixed_wing = ('Air Vehicle', 'Fixed Wing')
#sapient_class_rotorcraft = ('Air Vehicle', 'Rotary Wing')
#sapient_class_lighter_than_air = ('Air Vehicle', 'Lighter Than Air')
#sapient_class_aircraft = ('Air Vehicle', None)

# Position of sensors without GPS: normally set it once for all outputs in config.py
# (sensor_locations: used while the sensor has no GPS fix). This one is for SAPIENT only and always
# overrides the GPS position: sensor ID -> (latitude, longitude, altitude MSL in m)
#sapient_sensor_positions = {
#    'ds02261000000948': (52.21852, 6.97486, 30.0),
#}
