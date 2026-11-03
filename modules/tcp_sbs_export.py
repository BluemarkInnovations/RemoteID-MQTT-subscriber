import socket
import struct
import config
from modules import exporters
from modules.decode import nan_to

sbs_connection = None # one SBS client at a time, set by connect()

def connect(name):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((config.sbs_server_ip_address, config.sbs_server_port))
        print("Listen for TCP connection")
        s.listen()
        while True:
            global sbs_connection
            sbs_connection, addr = s.accept()
            print("Accepted")

def ICAO(data):
    ICAO = "FF" # start with FF to indicate an invalid ICAO 24-digit code
    i = 0
    checksum1 = 0
    checksum2 = 0

    data_hex = data.encode("utf-8").hex()
    while i < len(data_hex):
        checksum1 = checksum1 ^ ord(data_hex[i])
        checksum2 = checksum1 ^ checksum2
        i += 1

    ICAO += str(format(checksum1, '02X')) # add two bytes checksum as ICAO code
    ICAO += str(format(checksum2, '02X'))
    return ICAO

def callsign(data):
    callsign = str(data)
    if len(data) > 8:
        callsign = str(data[0:4]) # call sign is manufacturer code
        callsign += str(data[-4:]) # last 4 digit SN
        callsign = callsign.ljust(8)
    return callsign

def transmit(data):
    global sbs_connection
    if sbs_connection is None: # no client connected (yet), or the last one disconnected
        return
    try:
        sbs_connection.sendall(bytes(data,"ascii"))

    except (OSError, socket.error) as e:
        print("SBS client disconnected:", e)
        # reset connection, connect() accepts the next client
        try:
            sbs_connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            sbs_connection.close()
        except OSError:
            pass
        sbs_connection = None
    return

def export(uas):
    """Send a decoded RemoteID message (decode.UASData) as SBS MSG 1 (+ MSG 2/3/4 with a location).
    Only drones with a serial number in Basic ID 0 are sent, the ICAO code is derived from it."""
    if sbs_connection is None:
        return
    basic_id = uas.basic_id[0]
    if not basic_id.valid or basic_id.id_type != 1: #only include serial number type
        return
    try:
        basicID_data = basic_id.id_raw.decode('ascii').split('\x00')[0] #remove trailing data (zeroes)
    except UnicodeDecodeError:
        return
    icao = ICAO(basicID_data)

    # MSG 1 identification
    transmit("MSG,1,,," + icao + ",,,,,," + callsign(basicID_data) + ",,,,,,,,0,0,0,0\n")

    loc = uas.location
    if not loc.valid:
        return

    #MSG 2, 3, or 4 generation depending on in air or on ground status
    Status = loc.status
    output_str = "MSG,2,,,"
    Emergency_flag = 0
    Is_on_ground_flag = 1
    if Status == 2: #in air
        output_str = "MSG,3,,,"
        Is_on_ground_flag = 0
    if Status in (3, 4): # emergency codes
        Emergency_flag = 1
    flag_str = ",,,0," + str(Emergency_flag) + ",0," + str(Is_on_ground_flag) + "\n"

    # SBS has no "unknown": unknown values are sent as 0
    Latitude = nan_to(loc.latitude, 0.0)
    Longitude = nan_to(loc.longitude, 0.0)
    AltitudeGeo = nan_to(loc.altitude_geo, 0.0)
    Direction = nan_to(loc.direction, 0.0)
    SpeedHorizontal = nan_to(loc.speed_horizontal, 0.0)

    output_str += icao + ",,,,,,,"
    if Status == 1: #ground, MSG 2 includes altitude and speed
        output_str += str(round(AltitudeGeo*3.28084))
    output_str += ","
    if Status == 1:
        output_str += str(round(SpeedHorizontal*3.28084))
    output_str += "," + str(round(Direction)) + "," + str(Latitude) + "," + str(Longitude) + flag_str
    transmit(output_str)

    if Status == 2: #air, send msg 4 also
        transmit("MSG,4,,," + icao + ",,,,,,," + str(round(AltitudeGeo*3.28084)) + "," + str(round(SpeedHorizontal*3.28084))
                 + "," + str(round(Direction)) + ",,,,0,0,0,0\n")


class SBSExporter(exporters.Exporter):
    """SBS BaseStation output on a TCP port (Virtual Radar Server, FlightAirMap, adsbcot, ...)."""
    name = "sbs"

    def on_remote_id(self, msg):
        export(msg.uas)

    def on_aircraft(self, msg):
        # the sensor generates the SBS lines for aircraft itself
        for sbs_line in msg.sbs_lines:
            transmit(sbs_line + '\n')
