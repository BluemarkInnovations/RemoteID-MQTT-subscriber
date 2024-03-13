from bitstruct import *
import socket
import config

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
    try:
        sbs_connection.sendall(bytes(data,"ascii"))

    except (OSError, socket.error) as e:
        print("Error:", e)
        # reset connection
        sbs_connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        sbs_connection.close()
    return
                        
def export(payload, valid_blocks):

    try:
        sbs_connection
    except NameError:
        pass
    else:
        output_str = ""
        if valid_blocks.BasicID0_valid == 1:
            #send MSG 1 identification
            output_str = ""
            output_str += str("MSG,1,,,")
            
            BasicID0_start_byte = 0
            ID_length = 0
            [IDType] =  struct.unpack('i', payload[BasicID0_start_byte + 4:BasicID0_start_byte + 4 + 4])
            
            if IDType == 1: #only include serial number type
                basicID_data = str(payload[BasicID0_start_byte + 8:BasicID0_start_byte+ 8 + 21].decode('ascii'))
                
                i = 0
                while i < len(basicID_data): #remove trailing data (zeroes)
                    if ord(basicID_data[i]) == 0:
                        ID_length = i
                        break
                    i += 1
                basicID_data = basicID_data[0:ID_length] 
                icao = ICAO(basicID_data)

                # MSG 1 generation and transmit
                output_str += icao
                output_str += str(",,,,,,")                
                output_str += callsign(basicID_data)
                output_str += str(",,,,,,,,0,0,0,0\n")
                

                transmit(output_str) 
                
                if valid_blocks.LocationValid == 1:
                    #MSG 2, 3, or 4 generation depending on in air or on ground status
                    
                    Location_start_byte = 32 + 32
                    output_str = str("MSG,2,,,")
                    [Status] =  struct.unpack('i', payload[Location_start_byte:Location_start_byte + 4])

                    Emergency_flag = 0
                    Is_on_ground_flag = 1

                    if Status == 2: #in air
                       output_str = str("MSG,3,,,")
                       Is_on_ground_flag = 0

                    if Status in (3, 4): # emergency codes
                        Emergency_flag = 1

                    flag_str = str(",,,0,") + str(Emergency_flag) + ",0," + str(Is_on_ground_flag) + str("\n")

                    output_str += icao
                    output_str += str(",,,,,,,")

                    [Latitude] =  struct.unpack('d', payload[Location_start_byte + 16:Location_start_byte + 16 + 8])
                    if Latitude > 90.0 or Latitude < -90.0:
                        Latitude = 0.0

                    [Longitude] =  struct.unpack('d', payload[Location_start_byte + 24:Location_start_byte + 24 + 8])
                    if Longitude > 180.0 or Longitude < -180.0:
                        Longitude =0.0

                    [AltitudeGeo] =  struct.unpack('f', payload[Location_start_byte + 36:Location_start_byte + 36 + 4])
                    if AltitudeGeo <= -1000.0 or AltitudeGeo > 31767.5:
                        AltitudeGeo = 0.0

                    [Direction] =  struct.unpack('f', payload[Location_start_byte + 4:Location_start_byte + 4 + 4])
                    if Direction > 360 or Direction < 0:
                        Direction = 0.0

                    [SpeedHorizontal] =  struct.unpack('f', payload[Location_start_byte + 8:Location_start_byte + 8 + 4])
                    if SpeedHorizontal > 254.25 or SpeedHorizontal < 0:
                        SpeedHorizontal = 0.0
                    
                    #for MSG 2 include these fields
                    if Status == 1: #ground
                        output_str += str(round(AltitudeGeo*3.28084))

                    output_str += str(",")
                    if Status == 1: #ground
                        output_str += str(round(SpeedHorizontal*3.28084))

                    output_str += str(",")
                    output_str += str(round(Direction))
                    output_str += str(",")
                    output_str += str(Latitude)
                    output_str += str(",")
                    output_str += str(Longitude)
                    output_str += flag_str

                    transmit(output_str)

                    if Status == 2: #air, send msg 4 also
                         output_str = str("MSG,4,,,")
                         output_str += ICAO
                         output_str += str(",,,,,,,")
                         output_str += str(round(AltitudeGeo*3.28084))
                         output_str += str(",")
                         output_str += str(round(SpeedHorizontal*3.28084))
                         output_str += str(",")
                         output_str += str(round(Direction))
                         output_str += str(",,,,0,0,0,0\n")

                         transmit(output_str)
