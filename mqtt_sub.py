#!/usr/bin/python3
# (c) Bluemark Innovations BV
# MIT license

# use
# pip3 install paho-mqtt bitstruct
# to install missing modules (tested under ubuntu)


import random
import ssl
import json
import base64
from paho.mqtt import client as mqtt_client
from bitstruct import *
import lzma
import datetime
import pytz

import config # config.py with MQTT broker configuration

def connect_mqtt() -> mqtt_client:
    client = mqtt_client.Client(config.client_id)
    
    def on_connect(client, userdata, flags, rc):
        if rc == 0:            
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    #if username and password is set
    if 'username' in globals():
            print("username/password enabled")
            client.username_pw_set(config.username, config.password)

    #if ssl is enabled
    if 'config.client_pem' in globals():
            print("ssl enabled")
            client.tls_set(config.client_pem, tls_version=ssl.PROTOCOL_TLSv1_2)
            client.tls_insecure_set(True)

    client.on_connect = on_connect
    client.connect(config.broker, config.port)

    subscribe(client)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received message from topic `{msg.topic}`")

        try:
			#not compressed
            payload = msg.payload.decode()
            # remove newline (\n) char or \0 char as it will prevent decoding of json
            if ord(payload[-1:]) == 0 or ord(payload[-1:]) == 10:
                payload = payload[:-1]
        except (UnicodeDecodeError, AttributeError):
			#lzma compressed
            payload = lzma.decompress(msg.payload)
            # remove \0 char as it will prevent decoding of json
            if ord(payload[-1:]) == 0:
                payload = payload[:-1]

        #print(payload) #uncomment tp print raw payload
        json_obj = json.loads(payload)

        try:
            if json_obj.get('protocol') == 1.0:
                try:
                    data_json = json_obj.get('data')
                    UASdata = base64.b64decode(data_json.get('UASdata'))
                    print("data message")
                    print("sensor ID......",  data_json.get('sensor ID'))
                    print("RSSI......",  data_json.get('RSSI'))
                    print("channel......",  data_json.get('channel'))
                    print("timestamp......",  data_json.get('timestamp'))
                    epoch_timestamp = datetime.datetime.fromtimestamp(data_json.get('timestamp')/1000, pytz.UTC)
                    print("time (of timestamp)......",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4])
                    print("MAC address......",  data_json.get('MAC address'))
                    print("type......",  data_json.get('type'))

                    BasicID0_valid = UASdata[892]
                    BasicID1_valid = UASdata[893]
                    LocationValid = UASdata[894]

                    SelfIDValid = UASdata[911]
                    SystemValid = UASdata[912]
                    OperatorIDValid = UASdata[913]

                    #BasicID message 0
                    if BasicID0_valid == 1:
                        BasicID0_start_byte = 0
                        [UAType] =  struct.unpack('i', UASdata[BasicID0_start_byte:BasicID0_start_byte + 4])
                        [IDType] =  struct.unpack('i', UASdata[BasicID0_start_byte + 4:BasicID0_start_byte + 4 + 4])
                        print("Basic ID 0 data")
                        print("UAType......",  UAType)
                        print("IDType......",  IDType)
                        print("Basic ID......",  UASdata[BasicID0_start_byte + 8:BasicID0_start_byte+ 8 + 21].decode('ascii'))
                        print("")

                    #BasicID message 1
                    if BasicID1_valid == 1:
                        BasicID1_start_byte = 32
                        [UAType] =  struct.unpack('i', UASdata[BasicID1_start_byte:BasicID1_start_byte + 4])
                        [IDType] =  struct.unpack('i', UASdata[BasicID1_start_byte + 4:BasicID1_start_byte + 4 + 4])
                        print("Basic ID 1 data")
                        print("UAType......",  UAType)
                        print("IDType......",  IDType)
                        print("Basic ID......",  UASdata[BasicID1_start_byte + 8:BasicID1_start_byte+ 8 + 21].decode('ascii'))
                        print("")

                    #LocationValid message
                    if LocationValid == 1:
                        Location_start_byte = 32 + 32
                        print("Location data")
                        [Status] =  struct.unpack('i', UASdata[Location_start_byte:Location_start_byte + 4])
                        print("Status......",  Status)
                        [Direction] =  struct.unpack('f', UASdata[Location_start_byte + 4:Location_start_byte + 4 + 4])
                        print("Direction......",  Direction)
                        [SpeedHorizontal] =  struct.unpack('f', UASdata[Location_start_byte + 8:Location_start_byte + 8 + 4])
                        [SpeedVertical] =  struct.unpack('f', UASdata[Location_start_byte + 12:Location_start_byte + 12 + 4])
                        print("SpeedHorizontal......",  SpeedHorizontal)
                        print("SpeedVertical......",  SpeedVertical)
                        [Latitude] =  struct.unpack('d', UASdata[Location_start_byte + 16:Location_start_byte + 16 + 8])
                        [Longitude] =  struct.unpack('d', UASdata[Location_start_byte + 24:Location_start_byte + 24 + 8])
                        print("Latitude......",  Latitude)
                        print("Longitude......",  Longitude)

                        [AltitudeBaro] =  struct.unpack('f', UASdata[Location_start_byte + 32:Location_start_byte + 32 + 4])
                        [AltitudeGeo] =  struct.unpack('f', UASdata[Location_start_byte + 36:Location_start_byte + 36 + 4])
                        print("AltitudeBaro......",  AltitudeBaro)
                        print("AltitudeGeo......",  AltitudeGeo)
                        [HeightType] =  struct.unpack('i', UASdata[Location_start_byte + 40:Location_start_byte + 40 + 4])
                        [Height] =  struct.unpack('f', UASdata[Location_start_byte + 44:Location_start_byte + 44 + 4])
                        print("HeightType......",  HeightType)
                        print("Height......",  Height)
                        [HorizAccuracy] =  struct.unpack('i', UASdata[Location_start_byte + 48:Location_start_byte + 48 + 4])
                        [VertAccuracy] =  struct.unpack('i', UASdata[Location_start_byte + 52:Location_start_byte + 52 + 4])
                        [BaroAccuracy] =  struct.unpack('i', UASdata[Location_start_byte + 56:Location_start_byte + 56 + 4])
                        [SpeedAccuracy] =  struct.unpack('i', UASdata[Location_start_byte + 60:Location_start_byte + 60 + 4])
                        [TSAccuracy] =  struct.unpack('i', UASdata[Location_start_byte + 64:Location_start_byte + 64 + 4])
                        [TimeStamp] =  struct.unpack('f', UASdata[Location_start_byte + 68:Location_start_byte + 68 + 4])

                        print("HorizAccuracy......",  HorizAccuracy)
                        print("VertAccuracy......",  VertAccuracy)
                        print("BaroAccuracy......",  BaroAccuracy)
                        print("SpeedAccuracy......",  SpeedAccuracy)
                        print("TSAccuracy......",  TSAccuracy)
                        print("TimeStamp (MM:SS.mm).....%02i:%02i.%02i" % (int(TimeStamp/60), int(TimeStamp % 60), int(100*(TimeStamp - int(TimeStamp)))))
                        print("")

                    #SelfIDValid message
                    if SelfIDValid == 1:
                        print("Self ID data")
                        SelfID_start_byte = 776
                        [DescType] =  struct.unpack('i', UASdata[SelfID_start_byte:SelfID_start_byte + 4])
                        Desc = UASdata[SelfID_start_byte + 4:SelfID_start_byte + 4 + 23]
                        print("Desc Type......",  DescType)
                        print("Desc......",  Desc.decode('ascii'))
                        print("")

                    #SystemValid message
                    if SystemValid == 1:
                        print("System data")
                        System_start_byte = 808
                        [OperatorLocationType] =  struct.unpack('i', UASdata[System_start_byte:System_start_byte + 4])
                        [ClassificationType] =  struct.unpack('i', UASdata[System_start_byte + 4:System_start_byte + 4+ 4])

                        print("Operator Location Type......",  OperatorLocationType)
                        print("Classification Type......",  ClassificationType)

                        [OperatorLatitude] =  struct.unpack('d', UASdata[System_start_byte + 8:System_start_byte + 8 + 8])
                        [OperatorLongitude] = struct.unpack('d', UASdata[System_start_byte + 16:System_start_byte + 16 + 8])
                        print("Operator Latitude......",  OperatorLatitude)
                        print("Operator Longitude......",  OperatorLongitude)

                        [AreaCount] =  struct.unpack('H', UASdata[System_start_byte + 24:System_start_byte + 24 + 2])
                        [AreaRadius] =  struct.unpack('H', UASdata[System_start_byte + 26:System_start_byte + 26 + 2])
                        [AreaCeiling] =  struct.unpack('f', UASdata[System_start_byte + 28:System_start_byte + 28 + 4])
                        [AreaFloor] =  struct.unpack('f', UASdata[System_start_byte + 32:System_start_byte + 32 + 4])
                        [CategoryEU] =  struct.unpack('i', UASdata[System_start_byte + 36:System_start_byte + 36 + 4])
                        [ClassEU] =  struct.unpack('i', UASdata[System_start_byte + 40:System_start_byte + 40 + 4])
                        [OperatorAltitudeGeo] =  struct.unpack('f', UASdata[System_start_byte + 44:System_start_byte + 44 + 4])
                        [Timestamp] =  struct.unpack('I', UASdata[System_start_byte + 48:System_start_byte + 48 + 4])
                        print("Area Count......",  AreaCount)
                        print("Area Radius......",  AreaRadius)
                        print("Area Ceiling......",  AreaCeiling)
                        print("Area Floor......",  AreaFloor)
                        print("Category EU......",  CategoryEU)
                        print("Class EU......",  ClassEU)
                        print("Operator Altitude Geo......",  OperatorAltitudeGeo)
                        print("Timestamp......",  datetime.datetime.fromtimestamp((int(Timestamp) + 1546300800), pytz.UTC).strftime('%Y-%m-%d %H:%M %Z'))
                        print("")

                    #OperatorIDValid message
                    if OperatorIDValid == 1:
                        OperatorID_start_byte = 864
                        print("Operator ID data")
                        [OperatorIdType] =  struct.unpack('i', UASdata[OperatorID_start_byte:OperatorID_start_byte + 4])
                        print("Operator ID Type......",  OperatorIdType)
                        print("Operator ID......",  UASdata[OperatorID_start_byte + 4:OperatorID_start_byte + 4 + 20].decode('ascii'))
                        print("")

                except:
                    pass

                try:
                    status_json = json_obj.get('status')
                    status_json.get('sensor ID') # fail if it is data json
                    print("status message")
                    print("sensor ID......",  status_json.get('sensor ID'))
                    print("timestamp......",  status_json.get('timestamp'))
                    epoch_timestamp = datetime.datetime.fromtimestamp(status_json.get('timestamp')/1000)
                    print("time (local)......",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                    print("firmware version......",  status_json.get('firmware version'))
                    print("model......",  status_json.get('model'))
                    print("status......",  status_json.get('status'))
                    print("")
                except:
                    pass
        except:
            pass

    client.subscribe(config.topic)
    client.on_message = on_message

def run():

    client = connect_mqtt()
    client.loop_forever()


if __name__ == '__main__':
    run()
