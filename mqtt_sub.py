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
    if hasattr(config, 'username'):
            print("username/password enabled")
            client.username_pw_set(config.username, config.password)

    #if ssl is enabled
    if hasattr(config, 'client_pem'):
            print("ssl enabled")

            #use this line if you have valid certificates
            #client.tls_set(config.client_pem, tls_version=ssl.PROTOCOL_TLSv1_2)

            #if you use *self-generated certificates*, use these lines instead:
            client.tls_set(config.client_pem, tls_version=ssl.PROTOCOL_TLSv1_2,cert_reqs=ssl.CERT_NONE)
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

        payload = payload.decode()
        position = 0
        while position != -1:
            position = payload.find('}{')
            if position == -1:
                json_obj = json.loads(payload)
            else:
                message_payload = payload[0:position + 1]
                payload = payload[position + 1:]
                json_obj = json.loads(message_payload)

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
                            if Direction > 360 or Direction < 0:
                                Direction = float("NaN")
                            print("Direction......",  Direction)
                            [SpeedHorizontal] =  struct.unpack('f', UASdata[Location_start_byte + 8:Location_start_byte + 8 + 4])
                            if SpeedHorizontal > 254.25 or SpeedHorizontal < 0:
                                SpeedHorizontal = float("NaN")
                            [SpeedVertical] =  struct.unpack('f', UASdata[Location_start_byte + 12:Location_start_byte + 12 + 4])
                            print("SpeedHorizontal......",  SpeedHorizontal)
                            if SpeedVertical > 62 or SpeedVertical < -62:
                                SpeedVertical = float("NaN")
                            print("SpeedVertical......",  SpeedVertical)
                            [Latitude] =  struct.unpack('d', UASdata[Location_start_byte + 16:Location_start_byte + 16 + 8])
                            if Latitude == 0.0 or Latitude > 90.0 or Latitude < -90.0:
                                Latitude = float("NaN")
                            [Longitude] =  struct.unpack('d', UASdata[Location_start_byte + 24:Location_start_byte + 24 + 8])
                            if Longitude == 0.0 or Longitude > 180.0 or Longitude < -180.0:
                                Longitude = float("NaN")
                            print("Latitude......",  Latitude)
                            print("Longitude......",  Longitude)

                            [AltitudeBaro] =  struct.unpack('f', UASdata[Location_start_byte + 32:Location_start_byte + 32 + 4])
                            if AltitudeBaro <= -1000.0 or AltitudeBaro > 31767.5:
                                AltitudeBaro = float("NaN")
                            [AltitudeGeo] =  struct.unpack('f', UASdata[Location_start_byte + 36:Location_start_byte + 36 + 4])
                            if AltitudeGeo <= -1000.0 or AltitudeGeo > 31767.5:
                                AltitudeGeo = float("NaN")
                            print("AltitudeBaro......",  AltitudeBaro)
                            print("AltitudeGeo......",  AltitudeGeo)
                            [HeightType] =  struct.unpack('i', UASdata[Location_start_byte + 40:Location_start_byte + 40 + 4])
                            [Height] =  struct.unpack('f', UASdata[Location_start_byte + 44:Location_start_byte + 44 + 4])
                            if Height <= -1000.0 or Height > 31767.5:
                                Height = float("NaN")
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
                            if TimeStamp != float("NaN") and TimeStamp > 0 and TimeStamp <= 60*60:
                                print("TimeStamp (MM:SS.mm).....%02i:%02i.%02i" % (int(TimeStamp/60), int(TimeStamp % 60), int(100*(TimeStamp - int(TimeStamp)))))
                            else:
                                print("Timestamp......invalid")
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

                            if OperatorLatitude == 0.0 or OperatorLatitude > 90.0 or OperatorLatitude < -90.0:
                                OperatorLatitude = float("NaN")
                            if OperatorLongitude == 0.0 or OperatorLongitude > 180.0 or OperatorLongitude < -180.0:
                                OperatorLongitude = float("NaN")

                            print("Operator Latitude......",  OperatorLatitude)
                            print("Operator Longitude......",  OperatorLongitude)

                            [AreaCount] =  struct.unpack('H', UASdata[System_start_byte + 24:System_start_byte + 24 + 2])
                            [AreaRadius] =  struct.unpack('H', UASdata[System_start_byte + 26:System_start_byte + 26 + 2])
                            [AreaCeiling] =  struct.unpack('f', UASdata[System_start_byte + 28:System_start_byte + 28 + 4])
                            if AreaCeiling == -1000:
                                AreaCeiling = float("NaN")
                            [AreaFloor] =  struct.unpack('f', UASdata[System_start_byte + 32:System_start_byte + 32 + 4])
                            if AreaFloor == -1000:
                                AreaFloor = float("NaN")
                            [CategoryEU] =  struct.unpack('i', UASdata[System_start_byte + 36:System_start_byte + 36 + 4])
                            [ClassEU] =  struct.unpack('i', UASdata[System_start_byte + 40:System_start_byte + 40 + 4])
                            [OperatorAltitudeGeo] =  struct.unpack('f', UASdata[System_start_byte + 44:System_start_byte + 44 + 4])
                            if OperatorAltitudeGeo <= -1000.0 or OperatorAltitudeGeo > 31767.5:
                                OperatorAltitudeGeo = float("NaN")
                            [Timestamp] =  struct.unpack('I', UASdata[System_start_byte + 48:System_start_byte + 48 + 4])
                            print("Area Count......",  AreaCount)
                            print("Area Radius......",  AreaRadius)
                            print("Area Ceiling......",  AreaCeiling)
                            print("Area Floor......",  AreaFloor)
                            print("Category EU......",  CategoryEU)
                            print("Class EU......",  ClassEU)
                            print("Operator Altitude Geo......",  OperatorAltitudeGeo)

                            if Timestamp != float("NaN") and Timestamp != 0:
                                print("Timestamp......",  datetime.datetime.fromtimestamp((int(Timestamp) + 1546300800), pytz.UTC).strftime('%Y-%m-%d %H:%M %Z'))
                            else:
                                print("Timestamp......invalid")
                            print("Timestamp raw......",  Timestamp)
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
