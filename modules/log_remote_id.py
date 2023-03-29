import csv
import datetime
import json
import pytz
from bitstruct import *

def open_csv(log_path):

    filename = log_path + '/' + datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S') + '_remoteID_log.csv'
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        #write header
        csv_writer.writerow(['sensor ID'] + ['RSSI'] + ['channel'] + ['timestamp'] + ['time (of timestamp)'] + ['MAC address'] + ['type']
         + ['BasicID_0'] + ['UAType_0'] + ['IDType_0'] + ['BasicID_1'] + ['UAType_1'] + ['IDType_1']
         + ['Status'] + ['Direction'] + ['SpeedHorizontal'] + ['SpeedVertical'] + ['Latitude'] + ['Longitude'] + ['AltitudeBaro'] + ['AltitudeGeo'] + ['HeightType'] + ['Height'] + ['HorizAccuracy'] + ['VertAccuracy'] + ['BaroAccuracy'] + ['SpeedAccuracy'] + ['TSAccuracy'] + ['TimeStamp Location'] + ['time of TimeStamp Location']
         + ['DescType'] + ['Desc']
         + ['OperatorLocationType'] + ['ClassificationType'] + ['OperatorLatitude'] + ['OperatorLongitude'] + ['AreaCount'] + ['AreaRadius'] + ['AreaCeiling'] + ['AreaFloor'] + ['CategoryEU'] + ['ClassEU'] + ['OperatorAltitudeGeo'] + ['TimeStamp System'] + ['time of Timestamp System']
         + ['OperatorIdType'] + ['OperatorId']
         )
    return filename


def write_csv(data_json, payload, valid_blocks, filename):

    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        #write row
        epoch_timestamp = datetime.datetime.fromtimestamp(data_json.get('timestamp')/1000, pytz.UTC)

        BasicID0_start_byte = 0
        [UAType_0] =  struct.unpack('i', payload[BasicID0_start_byte:BasicID0_start_byte + 4])
        [IDType_0] =  struct.unpack('i', payload[BasicID0_start_byte + 4:BasicID0_start_byte + 4 + 4])
        BasicID_0 = payload[BasicID0_start_byte + 8:BasicID0_start_byte+ 8 + 21].decode('ascii').rstrip('\x00')

        if valid_blocks.BasicID0_valid == 0:
            UAType_0 = 0
            IDType_0 = 0
            BasicID_0 = ''

        BasicID1_start_byte = 32
        [UAType_1] =  struct.unpack('i', payload[BasicID1_start_byte:BasicID1_start_byte + 4])
        [IDType_1] =  struct.unpack('i', payload[BasicID1_start_byte + 4:BasicID1_start_byte + 4 + 4])
        BasicID_1 = payload[BasicID1_start_byte + 8:BasicID1_start_byte+ 8 + 21].decode('ascii').rstrip('\x00')

        if valid_blocks.BasicID1_valid == 0:
            UAType_1 = 0
            IDType_1 = 0
            BasicID_1 = ''

        Location_start_byte = 32 + 32
        [LocationStatus] =  struct.unpack('i', payload[Location_start_byte:Location_start_byte + 4])
        [Direction] =  struct.unpack('f', payload[Location_start_byte + 4:Location_start_byte + 4 + 4])
        if Direction > 360 or Direction < 0:
            Direction = float("NaN")
        [SpeedHorizontal] =  struct.unpack('f', payload[Location_start_byte + 8:Location_start_byte + 8 + 4])
        if SpeedHorizontal > 254.25 or SpeedHorizontal < 0:
            SpeedHorizontal = float("NaN")
        [SpeedVertical] =  struct.unpack('f', payload[Location_start_byte + 12:Location_start_byte + 12 + 4])
        if SpeedVertical > 62 or SpeedVertical < -62:
            SpeedVertical = float("NaN")

        [Latitude] =  struct.unpack('d', payload[Location_start_byte + 16:Location_start_byte + 16 + 8])
        if Latitude == 0.0 or Latitude > 90.0 or Latitude < -90.0:
            Latitude = float("NaN")
        [Longitude] =  struct.unpack('d', payload[Location_start_byte + 24:Location_start_byte + 24 + 8])
        if Longitude == 0.0 or Longitude > 180.0 or Longitude < -180.0:
            Longitude = float("NaN")

        [AltitudeBaro] =  struct.unpack('f', payload[Location_start_byte + 32:Location_start_byte + 32 + 4])
        if AltitudeBaro <= -1000.0 or AltitudeBaro > 31767.5:
            AltitudeBaro = float("NaN")
        [AltitudeGeo] =  struct.unpack('f', payload[Location_start_byte + 36:Location_start_byte + 36 + 4])
        if AltitudeGeo <= -1000.0 or AltitudeGeo > 31767.5:
            AltitudeGeo = float("NaN")

        [HeightType] =  struct.unpack('i', payload[Location_start_byte + 40:Location_start_byte + 40 + 4])
        [Height] =  struct.unpack('f', payload[Location_start_byte + 44:Location_start_byte + 44 + 4])
        if Height <= -1000.0 or Height > 31767.5:
            Height = float("NaN")

        [HorizAccuracy] =  struct.unpack('i', payload[Location_start_byte + 48:Location_start_byte + 48 + 4])
        [VertAccuracy] =  struct.unpack('i', payload[Location_start_byte + 52:Location_start_byte + 52 + 4])
        [BaroAccuracy] =  struct.unpack('i', payload[Location_start_byte + 56:Location_start_byte + 56 + 4])
        [SpeedAccuracy] =  struct.unpack('i', payload[Location_start_byte + 60:Location_start_byte + 60 + 4])
        [TSAccuracy] =  struct.unpack('i', payload[Location_start_byte + 64:Location_start_byte + 64 + 4])
        [TimeStampLocation] =  struct.unpack('f', payload[Location_start_byte + 68:Location_start_byte + 68 + 4])
        LocationTimeStamp = "invalid"
        if TimeStampLocation != float("NaN") and TimeStampLocation > 0 and TimeStampLocation <= 60*60:
            LocationTimeStamp = "%02i:%02i.%02i" % (int(TimeStampLocation/60), int(TimeStampLocation % 60), int(100*(TimeStampLocation - int(TimeStampLocation))))

        if valid_blocks.LocationValid == 0:
            LocationStatus = 0
            Direction = float("NaN")
            SpeedHorizontal = float("NaN")
            SpeedVertical = float("NaN")
            Latitude = float("NaN")
            Longitude = float("NaN")
            AltitudeBaro = float("NaN")
            AltitudeGeo = float("NaN")
            HeightType = 0
            Height = float("NaN")
            HorizAccuracy = 0
            VertAccuracy = 0
            BaroAccuracy = 0
            SpeedAccuracy = 0
            TSAccuracy = 0
            TimeStampLocation = 0
            LocationTimeStamp  = "invalid"

        SelfID_start_byte = 776
        [DescType] =  struct.unpack('i', payload[SelfID_start_byte:SelfID_start_byte + 4])
        Desc = payload[SelfID_start_byte + 4:SelfID_start_byte + 4 + 23].decode('ascii').rstrip('\x00')

        if valid_blocks.SelfIDValid == 0:
            Desc = ''
            DescType = 0

        System_start_byte = 808
        [OperatorLocationType] =  struct.unpack('i', payload[System_start_byte:System_start_byte + 4])
        [ClassificationType] =  struct.unpack('i', payload[System_start_byte + 4:System_start_byte + 4+ 4])

        [OperatorLatitude] =  struct.unpack('d', payload[System_start_byte + 8:System_start_byte + 8 + 8])
        [OperatorLongitude] = struct.unpack('d', payload[System_start_byte + 16:System_start_byte + 16 + 8])

        if OperatorLatitude == 0.0 or OperatorLatitude > 90.0 or OperatorLatitude < -90.0:
            OperatorLatitude = float("NaN")
        if OperatorLongitude == 0.0 or OperatorLongitude > 180.0 or OperatorLongitude < -180.0:
            OperatorLongitude = float("NaN")

        [AreaCount] =  struct.unpack('H', payload[System_start_byte + 24:System_start_byte + 24 + 2])
        [AreaRadius] =  struct.unpack('H', payload[System_start_byte + 26:System_start_byte + 26 + 2])
        [AreaCeiling] =  struct.unpack('f', payload[System_start_byte + 28:System_start_byte + 28 + 4])
        if AreaCeiling == -1000:
            AreaCeiling = float("NaN")
        [AreaFloor] =  struct.unpack('f', payload[System_start_byte + 32:System_start_byte + 32 + 4])
        if AreaFloor == -1000:
            AreaFloor = float("NaN")
        [CategoryEU] =  struct.unpack('i', payload[System_start_byte + 36:System_start_byte + 36 + 4])
        [ClassEU] =  struct.unpack('i', payload[System_start_byte + 40:System_start_byte + 40 + 4])
        [OperatorAltitudeGeo] =  struct.unpack('f', payload[System_start_byte + 44:System_start_byte + 44 + 4])
        if OperatorAltitudeGeo <= -1000.0 or OperatorAltitudeGeo > 31767.5:
            OperatorAltitudeGeo = float("NaN")
        [TimestampSystem] =  struct.unpack('I', payload[System_start_byte + 48:System_start_byte + 48 + 4])
        SystemTimestamp = "invalid"
        if TimestampSystem != float("NaN") and TimestampSystem != 0:
            SystemTimestamp = datetime.datetime.fromtimestamp((int(TimestampSystem) + 1546300800), pytz.UTC).strftime('%Y-%m-%d %H:%M %Z')


        if valid_blocks.SystemValid == 0:
            OperatorLocationType = 0
            ClassificationType = 0
            OperatorLatitude = float("NaN")
            OperatorLongitude = float("NaN")
            AreaCount = 0
            AreaRadius = 0
            AreaCeiling = float("NaN")
            AreaFloor = float("NaN")
            OperatorAltitudeGeo = float("NaN")
            CategoryEU = 0
            ClassEU = 0
            TimestampSystem = 0
            SystemTimestamp  = "invalid"

        OperatorID_start_byte = 864
        [OperatorIdType] =  struct.unpack('i', payload[OperatorID_start_byte:OperatorID_start_byte + 4])
        OperatorId = payload[OperatorID_start_byte + 4:OperatorID_start_byte + 4 + 20].decode('ascii').rstrip('\x00')

        if valid_blocks.OperatorIDValid == 0:
            OperatorIdType = 0
            OperatorId = ''

        csv_writer.writerow([data_json.get('sensor ID'), data_json.get('RSSI'), data_json.get('channel'), data_json.get('timestamp'),
            epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4], data_json.get('MAC address'), data_json.get('type'), str(BasicID_0), UAType_0, IDType_0, str(BasicID_1), UAType_1, IDType_1,
            LocationStatus, Direction, SpeedHorizontal, SpeedVertical, Latitude, Longitude, AltitudeBaro, AltitudeGeo, HeightType, Height, HorizAccuracy, VertAccuracy, BaroAccuracy, SpeedAccuracy, TSAccuracy, TimeStampLocation, LocationTimeStamp,
            DescType,str(Desc),
            OperatorLocationType, ClassificationType, OperatorLatitude, OperatorLongitude, AreaCount, AreaRadius, AreaCeiling, AreaFloor, CategoryEU, ClassEU, OperatorAltitudeGeo, TimestampSystem, str(SystemTimestamp),
            OperatorIdType, OperatorId
            ])


