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
         + ['AuthPage_0_DataPage'] + ['AuthPage_0_AuthType'] + ['AuthPage_0_LastPageIndex'] + ['AuthPage_0_Length'] + ['AuthPage_0_Timestamp'] + ['AuthPage_0_Data']
         + ['AuthPage_1_DataPage'] + ['AuthPage_1_AuthType'] + ['AuthPage_1_Data']
         + ['AuthPage_2_DataPage'] + ['AuthPage_2_AuthType'] + ['AuthPage_2_Data']
         + ['AuthPage_3_DataPage'] + ['AuthPage_3_AuthType'] + ['AuthPage_3_Data']
         + ['AuthPage_4_DataPage'] + ['AuthPage_4_AuthType'] + ['AuthPage_4_Data']
         + ['AuthPage_5_DataPage'] + ['AuthPage_5_AuthType'] + ['AuthPage_5_Data']
         + ['AuthPage_6_DataPage'] + ['AuthPage_6_AuthType'] + ['AuthPage_6_Data']
         + ['AuthPage_7_DataPage'] + ['AuthPage_7_AuthType'] + ['AuthPage_7_Data']
         + ['AuthPage_8_DataPage'] + ['AuthPage_8_AuthType'] + ['AuthPage_8_Data']
         + ['AuthPage_9_DataPage'] + ['AuthPage_9_AuthType'] + ['AuthPage_9_Data']
         + ['AuthPage_10_DataPage'] + ['AuthPage_10_AuthType'] + ['AuthPage_10_Data']
         + ['AuthPage_11_DataPage'] + ['AuthPage_11_AuthType'] + ['AuthPage_11_Data']
         + ['AuthPage_12_DataPage'] + ['AuthPage_12_AuthType'] + ['AuthPage_12_Data']
         + ['AuthPage_13_DataPage'] + ['AuthPage_13_AuthType'] + ['AuthPage_13_Data']
         + ['AuthPage_14_DataPage'] + ['AuthPage_14_AuthType'] + ['AuthPage_14_Data']
         + ['AuthPage_15_DataPage'] + ['AuthPage_15_AuthType'] + ['AuthPage_15_Data']
         )

    return filename


def write_csv(data_json, payload, valid_blocks, filename):

    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        #write row
        epoch_timestamp = datetime.datetime.fromtimestamp(data_json.get('timestamp')/1000, pytz.UTC)

        BasicID0_start_byte = 0
        [UAType_0] =  struct.unpack('I', payload[BasicID0_start_byte:BasicID0_start_byte + 4])
        [IDType_0] =  struct.unpack('I', payload[BasicID0_start_byte + 4:BasicID0_start_byte + 4 + 4])
        if IDType_0 == 1 or IDType_0 == 2:
            BasicID_0 = payload[BasicID0_start_byte + 8:BasicID0_start_byte + 8 + 21].decode('ascii').rstrip('\x00')
        else: #save as hex values
            BasicID_0 = payload[BasicID0_start_byte + 8:BasicID0_start_byte + 8 + 21].hex()

        if valid_blocks.BasicID0_valid == 0:
            UAType_0 = ''
            IDType_0 = ''
            BasicID_0 = ''

        BasicID1_start_byte = 32
        [UAType_1] =  struct.unpack('I', payload[BasicID1_start_byte:BasicID1_start_byte + 4])
        [IDType_1] =  struct.unpack('I', payload[BasicID1_start_byte + 4:BasicID1_start_byte + 4 + 4])
        if UAType_1 == 1 or UAType_1 == 2:
            BasicID_1 = payload[BasicID1_start_byte + 8:BasicID1_start_byte + 8 + 21].decode('ascii').rstrip('\x00')
        else: #save as hex values
            BasicID_1 = payload[BasicID1_start_byte + 8:BasicID1_start_byte + 8 + 21].hex()

        if valid_blocks.BasicID1_valid == 0:
            UAType_1 = ''
            IDType_1 = ''
            BasicID_1 = ''

        Location_start_byte = 32 + 32
        [LocationStatus] =  struct.unpack('I', payload[Location_start_byte:Location_start_byte + 4])
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

        [HeightType] =  struct.unpack('I', payload[Location_start_byte + 40:Location_start_byte + 40 + 4])
        [Height] =  struct.unpack('f', payload[Location_start_byte + 44:Location_start_byte + 44 + 4])
        if Height <= -1000.0 or Height > 31767.5:
            Height = float("NaN")

        [HorizAccuracy] =  struct.unpack('I', payload[Location_start_byte + 48:Location_start_byte + 48 + 4])
        [VertAccuracy] =  struct.unpack('I', payload[Location_start_byte + 52:Location_start_byte + 52 + 4])
        [BaroAccuracy] =  struct.unpack('I', payload[Location_start_byte + 56:Location_start_byte + 56 + 4])
        [SpeedAccuracy] =  struct.unpack('I', payload[Location_start_byte + 60:Location_start_byte + 60 + 4])
        [TSAccuracy] =  struct.unpack('I', payload[Location_start_byte + 64:Location_start_byte + 64 + 4])
        [TimeStampLocation] =  struct.unpack('f', payload[Location_start_byte + 68:Location_start_byte + 68 + 4])
        LocationTimeStamp = "invalid"
        if TimeStampLocation != float("NaN") and TimeStampLocation > 0 and TimeStampLocation <= 60*60:
            LocationTimeStamp = "%02i:%02i.%02i" % (int(TimeStampLocation/60), int(TimeStampLocation % 60), int(100*(TimeStampLocation - int(TimeStampLocation))))

        if valid_blocks.LocationValid == 0:
            LocationStatus = ''
            Direction = ''
            SpeedHorizontal = ''
            SpeedVertical = ''
            Latitude = ''
            Longitude = ''
            AltitudeBaro = ''
            AltitudeGeo = ''
            HeightType = ''
            Height = ''
            HorizAccuracy = ''
            VertAccuracy = ''
            BaroAccuracy = ''
            SpeedAccuracy = ''
            TSAccuracy = ''
            TimeStampLocation = ''
            LocationTimeStamp  = ''

        SelfID_start_byte = 776
        [DescType] =  struct.unpack('I', payload[SelfID_start_byte:SelfID_start_byte + 4])
        Desc = payload[SelfID_start_byte + 4:SelfID_start_byte + 4 + 23].decode('ascii').rstrip('\x00')

        if valid_blocks.SelfIDValid == 0:
            Desc = ''
            DescType = 0

        System_start_byte = 808
        [OperatorLocationType] =  struct.unpack('I', payload[System_start_byte:System_start_byte + 4])
        [ClassificationType] =  struct.unpack('I', payload[System_start_byte + 4:System_start_byte + 4+ 4])

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
        [CategoryEU] =  struct.unpack('I', payload[System_start_byte + 36:System_start_byte + 36 + 4])
        [ClassEU] =  struct.unpack('I', payload[System_start_byte + 40:System_start_byte + 40 + 4])
        [OperatorAltitudeGeo] =  struct.unpack('f', payload[System_start_byte + 44:System_start_byte + 44 + 4])
        if OperatorAltitudeGeo <= -1000.0 or OperatorAltitudeGeo > 31767.5:
            OperatorAltitudeGeo = float("NaN")
        [TimestampSystem] =  struct.unpack('I', payload[System_start_byte + 48:System_start_byte + 48 + 4])
        SystemTimestamp = "invalid"
        if TimestampSystem != float("NaN") and TimestampSystem != 0:
            SystemTimestamp = datetime.datetime.fromtimestamp((int(TimestampSystem) + 1546300800), pytz.UTC).strftime('%Y-%m-%d %H:%M %Z')


        if valid_blocks.SystemValid == 0:
            OperatorLocationType = ''
            ClassificationType = ''
            OperatorLatitude = ''
            OperatorLongitude = ''
            AreaCount = ''
            AreaRadius = ''
            AreaCeiling = ''
            AreaFloor = ''
            OperatorAltitudeGeo = ''
            CategoryEU = ''
            ClassEU = ''
            TimestampSystem = ''
            SystemTimestamp  = ''

        OperatorID_start_byte = 864
        [OperatorIdType] =  struct.unpack('I', payload[OperatorID_start_byte:OperatorID_start_byte + 4])
        OperatorId = payload[OperatorID_start_byte + 4:OperatorID_start_byte + 4 + 20].decode('ascii').rstrip('\x00')

        if valid_blocks.OperatorIDValid == 0:
            OperatorIdType = ''
            OperatorId = ''

 #set to empty auth data first
        AuthPage_0_DataPage = ''
        AuthPage_0_AuthType = ''
        AuthPage_0_Length = 0
        AuthPage_0_LastPageIndex = ''
        AuthPage_0_Timestamp = ''
        AuthPage_0_Data = ''
        AuthPage_1_DataPage = ''
        AuthPage_1_AuthType = ''
        AuthPage_1_Data = ''
        AuthPage_2_DataPage = ''
        AuthPage_2_AuthType = ''
        AuthPage_2_Data = ''
        AuthPage_3_DataPage = ''
        AuthPage_3_AuthType = ''
        AuthPage_3_Data = ''
        AuthPage_4_DataPage = ''
        AuthPage_4_AuthType = ''
        AuthPage_4_Data = ''
        AuthPage_5_DataPage = ''
        AuthPage_5_AuthType = ''
        AuthPage_5_Data = ''
        AuthPage_6_DataPage = ''
        AuthPage_6_AuthType = ''
        AuthPage_6_Data = ''
        AuthPage_7_DataPage = ''
        AuthPage_7_AuthType = ''
        AuthPage_7_Data = ''
        AuthPage_8_DataPage = ''
        AuthPage_8_AuthType = ''
        AuthPage_8_Data = ''
        AuthPage_9_DataPage = ''
        AuthPage_9_AuthType = ''
        AuthPage_9_Data = ''
        AuthPage_10_DataPage = ''
        AuthPage_10_AuthType = ''
        AuthPage_10_Data = ''
        AuthPage_11_DataPage = ''
        AuthPage_11_AuthType = ''
        AuthPage_11_Data = ''
        AuthPage_12_DataPage = ''
        AuthPage_12_AuthType = ''
        AuthPage_12_Data = ''
        AuthPage_13_DataPage = ''
        AuthPage_13_AuthType = ''
        AuthPage_13_Data = ''

        #only save if AuthValid for this auth page is valid
        #in case of BT4 the length parameter cannot be used for the last page, so all data in that page is saved
        AuthPage_start_byte = 136 + 40*0
        if valid_blocks.AuthValid[0] == 1:
            [AuthPage_0_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            [AuthPage_0_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_0_LastPageIndex] = struct.unpack('B', payload[AuthPage_start_byte + 8:AuthPage_start_byte + 9])
            [AuthPage_0_Length] = struct.unpack('B', payload[AuthPage_start_byte + 9:AuthPage_start_byte + 10])
            [AuthPage_0_Timestamp] =  struct.unpack('I', payload[AuthPage_start_byte + 12:AuthPage_start_byte + 12 + 4])
            if AuthPage_0_Length > 17:
                AuthPage_0_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 17].hex()
            else:
                AuthPage_0_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + AuthPage_0_Length].hex()

        auth_page = 1
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_1_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_1_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (23 + 17) or AuthPage_0_Length == 0:
                AuthPage_1_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + auth_page*23].hex()
            else:
                AuthPage_1_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 2
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_2_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_2_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_2_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_2_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 3
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_3_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_3_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_3_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_3_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 4
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_4_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_4_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_4_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_4_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 5
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_5_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_5_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_5_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_5_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 6
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_6_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_6_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_6_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_6_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 7
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_7_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_7_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_7_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_7_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 8
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_8_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_8_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_8_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_8_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 9
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_9_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_9_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_9_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_9_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 10
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_10_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_10_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_10_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_10_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 11
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_11_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_11_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_11_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_11_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 12
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_12_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_12_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_12_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_12_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        auth_page = 13
        AuthPage_start_byte = 136 + 40*auth_page
        if valid_blocks.AuthValid[auth_page] == 1:
            [AuthPage_13_AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
            [AuthPage_13_DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
            if AuthPage_0_Length > (auth_page*23 + 17) or AuthPage_0_Length == 0:
                AuthPage_13_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23].hex()
            else:
                AuthPage_13_Data = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (AuthPage_0_Length - 17 - 23*(auth_page - 1))].hex()

        csv_writer.writerow([data_json.get('sensor ID'), data_json.get('RSSI'), data_json.get('channel'), data_json.get('timestamp'),
            epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4], data_json.get('MAC address'), data_json.get('type'), str(BasicID_0), UAType_0, IDType_0, str(BasicID_1), UAType_1, IDType_1,
            LocationStatus, Direction, SpeedHorizontal, SpeedVertical, Latitude, Longitude, AltitudeBaro, AltitudeGeo, HeightType, Height, HorizAccuracy, VertAccuracy, BaroAccuracy, SpeedAccuracy, TSAccuracy, TimeStampLocation, LocationTimeStamp,
            DescType,str(Desc),
            OperatorLocationType, ClassificationType, OperatorLatitude, OperatorLongitude, AreaCount, AreaRadius, AreaCeiling, AreaFloor, CategoryEU, ClassEU, OperatorAltitudeGeo, TimestampSystem, str(SystemTimestamp),
            OperatorIdType, OperatorId,
            AuthPage_0_DataPage, AuthPage_0_AuthType, AuthPage_0_LastPageIndex, AuthPage_0_Length,AuthPage_0_Timestamp,AuthPage_0_Data,
            AuthPage_1_DataPage, AuthPage_1_AuthType, AuthPage_1_Data,
            AuthPage_2_DataPage, AuthPage_2_AuthType, AuthPage_2_Data,
            AuthPage_3_DataPage, AuthPage_3_AuthType, AuthPage_3_Data,
            AuthPage_4_DataPage, AuthPage_4_AuthType, AuthPage_4_Data,
            AuthPage_5_DataPage, AuthPage_5_AuthType, AuthPage_5_Data,
            AuthPage_6_DataPage, AuthPage_6_AuthType, AuthPage_6_Data,
            AuthPage_7_DataPage, AuthPage_7_AuthType, AuthPage_7_Data,
            AuthPage_8_DataPage, AuthPage_8_AuthType, AuthPage_8_Data,
            AuthPage_9_DataPage, AuthPage_9_AuthType, AuthPage_9_Data,
            AuthPage_10_DataPage, AuthPage_10_AuthType, AuthPage_10_Data,
            AuthPage_11_DataPage, AuthPage_11_AuthType, AuthPage_11_Data,
            AuthPage_12_DataPage, AuthPage_12_AuthType, AuthPage_12_Data,
            AuthPage_13_DataPage, AuthPage_13_AuthType, AuthPage_13_Data
            ])
