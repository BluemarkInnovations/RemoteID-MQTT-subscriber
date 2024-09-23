from bitstruct import *
import datetime
import pytz

def decode_valid_blocks(payload, valid_blocks):

    valid_blocks.BasicID0_valid = payload[892]
    valid_blocks.BasicID1_valid = payload[893]
    valid_blocks.LocationValid = payload[894]
    valid_blocks.AuthValid[0] = payload[895]
    valid_blocks.AuthValid[1] = payload[896]
    valid_blocks.AuthValid[2] = payload[897]
    valid_blocks.AuthValid[3] = payload[898]
    valid_blocks.AuthValid[4] = payload[899]
    valid_blocks.AuthValid[5] = payload[900]
    valid_blocks.AuthValid[6] = payload[901]
    valid_blocks.AuthValid[7] = payload[902]
    valid_blocks.AuthValid[8] = payload[903]
    valid_blocks.AuthValid[9] = payload[904]
    valid_blocks.AuthValid[10] = payload[905]
    valid_blocks.AuthValid[11] = payload[906]
    valid_blocks.AuthValid[12] = payload[907]
    valid_blocks.AuthValid[13] = payload[908] #The current RemoteID standards allow up to 13 pages of Auth data.
    #valid_blocks.AuthValid[14] = payload[909]
    #valid_blocks.AuthValid[15] = payload[910]
    valid_blocks.SelfIDValid = payload[911]
    valid_blocks.SystemValid = payload[912]
    valid_blocks.OperatorIDValid = payload[913]



def print_payload(payload, valid_blocks, data_json):

	print("data message")
	print("sensor ID......",  data_json.get('sensor ID'))
	print("RSSI......",  data_json.get('RSSI'))
	print("channel......",  data_json.get('channel'))
	print("timestamp......",  data_json.get('timestamp'))
	epoch_timestamp = datetime.datetime.fromtimestamp(data_json.get('timestamp')/1000, pytz.UTC)
	print("time (of timestamp)......",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4])
	print("MAC address......",  data_json.get('MAC address'))
	print("type......",  data_json.get('type'))

	try:
		print("raw......",  raw.hex())
	except:
		pass

	if valid_blocks.BasicID0_valid == 1:
		print_basicID0(payload)

	if valid_blocks.BasicID1_valid == 1:
		print_basicID1(payload)

	if valid_blocks.LocationValid == 1:
		print_Location(payload)

	if valid_blocks.SelfIDValid == 1:
		print_SelfID(payload)

	if valid_blocks.SystemValid == 1:
		print_System(payload)

	if valid_blocks.OperatorIDValid == 1:
		print_OperatorID(payload)

	for x in range(16):
		if valid_blocks.AuthValid[x] == 1:
			print_AuthPage(payload, x)

def print_basicID0(payload):

	BasicID0_start_byte = 0
	[UAType] =  struct.unpack('I', payload[BasicID0_start_byte:BasicID0_start_byte + 4])
	[IDType] =  struct.unpack('I', payload[BasicID0_start_byte + 4:BasicID0_start_byte + 4 + 4])
	print("Basic ID 0 data")
	print("UAType......",  UAType)
	print("IDType......",  IDType)
	if IDType == 1 or IDType == 2:
	    print("Basic ID......",  payload[BasicID0_start_byte + 8:BasicID0_start_byte + 8 + 21].decode('ascii'))
	else:
	    print("Basic ID......",  payload[BasicID0_start_byte + 8:BasicID0_start_byte+ 8 + 21].hex())

	print("")



def print_basicID1(payload):

	BasicID1_start_byte = 32
	[UAType] =  struct.unpack('I', payload[BasicID1_start_byte:BasicID1_start_byte + 4])
	[IDType] =  struct.unpack('I', payload[BasicID1_start_byte + 4:BasicID1_start_byte + 4 + 4])
	print("Basic ID 1 data")
	print("UAType......",  UAType)
	print("IDType......",  IDType)
	if IDType == 1 or IDType == 2:
	    print("Basic ID......",  payload[BasicID1_start_byte + 8:BasicID1_start_byte + 8 + 21].decode('ascii'))
	else:
	    print("Basic ID......",  payload[BasicID1_start_byte + 8:BasicID1_start_byte+ 8 + 21].hex())

	print("")



def print_Location(payload):

	Location_start_byte = 32 + 32
	print("Location data")
	[Status] =  struct.unpack('I', payload[Location_start_byte:Location_start_byte + 4])
	print("Status......",  Status)
	[Direction] =  struct.unpack('f', payload[Location_start_byte + 4:Location_start_byte + 4 + 4])
	if Direction > 360 or Direction < 0:
	    Direction = float("NaN")
	print("Direction......",  Direction)
	[SpeedHorizontal] =  struct.unpack('f', payload[Location_start_byte + 8:Location_start_byte + 8 + 4])
	if SpeedHorizontal > 254.25 or SpeedHorizontal < 0:
	    SpeedHorizontal = float("NaN")
	[SpeedVertical] =  struct.unpack('f', payload[Location_start_byte + 12:Location_start_byte + 12 + 4])
	print("SpeedHorizontal......",  SpeedHorizontal)
	if SpeedVertical > 62 or SpeedVertical < -62:
	    SpeedVertical = float("NaN")
	print("SpeedVertical......",  SpeedVertical)
	[Latitude] =  struct.unpack('d', payload[Location_start_byte + 16:Location_start_byte + 16 + 8])
	if Latitude == 0.0 or Latitude > 90.0 or Latitude < -90.0:
	    Latitude = float("NaN")
	[Longitude] =  struct.unpack('d', payload[Location_start_byte + 24:Location_start_byte + 24 + 8])
	if Longitude == 0.0 or Longitude > 180.0 or Longitude < -180.0:
	    Longitude = float("NaN")
	print("Latitude......",  Latitude)
	print("Longitude......",  Longitude)

	[AltitudeBaro] =  struct.unpack('f', payload[Location_start_byte + 32:Location_start_byte + 32 + 4])
	if AltitudeBaro <= -1000.0 or AltitudeBaro > 31767.5:
	    AltitudeBaro = float("NaN")
	[AltitudeGeo] =  struct.unpack('f', payload[Location_start_byte + 36:Location_start_byte + 36 + 4])
	if AltitudeGeo <= -1000.0 or AltitudeGeo > 31767.5:
	    AltitudeGeo = float("NaN")
	print("AltitudeBaro......",  AltitudeBaro)
	print("AltitudeGeo......",  AltitudeGeo)
	[HeightType] =  struct.unpack('I ', payload[Location_start_byte + 40:Location_start_byte + 40 + 4])
	[Height] =  struct.unpack('f', payload[Location_start_byte + 44:Location_start_byte + 44 + 4])
	if Height <= -1000.0 or Height > 31767.5:
	    Height = float("NaN")
	print("HeightType......",  HeightType)
	print("Height......",  Height)
	[HorizAccuracy] =  struct.unpack('I', payload[Location_start_byte + 48:Location_start_byte + 48 + 4])
	[VertAccuracy] =  struct.unpack('I', payload[Location_start_byte + 52:Location_start_byte + 52 + 4])
	[BaroAccuracy] =  struct.unpack('I', payload[Location_start_byte + 56:Location_start_byte + 56 + 4])
	[SpeedAccuracy] =  struct.unpack('I', payload[Location_start_byte + 60:Location_start_byte + 60 + 4])
	[TSAccuracy] =  struct.unpack('I', payload[Location_start_byte + 64:Location_start_byte + 64 + 4])
	[TimeStamp] =  struct.unpack('f', payload[Location_start_byte + 68:Location_start_byte + 68 + 4])

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



def print_SelfID(payload):

	print("Self ID data")
	SelfID_start_byte = 776
	[DescType] =  struct.unpack('I', payload[SelfID_start_byte:SelfID_start_byte + 4])
	Desc = payload[SelfID_start_byte + 4:SelfID_start_byte + 4 + 23]
	print("Desc Type......",  DescType)
	print("Desc......",  Desc.decode('ascii'))
	print("")



def print_System(payload):

	print("System data")
	System_start_byte = 808
	[OperatorLocationType] =  struct.unpack('I', payload[System_start_byte:System_start_byte + 4])
	[ClassificationType] =  struct.unpack('I', payload[System_start_byte + 4:System_start_byte + 4+ 4])

	print("Operator Location Type......",  OperatorLocationType)
	print("Classification Type......",  ClassificationType)

	[OperatorLatitude] =  struct.unpack('d', payload[System_start_byte + 8:System_start_byte + 8 + 8])
	[OperatorLongitude] = struct.unpack('d', payload[System_start_byte + 16:System_start_byte + 16 + 8])

	if OperatorLatitude == 0.0 or OperatorLatitude > 90.0 or OperatorLatitude < -90.0:
	    OperatorLatitude = float("NaN")
	if OperatorLongitude == 0.0 or OperatorLongitude > 180.0 or OperatorLongitude < -180.0:
	    OperatorLongitude = float("NaN")

	print("Operator Latitude......",  OperatorLatitude)
	print("Operator Longitude......",  OperatorLongitude)

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
	[Timestamp] =  struct.unpack('I', payload[System_start_byte + 48:System_start_byte + 48 + 4])
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



def print_OperatorID(payload):

	OperatorID_start_byte = 864
	print("Operator ID data")
	[OperatorIdType] =  struct.unpack('I', payload[OperatorID_start_byte:OperatorID_start_byte + 4])
	print("Operator ID Type......",  OperatorIdType)
	print("Operator ID......",  payload[OperatorID_start_byte + 4:OperatorID_start_byte + 4 + 20].decode('ascii'))
	print("")


def print_AuthPage(payload, page):

	AuthPage_start_byte = 136 + 40*page
	print("Auth data")
	[DataPage] = struct.unpack('B', payload[AuthPage_start_byte + 0:AuthPage_start_byte + 1])
	[AuthType] = struct.unpack('B', payload[AuthPage_start_byte + 4:AuthPage_start_byte + 5])
	print("Data Page.........",  DataPage)
	print("Auth Type.........",  AuthType)

	if page == 0:
	    global LastPageIndex
	    global Length
	    [LastPageIndex] = struct.unpack('B', payload[AuthPage_start_byte + 8:AuthPage_start_byte + 9])
	    [Length] = struct.unpack('B', payload[AuthPage_start_byte + 9:AuthPage_start_byte + 10])
	    [Timestamp] =  struct.unpack('I', payload[AuthPage_start_byte + 12:AuthPage_start_byte + 12 + 4])

	    print("Last Page Index...",  LastPageIndex)
	    print("Length............",  Length)

	    if Timestamp != float("NaN") and Timestamp != 0:
	        print("Timestamp.........",  datetime.datetime.fromtimestamp((int(Timestamp) + 1546300800), pytz.UTC).strftime('%Y-%m-%d %H:%M %Z'))
	    else:
	        print("Timestamp.........invalid")

	    AuthData = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 17]
	    print("Auth Data.........",AuthData.hex())
	else:
	    if page == LastPageIndex:
	        #only print the chars within the specified length of the pages auth message
	        AuthData = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + (Length - 17) % 23]
	    else:
	        AuthData = payload[AuthPage_start_byte + 16:AuthPage_start_byte + 16 + 23]
	    print("Auth Data.........",AuthData.hex())
	print("")
