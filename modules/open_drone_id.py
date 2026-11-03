from bitstruct import *
from datetime import datetime, timezone

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



def print_payload(msg):
	"""Print a decoded RemoteIDMessage (modules/decode.py)."""
	data_json = msg.json
	uas = msg.uas
	extra_json = msg.extra

	print("data message")
	print("sensor ID......",  data_json.get('sensor ID'))
	print("RSSI......",  data_json.get('RSSI'))
	print("channel......",  data_json.get('channel'))
	print("timestamp......",  data_json.get('timestamp'))
	epoch_timestamp = datetime.fromtimestamp(data_json.get('timestamp')/1000, tz=timezone.utc)
	print("time (of timestamp)......",  epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4])
	print("MAC address......",  data_json.get('MAC address'))
	print("type......",  data_json.get('type'))

	if msg.raw is not None:
		print("raw......",  msg.raw.hex())

	if uas.basic_id[0].valid:
		print_basicID0(uas.basic_id[0])

	if uas.basic_id[1].valid:
		print_basicID1(uas.basic_id[1])

	if uas.location.valid:
		print_Location(uas.location)

	if uas.self_id.valid:
		print_SelfID(uas.self_id)

	if uas.system.valid:
		print_System(uas.system)

	if uas.operator_id.valid:
		print_OperatorID(uas.operator_id)

	for page in range(len(uas.auth)):
		if uas.auth[page].valid:
			print_AuthPage(uas, page)

	if len(extra_json) > 0:
		if extra_json.get('SN present') == 1:
			print("extra")
			print("SN valid......",  extra_json.get('SN valid'))
			print("manufacturer......",  extra_json.get('manufacturer'))
			print("model......",  extra_json.get('model'))
			print("type......",  extra_json.get('type'))
			print("application......",  extra_json.get('application'))
			print("weight mtow [kg]......",  extra_json.get('weight'))
			print("dimensions [mm]......",  extra_json.get('dimensions'))
			print("")


#removes characters like \t \n \r space from string
def clean_string(string):
    string = string.replace(" ", "")
    string = string.replace("\t", "")
    string = string.replace("\n", "")
    string = string.replace("\r", "")

    return string



def _print_basic_id(title, basic_id):
	print(title)
	print("UAType......",  decode_basicID_UA_type(basic_id.ua_type))
	print("IDType......",  decode_basicID_ID_type(basic_id.id_type))
	if basic_id.is_text:
	    print("Basic ID......",  clean_string(basic_id.id_raw.decode('ascii')))
	else:
	    print("Basic ID......",  basic_id.id_raw.hex())

	print("")


def print_basicID0(basic_id):
	_print_basic_id("Basic ID 0 data", basic_id)


def print_basicID1(basic_id):
	_print_basic_id("Basic ID 1 data", basic_id)



def print_Location(loc):

	print("Location data")
	print("Status......",  decode_location_status(loc.status))
	print("Direction......",  loc.direction)
	print("SpeedHorizontal......",  loc.speed_horizontal)
	print("SpeedVertical......",  loc.speed_vertical)
	print("Latitude......",  loc.latitude)
	print("Longitude......",  loc.longitude)
	print("AltitudeBaro......",  loc.altitude_baro)
	print("AltitudeGeo......",  loc.altitude_geo)
	print("HeightType......",  decode_location_height_type(loc.height_type))
	print("Height......",  loc.height)
	print("HorizAccuracy......",  decode_location_horiz_accuracy(loc.horiz_accuracy))
	print("VertAccuracy......",  decode_location_vert_accuracy(loc.vert_accuracy))
	print("BaroAccuracy......",  decode_location_vert_accuracy(loc.baro_accuracy))
	print("SpeedAccuracy......",  decode_location_speed_accuracy(loc.speed_accuracy))
	print("TSAccuracy......",  decode_location_ts_accuracy(loc.ts_accuracy))
	TimeStamp = loc.timestamp
	if TimeStamp > 0 and TimeStamp <= 60*60:
	    print("TimeStamp (MM:SS.mm).....%02i:%02i.%02i" % (int(TimeStamp/60), int(TimeStamp % 60), int(100*(TimeStamp - int(TimeStamp)))))
	else:
	    print("Timestamp......invalid")
	print("")



def print_SelfID(self_id):

	print("Self ID data")
	print("Desc Type......",  decode_selfID_type(self_id.desc_type))
	print("Desc......",  self_id.desc_raw.decode('ascii'))
	print("")



def print_System(system):

	print("System data")
	print("Operator Location Type......",  decode_system_operator_location_type(system.operator_location_type))
	print("Classification Type......",  decode_system_classification_type(system.classification_type))
	print("Operator Latitude......",  system.operator_latitude)
	print("Operator Longitude......",  system.operator_longitude)
	print("Area Count......",  system.area_count)
	print("Area Radius......",  system.area_radius)
	print("Area Ceiling......",  system.area_ceiling)
	print("Area Floor......",  system.area_floor)
	print("Category EU......",  decode_system_ua_category(system.category_eu))
	print("Class EU......",  decode_system_ua_class(system.class_eu))
	print("Operator Altitude Geo......",  system.operator_altitude_geo)

	if system.timestamp != 0:
	    print("Timestamp......",  datetime.fromtimestamp((int(system.timestamp) + 1546300800), tz=timezone.utc).strftime('%Y-%m-%d %H:%M %Z'))
	else:
	    print("Timestamp......invalid")
	print("Timestamp raw......",  system.timestamp)
	print("")



def print_OperatorID(operator_id):

	print("Operator ID data")
	print("Operator ID Type......",  decode_operatorID_type(operator_id.id_type))
	print("Operator ID......",  operator_id.id_raw.decode('ascii'))
	print("")


def print_AuthPage(uas, page):
	"""Print one auth page. The total length and last page index come from page 0."""
	auth = uas.auth[page]
	print("Auth data")
	print("Data Page.........",  auth.data_page)
	print("Auth Type.........",  decode_auth_type(auth.auth_type))

	if page == 0:
	    print("Last Page Index...",  auth.last_page_index)
	    print("Length............",  auth.length)

	    if auth.timestamp != 0:
	        print("Timestamp.........",  datetime.fromtimestamp((int(auth.timestamp) + 1546300800), tz=timezone.utc).strftime('%Y-%m-%d %H:%M %Z'))
	    else:
	        print("Timestamp.........invalid")

	    print("Auth Data.........",auth.data_raw[:17].hex())
	else:
	    if page == uas.auth[0].last_page_index:
	        #only print the chars within the specified length of the pages auth message
	        AuthData = auth.data_raw[:(uas.auth[0].length - 17) % 23]
	    else:
	        AuthData = auth.data_raw
	    print("Auth Data.........",AuthData.hex())
	print("")



def decode_basicID_ID_type(IDType):
    string = ""
    if IDType == 0:
        string = "None"
    elif IDType == 1:
        string = "Serial Number"
    elif IDType == 2:
        string = "CAA Registration ID"
    elif IDType == 3:
        string = "UTM Assigned UUID"
    elif IDType == 4:
        string = "specific session ID"

    return string

def decode_basicID_UA_type(UAType):
    string = ""
    if UAType == 0:
        string = "None"
    elif UAType == 1:
        string = "Aeroplane"
    elif UAType == 2:
        string = "Helicopter (or Multirotor)"
    elif UAType == 3:
        string = "Gyroplane"
    elif UAType == 4:
        string = "Hybrid Lift"
    elif UAType == 5:
        string = "Ornithopter"
    elif UAType == 6:
        string = "Glider"
    elif UAType == 7:
        string = "Kite"
    elif UAType == 8:
        string = "Free Balloon"
    elif UAType == 9:
        string = "Captive Balloon"
    elif UAType == 10:
        string = "Airship (such as a blimp)"
    elif UAType == 11:
        string = "Free Fall/Parachute (unpowered)"
    elif UAType == 12:
        string = "Rocket"
    elif UAType == 13:
        string = "Tethered Powered Aircraft"
    elif UAType == 14:
        string = "Ground Obstacle"
    elif UAType == 15:
        string = "Other"

    return string

def decode_location_status(status):
    string = ""
    if status == 0:
        string = "Undeclared"
    elif status == 1:
        string = "Ground"
    elif status == 2:
        string = "Airborne"
    elif status == 3:
        string = "Emergency"
    elif status == 4:
        string = "Remote ID System Failure"

    return string

def location_decode_speed_horizontal(speed_enc, speed_mult):
    if speed_enc == 255:
	    return float('NaN')
    speed_enc = float (speed_enc)
    if speed_mult == 1:
        return float ((float(speed_enc) * 0.75) + (255 * 0.25))
    else:
        return speed_enc * 0.75

def decode_system_ua_category(ua_category):
    string = ""
    if ua_category == 0:
        string = "Undefined"
    elif ua_category == 1:
        string = "Open"
    elif ua_category == 2:
        string = "Specific"
    elif ua_category == 3:
        string = "Certified"

    return string

def decode_system_ua_class(ua_class):
    string = ""
    if ua_class == 0:
        string = "Undefined"
    elif ua_class == 1:
        string = "Class 0"
    elif ua_class == 2:
        string = "Class 1"
    elif ua_class == 3:
        string = "Class 2"
    elif ua_class == 4:
        string = "Class 3"
    elif ua_class == 5:
        string = "Class 4"
    elif ua_class == 6:
        string = "Class 5"
    elif ua_class == 7:
        string = "Class 6"

    return string

def decode_system_operator_location_type(operator_location_type):
    string = ""
    if operator_location_type == 0:
        string = "Take Off"
    elif operator_location_type == 1:
        string = "Dynamic"
    elif operator_location_type == 2:
        string = "Fixed"

    return string

def decode_system_classification_type(system_classification_type):
    string = ""
    if system_classification_type == 0:
        string = "Undeclared"
    elif system_classification_type == 1:
        string = "European Union"

    return string

def decode_location_height_type(height_type):
    string = ""
    if height_type == 0:
        string = "Above Takeoff"
    elif height_type == 1:
        string = "Above Ground Level"

    return string

def decode_selfID_type(selfID_type):
    string = ""
    if selfID_type == 0:
        string = "Text"
    elif selfID_type == 1:
        string = "Emergency"
    elif selfID_type == 2:
        string = "Extended Status"

    return string

def decode_operatorID_type(operatorID_type):
    string = ""
    if operatorID_type == 0:
        string = "Operator ID"

    return string

def decode_location_horiz_accuracy(horiz_accuracy):
    string = ""
    if horiz_accuracy == 0:
        string = "Unknown"
    elif horiz_accuracy == 1:
        string = "10 NM (18.52 km)"
    elif horiz_accuracy == 2:
        string = "4 NM (7.408 km)"
    elif horiz_accuracy == 3:
        string = "2 NM (3.704 km)"
    elif horiz_accuracy == 4:
        string = "1 NM (1.852 km)"
    elif horiz_accuracy == 5:
        string = "0.5 NM (926 m)"
    elif horiz_accuracy == 6:
        string = "0.3 NM (555.6 m)"
    elif horiz_accuracy == 7:
        string = "0.1 NM (185.2 m)"
    elif horiz_accuracy == 8:
        string = "0.05 NM (92.6 m)"
    elif horiz_accuracy == 9:
        string = "30 m"
    elif horiz_accuracy == 10:
        string = "10 m"
    elif horiz_accuracy == 11:
        string = "3 m"
    elif horiz_accuracy == 12:
        string = "1 m"
    else:
        string = "Reserved (%d)" % horiz_accuracy

    return string

def decode_location_vert_accuracy(vert_accuracy):
    string = ""
    if vert_accuracy == 0:
        string = "Unknown"
    elif vert_accuracy == 1:
        string = "150 m"
    elif vert_accuracy == 2:
        string = "45 m"
    elif vert_accuracy == 3:
        string = "25 m"
    elif vert_accuracy == 4:
        string = "10 m"
    elif vert_accuracy == 5:
        string = "3 m"
    elif vert_accuracy == 6:
        string = "1 m"
    else:
        string = "Reserved (%d)" % vert_accuracy

    return string

def decode_location_speed_accuracy(speed_accuracy):
    string = ""
    if speed_accuracy == 0:
        string = "Unknown"
    elif speed_accuracy == 1:
        string = "10 m/s"
    elif speed_accuracy == 2:
        string = "3 m/s"
    elif speed_accuracy == 3:
        string = "1 m/s"
    elif speed_accuracy == 4:
        string = "0.3 m/s"
    else:
        string = "Reserved (%d)" % speed_accuracy

    return string

def decode_location_ts_accuracy(ts_accuracy):
    string = ""
    if ts_accuracy == 0:
        string = "Unknown"
    elif ts_accuracy >= 1 and ts_accuracy <= 15:
        string = "%.1f s" % (ts_accuracy * 0.1)
    else:
        string = "Reserved (%d)" % ts_accuracy

    return string

def decode_auth_type(auth_type):
    string = ""
    if auth_type == 0:
        string = "None"
    elif auth_type == 1:
        string = "UAS ID Signature"
    elif auth_type == 2:
        string = "Operator ID Signature"
    elif auth_type == 3:
        string = "Message Set Signature"
    elif auth_type == 4:
        string = "Network Remote ID"
    elif auth_type == 5:
        string = "Specific Authentication Method"
    elif auth_type >= 6 and auth_type <= 9:
        string = "Reserved (%d)" % auth_type
    elif auth_type >= 10 and auth_type <= 15:
        string = "Private Use (%d)" % auth_type

    return string
