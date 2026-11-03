import csv
from modules import csv_file
from datetime import datetime, timezone

def open_csv(log_path):

    filename = log_path + '/' + datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + '_remoteID_log.csv'
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
         + ['raw data'] + ['SN valid'] + ['manufacturer'] + ['model'] + ['type'] + ['application'] + ['weight mtow [kg]'] + ['dimensions [mm]']
         )
    return filename


def _auth_csv_fields(uas):
    """Auth pages as CSV columns. Page 0 holds the total length; with length 0
    (unknown, e.g. BT4) a later page is saved complete."""
    length = uas.auth[0].length if uas.auth[0].valid else 0
    fields = []
    for page, a in enumerate(uas.auth):
        if not a.valid:
            # page 0 columns: data page, auth type, last page index, length (0, not empty), timestamp, data
            fields += ['', '', '', 0, '', ''] if page == 0 else ['', '', '']
            continue
        if page == 0:
            data = a.data_raw[:17] if length > 17 else a.data_raw[:length]
            fields += [a.data_page, a.auth_type, a.last_page_index, a.length, a.timestamp, data.hex()]
        else:
            if length > (page * 23 + 17) or length == 0:
                n = 23
            else:
                n = length - 17 - 23 * (page - 1)
            data = a.data_raw[:n] if n > 0 else b''
            fields += [a.data_page, a.auth_type, data.hex()]
    return fields


def _basic_id_csv_fields(b):
    if not b.valid:
        return ['', '', '']
    if b.is_text:
        basic_id = b.id_raw.decode('ascii').rstrip('\x00')
    else: # save as hex values
        basic_id = b.id_raw.hex()
    return [str(basic_id), b.ua_type, b.id_type]


def write_csv(msg, filename):
    """Append one row for a decoded RemoteIDMessage (modules/decode.py)."""
    data_json = msg.json
    uas = msg.uas
    epoch_timestamp = datetime.fromtimestamp(data_json.get('timestamp')/1000, tz=timezone.utc)

    row = [data_json.get('sensor ID'), data_json.get('RSSI'), data_json.get('channel'), data_json.get('timestamp'),
           epoch_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-4], data_json.get('MAC address'), data_json.get('type')]
    row += _basic_id_csv_fields(uas.basic_id[0])
    row += _basic_id_csv_fields(uas.basic_id[1])

    loc = uas.location
    if loc.valid:
        location_timestamp = "invalid"
        if loc.timestamp > 0 and loc.timestamp <= 60*60:
            location_timestamp = "%02i:%02i.%02i" % (int(loc.timestamp/60), int(loc.timestamp % 60), int(100*(loc.timestamp - int(loc.timestamp))))
        row += [loc.status, loc.direction, loc.speed_horizontal, loc.speed_vertical, loc.latitude, loc.longitude,
                loc.altitude_baro, loc.altitude_geo, loc.height_type, loc.height, loc.horiz_accuracy, loc.vert_accuracy,
                loc.baro_accuracy, loc.speed_accuracy, loc.ts_accuracy, loc.timestamp, location_timestamp]
    else:
        row += [''] * 17

    if uas.self_id.valid:
        row += [uas.self_id.desc_type, str(uas.self_id.desc_raw.decode('ascii').rstrip('\x00'))]
    else:
        row += [0, '']

    sys_ = uas.system
    if sys_.valid:
        system_timestamp = "invalid"
        if sys_.timestamp != 0:
            system_timestamp = datetime.fromtimestamp((int(sys_.timestamp) + 1546300800), tz=timezone.utc).strftime('%Y-%m-%d %H:%M %Z')
        row += [sys_.operator_location_type, sys_.classification_type, sys_.operator_latitude, sys_.operator_longitude,
                sys_.area_count, sys_.area_radius, sys_.area_ceiling, sys_.area_floor, sys_.category_eu, sys_.class_eu,
                sys_.operator_altitude_geo, sys_.timestamp, str(system_timestamp)]
    else:
        row += [''] * 13

    if uas.operator_id.valid:
        row += [uas.operator_id.id_type, uas.operator_id.id_raw.decode('ascii').rstrip('\x00')]
    else:
        row += ['', '']

    row += _auth_csv_fields(uas)

    raw = msg.raw if msg.raw is not None else b''
    if raw[-1:] in (b'\x00', b'\n'):
        raw = raw[:-1]
    row.append(raw.hex())

    extra = msg.extra
    if extra.get('SN present') == 1:
        row += [extra.get('SN valid'), extra.get('manufacturer'), extra.get('model'), extra.get('type'),
                extra.get('application'), extra.get('weight'), extra.get('dimensions')]
    else:
        row += [''] * 7

    csv_file.append_row(filename, row)
