import base64
import csv

from modules import log_remote_id


def _data_json(**overrides):
    base = {
        'sensor ID': 'DS-TEST-1',
        'RSSI': -60,
        'channel': 6,
        'timestamp': 1700000000000,
        'MAC address': 'AA:BB:CC:DD:EE:FF',
        'type': 'WLAN Beacon',
        'raw': base64.b64encode(b'\x01\x02\x03').decode('ascii'),
    }
    base.update(overrides)
    return base


def _extra_json(**overrides):
    base = {
        'SN present': 1, 'SN valid': 1, 'manufacturer': 'Acme',
        'model': 'Falcon', 'type': 'multirotor', 'application': 'mapping',
        'weight': 1.5, 'dimensions': '400x400x150',
    }
    base.update(overrides)
    return base


def _read_csv(filename):
    with open(filename, newline='', encoding='utf-8') as f:
        return list(csv.reader(f))


def test_header_and_row_have_same_column_count(tmp_path, golden):
    filename = log_remote_id.open_csv(str(tmp_path))
    log_remote_id.write_csv(_data_json(), golden.payload(), golden.valid_blocks(),
                             filename, _extra_json())

    rows = _read_csv(filename)
    header, data_row = rows[0], rows[1]
    assert len(header) == len(data_row)


def test_basic_id1_decoded_by_idtype_not_uatype(tmp_path, golden):
    """Regression test for the historical copy-paste bug where BasicID_1
    branched on UAType_1 instead of IDType_1. golden.ua_type_1 (3) is
    deliberately NOT in {1, 2} while golden.id_type_1 (2) IS, so the
    correct behavior is an ASCII-decoded serial, not a hex dump.
    """
    from tests.conftest import BASIC_ID1_STR

    filename = log_remote_id.open_csv(str(tmp_path))
    log_remote_id.write_csv(_data_json(), golden.payload(), golden.valid_blocks(),
                             filename, _extra_json())

    header, data_row = _read_csv(filename)
    row = dict(zip(header, data_row))
    assert row['BasicID_1'] == BASIC_ID1_STR


def test_valid_fields_present_in_row(tmp_path, golden):
    filename = log_remote_id.open_csv(str(tmp_path))
    log_remote_id.write_csv(_data_json(), golden.payload(), golden.valid_blocks(),
                             filename, _extra_json())

    header, data_row = _read_csv(filename)
    row = dict(zip(header, data_row))

    assert row['sensor ID'] == 'DS-TEST-1'
    assert float(row['Latitude']) == golden.latitude
    assert float(row['Longitude']) == golden.longitude
    assert row['manufacturer'] == 'Acme'
    assert row['model'] == 'Falcon'


def test_invalid_blocks_write_empty_fields(tmp_path, golden):
    """When a block's *_valid flag is 0, its CSV columns must be blank,
    not stale/garbage bytes decoded from the (irrelevant) payload region.
    """
    vb = golden.valid_blocks()
    vb.LocationValid = 0
    vb.SystemValid = 0
    vb.OperatorIDValid = 0
    vb.SelfIDValid = 0
    vb.BasicID0_valid = 0
    vb.BasicID1_valid = 0

    filename = log_remote_id.open_csv(str(tmp_path))
    log_remote_id.write_csv(_data_json(), golden.payload(), vb, filename, {})

    header, data_row = _read_csv(filename)
    row = dict(zip(header, data_row))

    for col in ['Status', 'Latitude', 'Longitude', 'OperatorLocationType',
                'OperatorId', 'Desc', 'BasicID_0', 'BasicID_1']:
        assert row[col] == ''


def test_auth_page_not_valid_defaults_to_empty(tmp_path, golden):
    vb = golden.valid_blocks()
    vb.AuthValid[0] = 0
    vb.AuthValid[1] = 0

    filename = log_remote_id.open_csv(str(tmp_path))
    log_remote_id.write_csv(_data_json(), golden.payload(), vb, filename, {})

    header, data_row = _read_csv(filename)
    row = dict(zip(header, data_row))
    assert row['AuthPage_0_Data'] == ''
    assert row['AuthPage_1_Data'] == ''


def test_no_extra_json_does_not_crash(tmp_path, golden):
    filename = log_remote_id.open_csv(str(tmp_path))
    # extra_json empty dict, as mqtt_sub.py passes when 'extra' key is absent
    log_remote_id.write_csv(_data_json(), golden.payload(), golden.valid_blocks(),
                             filename, {})
    header, data_row = _read_csv(filename)
    row = dict(zip(header, data_row))
    assert row['manufacturer'] == ''
