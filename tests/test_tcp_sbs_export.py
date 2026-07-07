from modules import tcp_sbs_export as sbs


def test_ICAO_is_deterministic_and_prefixed_with_FF():
    a = sbs.ICAO("SN1234567890123456")
    b = sbs.ICAO("SN1234567890123456")
    assert a == b
    assert a.startswith("FF")
    assert len(a) == 6  # "FF" + 2-byte checksum as hex


def test_ICAO_known_values():
    # Locks in the XOR-checksum algorithm itself, not just determinism.
    assert sbs.ICAO("A") == "FF0531"
    assert sbs.ICAO("SN12345678901234567") == "FF5567"


def test_ICAO_differs_for_different_input():
    assert sbs.ICAO("DRONE0001") != sbs.ICAO("DRONE0002")


def test_callsign_short_string_not_padded():
    # Documents current behavior: only strings > 8 chars go through
    # first4+last4 and end up 8 chars; shorter strings pass through as-is.
    assert sbs.callsign("SN123") == "SN123"


def test_callsign_exactly_eight_chars_unchanged():
    assert sbs.callsign("12345678") == "12345678"


def test_callsign_long_string_uses_manufacturer_and_serial_prefix_suffix():
    result = sbs.callsign("MANUFACTURERSN12345678")
    assert result == "MANU5678"
    assert len(result) == 8
