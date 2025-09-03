import util

# OMG LOTS OF TESTS STILL NEED TO BE WRITTEN


def test_parse_sexigesimal():
    assert util.parse_sexigesimal( "00:00:00" ) == 0.
    assert util.parse_sexigesimal( "-00:00:00" ) == 0.
    assert util.parse_sexigesimal( "+00:00:00" ) == 0.

    assert util.parse_sexigesimal( "1:30:0" ) == 1.5
    assert util.parse_sexigesimal( "-00:30:00" ) == -0.5
    assert util.parse_sexigesimal( "+00:30:00" ) == 0.5

    # TODO : more, including lots of things with spaces in places, decimal seconds, etc.
