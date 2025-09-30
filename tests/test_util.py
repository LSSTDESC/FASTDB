import pathlib
import util

# OMG LOTS OF TESTS STILL NEED TO BE WRITTEN


def test_get_alert_schema():
    schema = util.get_alert_schema()
    assert set( schema.keys() ) == { 'alert', 'diaobject', 'diasource', 'diaforcedsource', 'MPCORB',
                                     'sssource', 'brokermessage', 'alert_schema_file', 'brokermessage_schema_file' }
    for key in [ 'alert', 'diaobject', 'diasource', 'diaforcedsource', 'MPCORB', 'sssource', 'brokermessage' ]:
        assert isinstance( schema[key], dict )
    assert isinstance( schema['alert_schema_file'], pathlib.Path )
    assert isinstance( schema['brokermessage_schema_file'], pathlib.Path )



def test_parse_sexigesimal():
    assert util.parse_sexigesimal( "00:00:00" ) == 0.
    assert util.parse_sexigesimal( "-00:00:00" ) == 0.
    assert util.parse_sexigesimal( "+00:00:00" ) == 0.

    assert util.parse_sexigesimal( "1:30:0" ) == 1.5
    assert util.parse_sexigesimal( "-00:30:00" ) == -0.5
    assert util.parse_sexigesimal( "+00:30:00" ) == 0.5

    # TODO : more, including lots of things with spaces in places, decimal seconds, etc.
