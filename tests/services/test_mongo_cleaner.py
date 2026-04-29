import pytest
import datetime

import db
from services.source_importer import SourceImporter
from services.mongo_cleaner import MongoCleaner


@pytest.fixture( scope='module' )
def import_30days( barf, alerts_30days_sent_and_brokermessage_consumed, procver_collection ):
    bpv, _pv = procver_collection
    collection_name = f'fastdb_{barf}'
    t0 = alerts_30days_sent_and_brokermessage_consumed

    try:
        si = SourceImporter( bpv['realtime'].id,
                             bpv['realtime_diaobject_position_60000'].id,
                             bpv['realtime_diasource'].id,
                             bpv['realtime_diaforcedsource'].id,
                             None )
        with db.MG() as mongoclient:
            collection = db.get_mongo_collection( mongoclient, collection_name )
            nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = si.import_from_mongo( collection )

        with db.DBCon() as pqcon:
            t1 = pqcon.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                                { 'col': collection_name } )[0][0][0]
            assert t1 > t0
            assert datetime.datetime.now( tz=datetime.UTC ) > t1

        yield nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo

    finally:
        # This fixture is the one everybody else includes, so do all cleanup here
        with db.DBCon() as pqcon:
            pqcon.execute( "DELETE FROM diaforcedsource_extra" )
            pqcon.execute( "DELETE FROM diaforcedsource" )
            pqcon.execute( "DELETE FROM diasource_brokerinfo" )
            pqcon.execute( "DELETE FROM diasource_extra" )
            pqcon.execute( "DELETE FROM diasource" )
            pqcon.execute( "DELETE FROM diaobject_position" )
            pqcon.execute( "DELETE FROM diaobject" )
            pqcon.execute( "DELETE FROM root_diaobject" )
            pqcon.execute( "DELETE FROM diasource_import_time" )
            pqcon.commit()

        with db.MG() as mg:
            collection = db.get_mongo_collection( mg, collection_name )
            collection.delete_many( {} )
            collection = db.get_mongo_collection( mg, "source_thumbnails" )
            collection.delete_many( {} )


@pytest.fixture
def clean_30days_after_consume_60days( barf, import_30days, alerts_60moredays_sent_and_brokermessage_consumed ):
    nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = import_30days
    t1 = alerts_60moredays_sent_and_brokermessage_consumed

    with db.DBCon() as pqconn:
        t0 = pqconn.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                             { 'col': f'fastdb_{barf}' } )[0][0][0]
        assert t0 < t1

    with db.MG() as mongoclient:
        coll = db.get_mongo_collection( mongoclient, f'fastdb_{barf}' )
        nalert = coll.count_documents( {} )
        assert nalert > 2 * nsrc

    cleaner = MongoCleaner()
    cleaner.clean( f'fastdb_{barf}' )

    return t1, nalert


@pytest.mark.skip( reason="mongo_cleaner isn't fully written yet" )
def test_first_import( barf, import_30days ):

    nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = import_30days

    with db.DBCon() as conn:
        assert conn.execute( "SELECT COUNT(*) FROM diaobject" )[0][0][0] == nobj
        assert conn.execute( "SELECT COUNT(*) FROM diaobject_position" )[0][0][0] == nobj
        assert conn.execute( "SELECT COUNT(*) FROM root_diaobject" )[0][0][0] == nroot
        assert conn.execute( "SELECT COUNT(*) FROM diasource" )[0][0][0] == nsrc + nprvsrc
        assert conn.execute( "SELECT COUNT(*) FROM diasource_extra" )[0][0][0] == nsrc + nprvsrc
        assert conn.execute( "SELECT COUNT(*) FROM diaforcedsource" )[0][0][0] == nfrc
        assert conn.execute( "SELECT COUNT(*) FROM diaforcedsource_extra" )[0][0][0] == nfrc
        assert conn.execute( "SELECT COUNT(*) FROM diasource_brokerinfo" )[0][0][0] == ninfo
    with db.MG() as mg:
        coll = db.get_mongo_collection( mg, f'fastdb_{barf}' )
        assert 2 * nsrc == coll.count_documents( {} )
        thumbs = db.get_mongo_collection( mg, 'source_thumbnails' )
        assert nsrc == thumbs.count_documents( {} )


@pytest.mark.skip( reason="mongo_cleaner isn't fully written yet" )
def test_clean_30days_after_consume_60days( barf, procver_collection, import_30days,
                                            clean_30days_after_consume_60days ):
    bpv, _pv = procver_collection
    nobj30, nroot30, npos30, nsrc30, nprvsrc30, nfrc30, ninfo30 = import_30days
    t1, totsofar = clean_30days_after_consume_60days

    with db.MG() as mongoclient:
        coll = db.get_mongo_collection( mongoclient, f'fastdb_{barf}' )
        nleft = coll.count_documents({})

    # The 2 * is because there were two messages per source (two broker classifiers)
    assert nleft + ( 2 * nsrc30 ) == totsofar

    # Now import what's left
    si = SourceImporter( bpv['realtime'].id,
                         bpv['realtime_diaobject_position_60000'].id,
                         bpv['realtime_diasource'].id,
                         bpv['realtime_diaforcedsource'].id,
                         None )
    with db.MG() as mongoclient:
        col = db.get_mongo_collection( mongoclient, f'fastdb_{barf}' )
        nobj, nroot, npos, nsrc, nprvsrc, nfrc, ninfo = si.import_from_mongo( col )

    assert nsrc == nsrc30 + nleft // 2

    with db.DBCon() as conn:
        t2 = conn.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                             { 'col': f'fastdb_{barf}' } )[0][0][0]
        assert t2 > t1
        assert conn.execute( "SELECT COUNT(*) FROM diaobject" )[0][0][0] == nobj + nobj30
        assert conn.execute( "SELECT COUNT(*) FROM diaobject_position" )[0][0][0] == nobj + nobj30
        assert conn.execute( "SELECT COUNT(*) FROM root_diaobject" )[0][0][0] == nroot + nroot30
        assert conn.execute( "SELECT COUNT(*) FROM diasource" )[0][0][0] == nsrc + nprvsrc + nsrc30 + nprvsrc30
        assert conn.execute( "SELECT COUNT(*) FROM diasource_extra" )[0][0][0] == nsrc + nprvsrc + nsrc30 + nprvsrc30
        assert conn.execute( "SELECT COUNT(*) FROM diaforcedsource" )[0][0][0] == nfrc + nfrc30
        assert conn.execute( "SELECT COUNT(*) FROM diaforcedsource_extra" )[0][0][0] == nfrc + nfrc30
        assert conn.execute( "SELECT COUNT(*) FROM diasource_brokerinfo" )[0][0][0] == ninfo + ninfo30

    with db.MG() as mg:
        thumbs = mg.get_mongo_collection( mg, 'source_thumbnails' )
        assert nsrc30 + nsrc == thumbs.count_documents( {} )

    cleaner = MongoCleaner()
    cleaner.clean( f'fastdb_{barf}' )

    with db.MG() as mg:
        col = mg.get_mongo_colletion( mg, f'fastdb_{barf}' )
        assert col.count_documents({}) == 0
