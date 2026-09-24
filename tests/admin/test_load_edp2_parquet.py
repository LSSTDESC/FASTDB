import db


def test_load_edp2_parquet( edp2_loaded_module ):
    with db.DBCon( dictcursor=True ) as dbcon:
        rows = dbcon.execute( "SELECT rootid, diaobjectid FROM diaobject" )
        assert len(rows) == 100
        # rootids = set( r['rootid'] for r in rows )
        diaobjectids = set( r['diaobjectid'] for r in rows )

        rows = dbcon.execute( "SELECT diasourceid, base_procver_id, diaobjectid FROM diasource" )
        assert len(rows) == 716
        diasources = set( ( r['diasourceid'], r['base_procver_id'] ) for r in rows )
        assert all( r['diaobjectid'] in diaobjectids for r in rows )
        rows = dbcon.execute( "SELECT diasourceid, base_procver_id FROM diasource_extra" )
        assert len(rows) == 716
        diasourceextras = set( ( r['diasourceid'], r['base_procver_id'] ) for r in rows )
        assert diasourceextras == diasources

        rows = dbcon.execute( "SELECT diaforcedsourceid, base_procver_id, diaobjectid, visit FROM diaforcedsource" )
        assert len(rows) == 30620
        diaforcedsources = set( ( r['base_procver_id'], r['diaobjectid'], r['visit'] ) for r in rows )
        forcedobjs = set( r['diaobjectid'] for r in rows )
        assert forcedobjs == diaobjectids
        assert all( r['diaforcedsourceid'] is None for r in rows )
        rows = dbcon.execute( "SELECT diaforcedsourceid, base_procver_id, diaobjectid, visit "
                              "FROM diaforcedsource_extra" )
        assert len(rows) == 30620
        diaforcedsourceextras = set( ( r['base_procver_id'], r['diaobjectid'], r['visit'] ) for r in rows )
        assert diaforcedsourceextras == diaforcedsources
