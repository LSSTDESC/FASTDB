import pathlib

import db
from admin.load_snana_fits import FITSLoader


def test_load_snana_fits():
    e2td = pathlib.Path( "elasticc2_test_data" )
    assert e2td.is_dir()
    dirs = e2td.glob( "*" )
    dirs = [ d for d in dirs if d.is_dir() ]
    assert len(dirs) > 0

    try:
        loader=FITSLoader( nprocs=5, directories=dirs, processing_version='test_procver',
                           verbose=True, really_do=True )
        loader()

        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "SELECT COUNT(*) FROM host_galaxy" )
            assert cursor.fetchone()[0] == 356
            cursor.execute( "SELECT COUNT(*) FROM diaobject" )
            assert cursor.fetchone()[0] == 346
            cursor.execute( "SELECT COUNT(*) from diasource" )
            assert cursor.fetchone()[0] == 1862
            cursor.execute( "SELECT COUNT(*) FROM diaforcedsource" )
            assert cursor.fetchone()[0] == 52172

            for tab in [ 'ppdb_host_galaxy', 'ppdb_diaobject', 'ppdb_diasource', 'ppdb_diaforcedsource' ]:
                cursor.execute( f"SELECT COUNT(*) FROM {tab}" )
                assert cursor.fetchone()[0] == 0

    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            for tab in [ 'host_galaxy', 'diaobject', 'diasource', 'diaforcedsource', 'root_diaobject' ]:
                cursor.execute( f"TRUNCATE TABLE {tab} CASCADE" )
            cursor.execute( "SELECT id FROM processing_version WHERE description='test_procver'" )
            pid = cursor.fetchone()
            if pid is not None:
                pid = pid[0]
                cursor.execute( "DELETE FROM base_procver_of_procver WHERE procver_id=%(pv)s", { 'pv': pid } )
                cursor.execute( "DELETE FROM processing_version WHERE description='test_procver'" )
                cursor.execute( "DELETE FROM base_processing_version WHERE description='test_procver'" )
            conn.commit()


def test_load_snana_fits_ppdb( snana_fits_ppdb_loaded ):
    with db.DB() as conn:
        cursor = conn.cursor()
        cursor.execute( "SELECT COUNT(*) FROM ppdb_host_galaxy" )
        assert cursor.fetchone()[0] == 356
        cursor.execute( "SELECT COUNT(*) FROM ppdb_diaobject" )
        assert cursor.fetchone()[0] == 346
        cursor.execute( "SELECT COUNT(*) from ppdb_diasource" )
        assert cursor.fetchone()[0] == 1862
        cursor.execute( "SELECT COUNT(*) FROM ppdb_diaforcedsource" )
        assert cursor.fetchone()[0] == 52172

        for tab in [ 'host_galaxy', 'diaobject', 'diasource', 'diaforcedsource' ]:
            cursor.execute( f"SELECT COUNT(*) FROM {tab}" )
            assert cursor.fetchone()[0] == 0
