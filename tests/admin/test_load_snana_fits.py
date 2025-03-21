import pathlib
import subprocess

import db


def test_load_snana_fits():
    e2td = pathlib.Path( "elasticc2_test_data" )
    assert e2td.is_dir()
    dirs = e2td.glob( "*" )
    dirs = [ d for d in dirs if d.is_dir() ]
    assert len(dirs) > 0

    try:
        com = [ "python", "/code/src/admin/load_snana_fits.py",
                "-n", "5",
                "--pv", "test_procver",
                "-s", "test_snapshot",
                "-v",
                "-d"
               ]
        com.extend( dirs )
        com.append( "--do" )

        res = subprocess.run( com, capture_output=True )

        assert res.returncode == 0

        with db.DB() as conn:
            cursor = conn.cursor()
            cursor.execute( "SELECT COUNT(*) FROM processing_version" )
            assert cursor.fetchone()[0] == 1
            cursor.execute( "SELECT COUNT(*) FROM snapshot" )
            assert cursor.fetchone()[0] == 1
            cursor.execute( "SELECT COUNT(*) FROM host_galaxy" )
            assert cursor.fetchone()[0] == 356
            cursor.execute( "SELECT COUNT(*) FROM diaobject" )
            assert cursor.fetchone()[0] == 346
            cursor.execute( "SELECT COUNT(*) FROM diaobject_snapshot" )
            assert cursor.fetchone()[0] == 346
            cursor.execute( "SELECT COUNT(*) from diasource" )
            assert cursor.fetchone()[0] == 1862
            cursor.execute( "SELECT COUNT(*) from diasource_snapshot" )
            assert cursor.fetchone()[0] == 1862
            cursor.execute( "SELECT COUNT(*) FROM diaforcedsource" )
            assert cursor.fetchone()[0] == 52172
            cursor.execute( "SELECT COUNT(*) FROM diaforcedsource_snapshot" )
            assert cursor.fetchone()[0] == 52172

            for tab in [ 'ppdb_host_galaxy', 'ppdb_diaobject', 'ppdb_diasource', 'ppdb_diaforcedsource' ]:
                cursor.execute( f"SELECT COUNT(*) FROM {tab}" )
                assert cursor.fetchone()[0] == 0

    finally:
        with db.DB() as conn:
            cursor = conn.cursor()
            for tab in [ 'processing_version', 'snapshot', 'host_galaxy',
                         'diaobject', 'diaobject_snapshot',
                         'diasource', 'diasource_snapshot',
                         'diaforcedsource', 'diaforcedsource_snapshot' ]:
                cursor.execute( f"TRUNCATE TABLE {tab} CASCADE" )
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

        for tab in [ 'processing_version', 'snapshot', 'host_galaxy',
                     'diaobject', 'diaobject_snapshot', 'diasource', 'diasource_snapshot',
                     'diaforcedsource', 'diaforcedsource_snapshot' ]:
            cursor.execute( f"SELECT COUNT(*) FROM {tab}" )
            assert cursor.fetchone()[0] == 0
