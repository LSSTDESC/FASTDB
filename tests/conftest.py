import sys
import os
import pytest
import pathlib
import datetime
import subprocess

from pymongo import MongoClient

from db import ProcessingVersion, DiaObject, DiaSource, DiaForcedSource, Snapshot, DB, AuthUser
from util import asUUID

sys.path.insert( 0, pathlib.Path(__file__).parent )
# For cleanliness, a bunch of fixtures are broken
#   out into their own files.  To be able to see
#   them, put those files in this list below.
#   (pytest is kind of a beast).  Those files
#   should all live in the fixtures subdirectory.
pytest_plugins = [ 'fixtures.alertcycle' ]


@pytest.fixture( scope='session' )
def procver():
    pv = ProcessingVersion( id=1, description='test_procesing_version',
                            validity_start=datetime.datetime( 2025, 2, 14, 0, 0, 0 ),
                            validity_end=datetime.datetime( 2999, 2, 14, 0, 0, 0 )
                           )
    pv.insert()

    yield pv
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM processing_version WHERE id=%(id)s",
                        { 'id': pv.id } )
        con.commit()


@pytest.fixture
def procver1():
    pv = ProcessingVersion( id=42,
                            description='pv42',
                            validity_start=datetime.datetime( 2025, 2, 14, 1, 2, 3 ),
                            validity_end=datetime.datetime( 2030, 2, 14, 1, 2, 3 )
                           )
    pv.insert()

    yield pv
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM processing_version WHERE id=%(id)s",
                        { 'id': pv.id } )
        con.commit()


@pytest.fixture
def procver2():
    pv = ProcessingVersion( id=23,
                            description='pv23',
                            validity_start=datetime.datetime( 2015, 10, 21, 12, 15, 0 ),
                            validity_end=datetime.datetime( 2045, 10, 21, 12, 15, 0 )
                           )
    pv.insert()

    yield pv
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM processing_version WHERE id=%(id)s",
                        { 'id': pv.id } )
        con.commit()


@pytest.fixture
def snapshot1():
    ss = Snapshot( id=23,
                   description='ss23',
                   creation_time=datetime.datetime( 2000, 1, 1, 0, 0, 0 )
                  )
    ss.insert()

    yield ss
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM snapshot WHERE id=%(id)s",
                        { 'id': ss.id } )
        con.commit()


@pytest.fixture
def snapshot2():
    ss = Snapshot( id=42,
                   description='ss42',
                   creation_time=datetime.datetime( 2001, 12, 31, 11, 59, 59 )
                  )
    ss.insert()

    yield ss
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM snapshot WHERE id=%(id)s",
                        { 'id': ss.id } )
        con.commit()


@pytest.fixture
def obj1( procver1 ):
    obj = DiaObject( diaobjectid=42,
                     processing_version=procver1.id,
                     radecmjdtai=60000.,
                     ra=42.,
                     dec=13
                    )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaobject WHERE diaobjectid=%(id)s AND processing_Version=%(pv)s",
                        { 'id': obj.diaobjectid, 'pv': procver1.id } )
        con.commit()


@pytest.fixture
def src1( obj1, procver1 ):
    src = DiaSource( diasourceid=42,
                     processing_version=procver1.id,
                     diaobjectid=obj1.diaobjectid,
                     diaobject_procver=obj1.processing_version,
                     visit=64,
                     detector=9,
                     band='r',
                     midpointmjdtai=59000.,
                     ra=obj1.ra + 0.0001,
                     dec=obj1.dec + 0.0001,
                     psfflux=3.,
                     psffluxerr=0.1
                    )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diasource WHERE diasourceid=%(id)s",
                        { 'id': src.diasourceid } )
        con.commit()


@pytest.fixture
def src1_pv2( obj1, procver2 ):
    src = DiaSource( diasourceid=42,
                     processing_version=procver2.id,
                     diaobjectid=obj1.diaobjectid,
                     diaobject_procver=obj1.processing_version,
                     visit=64,
                     detector=9,
                     band='r',
                     midpointmjdtai=59000.,
                     ra=obj1.ra + 0.0001,
                     dec=obj1.dec + 0.0001,
                     psfflux=3.,
                     psffluxerr=0.1
                    )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diasource WHERE diasourceid=%(id)s",
                        { 'id': src.diasourceid } )
        con.commit()


@pytest.fixture
def forced1( obj1, procver1 ):
    src = DiaForcedSource( diaforcedsourceid=42,
                           processing_version=procver1.id,
                           diaobjectid=obj1.diaobjectid,
                           diaobject_procver=obj1.processing_version,
                           visit=128.,
                           detector=10.,
                           midpointmjdtai=59100.,
                           band='i',
                           ra=obj1.ra - 0.0001,
                           dec=obj1.dec - 0.0001,
                           psfflux=4.,
                           psffluxerr=0.2,
                           scienceflux=7.,
                           sciencefluxerr=0.5,
                           time_processed=None,
                           time_withdrawn=None
                          )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaforcedsource WHERE diaforcedsourceid=%(id)s",
                        { 'id': src.diaforcedsourceid } )
        con.commit()


@pytest.fixture
def forced1_pv2( obj1, procver2 ):
    src = DiaForcedSource( diaforcedsourceid=42,
                           processing_version=procver2.id,
                           diaobjectid=obj1.diaobjectid,
                           diaobject_procver=obj1.processing_version,
                           visit=128.,
                           detector=10.,
                           midpointmjdtai=59100.,
                           band='i',
                           ra=obj1.ra - 0.0001,
                           dec=obj1.dec - 0.0001,
                           psfflux=4.,
                           psffluxerr=0.2,
                           scienceflux=7.,
                           sciencefluxerr=0.5,
                           time_processed=None,
                           time_withdrawn=None
                          )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaforcedsource WHERE diaforcedsourceid=%(id)s",
                        { 'id': src.diaforcedsourceid } )
        con.commit()


@pytest.fixture( scope='session' )
def test_user():
    # Test user with password 'test_password'
    user = AuthUser( id=asUUID('788e391e-ca63-4057-8788-25cc8647e722'),
                     username='test',
                     displayname='test user',
                     email='test@nowhere.org',
                     pubkey="""-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA1QLihZJ78NHKppUBUaZI
sel7WFKp/3Pr14nbel+BpfOVWrIIIiMegQSAliWRszNLQezKwHTXM4DUxZu7LG/q
zut37v5WSVWCK8wSW+zy6e9vnuVkcrzdEJgkztUaiC8lMnHVE0ycpLTICcAu0wtv
WP32ScyNbiHidyPZwNd9XB4juLl9j7K6hs7WQwmeMOyw8dUZuE8b/jiHrAxxnHjE
Sli8bjR7I6X3AX8U81bP4qFjTjGuy85dIeZEbyS6UpbmkZ+imr/0wLa9knRoW0hU
Uz8p+P/Vts3rimpQtPajtRzCpTY4lRfh05YDmr2rc1WHJ/IPu3v7sIUg8K/egoPJ
VU3c2QYGpwmpnldbb+bpSUXxpsQVtFw5pHmqEbfKXWNM8CTkii8s6bI03/JQREBU
L3OzCGclvS8lQ+ZXAQaMyjshMqMFud3E9RS5EFxpSfk92r+RY7PgaYs9PX7x33zU
k/937nk7sTR5OEKFgxRDx61svk5UJIPQib5SnIDRNAqeKhxg23q5ZqDMBVk1rAhI
xFuX4Hj8VtG89J3DSVJue4psF0wTYceUhUleJCG3gPxAyE2g4ObZZ9mh/gI1KG6v
Np9CFWk9eMSeehEI1YKyPY8Hdv50PmIvN2zgxbo2wccspwCVTrtdKoQebpVAAu3v
tyOci9saPPfI1bNnKD202zsCAwEAAQ==
-----END PUBLIC KEY-----
""",
                     privkey={"iv": "z84DFtRURdKFhn3b",
                              "salt": "57B2Nq/ZToHhVM+1DEq30g==",
                              "privkey": "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"} # noqa: E501
                    )
    user.insert()

    yield user

    user.delete_from_db()


@pytest.fixture( scope='session' )
def snana_fits_ppdb_loaded():
    e2td = pathlib.Path( "elasticc2_test_data" )
    assert e2td.is_dir()
    dirs = e2td.glob( "*" )
    dirs = [ d for d in dirs if d.is_dir() ]
    assert len(dirs) > 0

    try:
        com = [ "python", "/code/src/admin/load_snana_fits.py",
                "-n", "5",
                "-v",
                "--ppdb",
                "-d",
               ]
        com.extend( dirs )
        com.append( "--do" )

        res = subprocess.run( com, capture_output=True )
        assert res.returncode == 0

        yield True

    finally:
        with DB() as conn:
            cursor = conn.cursor()
            for tab in [ 'ppdb_host_galaxy', 'ppdb_diaobject', 'ppdb_diasource', 'ppdb_diaforcedsource' ]:
                cursor.execute( f"TRUNCATE TABLE {tab} CASCADE" )
            conn.commit()


@pytest.fixture
def mongoclient():
    host = os.getenv( 'MONGODB_HOST' )
    dbname = os.getenv( 'MONGODB_DBNAME' )
    user = os.getenv( "MONGODB_ALERT_READER_USER" )
    password = os.getenv( "MONGODB_ALERT_READER_PASSWD" )
    client = MongoClient( f"mongodb://{user}:{password}@{host}:27017/{dbname}?authSource={dbname}" )
    return client


@pytest.fixture
def mongoclient_rw():
    host = os.getenv( 'MONGODB_HOST' )
    dbname = os.getenv( 'MONGODB_DBNAME' )
    user = os.getenv( "MONGODB_ALERT_WRITER_USER" )
    password = os.getenv( "MONGODB_ALERT_WRITER_PASSWD" )
    client = MongoClient( f"mongodb://{user}:{password}@{host}:27017/{dbname}?authSource={dbname}" )
    return client
