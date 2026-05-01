import uuid
import pytest

from db import DB, PPDBHostGalaxy, PPDBDiaObject, PPDBDiaSource, PPDBDiaForcedSource
from util import env_as_bool

from basetest import BaseTestDB

# IMPORTANT: Because sana_fits_ppdb_loaded is a session fixture, if any
# test that uses it runs before these tests, some of these tests will
# fail.  Surprsiingly, many of them still work.  That was a combination
# of luck, and moving the "band" in TestPPDBDiaSource's safe_to_modify
# list further down so that it wouldn't be used.  However, some tests still fail.

# As such, the classes in this file are not routinely run, but are
# marked to be skipped unless the env var RUN_PPDB_DBTESTS is set.
# These basic database tests have been stable for a while, so I'm not
# *too* worried, but sometimes I should probably actually run those
# tests to make sure it's still good.


@pytest.fixture
def ppdbobj1():
    obj = PPDBDiaObject( diaobjectid=42,
                         ra=42.,
                         dec=13,
                         validitystartmjdtai=60000.
                        )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM ppdb_diaobject WHERE diaobjectid=%(id)s",
                        { 'id': obj.diaobjectid } )
        con.commit()


@pytest.fixture
def ppdbobj2():
    obj = PPDBDiaObject( diaobjectid=23,
                         ra=42.,
                         dec=14,
                         validitystartmjdtai=60001.
                        )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM ppdb_diaobject WHERE diaobjectid=%(id)s",
                        { 'id': obj.diaobjectid } )
        con.commit()


@pytest.fixture
def ppdbobj3():
    obj = PPDBDiaObject( diaobjectid=64738,
                         ra=42.,
                         dec=15,
                         validitystartmjdtai=60002.
                        )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM ppdb_diaobject WHERE diaobjectid=%(id)s",
                        { 'id': obj.diaobjectid } )
        con.commit()


# These have lots of redundancies with test_diaobject.py,
# test_host_galaxy.py, test_diasource.py, test_diaforcedsource.py

@pytest.mark.skipif( not env_as_bool('RUN_PPDB_DBTESTS'), reason='Set RUN_PPDB_DBTESTS to run tet' )
class TestPPDBHostGalaxy( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self ):
        self.cls = PPDBHostGalaxy
        self.columns = {
            'id',
            'objectid',
            'ra',
            'dec',
            'petroflux_r',
            'petroflux_r_err',
            'stdcolor_u_g',
            'stdcolor_g_r',
            'stdcolor_r_i',
            'stdcolor_i_z',
            'stdcolor_z_y',
            'stdcolor_u_g_err',
            'stdcolor_g_r_err',
            'stdcolor_r_i_err',
            'stdcolor_i_z_err',
            'stdcolor_z_y_err',
            'pzmode',
            'pzmean',
            'pzstd',
            'pzskew',
            'pzkurt',
            'pzquant000',
            'pzquant010',
            'pzquant020',
            'pzquant030',
            'pzquant040',
            'pzquant050',
            'pzquant060',
            'pzquant070',
            'pzquant080',
            'pzquant090',
            'pzquant100',
            'flags',
        }
        self.safe_to_modify = [
            'objectid',
            'ra',
            'dec',
            'petroflux_r',
            'petroflux_r_err',
            'stdcolor_u_g',
            'stdcolor_g_r',
            'stdcolor_r_i',
            'stdcolor_i_z',
            'stdcolor_z_y',
            'stdcolor_u_g_err',
            'stdcolor_g_r_err',
            'stdcolor_r_i_err',
            'stdcolor_i_z_err',
            'stdcolor_z_y_err',
            'pzmode',
            'pzmean',
            'pzstd',
            'pzskew',
            'pzkurt',
            'pzquant000',
            'pzquant010',
            'pzquant020',
            'pzquant030',
            'pzquant040',
            'pzquant050',
            'pzquant060',
            'pzquant070',
            'pzquant080',
            'pzquant090',
            'pzquant100',
            'flags',
        ]
        self.uniques = []

        self.obj1 = PPDBHostGalaxy( id=uuid.uuid4(),
                                    objectid=42,
                                    ra=13.,
                                    dec=-66.
                                   )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = PPDBHostGalaxy( id=uuid.uuid4(),
                                    objectid=23,
                                    ra=137.,
                                    dec=42.,
                                )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'id': uuid.uuid4(),
                       'objectid': 31337,
                       'ra': 32.,
                       'dec': 64.
                       }


@pytest.mark.skipif( not env_as_bool('RUN_PPDB_DBTESTS'), reason='Set RUN_PPDB_DBTESTS to run tet' )
class TestPPDBDiaObject( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self ):
        self.cls = PPDBDiaObject
        self.columns = {
            'diaobjectid',
            'validitystartmjdtai',
            'ra',
            'raerr',
            'dec',
            'decerr',
            'ra_dec_cov',
            'nearbyextobj1id',
            'nearbyextobj1',
            'nearbyextobj1sep',
            'nearbyextobj2id',
            'nearbyextobj2',
            'nearbyextobj2sep',
            'nearbyextobj3id',
            'nearbyextobj3',
            'nearbyextobj3sep',
            'nearbylowzgal',
            'nearbylowzgalsep',
        }
        self.safe_to_modify = [
            'validitystartmjdtai',
            'ra',
            'raerr',
            'dec',
            'decerr',
            'ra_dec_cov',
            'nearbyextobj1',
            'nearbyextobj1sep',
            'nearbyextobj2',
            'nearbyextobj2sep',
            'nearbyextobj3',
            'nearbyextobj3sep',
            'nearbylowzgal',
            'nearbylowzgalsep',
        ]
        self.uniques = []

        self.obj1 = PPDBDiaObject( diaobjectid=1,
                                   validitystartmjdtai=60001.,
                                   ra=42.,
                                   dec=128. )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = PPDBDiaObject( diaobjectid=2,
                                   validitystartmjdtai=60002.,
                                   ra=23.,
                                   dec=-42. )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'validitystartmjdtai': 60003.,
                       'diaobjectid': 3,
                       'ra': 64.,
                       'dec': -23. }


@pytest.mark.skipif( not env_as_bool('RUN_PPDB_DBTESTS'), reason='Set RUN_PPDB_DBTESTS to run tet' )
class TestPPDBDiaSource( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, ppdbobj1 ):
        self.cls = PPDBDiaSource
        self.columns = {
            'diaobjectid',
            'visit',
            'ssobjectid',
            'detector',
            'x',
            'y',
            'xerr',
            'yerr',
            'x_y_cov',
            'band',
            'midpointmjdtai',
            'ra',
            'raerr',
            'dec',
            'decerr',
            'ra_dec_cov',
            'psfflux',
            'psffluxerr',
            'psflnl',
            'psfchi2',
            'psfndata',
            'snr',
            'scienceflux',
            'sciencefluxerr',
            'templateflux',
            'templatefluxerr',
            'extendedness',
            'reliability',
            'ixx',
            'iyy',
            'ixy',
            'ixxpsf',
            'iyypsf',
            'ixypsf',
            'flags',
            'pixelflags',
            'diasourceid',
            'parentdiasourceid',
            'apflux',
            'apfluxerr',
            'bboxsize',
            'timeprocessedmjdtai',
            'timewithdrawnmjdtai',
        }
        self.safe_to_modify = [
            'detector',
            'x',
            'y',
            'xerr',
            'yerr',
            'x_y_cov',
            # 'band',         # Moved down for reasons described in comments at the top of this file
            'midpointmjdtai',
            'ra',
            'raerr',
            'dec',
            'decerr',
            'ra_dec_cov',
            'psfflux',
            'psffluxerr',
            'psflnl',
            'psfchi2',
            'psfndata',
            'snr',
            'scienceflux',
            'sciencefluxerr',
            'templateflux',
            'templatefluxerr',
            'extendedness',
            'reliability',
            'ixx',
            'iyy',
            'ixy',
            'ixxpsf',
            'iyypsf',
            'ixypsf',
            'flags',
            'pixelflags',
            'diasourceid',
            'parentdiasourceid',
            'apflux',
            'apfluxerr',
            'bboxsize',
            'timeprocessedmjdtai',
            'timewithdrawnmjdtai',
            'band',
        ]
        self.uniques = []

        self.obj1 = PPDBDiaSource( diaobjectid=ppdbobj1.diaobjectid,
                                   visit=1,
                                   detector=1,
                                   band='r',
                                   midpointmjdtai=60000.,
                                   ra=42.0001,
                                   dec=12.9998,
                                   psfflux=123.4,
                                   psffluxerr=5.6,
                                   pixelflags=0,
                                   flags=0
                                  )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = PPDBDiaSource( diaobjectid=ppdbobj1.diaobjectid,
                                   visit=2,
                                   detector=2,
                                   band='i',
                                   midpointmjdtai=60010.,
                                   ra=42.0002,
                                   dec=13.0001,
                                   psfflux=124.6,
                                   psffluxerr=8.0,
                                   pixelflags=0,
                                   flags=0
                                  )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'diaobjectid': ppdbobj1.diaobjectid,
                       'visit': 3,
                       'detector': 3,
                       'band': 'g',
                       'midpointmjdtai': 60015.,
                       'ra': 41.9999,
                       'dec': 13.0002,
                       'psfflux': 135.7,
                       'psffluxerr': 9.1 }


@pytest.mark.skipif( not env_as_bool('RUN_PPDB_DBTESTS'), reason='Set RUN_PPDB_DBTESTS to run tet' )
class TestPPDBDiaForcedSource( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, ppdbobj1 ):
        self.cls = PPDBDiaForcedSource
        self.columns = {
            'diaobjectid',
            'visit',
            'detector',
            'midpointmjdtai',
            'band',
            'ra',
            'dec',
            'psfflux',
            'psffluxerr',
            'scienceflux',
            'sciencefluxerr',
            'timeprocessedmjdtai',
            'timewithdrawnmjdtai',
            'diaforcedsourceid'
        }
        self.safe_to_modify = [
            'detector',
            'midpointmjdtai',
            'band',
            'ra',
            'dec',
            'psfflux',
            'psffluxerr',
            'scienceflux',
            'sciencefluxerr',
            'timeprocessedmjdtai',
            'timewithdrawnmjdtai',
            'diaforcedsourceid'
        ]
        self.uniques = []

        self.obj1 = PPDBDiaForcedSource( diaobjectid=ppdbobj1.diaobjectid,
                                         visit=1,
                                         detector=1,
                                         midpointmjdtai=60000.,
                                         band='r',
                                         ra=42.,
                                         dec=13.,
                                         psfflux=123.4,
                                         psffluxerr=5.6,
                                         scienceflux=234.5,
                                         sciencefluxerr=7.8,
                                         timeprocessedmjdtai=60000.,
                                         timewithdrawnmjdtai=None,
                                         diaforcedsourceid=1
                                        )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = PPDBDiaForcedSource( diaobjectid=ppdbobj1.diaobjectid,
                                         visit=2,
                                         detector=2,
                                         midpointmjdtai=60001.,
                                         band='i',
                                         ra=42.0001,
                                         dec=13.0001,
                                         psfflux=123.5,
                                         psffluxerr=5.7,
                                         scienceflux=235.5,
                                         sciencefluxerr=7.9,
                                         timeprocessedmjdtai=60001.,
                                         timewithdrawnmjdtai=60361.,
                                         diaforcedsourceid=2
                                        )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'diaobjectid': ppdbobj1.diaobjectid,
                       'visit': 3,
                       'detector': 3,
                       'midpointmjdtai': 600002.,
                       'band': 'g',
                       'ra': 41.9999,
                       'dec': 12.9999,
                       'psfflux': 122.3,
                       'psffluxerr': 4.5,
                       'scienceflux': 233.4,
                       'sciencefluxerr': 5.6,
                       'timeprocessedmjdtai': 60010.,
                       'timewithdrawnmjdtai': 60370.,
                       'diaforcedsourceid': 3
                      }
