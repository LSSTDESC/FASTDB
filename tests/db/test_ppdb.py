import datetime
import uuid
import pytest

from db import DB, PPDBHostGalaxy, PPDBDiaObject, PPDBDiaSource, PPDBDiaForcedSource

from basetest import BaseTestDB


# These fixture is much like one in conftest.py,but is specific to this file

@pytest.fixture
def ppdbobj1():
    obj = PPDBDiaObject( diaobjectid=42,
                         radecmjdtai=60000.,
                         ra=42.,
                         dec=13
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
                         radecmjdtai=60000.,
                         ra=42.,
                         dec=14
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
                         radecmjdtai=60000.,
                         ra=42.,
                         dec=15
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


class TestPPDBDiaObject( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self ):
        self.cls = PPDBDiaObject
        self.columns = {
            'diaobjectid',
            'radecmjdtai',
            'validitystart',
            'validityend',
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
            'parallax',
            'parallaxerr',
            'pmra',
            'pmraerr',
            'pmra_parallax_cov',
            'pmdec',
            'pmdecerr',
            'pmdec_parallax_cov',
            'pmra_pmdec_cov' }
        self.safe_to_modify = [
            'radecmjdtai',
            'validitystart',
            'validityend',
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
            'parallax',
            'parallaxerr',
            'pmra',
            'pmraerr',
            'pmra_parallax_cov',
            'pmdec',
            'pmdecerr',
            'pmdec_parallax_cov',
            'pmra_pmdec_cov'
        ]
        self.uniques = []

        self.obj1 = PPDBDiaObject( diaobjectid=1,
                                   radecmjdtai=60000.,
                                   ra=42.,
                                  dec=128. )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = PPDBDiaObject( diaobjectid=2,
                                   radecmjdtai=61000.,
                                   ra=23.,
                                  dec=-42. )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'diaobjectid': 3,
                       'radecmjdtai': 62000.,
                       'ra': 64.,
                       'dec': -23. }


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
            'psfra',
            'psfraerr',
            'psfdec',
            'psfdecerr',
            'psfra_psfdec_cov',
            'psfflux_psfra_cov',
            'psfflux_psfdec_cov',
            'psflnl',
            'psfchi2',
            'psfndata',
            'snr',
            'scienceflux',
            'sciencefluxerr',
            'fpbkgd',
            'fpbkgderr',
            'extendedness',
            'reliability',
            'ixx',
            'ixxerr',
            'iyy',
            'iyyerr',
            'ixy',
            'ixyerr',
            'ixx_ixy_cov',
            'ixx_iyy_cov',
            'iyy_ixy_cov',
            'ixxpsf',
            'iyypsf',
            'ixypsf',
            'flags',
            'pixelflags',
        }
        self.safe_to_modify = [
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
            'psfra',
            'psfraerr',
            'psfdec',
            'psfdecerr',
            'psfra_psfdec_cov',
            'psfflux_psfra_cov',
            'psfflux_psfdec_cov',
            'psflnl',
            'psfchi2',
            'psfndata',
            'snr',
            'scienceflux',
            'sciencefluxerr',
            'fpbkgd',
            'fpbkgderr',
            'extendedness',
            'reliability',
            'ixx',
            'ixxerr',
            'iyy',
            'iyyerr',
            'ixy',
            'ixyerr',
            'ixx_ixy_cov',
            'ixx_iyy_cov',
            'iyy_ixy_cov',
            'ixxpsf',
            'iyypsf',
            'ixypsf',
            'flags',
            'pixelflags',
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
                                   psffluxerr=8.0
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
            'time_processed',
            'time_withdrawn',
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
            'time_processed',
            'time_withdrawn',
        ]
        self.uniques = []

        t0 = datetime.datetime.now( tz=datetime.UTC )
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
                                         time_processed=t0,
                                         time_withdrawn=None
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
                                         time_processed=t0 + datetime.timedelta( days=1 ),
                                         time_withdrawn=t0 + datetime.timedelta( days=365 )
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
                       'time_processed': t0 + datetime.timedelta( days=2 ),
                       'time_withdrawn': t0 + datetime.timedelta( weeks=3 )
                      }
