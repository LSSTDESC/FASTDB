import datetime
import pytest

from db import DiaForcedSource, DiaForcedSourceExtra
import util

from basetest import BaseTestDB


class TestDiaForcedSource( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, procver_collection, obj1 ):
        bpv, _pv = procver_collection
        self.cls = DiaForcedSource
        self.columns = {
            'base_procver_id',
            'diaobjectid',
            'visit',
            'diaforcedsourceid',
            'midpointmjdtai',
            'band',
            'ra',
            'dec',
            'psfflux',
            'psffluxerr',
        }
        self.safe_to_modify = [
            'midpointmjdtai',
            'band',
            'psfflux',
            'psffluxerr',
            'ra',
            'dec',
        ]
        self.uniques = []

        self.obj1 = DiaForcedSource( base_procver_id=bpv['bpv1'].id,
                                     diaobjectid=obj1.diaobjectid,
                                     visit=1,
                                     diaforcedsourceid=1,
                                     band='r',
                                     psfflux=123.4,
                                     psffluxerr=5.6,
                                     midpointmjdtai=60000.,
                                     ra=42.,
                                     dec=13.
                                    )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaForcedSource( base_procver_id=bpv['bpv1'].id,
                                     diaobjectid=obj1.diaobjectid,
                                     visit=2,
                                     diaforcedsourceid=2,
                                     band='i',
                                     midpointmjdtai=60001.,
                                     psfflux=123.5,
                                     psffluxerr=5.7,
                                     ra=42.0001,
                                     dec=13.0001
                                    )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'base_procver_id': bpv['bpv1'].id,
                       'diaobjectid': obj1.diaobjectid,
                       'visit': 3,
                       'diaforcedsourceid': 3,
                       'band': 'g',
                       'midpointmjdtai': 600002.,
                       'psfflux': 122.3,
                       'psffluxerr': 4.5,
                       'ra': 41.9999,
                       'dec': 12.9999
                      }


class TestDiaForcedSourceExtra( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, obj1_frced1, obj1_frced2, obj1_frced3 ):
        self.cls = DiaForcedSourceExtra
        self.columns = {
            'base_procver_id',
            'detector',
            'scienceflux',
            'sciencefluxerr',
            'timeprocessedmjdtai',
            'timewithdrawnmjdtai',
        }
        self.safe_to_modify = [
            'detector',
            'scienceflux',
            'sciencefluxerr',
            'timeprocessedmjdtai',
            'timewithdrawnmjdtai',
        ]
        self.uniques = []

        t0 = datetime.datetime.now( tz=datetime.UTC )
        t1 = t0 + datetime.timedelta( days=1 )
        t2 = t0 + datetime.timedelta( days=365 )
        t3 = t0 + datetime.timedelta( days=2 )
        t4 = t0 + datetime.timedelta( weeks=3 )
        t0 = util.mjd_from_mjd_or_datetime_or_timestring( t0 )
        t1 = util.mjd_from_mjd_or_datetime_or_timestring( t1 )
        t2 = util.mjd_from_mjd_or_datetime_or_timestring( t2 )
        t3 = util.mjd_from_mjd_or_datetime_or_timestring( t3 )
        t4 = util.mjd_from_mjd_or_datetime_or_timestring( t4 )
        self.obj1 = DiaForcedSourceExtra( base_procver_id=obj1_frced1.base_procver_id,
                                          detector=1,
                                          scienceflux=234.5,
                                          sciencefluxerr=7.8,
                                          timeprocessedmjdtai=t0,
                                          timewithdrawnmjdtai=None
                                         )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaForcedSourceExtra( base_procver_id=obj1_frced2.base_procver_id,
                                          detector=2,
                                          scienceflux=235.5,
                                          sciencefluxerr=7.9,
                                          timeprocessedmjdtai=t1,
                                          timewithdrawnmjdtai=t2,
                                         )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'base_procver_id': obj1_frced3.base_procver_id,
                       'detector': 3,
                       'scienceflux': 233.4,
                       'sciencefluxerr': 5.6,
                       'timeprocessedmjdtai': t3,
                       'timewithdrawnmjdtai': t4,
                      }
