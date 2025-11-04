import datetime
import pytest

from db import DiaForcedSource
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
        self.obj1 = DiaForcedSource( base_procver_id=bpv['bpv1'].id,
                                     diaobjectid=obj1.diaobjectid,
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
                                     timeprocessedmjdtai=t0,
                                     timewithdrawnmjdtai=None
                                    )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaForcedSource( base_procver_id=bpv['bpv1'].id,
                                     diaobjectid=obj1.diaobjectid,
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
                                     timeprocessedmjdtai=t1,
                                     timewithdrawnmjdtai=t2,
                                    )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'base_procver_id': bpv['bpv1'].id,
                       'diaobjectid': obj1.diaobjectid,
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
                       'timeprocessedmjdtai': t3,
                       'timewithdrawnmjdtai': t4,
                      }
