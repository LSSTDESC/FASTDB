import pytest
from datetime import datetime

from db import DiaObject, DiaObjectPosition

from basetest import BaseTestDB


class TestDiaObject( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, procver_collection, rootobj1, rootobj2, rootobj3 ):
        bpv, _pv = procver_collection
        self.cls = DiaObject
        self.columns = {
            'diaobjectid',
            'base_procver_id',
            'rootid',
        }
        self.safe_to_modify = []
        self.uniques = []

        self.obj1 = DiaObject( diaobjectid=1,
                               base_procver_id=bpv['bpv1_diaobject'].id,
                               rootid=rootobj1.id )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaObject( diaobjectid=2,
                               base_procver_id=bpv['bpv1_diaobject'].id,
                               rootid=rootobj2.id )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'diaobjectid': 3,
                       'base_procver_id': bpv['bpv1_diaobject'].id,
                       'rootid': rootobj3.id }


class TestDiaObjectPosition( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, obj1, obj2, obj3 ):
        self.cls = DiaObjectPosition
        self.columns = {
            'diaobjectid',
            'base_procver_id',
            'ra',
            'dec',
            'raerr',
            'decerr',
            'ra_dec_cov',
            'created_at'
        }
        self.safe_to_modify = [
            'ra',
            'dec',
            'raerr',
            'decerr',
            'ra_dec_cov',
            'created_at'
        ]
        self.uniques = []

        self.obj1 = DiaObjectPosition( diaobjectid=obj1.diaobjectid,
                                       base_procver_id=obj1.base_procver_id,
                                       ra=132.,
                                       dec=45.,
                                       raerr=0.0011,
                                       decerr=0.0012,
                                       ra_dec_cov=0.0013,
                                       created_at=datetime.fromisoformat('2026-02-09T12:28Z') )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaObjectPosition( diaobjectid=obj2.diaobjectid,
                                       base_procver_id=obj2.base_procver_id,
                                       ra=13.,
                                       dec=-42.,
                                       raerr=0.0014,
                                       decerr=0.0015,
                                       ra_dec_cov=0.0016,
                                       created_at=datetime.fromisoformat('2026-02-09T12:28:01Z') )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'diaobjectid': obj3.diaobjectid,
                       'base_procver_id': obj3.base_procver_id,
                       'ra': 128.,
                       'dec': 64.,
                       'raerr': 0.0017,
                       'decerr': 0.0018,
                       'ra_dec_cov': 0.0019,
                       'created_at': datetime.fromisoformat( '2026-02-09T12:28:02Z' ) }
