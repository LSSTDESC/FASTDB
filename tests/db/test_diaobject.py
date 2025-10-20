import pytest

from db import DiaObject

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
            'validitystartmjdtai',
            'ra',
            'dec',
            'raerr',
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
            'nearbylowzgalsep'
        }
        self.safe_to_modify = [
            'ra',
            'dec',
            'raerr',
            'decerr',
            'ra_dec_cov',
            'validitystartmjdtai',
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

        self.obj1 = DiaObject( diaobjectid=1,
                               base_procver_id=bpv['bpv1'].id,
                               rootid=rootobj1.id,
                               ra=42.,
                               dec=128.,
                               raerr=0.1,
                               decerr=0.1,
                               validitystartmjdtai=60000. )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaObject( diaobjectid=2,
                               base_procver_id=bpv['bpv1'].id,
                               rootid=rootobj2.id,
                               ra=23.,
                               dec=-42.,
                               raerr=0.2,
                               decerr=0.2,
                               validitystartmjdtai=60001. )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'diaobjectid': 3,
                       'base_procver_id': bpv['bpv1'].id,
                       'rootid': rootobj3.id,
                       'ra': 64.,
                       'dec': -23.,
                       'raerr': 0.3,
                       'decerr': 0.3,
                       'validitystartmjdtai': 60002. }
