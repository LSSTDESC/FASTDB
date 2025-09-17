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

        self.obj1 = DiaObject( diaobjectid=1,
                               base_procver_id=bpv['bpv1'].id,
                               rootid=rootobj1.id,
                               radecmjdtai=60000.,
                               ra=42.,
                               dec=128. )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaObject( diaobjectid=2,
                               base_procver_id=bpv['bpv1'].id,
                               rootid=rootobj2.id,
                               radecmjdtai=61000.,
                               ra=23.,
                               dec=-42. )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'diaobjectid': 3,
                       'base_procver_id': bpv['bpv1'].id,
                       'rootid': rootobj3.id,
                       'radecmjdtai': 62000.,
                       'ra': 64.,
                       'dec': -23. }
