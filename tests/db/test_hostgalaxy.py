import uuid
import pytest

from db import HostGalaxy, DiaObjectHostMatch

from basetest import BaseTestDB


class TestHostGalaxy( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, procver_collection ):
        bpv, _pv, _pvinfo = procver_collection
        self.cls = HostGalaxy
        self.columns = {
            'id',
            'base_procver_id',
            'host_id',
            'host_catalog',
            'ra',
            'dec',
            'info'
        }
        self.safe_to_modify = [
            'ra',
            'dec',
            'info'
        ]
        self.uniques = []

        self.obj1 = HostGalaxy( id=uuid.uuid4(),
                                base_procver_id=bpv['bpv1_host_galaxy'].id,
                                host_id='fred',
                                host_catalog='some survey',
                                ra=128.,
                                dec=-42.,
                                info={}
                               )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = HostGalaxy( id=uuid.uuid4(),
                                base_procver_id=bpv['bpv1_host_galaxy'].id,
                                host_id='george',
                                host_catalog='some survey',
                                ra=137.,
                                dec=13.,
                                info={}
                               )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'id': uuid.uuid4(),
                       'base_procver_id': bpv['bpv1_host_galaxy'].id,
                       'host_id': 'ginny',
                       'host_catalog': 'a much better survey',
                       'ra': 0.,
                       'dec': -0.001,
                       'info': { 'cat': 'meow' }
                       }


class TestDiaObjectHostMatch( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, procver_collection, obj1, obj2, host1, host2 ):
        bpv, _pv = procver_collection
        self.cls = DiaObjectHostMatch
        self.columns = {
            'diaobjectid',
            'host_galaxy_id',
            'base_procver_id',
            'prio'
        }
        self.safe_to_modify = [ 'prio' ]
        self.uniques = []

        self.obj1 = DiaObjectHostMatch( diaobjectid=obj1.diaobjectid,
                                        host_galaxy_id=host1.id,
                                        base_procver_id=bpv['bpv3_host_galaxy'].id,
                                        prio=1 )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaObjectHostMatch( diaobjectid=obj1.diaobjectid,
                                        host_galaxy_id=host2.id,
                                        base_procver_id=bpv['bpv3_host_galaxy'].id,
                                        prio=2 )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'diaobjectid': obj2.diaobjectid,
                       'host_galaxy_id': host1.id,
                       'base_procver_id': bpv['bpv3_host_galaxy'].id,
                       'prio': 3 }
