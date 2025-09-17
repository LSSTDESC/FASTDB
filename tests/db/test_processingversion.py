import uuid
import datetime
import pytest

from db import ProcessingVersion, BaseProcessingVersion

from basetest import BaseTestDB


class TestProcessingVersion( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self ):
        self.cls = ProcessingVersion
        self.columns = { 'id', 'description', 'notes', 'created_at' }
        self.safe_to_modify = [ 'notes', 'created_at' ]
        self.uniques = [ 'description' ]
        t0 = datetime.datetime.now( tz=datetime.UTC )
        self.obj1 = ProcessingVersion( id=uuid.uuid4(),
                                       description='testprocver_pv1',
                                       notes='testprocver_pv1 notes',
                                       created_at=t0
                                      )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = ProcessingVersion( id=uuid.uuid4(),
                                       description='testprocver_pv2',
                                       notes='testprocver_pv2 notes',
                                       created_at=t0 + datetime.timedelta( minutes=1 )
                                      )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'id': uuid.uuid4(),
                       'description': 'testprocver_pv3',
                       'notes': 'testprocver_pv3 notes',
                       'created_at': t0 + datetime.timedelta( hours=2 )
                      }

    def test_procver_id( self, obj1_inserted, obj2_inserted ):
        gratuitous = uuid.uuid4()
        assert ProcessingVersion.procver_id( gratuitous ) == gratuitous
        assert ProcessingVersion.procver_id( str(gratuitous) ) == gratuitous

        assert ProcessingVersion.procver_id( 'testprocver_pv1' ) == self.obj1.id
        assert ProcessingVersion.procver_id( 'testprocver_pv2' ) == self.obj2.id

        with pytest.raises( ValueError, match="Unknown processing version foo" ):
            ProcessingVersion.procver_id( 'foo' )

    def test_highest_prio_base_procver( self, procver_collection ):
        bpv, pv = procver_collection
        assert pv['pv1'].highest_prio_base_procver().id == bpv['bpv1b'].id
        assert pv['pv2'].highest_prio_base_procver().id == bpv['bpv2a'].id
        assert pv['pv3'].highest_prio_base_procver().id == bpv['bpv3'].id


class TestBaseProcessingVersion( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self ):
        self.cls = BaseProcessingVersion
        self.columns = { 'id', 'description', 'notes', 'created_at' }
        self.safe_to_modify = [ 'notes', 'created_at' ]
        self.uniques = [ 'description' ]
        t0 = datetime.datetime.now( tz=datetime.UTC )
        self.obj1 = BaseProcessingVersion( id=uuid.uuid4(),
                                           description='testprocver_bpv1',
                                           notes='testprocver_bpv1 notes',
                                           created_at=t0
                                          )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = BaseProcessingVersion( id=uuid.uuid4(),
                                           description='testprocver_bpv2',
                                           notes='testprocver_bpv2 notes',
                                           created_at=t0 + datetime.timedelta( minutes=1 )
                                          )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'id': uuid.uuid4(),
                       'description': 'testprocver_bpv3',
                       'notes': 'testprocver_bpv3 notes',
                       'created_at': t0 + datetime.timedelta( hours=2 )
                      }

    def test_base_procver_id( self, obj1_inserted, obj2_inserted ):
        gratuitous = uuid.uuid4()
        assert BaseProcessingVersion.base_procver_id( gratuitous ) == gratuitous
        assert BaseProcessingVersion.base_procver_id( str(gratuitous) ) == gratuitous

        assert BaseProcessingVersion.base_procver_id( 'testprocver_bpv1' ) == self.obj1.id
        assert BaseProcessingVersion.base_procver_id( 'testprocver_bpv2' ) == self.obj2.id

        with pytest.raises( ValueError, match="Unknown base processing version foo" ):
            BaseProcessingVersion.base_procver_id( 'foo' )
