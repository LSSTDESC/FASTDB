import uuid
import datetime
import pytest

from db import ProcessingVersion, BaseProcessingVersion

from basetest import BaseTestDB


class TestBaseProcessingVersion( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self ):
        self.cls = BaseProcessingVersion
        self.columns = { 'id', 'description', '_table', 'notes', 'created_at' }
        self.safe_to_modify = [ 'notes', 'created_at' ]
        self.uniques = []
        t0 = datetime.datetime.now( tz=datetime.UTC )
        self.obj1 = BaseProcessingVersion( id=uuid.uuid4(),
                                           description='testprocver_bpv1',
                                           _table='table1',
                                           notes='testprocver_bpv1 notes',
                                           created_at=t0
                                          )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = BaseProcessingVersion( id=uuid.uuid4(),
                                           description='testprocver_bpv2',
                                           _table='table2',
                                           notes='testprocver_bpv2 notes',
                                           created_at=t0 + datetime.timedelta( minutes=1 )
                                          )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'id': uuid.uuid4(),
                       'description': 'testprocver_bpv3',
                       '_table': '_table3',
                       'notes': 'testprocver_bpv3 notes',
                       'created_at': t0 + datetime.timedelta( hours=2 )
                      }

    def test_base_procver_id( self, obj1_inserted, obj2_inserted ):
        gratuitous = uuid.uuid4()
        assert BaseProcessingVersion.base_procver_id( gratuitous ) == gratuitous
        assert BaseProcessingVersion.base_procver_id( str(gratuitous) ) == gratuitous

        assert BaseProcessingVersion.base_procver_id( 'testprocver_bpv1', 'table1' ) == self.obj1.id
        assert BaseProcessingVersion.base_procver_id( 'testprocver_bpv2', 'table2' ) == self.obj2.id

        with pytest.raises( ValueError, match="Unknown base processing version foo for table table1" ):
            BaseProcessingVersion.base_procver_id( 'foo', 'table1' )

        with pytest.raises( ValueError, match="Unknown base processing version testprocver_bpv1 for table table2" ):
            BaseProcessingVersion.base_procver_id( 'testprocver_bpv1', 'table2' )

        with pytest.raises( ValueError, match="Unknown base processing version testprocver_bpv2 for table table1" ):
            BaseProcessingVersion.base_procver_id( 'testprocver_bpv2', 'table1' )


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

    # THIS TEST HAS TO GO LAST because it runs the procver_collection fixture that's module scope
    def test_procver_functions( self, procver_collection ):
        bpvs, pvs = procver_collection

        for pv in pvs.values():
            assert ProcessingVersion.procver_id( pv.description ) == pv.id
            assert ProcessingVersion.procver_id( pv.id ) == pv.id
        assert ProcessingVersion.procver_id( 'default' ) == pvs['pv2'].id

        for bpv in bpvs.values():
            with pytest.raises( ValueError, match="table is required when base_processing_version is not a uuid" ):
                _ = BaseProcessingVersion.base_procver_id( bpv.description )
            assert BaseProcessingVersion.base_procver_id( bpv.description, bpv._table ) == bpv.id
            assert BaseProcessingVersion.base_procver_id( bpv.id ) == bpv.id

        assert pvs['pv1'].highest_prio_base_procver('diaobject').id == bpvs['bpv1b_diaobject'].id
        assert pvs['pv2'].highest_prio_base_procver('diaobject').id == bpvs['bpv2a_diaobject'].id
        assert pvs['pv3'].highest_prio_base_procver('diaobject').id == bpvs['bpv3_diaobject'].id
        assert pvs['realtime'].highest_prio_base_procver('diaobject').id == bpvs['realtime_diaobject'].id
        assert pvs['realtime'].highest_prio_base_procver('diasource').id == bpvs['realtime_diasource'].id
        assert ( pvs['realtime'].highest_prio_base_procver('diaobject_position').id
                 == bpvs['realtime_diaobject_position_60080'].id )
