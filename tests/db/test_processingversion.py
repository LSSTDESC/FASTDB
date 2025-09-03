import uuid
import datetime
import pytest

from db import ProcessingVersion

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
