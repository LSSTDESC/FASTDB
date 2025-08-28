import datetime
import pytest

from db import ProcessingVersion

from basetest import BaseTestDB


class TestPasswordLink( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self ):
        self.cls = ProcessingVersion
        self.columns = { 'id', 'description', 'validity_start', 'validity_end' }
        self.safe_to_modify = [ 'validity_start', 'validity_end' ]
        self.uniques = [ 'description' ]
        t0 = datetime.datetime.now( tz=datetime.UTC )
        self.obj1 = ProcessingVersion( id=1,
                                       description='testprocver_pv1',
                                       validity_start=t0,
                                       validity_end=t0 + datetime.timedelta( hours=1 )
                                      )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = ProcessingVersion( id=2,
                                       description='testprocver_pv2',
                                       validity_start=t0 + datetime.timedelta( minutes=1 ),
                                       validity_end=t0 + datetime.timedelta( days=1 )
                                      )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'id': 3,
                       'description': 'testprocver_pv3',
                       'validity_start': t0 + datetime.timedelta( hours=2 ),
                       'validity_end': t0 + datetime.timedelta( weeks=1 ) }
