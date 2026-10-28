import uuid
import pytest

from db import AuthUser

from basetest import BaseTestDB


class TestAuthUser( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self ):
        self.cls = AuthUser
        self.columns = { 'id', 'username', 'displayname','email', 'pubkey', 'privkey' }
        self.safe_to_modify = [ 'displayname', 'email', 'pubkey', 'privkey' ]
        self.uniques = [ 'username' ]
        self.obj1 = AuthUser( id=uuid.uuid4(),
                              username='test_authuser',
                              displayname='test authuser',
                              email='test@nowhere.org',
                              pubkey='',
                              privkey={} )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = AuthUser( id=uuid.uuid4(),
                              username='test_authuser2',
                              displayname='test authseruser 2',
                              email='test2@nowhere.org',
                              pubkey='',
                              privkey={} )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'id': uuid.uuid4(),
                       'username': 'test_authser3',
                       'displayname': 'test authuseruser 3',
                       'email': 'test3@nowhere.org',
                       'pubkey': 'blah',
                       'privkey': { 'blah': 'blah' } }
