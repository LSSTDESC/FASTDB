import pytest
import datetime
import uuid

from db import SpectrumInfo, WantedSpectra, PlannedSpectra

from basetest import BaseTestDB


class TestSpectrumInfo( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, rootobj1, rootobj2 ):
        self.cls = SpectrumInfo
        self.columns = { 'specinfo_id',
                         'root_diaobject_id',
                         'facility',
                         'inserted_at',
                         'mjd',
                         'ra',
                         'dec',
                         'is_host',
                         'z',
                         'classid',
                         'class_description' }
        self.safe_to_modify = [ 'facility',
                                'inserted_at',
                                'mjd',
                                'ra',
                                'dec',
                                'is_host',
                                'z',
                                'classid',
                                'class_description' ]
        self.uniques = []

        t0 = datetime.datetime.now( tz=datetime.UTC )
        t1 = t0 + datetime.timedelta( days=1 )
        t2 = t1 + datetime.timedelta( hours=1 )
        self.obj1 = SpectrumInfo( specinfo_id=uuid.UUID( 'aabc5b0f-860a-4d50-ac72-befccfe2a852' ),
                                  root_diaobject_id=rootobj1.id,
                                  facility="Test Facility 1",
                                  inserted_at=t0,
                                  mjd=60000.,
                                  ra=12.,
                                  dec=34.,
                                  is_host=False,
                                  z=0.42,
                                  classid=2222,
                                  class_description="SNIa" )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = SpectrumInfo( specinfo_id=uuid.UUID( '78429a22-5790-42ec-a825-13a4fada889d' ),
                                  root_diaobject_id=rootobj2.id,
                                  facility="Test Facility 2",
                                  inserted_at=t1,
                                  mjd=60001.,
                                  ra=56.,
                                  dec=78.,
                                  is_host=False,
                                  z=0.13,
                                  classid=2224,
                                  class_description="SNII" )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'specinfo_id': uuid.UUID( '73085b3b-1daf-43f8-a1a6-f83aa7315be4' ),
                       'root_diaobject_id': rootobj1.id,
                       'facility': "Test Facility 3",
                       'inserted_at': t2,
                       'mjd': 60001.04,
                       'ra': 123.,
                       'dec': -45.,
                       'is_host': True,
                       'z': 0.137,
                       'classid': 8,
                       'class_description': 'SBc Galaxy' }


class TestWantedSpectra( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, rootobj1, rootobj2, test_user ):
        self.cls = WantedSpectra
        self.columns = { 'wantspec_id',
                         'root_diaobject_id',
                         'wanttime',
                         'user_id',
                         'requester',
                         'priority'
                        }
        self.safe_to_modify = [ 'wanttime', 'requester', 'priority' ]
        self.uniques = []

        t0 = datetime.datetime.now( tz=datetime.UTC )
        t1 = t0 + datetime.timedelta( days=1 )
        t2 = t1 + datetime.timedelta( days=2 )
        self.obj1 = WantedSpectra( wantspec_id=f'{rootobj1.id} ; testquester1',
                                   root_diaobject_id=rootobj1.id,
                                   wanttime=t0,
                                   user_id=test_user.id,
                                   requester="Test Requester 1",
                                   priority=1 )
        self.dict1 = { 'wantspec_id': self.obj1.wantspec_id,
                       'root_diaobject_id': rootobj1.id,
                       'wanttime': t0,
                       'user_id': test_user.id,
                       'requester': "Test Requester 1",
                       'priority': 1 }
        self.obj2 = WantedSpectra( wantspec_id=f'{rootobj2.id} ; testquester2',
                                   root_diaobject_id=rootobj2.id,
                                   wanttime=t1,
                                   user_id=test_user.id,
                                   requester="Test Requester 2",
                                   priority=2 )
        self.dict2 = { 'wantspec_id': self.obj2.wantspec_id,
                       'root_diaobject_id': rootobj2.id,
                       'wanttime': t1,
                       'user_id': test_user.id,
                       'requester': "Test Requester 2",
                       'priority': 2 }
        self.dict3 = { 'wantspec_id': f'{rootobj1.id} ; testquester3',
                       'root_diaobject_id': rootobj1.id,
                       'wanttime': t2,
                       'user_id': test_user.id,
                       'requester': "Test Requester 3",
                       'priority': 3 }


class TestPlannedSpectra( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, rootobj1, rootobj2 ):
        self.cls = PlannedSpectra
        self.columns = { 'plannedspec_id',
                         'root_diaobject_id',
                         'facility',
                         'created_at',
                         'plantime',
                         'comment' }
        self.safe_to_modify = [ 'facility', 'created_at', 'plantime', 'comment' ]
        self.uniques = []

        ct0 = datetime.datetime.now( tz=datetime.UTC )
        pt0 = ct0 + datetime.timedelta( hours=12 )
        ct1 = ct0 + datetime.timedelta( days=1 )
        pt1 = ct1 + datetime.timedelta( hours=12, minutes=30 )
        ct2 = ct1 + datetime.timedelta( hours=1 )
        pt2 = ct2 + datetime.timedelta( hours=11, minutes=15 )
        self.obj1 = PlannedSpectra( plannedspec_id=uuid.UUID( '6167b5c8-c084-4b80-a748-8626a876a1e5' ),
                                    root_diaobject_id=rootobj1.id,
                                    facility="4Most",
                                    created_at=ct0,
                                    plantime=pt0,
                                    comment="This is the most important one." )
        self.dict1 = { 'plannedspec_id': self.obj1.plannedspec_id,
                       'root_diaobject_id': rootobj1.id,
                       'facility': "4Most",
                       'created_at': ct0,
                       'plantime': pt0,
                       'comment': "This is the most important one." }
        self.obj2 = PlannedSpectra( plannedspec_id=uuid.UUID( '5be6c122-2fa4-4b7d-aa76-7d617951d64c' ),
                                    root_diaobject_id=rootobj2.id,
                                    facility="Subaru",
                                    created_at=ct1,
                                    plantime=pt1,
                                    comment="No, this is the most important one." )
        self.dict2 = { 'plannedspec_id': self.obj2.plannedspec_id,
                       'root_diaobject_id': rootobj2.id,
                       'facility': "Subaru",
                       'created_at': ct1,
                       'plantime': pt1,
                       'comment': "No, this is the most important one." }
        self.dict3 = { 'plannedspec_id': uuid.UUID( '028cafa3-2fb8-4540-bacd-0702b8d6c01c' ),
                       'root_diaobject_id': rootobj1.id,
                       'facility': "My C8 in my back yard",
                       'created_at': ct2,
                       'plantime': pt2,
                       'comment': "Guys. You are wrong. This one is really the most important." }
