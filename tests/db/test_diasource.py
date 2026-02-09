import pytest

from db import DiaSource, DiaSourceExtra

from basetest import BaseTestDB


class TestDiaSource( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, obj1 ):
        self.cls = DiaSource
        self.columns = {
            "diasourceid",
            "base_procver_id",
            "diaobjectid",
            "visit",
            "band",
            "midpointmjdtai",
            "psfflux",
            "psffluxerr",
            "ra",
            "dec",
            "raerr",
            "decerr",
            "ra_dec_cov"
        }
        self.safe_to_modify = [
            "visit",
            "band",
            "midpointmjdtai",
            "psfflux",
            "psffluxerr",
            "ra",
            "dec",
            "raerr",
            "decerr",
            "ra_dec_cov"
        ]
        self.uniques = []

        self.obj1 = DiaSource( base_procver_id=obj1.base_procver_id,
                               diaobjectid=obj1.diaobjectid,
                               diasourceid=1,
                               visit=1,
                               band='r',
                               midpointmjdtai=60000.,
                               ra=42.0001,
                               dec=12.9998,
                               psfflux=123.4,
                               psffluxerr=5.6
                              )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaSource( base_procver_id=obj1.base_procver_id,
                               diaobjectid=obj1.diaobjectid,
                               diasourceid=2,
                               visit=2,
                               band='i',
                               midpointmjdtai=60010.,
                               ra=42.0002,
                               dec=13.0001,
                               psfflux=124.6,
                               psffluxerr=8.0
                              )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'base_procver_id': obj1.base_procver_id,
                       'diaobjectid': obj1.diaobjectid,
                       'diasourceid': 3,
                       'visit': 3,
                       'band': 'g',
                       'midpointmjdtai': 60015.,
                       'ra': 41.9999,
                       'dec': 13.0002,
                       'psfflux': 135.7,
                       'psffluxerr': 9.1 }


class TestDiaSourceExtra( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, obj1_src1, obj1_src2, obj1_src3 ):
        self.cls = DiaSourceExtra
        self.columns = {
            "diasourceid",
            "base_procver_id",
            "detector",
            "x",
            "y",
            "xerr",
            "yerr",
            "x_y_cov",
            "psflnl",
            "psfchi2",
            "psfndata",
            "snr",
            "scienceflux",
            "sciencefluxerr",
            "templateflux",
            "templatefluxerr",
            "extendedness",
            "reliability",
            "ixx",
            "iyy",
            "ixy",
            "ixxpsf",
            "iyypsf",
            "ixypsf",
            "flags",
            "pixelflags",
            "apflux",
            "apfluxerr",
            "bboxsize",
            "timeprocessedmjdtai",
            "timewithdrawnmjdtai",
            "parentdiasourceid",
            "info"
        }
        self.safe_to_modify = [
            "detector",
            "x",
            "y",
            "xerr",
            "yerr",
            "x_y_cov",
            "psflnl",
            "psfchi2",
            "psfndata",
            "snr",
            "scienceflux",
            "sciencefluxerr",
            "templateflux",
            "templatefluxerr",
            "extendedness",
            "reliability",
            "ixx",
            "iyy",
            "ixy",
            "ixxpsf",
            "iyypsf",
            "ixypsf",
            "flags",
            "pixelflags",
            "apflux",
            "apfluxerr",
            "bboxsize",
            "timeprocessedmjdtai",
            "timewithdrawnmjdtai",
            "parentdiasourceid",
            "info"
        ]
        self.uniques = []

        self.obj1 = DiaSourceExtra( diasourceid=obj1_src1.diasourceid,
                                    base_procver_id=obj1_src1.base_procver_id,
                                    detector=1,
                                    x=128,
                                    y=128,
                                    pixelflags=0,
                                    flags=0,
                                    info={}
                                   )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = DiaSourceExtra( diasourceid=obj1_src2.diasourceid,
                                    base_procver_id=obj1_src2.base_procver_id,
                                    detector=2,
                                    x=131.5,
                                    y=131.5,
                                    pixelflags=1,
                                    flags=2,
                                    info={}
                                   )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'diasourceid': obj1_src3.diasourceid,
                       'base_procver_id': obj1_src3.base_procver_id,
                       'detector': 3,
                       'x': 148.7,
                       'y': 148.7,
                       'pixelflags': 4,
                       'flags': 8,
                       'info': { 'cat': 'meow' } }
