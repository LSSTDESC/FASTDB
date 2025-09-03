import uuid
import pytest

from db import HostGalaxy

from basetest import BaseTestDB


class TestHostGalaxy( BaseTestDB ):

    @pytest.fixture
    def basetest_setup( self, procver_collection ):
        bpv, _pv = procver_collection
        self.cls = HostGalaxy
        self.columns = {
            'id',
            'base_procver_id',
            'objectid',
            'ra',
            'dec',
            'petroflux_r',
            'petroflux_r_err',
            'stdcolor_u_g',
            'stdcolor_g_r',
            'stdcolor_r_i',
            'stdcolor_i_z',
            'stdcolor_z_y',
            'stdcolor_u_g_err',
            'stdcolor_g_r_err',
            'stdcolor_r_i_err',
            'stdcolor_i_z_err',
            'stdcolor_z_y_err',
            'pzmode',
            'pzmean',
            'pzstd',
            'pzskew',
            'pzkurt',
            'pzquant000',
            'pzquant010',
            'pzquant020',
            'pzquant030',
            'pzquant040',
            'pzquant050',
            'pzquant060',
            'pzquant070',
            'pzquant080',
            'pzquant090',
            'pzquant100',
            'flags',
        }
        self.safe_to_modify = [
            'objectid',
            'ra',
            'dec',
            'petroflux_r',
            'petroflux_r_err',
            'stdcolor_u_g',
            'stdcolor_g_r',
            'stdcolor_r_i',
            'stdcolor_i_z',
            'stdcolor_z_y',
            'stdcolor_u_g_err',
            'stdcolor_g_r_err',
            'stdcolor_r_i_err',
            'stdcolor_i_z_err',
            'stdcolor_z_y_err',
            'pzmode',
            'pzmean',
            'pzstd',
            'pzskew',
            'pzkurt',
            'pzquant000',
            'pzquant010',
            'pzquant020',
            'pzquant030',
            'pzquant040',
            'pzquant050',
            'pzquant060',
            'pzquant070',
            'pzquant080',
            'pzquant090',
            'pzquant100',
            'flags',
        ]
        self.uniques = []

        self.obj1 = HostGalaxy( id=uuid.uuid4(),
                                base_procver_id=bpv['bpv1'].id,
                                objectid=42,
                                ra=13.,
                                dec=-66.
                               )
        self.dict1 = { k: getattr( self.obj1, k ) for k in self.columns }
        self.obj2 = HostGalaxy( id=uuid.uuid4(),
                                base_procver_id=bpv['bpv1'].id,
                                objectid=23,
                                ra=137.,
                                dec=42.,
                               )
        self.dict2 = { k: getattr( self.obj2, k ) for k in self.columns }
        self.dict3 = { 'id': uuid.uuid4(),
                       'base_procver_id': bpv['bpv1'].id,
                       'objectid': 31337,
                       'ra': 32.,
                       'dec': 64.
                       }
