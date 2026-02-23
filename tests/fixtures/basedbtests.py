# These fixtures are used by the tests in db/.  Avoid using them
#   elsewhere; use the set_of_lightcurves fixture in conftest.py as much
#   as possible.

import pytest
import uuid

from util import asUUID
from db import ( RootDiaObject,
                 DiaObject,
                 DiaSource,
                 DiaForcedSource,
                 HostGalaxy,
                 DB )


@pytest.fixture
def rootobj1():
    objid = asUUID( '00f85226-c42f-4e1d-8adf-f18b9353a176' )
    obj = RootDiaObject( id=objid )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM root_diaobject WHERE id=%(id)s", { 'id': objid } )
        con.commit()


@pytest.fixture
def rootobj2():
    objid = asUUID( 'a9f0b54b-dc70-4276-b07b-728ad7a1465d' )
    obj = RootDiaObject( id=objid )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM root_diaobject WHERE id=%(id)s", { 'id': objid } )
        con.commit()


@pytest.fixture
def rootobj3():
    objid = asUUID( '01c4fa2f-aa7d-487b-944c-67f05c57dbb7' )
    obj = RootDiaObject( id=objid )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM root_diaobject WHERE id=%(id)s", { 'id': objid } )
        con.commit()


@pytest.fixture
def obj1( procver_collection, rootobj1 ):
    bpvs, _pvs = procver_collection
    obj = DiaObject( diaobjectid=42, base_procver_id=bpvs['bpv1'].id, rootid=rootobj1.id )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        subdict = { 'id': obj.diaobjectid, 'pv': obj.base_procver_id }
        cursor.execute( "DELETE FROM diaobject WHERE diaobjectid=%(id)s AND base_procver_id=%(pv)s", subdict )
        con.commit()


@pytest.fixture
def obj2( procver_collection, rootobj2 ):
    bpvs, _pvs = procver_collection
    obj = DiaObject( diaobjectid=64, base_procver_id=bpvs['bpv1'].id, rootid=rootobj2.id )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        subdict = { 'id': obj.diaobjectid, 'pv': obj.base_procver_id }
        cursor.execute( "DELETE FROM diaobject WHERE diaobjectid=%(id)s AND base_procver_id=%(pv)s", subdict )
        con.commit()


@pytest.fixture
def obj3( procver_collection, rootobj3 ):
    bpvs, _pvs = procver_collection
    obj = DiaObject( diaobjectid=137, base_procver_id=bpvs['bpv1'].id, rootid=rootobj3.id )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        subdict = { 'id': obj.diaobjectid, 'pv': obj.base_procver_id }
        cursor.execute( "DELETE FROM diaobject WHERE diaobjectid=%(id)s AND base_procver_id=%(pv)s", subdict )
        con.commit()



@pytest.fixture
def obj1_src1( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    src = DiaSource( base_procver_id=bpvs['bpv1_diasource'].id,
                     diaobjectid=obj1.diaobjectid,
                     diasourceid=1,
                     visit=64,
                     band='r',
                     midpointmjdtai=59000.,
                     psfflux=137.,
                     psffluxerr=1.,
                     ra=128. + 0.0001,
                     dec=42. + 0.0001,
                     raerr=0.0002,
                     decerr=0.0002
                    )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diasource WHERE base_procver_id=%(pv)s "
                        "                        AND diaobjectid=%(objid)s AND visit=%(visit)s",
                        { 'pv': src.base_procver_id, 'objid': src.diaobjectid, 'visit': src.visit } )
        con.commit()


@pytest.fixture
def obj1_src1_pv2( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    src = DiaSource( base_procver_id=bpvs['bpv2_diasource'].id,
                     diaobjectid=obj1.diaobjectid,
                     diasourceid=2,
                     visit=64,
                     band='r',
                     midpointmjdtai=59000.,
                     psfflux=3.,
                     psffluxerr=0.1,
                     ra=128. - 0.0001,
                     dec=42. + 0.0001
                    )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diasource WHERE base_procver_id=%(pv)s "
                        "                        AND diaobjectid=%(objid)s AND visit=%(visit)s",
                        { 'pv': src.base_procver_id, 'objid': src.diaobjectid, 'visit': src.visit } )
        con.commit()


@pytest.fixture
def obj1_src2( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    src = DiaSource( base_procver_id=bpvs['bpv1_diasource'].id,
                     diaobjectid=obj1.diaobjectid,
                     diasourceid=2,
                     visit=128,
                     band='i',
                     midpointmjdtai=59020.,
                     psfflux=299792.,
                     psffluxerr=2000.,
                     ra=128. - 0.0001,
                     dec=42. - 0.0001,
                     raerr=0.0002,
                     decerr=0.0002
                    )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diasource WHERE base_procver_id=%(pv)s "
                        "                        AND diaobjectid=%(objid)s AND visit=%(visit)s",
                        { 'pv': src.base_procver_id, 'objid': src.diaobjectid, 'visit': src.visit } )
        con.commit()


@pytest.fixture
def obj1_src3( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    src = DiaSource( base_procver_id=bpvs['bpv1_diasource'].id,
                     diaobjectid=obj1.diaobjectid,
                     diasourceid=3,
                     visit=256,
                     band='z',
                     midpointmjdtai=59040.,
                     psfflux=6626.,
                     psffluxerr=60.,
                     ra=128. + 0.0001,
                     dec=42. - 0.0001,
                     raerr=0.0002,
                     decerr=0.0002
                    )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diasource WHERE base_procver_id=%(pv)s "
                        "                        AND diaobjectid=%(objid)s AND visit=%(visit)s",
                        { 'pv': src.base_procver_id, 'objid': src.diaobjectid, 'visit': src.visit } )
        con.commit()


@pytest.fixture
def obj1_frced1( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    frc = DiaForcedSource( base_procver_id=bpvs['bpv1_diaforcedsource'].id,
                           diaobjectid=obj1.diaobjectid,
                           visit=64,
                           band='r',
                           midpointmjdtai=59000.,
                           psfflux=138.,
                           psffluxerr=1.,
                           ra=128.,
                           dec=42.
                          )
    frc.insert()

    yield frc
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaforcedsource WHERE base_procver_id=%(pv)s "
                        "                              AND diaobjectid=%(objid)s "
                        "                              AND visit=%(visit)s",
                        { 'pv': frc.base_procver_id, 'objid': frc.diaobjectid, 'visit': frc.visit } )
        con.commit()


@pytest.fixture
def obj1_frced2( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    frc = DiaForcedSource( base_procver_id=bpvs['bpv1_diaforcedsource'].id,
                           diaobjectid=obj1.diaobjectid,
                           visit=128,
                           band='i',
                           midpointmjdtai=59020.,
                           psfflux=300000.,
                           psffluxerr=2000.,
                           ra=128.,
                           dec=42.
                          )
    frc.insert()

    yield frc
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaforcedsource WHERE base_procver_id=%(pv)s "
                        "                              AND diaobjectid=%(objid)s "
                        "                              AND visit=%(visit)s",
                        { 'pv': frc.base_procver_id, 'objid': frc.diaobjectid, 'visit': frc.visit } )
        con.commit()


@pytest.fixture
def obj1_frced3( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    frc = DiaForcedSource( base_procver_id=bpvs['bpv1_diaforcedsource'].id,
                           diaobjectid=obj1.diaobjectid,
                           visit=256,
                           band='z',
                           midpointmjdtai=59040.,
                           psfflux=6680.,
                           psffluxerr=60.,
                           ra=128.,
                           dec=42.
                          )
    frc.insert()

    yield frc
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaforcedsource WHERE base_procver_id=%(pv)s "
                        "                              AND diaobjectid=%(objid)s "
                        "                              AND visit=%(visit)s",
                        { 'pv': frc.base_procver_id, 'objid': frc.diaobjectid, 'visit': frc.visit } )
        con.commit()


@pytest.fixture
def host1( procver_collection ):
    bpvs, _pvs = procver_collection
    host = HostGalaxy( id=uuid.uuid4(),
                       host_catalog='foo',
                       host_id='bar',
                       base_procver_id=bpvs['bpv1'].id,
                       ra=1.,
                       dec=-2.,
                       info={} )
    host.insert()

    yield host
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM host_galaxy WHERE id=%(id)s", { 'id': host.id } )
        con.commit()


@pytest.fixture
def host2( procver_collection ):
    bpvs, _pvs = procver_collection
    host = HostGalaxy( id=uuid.uuid4(),
                       host_catalog='foo',
                       host_id='smol',
                       base_procver_id=bpvs['bpv1'].id,
                       ra=48.,
                       dec=-89.,
                       info={} )
    host.insert()

    yield host
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM host_galaxy WHERE id=%(id)s", { 'id': host.id } )
        con.commit()
