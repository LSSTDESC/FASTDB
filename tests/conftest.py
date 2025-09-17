import os
import pytest
import pathlib
import subprocess
import uuid

import numpy as np
from pymongo import MongoClient

from db import ( BaseProcessingVersion,
                 ProcessingVersion,
                 RootDiaObject,
                 DiaObject,
                 DiaSource,
                 DiaForcedSource,
                 DB,
                 DBCon,
                 AuthUser )
from util import asUUID, logger
from fastdb.fastdb_client import FASTDBClient


# sys.path.insert( 0, pathlib.Path(__file__).parent )
# For cleanliness, a bunch of fixtures are broken
#   out into their own files.  To be able to see
#   them, put those files in this list below.
#   (pytest is kind of a beast).  Those files
#   should all live in the fixtures subdirectory.
pytest_plugins = [ 'fixtures.alertcycle',
                   'fixtures.spectrum' ]


@pytest.fixture( scope='module' )
def procver_collection():
    # A set of processing versions loaded into the database, including:
    #   A set of base procesing versions pvc_bpv1, pvc_bpv1a, pvc_bpv1b, pvc_bpv2, pvc_bpv2a, pvc_bpv3, realtime
    #   A set of processing versions with priorities (high to low)
    #     pvc_pv1 : pvc_bpv1b, pvc_bpv1a, pvc_bpv1
    #     pvc_pv2 : pvc_bpv2a, pvc_bpv2
    #     pvc_pv3 : pvc_bpv3
    #     realtime : realtime
    #   A processing version alias default pointing at pvc_pv3
    #
    # (The fallbacks of pvc_pv2 and pvc_pv3 to the earlier bpvs are for Object handing in set_of_lightcurves)
    #
    # fixture value is a tuple of two dictionaries of str → BaseProcessingVersion, str → ProcessingVersion

    bpvs = {}
    pvs = {}

    try:
        with DBCon() as con:
            rows, _cols = con.execute( "SELECT * FROM base_processing_version WHERE description='realtime'" )
            if len(rows) > 0:
                raise RuntimeError( "procver_collection fixture can't proceed, realtime base processing "
                                    "version already exists" )
            rows, _cols = con.execute( "SELECT * FROM processing_version WHERE description='realtime'" )
            if len(rows) > 0:
                raise RuntimeError( "procver_collection fixture can't proceed, realtime processing version "
                                    "already exists" )
            rows, _cols = con.execute( "SELECT * FROM processing_version WHERE description='default'" )
            if len(rows) > 0:
                raise RuntimeError( "procver_collection fixture can't proceed, default processing version exists" )
            rows, _cols = con.execute( "SELECT * FROM processing_version_alias WHERE description='default'" )
            if len(rows) > 0:
                raise RuntimeError( "procver_collection fixture can't proceed, default processing version "
                                    "alias already exists" )

            # Gotta hardcode the uuids so that they will match when we do a pg_restore
            #  in the fixtures/alertycle.py::alerts_90days_sent_received_and_imported fixture
            bpvs['bpv1'] = BaseProcessingVersion( id=asUUID('7379926d-9825-4b1b-9dc8-d00cb043ea3e'),
                                                  description='pvc_bpv1' )
            bpvs['bpv1a'] = BaseProcessingVersion( id=asUUID('0b2c33ad-aa8e-4344-9da6-925c9f826269'),
                                                   description='pvc_bpv1a' )
            bpvs['bpv1b'] = BaseProcessingVersion( id=asUUID('1fbe60a1-4bab-454b-9598-dbac51d36adb'),
                                                   description='pvc_bpv1b' )
            bpvs['bpv2'] = BaseProcessingVersion( id=asUUID('eaad2a77-cab3-40d7-8f4b-c3f1b76af91c'),
                                                  description='pvc_bpv2' )
            bpvs['bpv2a'] = BaseProcessingVersion( id=asUUID('dc87f7a2-c313-496a-89ae-d85329e23b1a'),
                                                   description='pvc_bpv2a' )
            bpvs['bpv3'] = BaseProcessingVersion( id=asUUID('e074266d-4a1c-4045-b04b-deac609f5eb6'),
                                                  description='pvc_bpv3' )
            bpvs['realtime'] = BaseProcessingVersion( id=asUUID('46bffce7-7098-4261-ae32-0c9d78cd3c42'),
                                                      description='realtime' )
            for bpv in bpvs.values():
                bpv.insert( dbcon=con, nocommit=True, refresh=False )

            pvs['pv1'] = ProcessingVersion( id=uuid.uuid4(), description='pvc_pv1' )
            pvs['pv2'] = ProcessingVersion( id=uuid.uuid4(), description='pvc_pv2' )
            pvs['pv3'] = ProcessingVersion( id=uuid.uuid4(), description='pvc_pv3' )
            pvs['realtime'] = ProcessingVersion( id=uuid.uuid4(), description='realtime' )
            for pv in pvs.values():
                pv.insert( dbcon=con, nocommit=True, refresh=False )

            for pv, bpvae in zip( [ 'pv1', 'pv2', 'pv3', 'realtime' ],
                                  [ [ 'bpv1', 'bpv1a', 'bpv1b' ],
                                    [ 'bpv2', 'bpv2a' ],
                                    [ 'bpv3' ],
                                    [ 'realtime' ] ] ):
                for prio, bpv in enumerate( bpvae ):
                    con.execute_nofetch( "INSERT INTO base_procver_of_procver(procver_id,base_procver_id,priority) "
                                         "VALUES (%(pv)s,%(bpv)s,%(prio)s)",
                                         { 'pv': pvs[pv].id, 'bpv': bpvs[bpv].id, 'prio': prio } )


            con.execute_nofetch( "INSERT INTO processing_version_alias(description,procver_id) "
                                 "VALUES ('default',%(pvid)s)", { 'pvid': pvs['pv3'].id } )

            con.commit()

        yield bpvs, pvs

    finally:
        with DBCon() as con:
            con.execute_nofetch( "DELETE FROM base_procver_of_procver WHERE procver_id=ANY(%(pvs)s)",
                                 { 'pvs': [ p.id for p in pvs.values() ] } )
            con.execute_nofetch( "DELETE FROM processing_version_alias WHERE description='default'" )
            for bpv in bpvs.values():
                bpv.delete_from_db( dbcon=con, nocommit=True )
            for pv in pvs.values():
                pv.delete_from_db( dbcon=con, nocommit=True )
            con.commit()


@pytest.fixture( scope='module' )
def set_of_lightcurves( procver_collection ):
    # Define four root objects:
    #   0-1: 2 within 15" of each other
    #   2: 1 20" away
    #   3: 1 much further away
    #
    # The first root object has three diaobjects associated with it, under bpv1, bpv2, and realtime
    # The others only have diaobjects in bpv2 and realtime
    #
    # Objects have lightcurves:
    #     object 0 : first detection mjd 60000, last detection mjd 60030, peak 60010, mag 24
    #     object 1 : first detection mjd 60020, last detection mjd 60060, peak 60035, mag 22
    #     object 2 : first detection mjd 60040, last detection mjd 60080, peak 60050, mag 23
    #     object 3 : first detection mjd 60050, last detection mjd 60060, peak 60055, mag 25
    # Forced photometry starts 10 days before first detection and goes through 20 days after last detection.
    #
    # (I'm counting on floats being perfectly representable in these ranges at increments of 2.5.)
    #
    # Object 0 is complicated.  The first diaboject will only have detections through
    #     60015 and forced photometry through 60010 in bpv1a.
    #     It will have detections through 60030 and forced photometry through 60025 in bpv1
    # All objects have full lightcurves in bpv2, bpv2a, bpv3
    # Objects 0-2 have photometry through 60060 and forced through 60055 in realtime
    # All lightcurves are at a 2.5 day cadence, alternating bands r and i.

    roots = []
    rootobjs = []
    objobjs = []
    srcobjs = []
    frcobjs = []
    bpvs, _pvs = procver_collection

    try:
        flux = lambda mag: 10 ** ( ( mag - 31.4 ) / ( -2.5 ) )

        rootobjinfo = [ { 'ra': 42., 'dec': 13. },
                        { 'ra': 42., 'dec': 13.0036 },
                        { 'ra': 42., 'dec': 13.0056 },
                        { 'ra': 42., 'dec': 14. } ]

        detrange = [ ( 60000., 60030. ),
                     ( 60020., 60060. ),
                     ( 60040., 60080. ),
                     ( 60050., 60060. ) ]
        tmax = [ 60010., 60035., 60050., 60055. ]

        peakmag = [ 24., 22., 23., 25. ]
        firstmag = [ 26., 25., 25.5, 25.8 ]
        lastmag = [ 25.9, 25.1, 25.6, 26. ]
        # minmag = 26.
        zeromag = 32.

        visit = 0
        for i, rootobj in enumerate(rootobjinfo):
            robj = RootDiaObject( id=uuid.uuid4() )
            rootobjs.append( robj )

            rootdict = { 'root': robj,
                         'objs': [] }
            roots.append( rootdict )

            # diaobjects
            if i < 3:
                objr = DiaObject( diaobjectid=i, ra=rootobj['ra'], dec=rootobj['dec'],
                                  rootid=robj.id, base_procver_id=bpvs['realtime'].id )
                objobjs.append( objr )
                rootdict['objs'].append( { 'obj': objr, 'src': {}, 'frc': {} } )
            else:
                rootdict['objs'].append( None )

            obj = DiaObject( diaobjectid=200+i, ra=rootobj['ra'], dec=rootobj['dec'],
                             rootid=robj.id, base_procver_id=bpvs['bpv2'].id )
            objobjs.append( obj )
            rootdict['objs'].append( { 'obj': obj, 'src': {}, 'frc': {} } )

            if i == 0:
                obj1 = DiaObject( diaobjectid=100, ra=rootobj['ra'], dec=rootobj['dec'],
                                  rootid=robj.id, base_procver_id=bpvs['bpv1'].id )
                objobjs.append( obj1 )
                rootdict['objs'].append( { 'obj': obj1, 'src': {}, 'frc': {} } )

            # build the rest of the rootdict dicts
            if i < 3:
                rootdict['objs'][0]['src']['realtime'] = []
                rootdict['objs'][0]['frc']['realtime'] = []
            for bpv in [ 'bpv2', 'bpv2a', 'bpv3' ]:
                rootdict['objs'][1]['src'][bpv] = []
                rootdict['objs'][1]['frc'][bpv] = []
            if i == 0:
                for bpv in [ 'bpv1', 'bpv1a' ]:
                    rootdict['objs'][2]['src'][bpv] = []
                    rootdict['objs'][2]['frc'][bpv] = []

            # Detections
            for sourcemjd in np.arange( detrange[i][0], detrange[i][1]+1., 2.5 ):
                visit += 1
                mjdend = detrange[i][0] if sourcemjd < tmax[i] else detrange[i][1]
                endmag = firstmag[i] if sourcemjd < tmax[i] else lastmag[i]
                mag = endmag + ( sourcemjd - mjdend ) * ( peakmag[i] - endmag ) / ( tmax[i] - mjdend  )
                psfflux = flux( mag )
                psffluxerr = 0.1 * psfflux

                if ( i < 3 ) and ( sourcemjd <= 60060. ):
                    src = DiaSource( diaobjectid=objr.diaobjectid, visit=visit, detector=0,
                                     band=('r' if visit%2==0 else 'i'), midpointmjdtai=sourcemjd,
                                     ra=rootobj['ra'], dec=rootobj['dec'],
                                     psfflux=psfflux, psffluxerr=psffluxerr,
                                     base_procver_id=bpvs['realtime'].id )
                    srcobjs.append( src )
                    rootdict['objs'][0]['src']['realtime'].append( src )
                if ( i < 3 ) and ( sourcemjd <= 60055. ):
                    frc = DiaForcedSource( diaobjectid=objr.diaobjectid, visit=visit, detector=0,
                                           band=('r' if visit%2==0 else 'i'), midpointmjdtai=sourcemjd,
                                           ra=rootobj['ra'], dec=rootobj['dec'],
                                           psfflux=psfflux, psffluxerr=psffluxerr,
                                           base_procver_id=bpvs['realtime'].id )
                    frcobjs.append( frc )
                    rootdict['objs'][0]['frc']['realtime'].append( frc )

                for bpv in [ 'bpv2', 'bpv2a', 'bpv3' ]:
                    src = DiaSource( diaobjectid=obj.diaobjectid, visit=visit, detector=0,
                                     band=('r' if visit%2==0 else 'i'), midpointmjdtai=sourcemjd,
                                     ra=rootobj['ra'], dec=rootobj['dec'],
                                     psfflux=psfflux, psffluxerr=psffluxerr,
                                     base_procver_id=bpvs[bpv].id )
                    srcobjs.append( src )
                    rootdict['objs'][1]['src'][bpv].append( src )
                    frc = DiaForcedSource( diaobjectid=obj.diaobjectid, visit=visit, detector=0,
                                           band=('r' if visit%2==0 else 'i'), midpointmjdtai=sourcemjd,
                                           ra=rootobj['ra'], dec=rootobj['dec'],
                                           psfflux=psfflux, psffluxerr=psffluxerr,
                                           base_procver_id=bpvs[bpv].id )
                    frcobjs.append( frc )
                    rootdict['objs'][1]['frc'][bpv].append( frc )

                if i == 0:
                    for bpv in [ 'bpv1', 'bpv1a' ]:
                        if ( ( ( bpv == 'bpv1a' ) and ( sourcemjd > 60015 ) ) or
                             ( ( bpv == 'bpv1' ) and ( sourcemjd > 60030 ) ) ):
                            continue
                        src = DiaSource( diaobjectid=obj1.diaobjectid, visit=visit, detector=0,
                                         band=('r' if visit%2==0 else 'i'), midpointmjdtai=sourcemjd,
                                         ra=rootobj['ra'], dec=rootobj['dec'],
                                         psfflux=psfflux, psffluxerr=psffluxerr,
                                         base_procver_id=bpvs[bpv].id )
                        srcobjs.append( src )
                        rootdict['objs'][2]['src'][bpv].append( src )
                        if ( ( ( bpv == 'bpv1a' ) and ( sourcemjd > 60010 ) ) or
                             ( ( bpv == 'bpv1' ) and ( sourcemjd > 60025 ) ) ):
                            continue
                        frc = DiaForcedSource( diaobjectid=obj1.diaobjectid, visit=visit, detector=0,
                                               band=('r' if visit%2==0 else 'i'), midpointmjdtai=sourcemjd,
                                               ra=rootobj['dec'], dec=rootobj['dec'],
                                               psfflux=psfflux, psffluxerr=psffluxerr,
                                               base_procver_id=bpvs[bpv].id )
                        frcobjs.append( frc )
                        rootdict['objs'][2]['frc'][bpv].append( frc )


            # Nondetections
            mjds1 = np.arange( detrange[i][0] - 10., detrange[i][0], 2.5 )
            mjds2 = np.arange( detrange[i][1] + 2.5, detrange[i][1] + 21., 2.5 )
            mjds = np.concatenate( ( mjds1, mjds2 ) )
            for sourcemjd in mjds:
                visit += 1
                if sourcemjd < tmax[i]:
                    mag = firstmag[i] + ( detrange[i][0] - sourcemjd ) * ( zeromag - firstmag[i] ) / 10.
                else:
                    mag = lastmag[i] + ( sourcemjd - detrange[i][0] ) * ( zeromag - lastmag[i] ) / 20.
                psfflux = flux( mag )
                psffluxerr = 0.5 * psfflux

                if ( i < 3 ) and ( sourcemjd <= 60055. ):
                    frc = DiaForcedSource( diaobjectid=objr.diaobjectid, visit=visit, detector=0,
                                           band=('r' if visit%2==0 else 'i'), midpointmjdtai=sourcemjd,
                                           ra=rootobj['ra'], dec=rootobj['dec'],
                                           psfflux=psfflux, psffluxerr=psffluxerr,
                                           base_procver_id=bpvs['realtime'].id )
                    frcobjs.append( frc )
                    rootdict['objs'][0]['frc']['realtime'].append( frc )

                for bpv in [ 'bpv2', 'bpv2a', 'bpv3' ]:
                    frc = DiaForcedSource( diaobjectid=obj.diaobjectid, visit=visit, detector=0,
                                           band=('r' if visit%2==0 else 'i'), midpointmjdtai=sourcemjd,
                                           ra=rootobj['ra'], dec=rootobj['dec'],
                                           psfflux=psfflux, psffluxerr=psffluxerr,
                                           base_procver_id=bpvs[bpv].id )
                    frcobjs.append( frc )
                    rootdict['objs'][1]['frc'][bpv].append( frc )

                if i == 0:
                    for bpv in [ 'bpv1', 'bpv1a' ]:
                        if ( ( ( bpv == 'bpv1a' ) and ( sourcemjd > 60015. ) ) or
                             ( ( bpv == 'bpv1' ) and ( sourcemjd > 60025. ) ) ):
                            continue
                        frc = DiaForcedSource( diaobjectid=obj1.diaobjectid, visit=visit, detector=0,
                                               band=('r' if visit%2==0 else 'i'), midpointmjdtai=sourcemjd,
                                               ra=rootobj['ra'], dec=rootobj['dec'],
                                               psfflux=psfflux, psffluxerr=psffluxerr,
                                               base_procver_id=bpvs[bpv].id )
                        frcobjs.append( frc )
                        rootdict['objs'][2]['frc'][bpv].append( frc )

            # OMG HOST GALAXIES... TODO

        with DBCon() as con:
            RootDiaObject.bulk_insert_or_upsert( rootobjs, dbcon=con )
            DiaObject.bulk_insert_or_upsert( objobjs, dbcon=con )
            DiaSource.bulk_insert_or_upsert( srcobjs, dbcon=con )
            DiaForcedSource.bulk_insert_or_upsert( frcobjs, dbcon=con )

        # For usage convenience, sort all the sources and forced sources by mjd
        for rootdict in roots:
            for obj in rootdict['objs']:
                if obj is None:
                    continue
                for src in obj['src'].values():
                    src.sort( key=lambda x: x.midpointmjdtai )
                for frc in obj['frc'].values():
                    frc.sort( key=lambda x: x.midpointmjdtai )

        yield roots
    finally:
        with DBCon() as con:
            frcobjs.extend( srcobjs )
            frcobjs.extend( objobjs )
            frcobjs.extend( rootobjs )
            for obj in frcobjs:
                obj.delete_from_db( dbcon=con, nocommit=True )
            con.commit()


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
    obj = DiaObject( diaobjectid=42,
                     base_procver_id=bpvs['bpv1'].id,
                     radecmjdtai=60000.,
                     ra=42.,
                     dec=13,
                     rootid=rootobj1.id
                    )
    obj.insert()

    yield obj
    with DB() as con:
        cursor = con.cursor()
        subdict = { 'rootid': rootobj1.id, 'id': obj.diaobjectid, 'pv': obj.base_procver_id }
        cursor.execute( "DELETE FROM diaobject WHERE diaobjectid=%(id)s AND base_procver_id=%(pv)s", subdict )
        con.commit()


@pytest.fixture
def src1( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    src = DiaSource( base_procver_id=bpvs['bpv1'].id,
                     diaobjectid=obj1.diaobjectid,
                     visit=64,
                     detector=9,
                     band='r',
                     midpointmjdtai=59000.,
                     ra=obj1.ra + 0.0001,
                     dec=obj1.dec + 0.0001,
                     psfflux=3.,
                     psffluxerr=0.1
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
def src1_pv2( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    src = DiaSource( base_procver_id=bpvs['bpv2'].id,
                     diaobjectid=obj1.diaobjectid,
                     visit=64,
                     detector=9,
                     band='r',
                     midpointmjdtai=59000.,
                     ra=obj1.ra + 0.0001,
                     dec=obj1.dec + 0.0001,
                     psfflux=3.,
                     psffluxerr=0.1
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
def forced1( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    src = DiaForcedSource( base_procver_id=bpvs['bpv1'].id,
                           diaobjectid=obj1.diaobjectid,
                           visit=128.,
                           detector=10.,
                           midpointmjdtai=59100.,
                           band='i',
                           ra=obj1.ra - 0.0001,
                           dec=obj1.dec - 0.0001,
                           psfflux=4.,
                           psffluxerr=0.2,
                           scienceflux=7.,
                           sciencefluxerr=0.5,
                           time_processed=None,
                           time_withdrawn=None
                          )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaforcedsource WHERE base_procver_id=%(pv)s "
                        "                              AND objectid=%(id)s AND visit=%(visit)s",
                        { 'pv': src.processing_version, 'objid': src.diaobjectid, 'visit': src.visit } )
        con.commit()


@pytest.fixture
def forced1_pv2( obj1, procver_collection ):
    bpvs, _pvs = procver_collection
    src = DiaForcedSource( base_processing_version=bpvs['bpv2'].id,
                           diaobjectid=obj1.diaobjectid,
                           visit=128.,
                           detector=10.,
                           midpointmjdtai=59100.,
                           band='i',
                           ra=obj1.ra - 0.0001,
                           dec=obj1.dec - 0.0001,
                           psfflux=4.,
                           psffluxerr=0.2,
                           scienceflux=7.,
                           sciencefluxerr=0.5,
                           time_processed=None,
                           time_withdrawn=None
                          )
    src.insert()

    yield src
    with DB() as con:
        cursor = con.cursor()
        cursor.execute( "DELETE FROM diaforcedsource WHERE base_procver_id=%(pv)s "
                        "                              AND objectid=%(objid)s AND visit=%(visit)s",
                        { 'pv': src.processing_version, 'objid': src.diaobjectid, 'visit': src.visit } )
        con.commit()


@pytest.fixture( scope='session' )
def test_user():
    # Test user with password 'test_password'
    user = AuthUser( id=asUUID('788e391e-ca63-4057-8788-25cc8647e722'),
                     username='test',
                     displayname='test user',
                     email='test@nowhere.org',
                     pubkey="""-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA1QLihZJ78NHKppUBUaZI
sel7WFKp/3Pr14nbel+BpfOVWrIIIiMegQSAliWRszNLQezKwHTXM4DUxZu7LG/q
zut37v5WSVWCK8wSW+zy6e9vnuVkcrzdEJgkztUaiC8lMnHVE0ycpLTICcAu0wtv
WP32ScyNbiHidyPZwNd9XB4juLl9j7K6hs7WQwmeMOyw8dUZuE8b/jiHrAxxnHjE
Sli8bjR7I6X3AX8U81bP4qFjTjGuy85dIeZEbyS6UpbmkZ+imr/0wLa9knRoW0hU
Uz8p+P/Vts3rimpQtPajtRzCpTY4lRfh05YDmr2rc1WHJ/IPu3v7sIUg8K/egoPJ
VU3c2QYGpwmpnldbb+bpSUXxpsQVtFw5pHmqEbfKXWNM8CTkii8s6bI03/JQREBU
L3OzCGclvS8lQ+ZXAQaMyjshMqMFud3E9RS5EFxpSfk92r+RY7PgaYs9PX7x33zU
k/937nk7sTR5OEKFgxRDx61svk5UJIPQib5SnIDRNAqeKhxg23q5ZqDMBVk1rAhI
xFuX4Hj8VtG89J3DSVJue4psF0wTYceUhUleJCG3gPxAyE2g4ObZZ9mh/gI1KG6v
Np9CFWk9eMSeehEI1YKyPY8Hdv50PmIvN2zgxbo2wccspwCVTrtdKoQebpVAAu3v
tyOci9saPPfI1bNnKD202zsCAwEAAQ==
-----END PUBLIC KEY-----
""",
                     privkey={"iv": "z84DFtRURdKFhn3b",
                              "salt": "57B2Nq/ZToHhVM+1DEq30g==",
                              "privkey": "j/4EdYRmClt0K0tNEte8sLh3I92HHK90YEm7QdSw/x0ROUmv/Xh/6/YQOW7k02t5opZczzAhSHzySDbYR2vojjYyHoH3m7Z9IuNnDsVbJFyPyf6s/ZE99GRbu+dWL8GXuBEcCTeM0n+n7746T6xxp7Wo4ae+gmSrmqoTerC1NNeZ07dwnc/eQ0GIrjICt8Jrkf5fbNFFPG0V0KxOhClWLBunLxjC37yWSeneWtyVr1GrlUId3JarwATzzX2d6rG3ofC3GDDGohRVURgWG5Qy6Loj8v3bb6peEf3+sNpPpdqDkRF6FXVfO0jTPX6xgZFxBBPdkd8aw176KVqIoRxP+hbjYohqqw8u74xAg9xAVIiLgg4xg2U7lhb2JdMCfW0w56BbAlsGU6d0dZ7e/DM7qTitL+rYt2rGdOf3xlzw2hFUXsTwsVau6mZBLH5RH4uvS3lFzbtLq4KjMYLKJj+xuyCp0hcpHXbzVN+mOxlfyPn3mYcp0OzUp5hqQh9sl8773C3CJFt/44Kkq6QPvzpTwTs9f3JfShRh5MYZTGL21jGMnuGwZeLWJkezP59i5sngZOF4KK29FAJR6lFGzLWKwSgjmxrA6/ug6fPJJwvJIZNIwrGx4/HoEfsCqOytW+su/rCa/huNFqfVFGElm3RCQFLIkvlUC3DJYYgvOIXFhnhQlbwxjAuceUmlcHCLSOKybzNAJDSvSZ6sL/UbaODj9F27LQ423a+U7/V5KE+dTGi6VQHU1e0ZaniscMyCIU4+GWA5UE/Duj4ojbVITtZCpdKHYJxCXaeKYmP45bkdxyyEUihkEb12gGpgZ9JmXN7ucecVqzhv+HO149dG1fzdszN1eQEKhStFsdDHqDknt1oBbOMFR11y3XwCqq4pt+kmYrzhtz+vswG50cQRuoG/QRO35inXGoPCTBDRovWs/56FJMvj4f67N02rRVpKuI4hh5neBPQeoOHBrha5v2B7obfyeIjWDNdcB97TdHB6xDZLPpy28GMgQGcIzPzwZ2LXqIFRONBDPNK5o+p4NP55neKogwz57065CMcyqa7CQ0sMCjRz+WyVaTy7h0t6esDuZhBesf8GjtNXPHgTJB1oSkq83AnrQ+GBV+W3EeGcvGgK6c9ljszKxP0hbbFpG32Uz4mBtxLj8unf5lf5ctZSutLqRlPMycXYLVPpFg2L+3bbUZ1AR7HkoeHQ9od+ixRmMY4y3AQl6E7nr/YXAtJUsjlQeTxksO0nhL+l03mMaBsBnTEPVsUkPGa4pyi+FIYOyseNhJ9S7Cog8hhFIP95l09pTCWqHENjIa1bmT4VPjM1MTC6DR4BgWaBytrmJIxPYFa5g6eX9UvWd0vebjH+fSFXa952QjEwIJoHYsoWUcET+nIjEqjTUxff3DDqCC5gNvonG9E2xTwkciNzQCtcY941w2QBYwV2V0eKReLV8IPNFmm4dwe2bEZri6ywIVpaclVOpbHPMOlu4KKJA/W4lo+vgCOKz/Lni/mnigRrsuTPQWOOkPQgNjM6mv607eI570iH2F8RpSI6Lih3rw02YvsLOYYNYH5EvNL4rlK5W21ubdEAP8no1iXXwi//UiirCCzZYSAdSfmRRKEn6XC97U98e6Sn84HYFqgFWbAadULGEHBPadjSYUuQiFT0Gu7kAuQFNAse/M30eUCBqIyQXjsrGFkGC5za872J2mtJcFpH00KgNUaa7xmWOtqUl+19WF9kBQF0VuF1+7rBVlsDo1IZj8ajnMnq3Lgopgce07/dRgyj2QL5ddWIRRs5VdYLS5VnDgO6yNCIGuBV8Vtq75nhPAruuZN7FfLLkVUUouOdtH7d2U5D1Ewn3z1wcv202vL5zU6MwO0WMAxHgJDJbHANVOnuC+YYXnPJGN8DeqVpJueWu6rXPx71JzqjCvEHNDhefwJhUsCe9/JD1hVtfKRREY/4Q0gbztrNA+5tZJ64L56/orrxpDHaoHrqPsxqnKj5OQ8Z6eXrf98L+69vwKwAoYVpMdGdfDPPAVlj/Ia2+uekiYm5IXT5sG9z4kuns85fABajEZ3wb1sYzbXUFjvfpLX6wLGyUzOM3AEnbwrJyI/TMMQ1KEqzkn3wSfZptFs2hTkn7bnSdhv46dh6TW7BG/rng21p5zwnrx6VYcmtrXAM5yZWm0j18Pa2hypSFfMJnQjTfl7anmJkIxlGU2zdVBDAKk6wtx+47O7dUN7BVpUmc+/Pnlg5eVITXyZ3aRMTLfC4L8k2DxHWMT+7NWVUD+D60s0ilv5PxC0XODmE+VWu3mGH+Z51RUYXI+VVrIVC8lgTiU3Am+RdJbI9mn6FfgdxLVnBl+rx4UQ4qqKtnPX+An29T1xyLTwzLM2anxrU+q9eGVOptl9l4SeDGfG/qmSuOxbARYiCX9MP76JCoqc8nOmsOCF8CzW9e3C1w9cgf3wuxyWnn54sUzqHMAxiTiUlxhr/nb3u1fCc4kU7fjplk8MQCjcN1bxzX9RMBIcZ4mFpSRTS3q3B2lYJXpEE8kvoD9PfkqYAZO1L8DBwCk46+75AbWxfcS4c3PVBimIi+91PjH7oSqtMiAC3j5hCU2/PMEWE9r1NZ32qUo1zmEW63LXCjUEGFJhKsQgsc1g5P5neCy+IKT44pm/ZuH372MvmBTKQ83KB1t1LQhaxWadH5/GL1smYOKlzMKiCwYjtw77w1dG1SzDvwojD5Q877ecEEeF2zZdUrv+bJ8s2kyavWfjX3E3kFJYQh3z8GZeTjE+u+m8Wj0q6Z3+fVcgMbGpj5BpaZZ3XIWkxkc0KUL10QMuAOctgAu0p4mttWsZ7LIy7e/WoZhpk5OeCOL+RygFE/I1tfrvCXsk+p5xCiei/4VLT+tKLiKAcBFyPu3VZZIg8eHFG7Bnn4+k/m1glBprtSln84hbdIXGTzBe8Hmb79Fa9VvQp2+LldMAyaBHseFnBNg2/2SCZPQ9sXn96jp82NElQMSJJWOtBw8U/rmxVrJwdY8BdjlR5eA90y8HmCzrjh2Yq3hRVHHDvDWx1CKFc7OAvA2JA6fKamN4bXfzXHIo1G5ciS7WvGd5zXBgcWqnk1LxchSZAIlnDow0+JoR+RnK4EgyAw7r2+6FbJBkOfVnv8fb9qdSIVglY15OVNQNnstv3n0Tx/1qU7gvMvlxt0hS9Dh6+PKvl1VlSy5JZtMiI"} # noqa: E501
                    )
    user.insert()

    yield user

    user.delete_from_db()


@pytest.fixture( scope='session' )
def fastdb_client( test_user ):
    return FASTDBClient( 'http://webap:8080', username="test", password="test_password", verify=False, debug=True,
                         retrysleep=0.1, retries=2 )


@pytest.fixture( scope='session' )
def snana_fits_ppdb_loaded():
    e2td = pathlib.Path( "elasticc2_test_data" )
    assert e2td.is_dir()
    dirs = e2td.glob( "*" )
    dirs = [ d for d in dirs if d.is_dir() ]
    assert len(dirs) > 0

    try:
        com = [ "python", "/code/src/admin/load_snana_fits.py",
                "-n", "5",
                "-v",
                "--ppdb",
                "-d",
               ]
        com.extend( dirs )
        com.append( "--do" )

        logger.info( f"Running a subprocess with command: {com}" )
        res = subprocess.run( com, capture_output=True )
        assert res.returncode == 0

        yield True

    finally:
        with DB() as conn:
            cursor = conn.cursor()
            for tab in [ 'ppdb_host_galaxy', 'ppdb_diaobject', 'ppdb_diasource', 'ppdb_diaforcedsource' ]:
                cursor.execute( f"TRUNCATE TABLE {tab} CASCADE" )
            conn.commit()


# WARNING -- do not use this fixture together with other fixtures
#   that affect the diaobject, root_diaboject, diasource, or diaforcedsource tables!!!!
@pytest.fixture( scope='module' )
def snana_fits_maintables_loaded_module( procver_collection ):
    e2td = pathlib.Path( "elasticc2_test_data" )
    assert e2td.is_dir()
    dirs = e2td.glob( "*" )
    dirs = [ d for d in dirs if d.is_dir() ]
    assert len(dirs) > 0

    try:
        com = [ "python", "/code/src/admin/load_snana_fits.py",
                "-n", "5",
                "-v",
                "--pv", procver_collection[1]['pv1'].description,
                "-d",
               ]
        com.extend( dirs )
        com.append( "--do" )

        logger.info( f"Running a subprocess with command: {com}" )
        res = subprocess.run( com, capture_output=True )
        assert res.returncode == 0

        with DB() as dbcon:
            cursor = dbcon.cursor()
            cursor.execute( "SELECT COUNT(*) FROM diaobject" )
            nobj = cursor.fetchone()[0]
            assert nobj == 346
            cursor.execute( "SELECT COUNT(*) FROM diasource" )
            nsrc = cursor.fetchone()[0]
            assert nsrc == 1862
            cursor.execute( "SELECT COUNT(*) FROM diaforcedsource" )
            nfrc = cursor.fetchone()[0]
            assert nfrc == 52172
            cursor.execute( "SELECT COUNT(*) FROM host_galaxy" )
            nhost = cursor.fetchone()[0]
            assert nhost == 356
            cursor.execute( "SELECT COUNT(*) FROM root_diaobject" )
            assert cursor.fetchone()[0] == 346

        yield nobj, nsrc, nfrc, nhost

    finally:
        with DB() as conn:
            cursor = conn.cursor()
            for tab in [ 'root_diaobject', 'host_galaxy', 'diaobject', 'diasource', 'diaforcedsource' ]:
                cursor.execute( f"TRUNCATE TABLE {tab} CASCADE" )
            conn.commit()


@pytest.fixture
def mongoclient():
    host = os.getenv( 'MONGODB_HOST' )
    dbname = os.getenv( 'MONGODB_DBNAME' )
    user = os.getenv( "MONGODB_ALERT_READER_USER" )
    password = os.getenv( "MONGODB_ALERT_READER_PASSWD" )
    client = MongoClient( f"mongodb://{user}:{password}@{host}:27017/{dbname}?authSource={dbname}" )
    return client


@pytest.fixture
def mongoclient_rw():
    host = os.getenv( 'MONGODB_HOST' )
    dbname = os.getenv( 'MONGODB_DBNAME' )
    user = os.getenv( "MONGODB_ALERT_WRITER_USER" )
    password = os.getenv( "MONGODB_ALERT_WRITER_PASSWD" )
    client = MongoClient( f"mongodb://{user}:{password}@{host}:27017/{dbname}?authSource={dbname}" )
    return client
