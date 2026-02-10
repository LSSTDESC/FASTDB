import os
import pytest
import pathlib
import uuid

import numpy as np
import numpy.random
from pymongo import MongoClient

from db import ( BaseProcessingVersion,
                 ProcessingVersion,
                 RootDiaObject,
                 DiaObject,
                 DiaObjectPosition,
                 DiaSource,
                 DiaSourceExtra,
                 DiaForcedSource,
                 DiaForcedSourceExtra,
                 # HostGalaxy,
                 # DiaObjectHostMatch,
                 DB,
                 DBCon,
                 AuthUser )
from util import asUUID
from admin.load_snana_fits import FITSLoader
from fastdb.fastdb_client import FASTDBClient


# sys.path.insert( 0, pathlib.Path(__file__).parent )
# For cleanliness, a bunch of fixtures are broken
#   out into their own files.  To be able to see
#   them, put those files in this list below.
#   (pytest is kind of a beast).  Those files
#   should all live in the fixtures subdirectory.
pytest_plugins = [ 'fixtures.alertcycle',
                   'fixtures.spectrum',
                   'fixtures.basedbtests'
                  ]


@pytest.fixture( scope='session' )
def procver_postimes():
    """MJDs of positions used by procver_collection and set_of_lightcurves."""
    return [ 60000, 60030, 60050, 60060, 60080 ]


@pytest.fixture( scope='session' )
def procver_bases():
    """The first part of the base processing version description in procver_collection."""
    return [ 'bpv1', 'bpv1a', 'bpv1b', 'bpv2', 'bpv2a', 'bpv3', 'realtime' ]


@pytest.fixture( scope='module' )
def procver_collection( procver_postimes, procver_bases ):
    # A set of processing versions loaded into the database, including:
    #
    #   A set of base procesing versions pvc_bpv1, pvc_bpv1a, pvc_bpv1b, pvc_bpv2, pvc_bpv2a, pvc_bpv3, realtime
    #   For each of these, there is a base processing version for tables diaobject, diaobject_position,
    #     diasource, and diaforcedsource.
    #
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
    #
    # Keys for the processing versions are pv1, pv2, pv3, and realtime
    # Keys for the base processing versions are {name} or {name}_{table},
    #   where {name} is one of bpv1, bpv1a, bpv1b, bpv2, bpv2a, bpv3, realtime
    #   and {table} is one of diaobject_position, diasource, diaforcedsource
    #   (The bpvs for the diaboject table are named with just {name} for
    #   backwards compatibility with tests written before _table was
    #   included in base processing versions.

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

            tables = [ 'diaobject', 'diaobject_position', 'diasource', 'diaforcedsource' ]
            bases = procver_bases

            # Gotta hardcode the uuids so that they will match when we do a pg_restore
            #  in the fixtures/alertycle.py::alerts_90days_sent_received_and_imported fixture
            # Number of UUIDs is len(bpv_bases) * ( len(tables)-1 + len(procver_postimes ) )
            #   (For each bpv_base, we have one UUID for each table, except for diaobject_position
            #    which has as many as there are procver_postimes)
            uuidbatch = [
                asUUID('789dc3a0-514f-4c03-8445-8dd835dc24ed'),
                asUUID('78cb82f6-40bd-4a5f-99c9-00e8540cee22'),
                asUUID('4d6a0826-ce4a-401a-99e7-a611f5529e36'),
                asUUID('4a95ca56-c45e-44bd-bc22-c47616265b54'),
                asUUID('3f9ebc2c-c4b9-4e9d-8317-e7ce5d728115'),
                asUUID('b910ec64-8ba6-4a57-a011-67ddb227e946'),
                asUUID('83979a8c-fde9-4860-9e9d-00aa4a6de54a'),
                asUUID('f7047efb-9460-46f7-bbb4-fd944d2d9d18'),
                asUUID('8deb6362-41ed-43a5-9342-565c2d30fab3'),
                asUUID('77f65a35-d61b-462f-9a82-e8200cff6c5a'),
                asUUID('0d52e9c2-6521-4be4-b807-0286d09098f5'),
                asUUID('97f8d3de-842f-4b85-bbf9-5d76f7a4a02b'),
                asUUID('a44c3625-fd90-4251-841e-c45d693aacb8'),
                asUUID('6f487641-3462-4478-8251-7555dce1a6a1'),
                asUUID('83a3f8dc-b86e-480c-bbec-7d0ea32b3aac'),
                asUUID('792c7550-43df-4133-891a-84ecd6faf22f'),
                asUUID('74b86355-942e-452b-84e9-87be8bff2e31'),
                asUUID('5fa85b96-9ad5-4dc2-ae65-8e73ef390198'),
                asUUID('fef801c0-4530-49a6-a481-a80d39d275a8'),
                asUUID('9c5089a9-7169-4f10-8443-1d718f29edd1'),
                asUUID('158a729a-c1e1-4ca2-bdbe-88da6b2bda1d'),
                asUUID('1ba77c23-933d-4d2c-b6f3-5c8119b1d011'),
                asUUID('5ca93ccc-3ef7-42af-adce-7ae12ed9a151'),
                asUUID('9ffe144a-f262-4558-9171-47d6a927a8af'),
                asUUID('a452607b-f9e6-421b-a91d-62c96f3112ff'),
                asUUID('dcd5c7ef-1eef-496e-a984-a345d0873bb8'),
                asUUID('e4f9e1a1-12a0-431a-8e1c-3e3b2c27fcec'),
                asUUID('49f62670-d4f8-4d66-ae33-2d589a3fb0f3'),
                asUUID('6cf8f4fd-3eed-4b81-aae8-3d8239e8086d'),
                asUUID('0253e698-ff88-4dd2-899b-a11d6ea4c0bf'),
                asUUID('e47d37b7-bc99-4701-b41a-1e7bfc9929c0'),
                asUUID('35b5fd38-0dcc-40a8-9303-98f62c975234'),
                asUUID('a4b6e628-07ef-4980-a5ac-31e735b2b9e1'),
                asUUID('254a4269-9018-4d90-ad40-fdc4aa269df5'),
                asUUID('10e64dc4-dc0e-4dbf-a3e4-aae78e3b340b'),
                asUUID('4eddc698-9644-4aef-89cb-8036e5436e3f'),
                asUUID('1a9697e2-d713-4717-8435-180870a2dfc7'),
                asUUID('3feeb476-4906-4200-816b-d9d72a3c53ce'),
                asUUID('c6ce4813-15a8-4d6f-aa23-d69dacac0bb2'),
                asUUID('b0f80452-870f-499e-8058-f31ca780d483'),
                asUUID('12a8912a-f3e8-49b7-9760-4bf22e701195'),
                asUUID('68c75635-018e-447e-a135-3441616f815c'),
                asUUID('263795a1-e1c5-4d8e-b1a4-2c7c4fb44c19'),
                asUUID('42d8f241-2444-4852-94be-ea1f91468721'),
                asUUID('96222d5e-7e33-4b6d-bad7-116266d28618'),
                asUUID('2474290e-bc81-47f8-b1aa-b93f72554451'),
                asUUID('5b4a264b-91ab-4596-9008-2c5768d2b8bd'),
                asUUID('46cdb013-7a66-41ad-b4ab-67d2eab15221'),
                asUUID('4995f91b-4960-42e7-93ea-0f85197d2fcd'),
                asUUID('8884e193-6bd8-4329-ada2-04d44eaecfc2'),
                asUUID('dfa9da97-8931-406c-afdc-ec4c2b4e67a7'),
                asUUID('f64a1bd7-50c5-4d69-8295-2fde7e68a9b0'),
                asUUID('aeb7def7-e6ad-4856-934d-f87cf8d4ee96'),
                asUUID('22121e3e-21ff-4486-b6b2-795df70b65f7'),
                asUUID('9b8c728c-a8ea-4c18-be93-e4731fbdcea1'),
                asUUID('1959c4ea-11f5-4a94-bc35-cdad9e222641')
            ]
            # Just check that the hardcoding has kept up with everything else
            assert len(uuidbatch) == len(bases) * ( len(tables)-1 + len(procver_postimes) )

            uuiddex = -1
            for base in bases:
                for table in tables:
                    if table == 'diaobject_position':
                        for postime in procver_postimes:
                            uuiddex += 1
                            key = f'{base}_diaobject_position_{postime}'
                            desc = f'pvc_{base}_{postime}'
                            bpvs[key] = BaseProcessingVersion( id=uuidbatch[uuiddex], _table=table, description=desc )
                    else:
                        uuiddex += 1
                        key = base if table == 'diaobject' else f'{base}_{table}'
                        desc = base if base == 'realtime' else f'pvc_{base}'
                        bpvs[key] = BaseProcessingVersion( id=uuidbatch[uuiddex], _table=table, description=desc )

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
                    for table in tables:
                        if table == 'diaobject_position':
                            for posn, postime in enumerate( procver_postimes ):
                                # I know that there are never more than 10 bpvs for a given pv
                                subprio = prio * 10 + posn
                                bpvkey = f'{bpv}_diaobject_position_{postime}'
                                con.execute_nofetch( "INSERT INTO base_procver_of_procver(procver_id,base_procver_id,"
                                                     "                                    _table,priority) "
                                                     "VALUES (%(pv)s,%(bpv)s,%(tab)s,%(prio)s)",
                                                     { 'pv': pvs[pv].id, 'bpv': bpvs[bpvkey].id,
                                                       'tab': table, 'prio': subprio } )
                        else:
                            bpvkey = bpv if table == 'diaobject' else f'{bpv}_{table}'
                            con.execute_nofetch( "INSERT INTO base_procver_of_procver(procver_id,base_procver_id,"
                                                 "                                    _table,priority) "
                                                 "VALUES (%(pv)s,%(bpv)s,%(tab)s,%(prio)s)",
                                                 { 'pv': pvs[pv].id, 'bpv': bpvs[bpvkey].id,
                                                   'tab': table, 'prio': prio } )

            con.execute_nofetch( "INSERT INTO processing_version_alias(description,procver_id) "
                                 "VALUES ('default',%(pvid)s)", { 'pvid': pvs['pv3'].id } )

            con.commit()

        yield bpvs, pvs

    finally:
        with DBCon() as con:
            con.execute_nofetch( "DELETE FROM base_procver_of_procver WHERE procver_id=ANY(%(pvs)s)",
                                 { 'pvs': [ p.id for p in pvs.values() ] } )
            con.execute_nofetch( "DELETE FROM processing_version_alias WHERE description='default'" )
            con.execute_nofetch( "DELETE FROM processing_version WHERE id=ANY(%(pvs)s)",
                                 { 'pvs': [ p.id for p in pvs.values() ] } )
            con.execute_nofetch( "DELETE FROM base_processing_version WHERE id=ANY(%(bpvs)s)",
                                 { 'bpvs': [ b.id for b in bpvs.values() ] } )
            con.commit()


@pytest.fixture( scope='module' )
def set_of_lightcurves( procver_bases, procver_postimes, procver_collection ):
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
    # All objects are in bpv2
    # Object 0 only is in bpv1
    # Objects 0 through 2 are in realtime
    #
    # Objects have positions stored for every bpv the object is defined at the times in procver_postimes
    #
    # Forced photometry starts 10 days before first detection and goes through 20 days after last detection.
    #
    # DiaSource and DiaForcedSource have Extra entries for mjd <= 60050
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
    posobjs = []
    srcobjs = []
    srcexobjs = []
    frcobjs = []
    frcexobjs = []
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
        tpeak = [ 60010., 60035., 60050., 60055. ]

        # (The 10 offsets are for forced photometry.)
        lowestmjd = min( [ d[0]-10. for d in detrange ] )
        highestmjd = max( [ d[1]+10. for d in detrange ] )

        # We're going to have a random-but-deterministic position for
        #   every 0.1 days, with a different position for each base
        #   processing version.  Generate all these offsets, even though
        #   we'll only use <1/25 of them.  Computers are fast.
        # Do np.floor(mjd*10) to get the index into these arrays.
        # We'll use the mjd=lowestmjd time for all forced photometry.  I think.
        # 1σ scatter is going to be 0.2". (Yes, regardless of the S/N of hte
        #   detection.  See NOTE below.)
        # Not going to worry about cos(dec)
        # NOTE : every object is going to scatter in the same direction
        #   on a given day!  The use of this fixture is really just for
        #   database structure and searching, not real statistical tests
        #   on positions.  But, if I ever start to do the latter, I will
        #   then regret that I did this.  Future self can be all "I told
        #   you so" to present self.
        n_individual_poses = len(procver_bases) * ( int( ( highestmjd - lowestmjd ) / 0.1 ) + 1 )
        rng = np.random.default_rng( 42 )
        dra_list = rng.normal( 0., 5.56e-5, size=n_individual_poses )
        ddec_list = rng.normal( 0., 5.56e-5, size=n_individual_poses )

        def pos_offset( bpvkey, mjd ):
            assert mjd >= lowestmjd
            bpvdex = procver_bases.index( bpvkey )
            dex = bpvdex * len(procver_bases) + int( np.floor((mjd-lowestmjd) * 10) )
            return dra_list[dex], ddec_list[dex]

        peakmag = [ 24., 22., 23., 25. ]
        firstmag = [ 26., 25., 25.5, 25.8 ]
        lastmag = [ 25.9, 25.1, 25.6, 26. ]
        zeromag = 32.

        visit = 0
        for i, rootobj in enumerate(rootobjinfo):
            robj = RootDiaObject( id=uuid.uuid4() )
            rootobjs.append( robj )
            rootra = rootobjinfo[i]['ra']
            rootdec = rootobjinfo[i]['dec']

            # In rootdict, objs[0] is in realtime, objs[1] is in bpv2, and objs[2] is in bpv1
            rootdict = { 'root': robj,
                         'objs': []
                        }
            roots.append( rootdict )

            # Extract positions used for forced photometry
            forcedra = {}
            forceddec = {}
            for base in procver_bases:
                dra, ddec = pos_offset( base, lowestmjd )
                forcedra[base] = rootra + dra
                forceddec[base] = rootdec + dra

            # diaobjects
            if i < 3:
                objr = DiaObject( diaobjectid=i, rootid=robj.id, base_procver_id=bpvs['realtime'].id )
                objobjs.append( objr )
                rootdict['objs'].append( { 'obj': objr, 'pos': {}, 'src': {}, 'srcex': {}, 'frc': {}, 'frcex': {} } )
            else:
                objr = None
                rootdict['objs'].append( None )

            obj = DiaObject( diaobjectid=200+i, rootid=robj.id, base_procver_id=bpvs['bpv2'].id )
            objobjs.append( obj )
            rootdict['objs'].append( { 'obj': obj, 'pos':{}, 'src': {}, 'srcex': {}, 'frc': {}, 'frcex': {} } )

            if i == 0:
                obj1 = DiaObject( diaobjectid=100, rootid=robj.id, base_procver_id=bpvs['bpv1'].id )
                objobjs.append( obj1 )
                rootdict['objs'].append( { 'obj': obj1, 'pos': {}, 'src': {}, 'srcex': {}, 'frc': {}, 'frcex': {} } )
            else:
                obj1 = None
                rootdict['objs'].append( None )

            # build the rest of the rootdict dicts
            if objr is not None:
                rootdict['objs'][0]['pos']['realtime'] = []
                rootdict['objs'][0]['src']['realtime'] = []
                rootdict['objs'][0]['srcex']['realtime'] = []
                rootdict['objs'][0]['frc']['realtime'] = []
                rootdict['objs'][0]['frcex']['realtime'] = []
            for bpv in [ 'bpv2', 'bpv2a', 'bpv3' ]:
                rootdict['objs'][1]['pos'][bpv] = []
                rootdict['objs'][1]['src'][bpv] = []
                rootdict['objs'][1]['srcex'][bpv] = []
                rootdict['objs'][1]['frc'][bpv] = []
                rootdict['objs'][1]['frcex'][bpv] = []
            if obj1 is not None:
                for bpv in [ 'bpv1', 'bpv1a' ]:
                    rootdict['objs'][2]['pos'][bpv] = []
                    rootdict['objs'][2]['src'][bpv] = []
                    rootdict['objs'][2]['srcex'][bpv] = []
                    rootdict['objs'][2]['frc'][bpv] = []
                    rootdict['objs'][2]['frcex'][bpv] = []

            # Detections
            nextposdex = 0
            postime = None
            ratot = { b: 0. for b in procver_bases }
            dectot = { b: 0. for b in procver_bases }
            npos = 0
            for sourcemjd in np.arange( detrange[i][0], detrange[i][1]+1., 2.5 ):
                ra = {}
                dec = {}
                for base in procver_bases:
                    dra, ddec = pos_offset( base, sourcemjd )
                    ra[base] = rootra + dra
                    dec[base] = rootdec + ddec
                    ratot[base] += ra[base]
                    dectot[base] += dec[base]
                npos += 1

                dopos = False
                if sourcemjd >= procver_postimes[nextposdex]:
                    dopos = True
                    postime = procver_postimes[nextposdex]
                    nextposdex += 1
                assert postime is not None

                visit += 1
                mjdend = detrange[i][0] if sourcemjd < tpeak[i] else detrange[i][1]
                endmag = firstmag[i] if sourcemjd < tpeak[i] else lastmag[i]
                mag = endmag + ( sourcemjd - mjdend ) * ( peakmag[i] - endmag ) / ( tpeak[i] - mjdend  )
                psfflux = flux( mag )
                psffluxerr = 0.1 * psfflux

                # realtime sources
                if ( i < 3 ) and ( sourcemjd <= 60060. ):
                    src = DiaSource( diasourceid=objr.diaobjectid * 100000 + int(sourcemjd),
                                     base_procver_id=bpvs['realtime_diasource'].id,
                                     diaobjectid=objr.diaobjectid,
                                     visit=visit,
                                     band=( 'r' if visit%2==0 else 'i' ),
                                     midpointmjdtai=sourcemjd,
                                     psfflux=psfflux, psffluxerr=psffluxerr,
                                     ra=ra['realtime'], dec=dec['realtime'] )
                    srcobjs.append( src )
                    rootdict['objs'][0]['src']['realtime'].append( src )

                    if sourcemjd < 60050.:
                        srcex = DiaSourceExtra( diasourceid=src.diasourceid,
                                                base_procver_id=src.base_procver_id,
                                                detector=0,
                                                x=1,
                                                y=1 )
                        srcexobjs.append( srcex )
                        rootdict['objs'][0]['srcex']['realtime'].append( srcex )
                    else:
                        rootdict['objs'][0]['srcex']['realtime'].append( None )

                    if dopos:
                        pos = DiaObjectPosition( diaobjectid=objr.diaobjectid,
                                                 base_procver_id=bpvs[f'realtime_diaobject_position_{postime}'].id,
                                                 ra=ratot['realtime'] / npos,
                                                 dec=dectot['realtime'] / npos,
                                                 raerr=0.2 / np.sqrt(npos),
                                                 decerr=0.2 / np.sqrt(npos),
                                                 ra_dec_cov=0.05 ** 2 / npos )
                        posobjs.append( pos )
                        rootdict['objs'][0]['pos']['realtime'].append( pos )

                # realtime forced sources
                if ( i < 3 ) and ( sourcemjd <= 60055. ):
                    frc = DiaForcedSource( diaforcedsourceid=objr.diaobjectid * 100000 + int(sourcemjd),
                                           base_procver_id=bpvs['realtime_diaforcedsource'].id,
                                           diaobjectid=objr.diaobjectid,
                                           visit=visit,
                                           band=('r' if visit%2==0 else 'i'),
                                           midpointmjdtai=sourcemjd,
                                           psfflux=psfflux,
                                           psffluxerr=psffluxerr,
                                           ra=forcedra['realtime'],
                                           dec=forceddec['realtime']
                                          )
                    frcobjs.append( frc )
                    rootdict['objs'][0]['frc']['realtime'].append( frc )

                    if sourcemjd < 60050.:
                        frcex = DiaForcedSourceExtra( diaobjectid=objr.diaobjectid,
                                                      visit=visit,
                                                      base_procver_id=bpvs['realtime_diaforcedsource'].id,
                                                      detector=0,
                                                      scienceflux=1.1*psfflux,
                                                      sciencefluxerr=psffluxerr,
                                                      timeprocessedmjdtai=60365.,
                                                      timewithdrawnmjdtai=None )
                        frcexobjs.append( frcex )
                        rootdict['objs'][0]['frcex']['realtime'].append( frcex )
                    else:
                        rootdict['objs'][0]['frcex']['realtime'].append( None )


                # everything is in bpv2, bpv2a, bp3
                for bpv in [ 'bpv2', 'bpv2a', 'bpv3' ]:
                    src = DiaSource( diasourceid=obj.diaobjectid * 100000 + int(sourcemjd),
                                     base_procver_id=bpvs[f'{bpv}_diasource'].id,
                                     diaobjectid=obj.diaobjectid,
                                     visit=visit,
                                     band=('r' if visit%2==0 else 'i'),
                                     midpointmjdtai=sourcemjd,
                                     psfflux=psfflux,
                                     psffluxerr=psffluxerr,
                                     ra=ra[bpv],
                                     dec=dec[bpv] )
                    srcobjs.append( src )
                    rootdict['objs'][1]['src'][bpv].append( src )

                    frc = DiaForcedSource( diaforcedsourceid=obj.diaobjectid * 100000 + int(sourcemjd),
                                           base_procver_id=bpvs[f'{bpv}_diaforcedsource'].id,
                                           diaobjectid=obj.diaobjectid,
                                           visit=visit,
                                           band=('r' if visit%2==0 else 'i'),
                                           midpointmjdtai=sourcemjd,
                                           psfflux=psfflux,
                                           psffluxerr=psffluxerr,
                                           ra=forcedra[bpv],
                                           dec=forceddec[bpv] )
                    frcobjs.append( frc )
                    rootdict['objs'][1]['frc'][bpv].append( frc )

                    if sourcemjd < 60050.:
                        srcex = DiaSourceExtra( diasourceid=src.diasourceid,
                                                base_procver_id=src.base_procver_id,
                                                detector=0,
                                                x=1,
                                                y=1 )
                        srcexobjs.append( srcex )
                        rootdict['objs'][1]['srcex'][bpv].append( srcex )
                        frcex = DiaForcedSourceExtra( diaobjectid=obj.diaobjectid,
                                                      visit=visit,
                                                      base_procver_id=frc.base_procver_id,
                                                      detector=0,
                                                      scienceflux=1.1*psfflux,
                                                      sciencefluxerr=psffluxerr,
                                                      timeprocessedmjdtai=60365.,
                                                      timewithdrawnmjdtai=None )
                        frcexobjs.append( frcex )
                        rootdict['objs'][1]['frcex'][bpv].append( frcex )
                    else:
                        rootdict['objs'][1]['srcex'][bpv].append( None )
                        rootdict['objs'][1]['frcex'][bpv].append( None )

                    if dopos:
                        pos = DiaObjectPosition( diaobjectid=obj.diaobjectid,
                                                 base_procver_id=bpvs[f'{bpv}_diaobject_position_{postime}'].id,
                                                 ra=ratot[bpv] / npos,
                                                 dec=dectot[bpv] / npos,
                                                 raerr=0.2 / np.sqrt(npos),
                                                 decerr=0.2 / np.sqrt(npos),
                                                 ra_dec_cov=0.05 **2 / npos )
                        posobjs.append( pos )
                        rootdict['objs'][1]['pos'][bpv].append( pos )

                # first object is in bpv1 and bpv1a
                if i == 0:
                    for bpv in [ 'bpv1', 'bpv1a' ]:
                        if ( ( ( bpv == 'bpv1a' ) and ( sourcemjd > 60015 ) ) or
                             ( ( bpv == 'bpv1' ) and ( sourcemjd > 60030 ) ) ):
                            continue
                        src = DiaSource( diasourceid=obj1.diaobjectid * 100000 + int(sourcemjd),
                                         base_procver_id=bpvs[f'{bpv}_diasource'].id,
                                         diaobjectid=obj1.diaobjectid,
                                         visit=visit,
                                         band=('r' if visit%2==0 else 'i'),
                                         midpointmjdtai=sourcemjd,
                                         psfflux=psfflux,
                                         psffluxerr=psffluxerr,
                                         ra=ra[bpv],
                                         dec=dec[bpv] )
                        srcobjs.append( src )
                        rootdict['objs'][2]['src'][bpv].append( src )

                        if sourcemjd < 60050.:
                            srcex = DiaSourceExtra( diasourceid=src.diasourceid,
                                                    base_procver_id=src.base_procver_id,
                                                    detector=0,
                                                    x=1,
                                                    y=1 )
                            srcexobjs.append( srcex )
                            rootdict['objs'][2]['srcex'][bpv].append( srcex )
                        else:
                            rootdict['objs'][2]['srcex'][bpv].append( None )

                        if ( ( ( bpv == 'bpv1a' ) and ( sourcemjd > 60010 ) ) or
                             ( ( bpv == 'bpv1' ) and ( sourcemjd > 60025 ) ) ):
                            continue
                        frc = DiaForcedSource( diaforcedsourceid=obj1.diaobjectid * 100000 + int(sourcemjd),
                                               base_procver_id=bpvs[f'{bpv}_diaforcedsource'].id,
                                               diaobjectid=obj1.diaobjectid,
                                               visit=visit,
                                               band=('r' if visit%2==0 else 'i'),
                                               midpointmjdtai=sourcemjd,
                                               psfflux=psfflux,
                                               psffluxerr=psffluxerr,
                                               ra=rootobj['dec'],
                                               dec=rootobj['dec'] )
                        frcobjs.append( frc )
                        rootdict['objs'][2]['frc'][bpv].append( frc )

                        if sourcemjd < 60050.:
                            frcex = DiaForcedSourceExtra( diaobjectid=frc.diaobjectid,
                                                          visit=visit,
                                                          base_procver_id=frc.base_procver_id,
                                                          detector=0,
                                                          scienceflux=1.1*psfflux,
                                                          sciencefluxerr=psffluxerr,
                                                          timeprocessedmjdtai=60365.,
                                                          timewithdrawnmjdtai=None )
                            frcexobjs.append( frcex )
                            rootdict['objs'][2]['frcex'][bpv].append( frcex )
                        else:
                            rootdict['objs'][2]['frcex'][bpv].append( None )

                        if dopos:
                            pos = DiaObjectPosition( diaobjectid=obj1.diaobjectid,
                                                     base_procver_id=bpvs[f'{bpv}_diaobject_position_{postime}'].id,
                                                     ra=ratot[bpv] / npos,
                                                     dec=dectot[bpv] / npos,
                                                     raerr=0.2 / np.sqrt(npos),
                                                     decerr=0.2 / np.sqrt(npos),
                                                     ra_dec_cov=0.05 **2 / npos )
                            posobjs.append( pos )
                            rootdict['objs'][2]['pos'][bpv].append( pos )


            # Nondetections
            mjds1 = np.arange( detrange[i][0] - 10., detrange[i][0], 2.5 )
            mjds2 = np.arange( detrange[i][1] + 2.5, detrange[i][1] + 21., 2.5 )
            mjds = np.concatenate( ( mjds1, mjds2 ) )
            for sourcemjd in mjds:
                visit += 1
                if sourcemjd < tpeak[i]:
                    mag = firstmag[i] + ( detrange[i][0] - sourcemjd ) * ( zeromag - firstmag[i] ) / 10.
                else:
                    mag = lastmag[i] + ( sourcemjd - detrange[i][0] ) * ( zeromag - lastmag[i] ) / 20.
                    psfflux = flux( mag )
                    psffluxerr = 0.5 * psfflux

                if ( i < 3 ) and ( sourcemjd <= 60055. ):
                    frc = DiaForcedSource( diaforcedsourceid=objr.diaobjectid * 100000 + int(sourcemjd),
                                           base_procver_id=bpvs['realtime'].id,
                                           diaobjectid=objr.diaobjectid,
                                           visit=visit,
                                           band=('r' if visit%2==0 else 'i'),
                                           midpointmjdtai=sourcemjd,
                                           psfflux=psfflux,
                                           psffluxerr=psffluxerr,
                                           ra=forcedra['realtime'],
                                           dec=forceddec['realtime'] )
                    frcobjs.append( frc )
                    rootdict['objs'][0]['frc']['realtime'].append( frc )

                    if sourcemjd < 60050.:
                        frcex = DiaForcedSourceExtra( diaobjectid=frc.diaobjectid,
                                                      visit=visit,
                                                      base_procver_id=frc.base_procver_id,
                                                      detector=0,
                                                      scienceflux=1.1*psfflux,
                                                      sciencefluxerr=psffluxerr,
                                                      timeprocessedmjdtai=60365.,
                                                      timewithdrawnmjdtai=None )
                        frcexobjs.append( frcex )
                        rootdict['objs'][0]['frcex']['realtime'].append( frcex )

                for bpv in [ 'bpv2', 'bpv2a', 'bpv3' ]:
                    frc = DiaForcedSource( diaforcedsourceid=obj.diaobjectid * 100000 + int(sourcemjd),
                                           base_procver_id=bpvs[bpv].id,
                                           diaobjectid=obj.diaobjectid,
                                           visit=visit,
                                           band=('r' if visit%2==0 else 'i'),
                                           midpointmjdtai=sourcemjd,
                                           psfflux=psfflux,
                                           psffluxerr=psffluxerr,
                                           ra=forcedra[bpv],
                                           dec=forceddec[bpv] )
                    frcobjs.append( frc )
                    rootdict['objs'][1]['frc'][bpv].append( frc )

                    if sourcemjd < 60050.:
                        frcex = DiaForcedSourceExtra( diaobjectid=frc.diaobjectid,
                                                      visit=visit,
                                                      base_procver_id=frc.base_procver_id,
                                                      detector=0,
                                                      scienceflux=1.1*psfflux,
                                                      sciencefluxerr=psffluxerr,
                                                      timeprocessedmjdtai=60365.,
                                                      timewithdrawnmjdtai=None )
                        frcexobjs.append( frcex )
                        rootdict['objs'][1]['frcex'][bpv].append( frcex )

                if i == 0:
                    for bpv in [ 'bpv1', 'bpv1a' ]:
                        if ( ( ( bpv == 'bpv1a' ) and ( sourcemjd > 60015. ) ) or
                             ( ( bpv == 'bpv1' ) and ( sourcemjd > 60025. ) ) ):
                            continue
                        frc = DiaForcedSource( diaforcedsourceid=obj.diaobjectid * 100000 + int(sourcemjd),
                                               base_procver_id=bpvs[bpv].id,
                                               diaobjectid=obj1.diaobjectid,
                                               visit=visit,
                                               band=('r' if visit%2==0 else 'i'),
                                               midpointmjdtai=sourcemjd,
                                               psfflux=psfflux,
                                               psffluxerr=psffluxerr,
                                               ra=forcedra[bpv],
                                               dec=forceddec[bpv] )
                        frcobjs.append( frc )
                        rootdict['objs'][2]['frc'][bpv].append( frc )

                        if sourcemjd < 60050.:
                            frcex = DiaForcedSourceExtra( diaobjectid=frc.diaobjectid,
                                                          visit=visit,
                                                          base_procver_id=frc.base_procver_id,
                                                          detector=0,
                                                          scienceflux=1.1*psfflux,
                                                          sciencefluxerr=psffluxerr,
                                                          timeprocessedmjdtai=60365.,
                                                          timewithdrawnmjdtai=None )
                            frcexobjs.append( frcex )
                            rootdict['objs'][2]['frcex'][bpv].append( frcex )

            # OMG HOST GALAXIES... TODO

        with DBCon() as con:
            RootDiaObject.bulk_insert_or_upsert( rootobjs, dbcon=con )
            DiaObject.bulk_insert_or_upsert( objobjs, dbcon=con )
            DiaObjectPosition.bulk_insert_or_upsert( posobjs, dbcon=con )
            DiaSource.bulk_insert_or_upsert( srcobjs, dbcon=con )
            DiaSourceExtra.bulk_insert_or_upsert( srcexobjs, dbcon=con )
            DiaForcedSource.bulk_insert_or_upsert( frcobjs, dbcon=con )
            DiaForcedSourceExtra.bulk_insert_or_upsert( frcexobjs, dbcon=con )

        # For usage convenience, sort all the sources and forced sources by mjd
        # TODO : need to sort srcex and frcex to go with this!!!!
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
            frcexobjs.extend( frcobjs )
            frcexobjs.extend( srcexobjs )
            frcexobjs.extend( srcobjs )
            frcexobjs.extend( posobjs )
            frcexobjs.extend( objobjs )
            frcexobjs.extend( rootobjs )
            for obj in frcexobjs:
                obj.delete_from_db( dbcon=con, nocommit=True )
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
                              "privkey": "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"} # noqa: E501
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
        loader = FITSLoader( nprocs=5, directories=dirs, verbose=True, ppdb=True, really_do=True )
        loader()

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
        loader = FITSLoader( nprocs=5, directories=dirs, verbose=True, really_do=True,
                             processing_version=procver_collection[1]['pv1'].description )
        loader()


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
