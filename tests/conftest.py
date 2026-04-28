import os
import pytest
import pathlib
import uuid
import itertools

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
import admin.load_snana_fits_ppdb
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
    #   A processing version alias default pointing at pvc_pv2
    #
    # (The fallbacks of pvc_pv2 and pvc_pv3 to the earlier bpvs are for Object handing in set_of_lightcurves)
    #
    # fixture value is a tuple of two dictionaries of str → BaseProcessingVersion, str → ProcessingVersion
    #
    # Keys for the processing versions are pv1, pv2, pv3, and realtime
    # Keys for the base processing versions are {name} or {name}_{table},
    #   where {name} is one of bpv1, bpv1a, bpv1b, bpv2, bpv2a, bpv3, realtime
    #   and {table} is one of diaobject, diasource, diaforcedsource
    # Base processing versions also have {name}_diaoject_position_{mjd} for diaobject_position

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

            tables = [ 'diaobject', 'diaobject_position', 'diasource', 'diaforcedsource', 'host_galaxy' ]
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
                asUUID('1959c4ea-11f5-4a94-bc35-cdad9e222641'),
                asUUID('798591a2-548f-4733-bd7a-db9e688b13f7'),
                asUUID('0d34e1b5-ba4a-42ab-8946-947cf6171211'),
                asUUID('cb53beac-bbd9-40e9-a374-1f7895c43603'),
                asUUID('2cfdce23-448f-4e3c-92b5-56c8b285d25a'),
                asUUID('2210759c-996f-4519-9985-e160516bd88d'),
                asUUID('2d66e355-c2eb-40e0-b738-460e2e399e8b'),
                asUUID('0ff513de-a421-4022-9a9e-c57de08373cf'),
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
                            desc = f'{base}_{postime}' if base == 'realtime' else f'pvc_{base}_{postime}'
                            bpvs[key] = BaseProcessingVersion( id=uuidbatch[uuiddex], _table=table, description=desc )
                    else:
                        uuiddex += 1
                        key = f'{base}_{table}'
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

            pvinfo = []

            for pv, bpvae in zip( [ 'pv1', 'pv2', 'pv3', 'realtime' ],
                                  [ [ 'bpv1', 'bpv1a', 'bpv1b' ],
                                    [ 'bpv2', 'bpv2a' ],
                                    [ 'bpv3' ],
                                    [ 'realtime' ] ] ):
                thisinfo = { 'procver': pvs[pv] }
                for prio, bpv in enumerate( bpvae ):
                    for table in tables:
                        if table not in thisinfo:
                            thisinfo[table] = []
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
                                thisinfo[table].append( ( bpvs[bpvkey], subprio, bpvkey ) )
                        else:
                            bpvkey = f'{bpv}_{table}'
                            con.execute_nofetch( "INSERT INTO base_procver_of_procver(procver_id,base_procver_id,"
                                                 "                                    _table,priority) "
                                                 "VALUES (%(pv)s,%(bpv)s,%(tab)s,%(prio)s)",
                                                 { 'pv': pvs[pv].id, 'bpv': bpvs[bpvkey].id,
                                                   'tab': table, 'prio': prio } )
                            # This is becoming an increasingly ugly hack.  This is what happens
                            #  when you really need to get things done but should really
                            #  be refactoring previous mistakes.
                            thisinfo[table].append( ( bpvs[bpvkey], prio, bpvkey ) )

                # Reverse all the table lists so they go from high prio to low prio
                for table in thisinfo.keys():
                    if table == 'procver':
                        continue
                    thisinfo[table].reverse()

                pvinfo.append( thisinfo )


            con.execute_nofetch( "INSERT INTO processing_version_alias(description,procver_id) "
                                 "VALUES ('default',%(pvid)s)", { 'pvid': pvs['pv2'].id } )

            con.commit()

        yield bpvs, pvs, pvinfo

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
    # Objects 0 through 2 are in realtime as [ 0, 1, 2 ]
    # Object 0 is in bpv1 as 100
    # All objects are in bpv2 as [ 200, 201, 202, 203 ]
    #   BUT object 1 is also in bpv2 as 2011
    #
    # Positions are stored at procver_postimes *if* the object still has a detection by the time
    #   (diaobjects 203 and 2011 have no positions stored)
    #
    # Objects have lightcurves:
    #     object 0 : first detection mjd 60000, last detection mjd 60030, peak 60010, mag 24
    #     object 1 : first detection mjd 60020, last detection mjd 60060, peak 60035, mag 22
    #     object 2 : first detection mjd 60040, last detection mjd 60080, peak 60050, mag 23
    #     object 3 : first detection mjd 60050, last detection mjd 60060, peak 60055, mag 23.5
    #
    # All lightcurves have a candence of 2.5 days.  (I'm counting on
    #   floats being perfectly representable in these ranges at
    #   increments of 2.5, which I believe is true.)
    #
    # Full lightcurves are in bpv2 and bpv3
    # Times [ 60020, 60030 ] have forcedsources in bpv2a
    # Times [ 60020, 60025 ] have sources in bpv2a
    # FOR OBJECT 1 IN bpv2 ONLY:
    #    if mjd = floor(mjd), diasource and diaforcedsource are associated with object 201
    #       otherwise diasource and diaforcedsource are associated with object 2011
    #
    # Photometry through 60060 and forced through 60055 is in realtime for objects 0 through 2
    #
    # Object 0:
    #    photometry through 60015 and forced through 60010 in bpv1a
    #    photometry through 60030 and forced through 60025 in bpv1
    #
    # RETURN STRUCTURE:
    # [
    #    { 'root':  RootDiaObject,
    #      'obj':   { diaobjectid: DiaObject for all relevant object base processing versions },
    #      'pos':   { (diaobjectid, bpv): DiaObjectPosition for all relevant position base processing versions },
    #      'src':   { bpv: [ list of DiaSource ] },
    #      'srcex': { bpv: [ list of DiaSourceExtra ] },
    #      'frc':   { bpv: [ list of DiaForcedSource ] },
    #      'frcex'  { bpv: [ list of DiaForcedSourceExtra ] }
    #    }
    # ]
    #
    # Ordered for object 0, 1, 2, 3
    #
    # all bpv keys are keys into the bpvs dictionary of the procver_collection fixture

    roots = []
    rootobjs = []
    objobjs = []
    posobjs = []
    srcobjs = []
    srcexobjs = []
    frcobjs = []
    frcexobjs = []
    bpvs, _pvs, _pvinfo = procver_collection

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

        peakmag = [ 24., 22., 23., 23.5 ]
        firstmag = [ 26., 25., 25.5, 25.8 ]
        lastmag = [ 25.9, 25.1, 25.6, 26. ]
        zeromag = 32.

        visit = 0
        for i, rootobj in enumerate(rootobjinfo):
            robj = RootDiaObject( id=uuid.uuid4(), ra=rootobjinfo[i]['ra'], dec=rootobjinfo[i]['dec'] )
            rootobjs.append( robj )
            rootra = rootobjinfo[i]['ra']
            rootdec = rootobjinfo[i]['dec']

            # In rootdict, objs[0] is in realtime, objs[1] is in bpv2, and objs[2] is in bpv1
            rootdict = { 'root': robj,
                         'obj': {},
                         'pos': {},
                         'src': {},
                         'srcex': {},
                         'frc': {},
                         'frcex': {}
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
                objr = DiaObject( diaobjectid=i, rootid=robj.id, base_procver_id=bpvs['realtime_diaobject'].id )
                objobjs.append( objr )
                rootdict['obj'][i] = objr

            obj = DiaObject( diaobjectid=200+i, rootid=robj.id, base_procver_id=bpvs['bpv2_diaobject'].id )
            objobjs.append( obj )
            rootdict['obj'][200+i] = obj

            if i == 1:
                obj2011 = DiaObject( diaobjectid=2011, rootid=robj.id, base_procver_id=bpvs['bpv2_diaobject'].id )
                objobjs.append( obj2011 )
                rootdict['obj'][2011] = obj2011

            if i == 0:
                obj1 = DiaObject( diaobjectid=100, rootid=robj.id, base_procver_id=bpvs['bpv1_diaobject'].id )
                objobjs.append( obj1 )
                rootdict['obj'][100] = obj1

            # build the rest of the rootdict dicts
            if objr is not None:
                rootdict['src']['realtime_diasource'] = []
                rootdict['srcex']['realtime_diasource'] = []
                rootdict['frc']['realtime_diaforcedsource'] = []
                rootdict['frcex']['realtime_diaforcedsource'] = []
            for bpv in [ 'bpv2', 'bpv2a', 'bpv3' ]:
                rootdict['src'][f'{bpv}_diasource'] = []
                rootdict['srcex'][f'{bpv}_diasource'] = []
                rootdict['frc'][f'{bpv}_diaforcedsource'] = []
                rootdict['frcex'][f'{bpv}_diaforcedsource'] = []
            if obj1 is not None:
                for bpv in [ 'bpv1', 'bpv1a' ]:
                    rootdict['src'][f'{bpv}_diasource'] = []
                    rootdict['srcex'][f'{bpv}_diasource'] = []
                    rootdict['frc'][f'{bpv}_diaforcedsource'] = []
                    rootdict['frcex'][f'{bpv}_diaforcedsource'] = []

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
                # We have objects in magnitude range 22-25
                # nJy flux (zp 31.4) at mag 26 is 145
                # nJy flux (zp 31.4) at mag 22 is 5754
                # Let's say sky noise is mag 26 over the psf area, just for the hell of it
                # Let's say the gain is 1, just for the hell of it.
                psffluxerr = np.sqrt( 145.**2 + max(psfflux, 0) )

                # realtime sources
                if ( i < 3 ) and ( sourcemjd <= 60060. ):
                    src = DiaSource( diasourceid=objr.diaobjectid * 1000000 + int(sourcemjd),
                                     base_procver_id=bpvs['realtime_diasource'].id,
                                     diaobjectid=objr.diaobjectid,
                                     visit=visit,
                                     band=( 'r' if visit%2==0 else 'i' ),
                                     midpointmjdtai=sourcemjd,
                                     psfflux=psfflux, psffluxerr=psffluxerr,
                                     ra=ra['realtime'], dec=dec['realtime'] )
                    srcobjs.append( src )
                    rootdict['src']['realtime_diasource'].append( src )

                    if sourcemjd < 60050.:
                        srcex = DiaSourceExtra( diasourceid=src.diasourceid,
                                                base_procver_id=src.base_procver_id,
                                                detector=0,
                                                x=1,
                                                y=1 )
                        srcexobjs.append( srcex )
                        rootdict['srcex']['realtime_diasource'].append( srcex )
                    else:
                        rootdict['srcex']['realtime_diasource'].append( None )

                    if dopos and ( i < 3 ):
                        pos = DiaObjectPosition( diaobjectid=objr.diaobjectid,
                                                 base_procver_id=bpvs[f'realtime_diaobject_position_{postime}'].id,
                                                 ra=ratot['realtime'] / npos,
                                                 dec=dectot['realtime'] / npos,
                                                 raerr=0.2 / np.sqrt(npos),
                                                 decerr=0.2 / np.sqrt(npos),
                                                 ra_dec_cov=0.05 ** 2 / npos )
                        posobjs.append( pos )
                        rootdict['pos'][(objr.diaobjectid, f'realtime_diaobject_position_{postime}')] = pos

                # realtime forced sources
                if ( i < 3 ) and ( sourcemjd <= 60055. ):
                    frc = DiaForcedSource( diaforcedsourceid=objr.diaobjectid * 1000000 + int(sourcemjd),
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
                    rootdict['frc']['realtime_diaforcedsource'].append( frc )

                    if sourcemjd < 60050.:
                        frcex = DiaForcedSourceExtra( diaforcedsourceid=objr.diaobjectid * 1000000 + int(sourcemjd),
                                                      base_procver_id=frc.base_procver_id,
                                                      detector=0,
                                                      scienceflux=1.1*psfflux,
                                                      sciencefluxerr=psffluxerr,
                                                      timeprocessedmjdtai=60365.,
                                                      timewithdrawnmjdtai=None )
                        frcexobjs.append( frcex )
                        rootdict['frcex']['realtime_diaforcedsource'].append( frcex )
                    else:
                        rootdict['frcex']['realtime_diaforcedsource'].append( None )

                # everything is in bpv2, bpv2a, bp3
                for bpv in [ 'bpv2', 'bpv2a', 'bpv3' ]:
                    # bpv2a only has sources for [ 60020, 60030 ] and forced sources for [ 60020, 60025 ]
                    if ( bpv == 'bpv2a' ) and ( ( sourcemjd < 60020. ) or ( sourcemjd > 60030. ) ):
                        continue

                    if ( i == 1 ) and ( bpv == 'bpv2' ):
                        # Special case handling for object1 in bpv2, because there are
                        #   two different diaobjects associated with the root object
                        objtouse = obj if sourcemjd == np.floor(sourcemjd) else obj2011
                    else:
                        objtouse = obj

                    src = DiaSource( diasourceid=objtouse.diaobjectid * 1000000 + int(sourcemjd),
                                     base_procver_id=bpvs[f'{bpv}_diasource'].id,
                                     diaobjectid=objtouse.diaobjectid,
                                     visit=visit,
                                     band=('r' if visit%2==0 else 'i'),
                                     midpointmjdtai=sourcemjd,
                                     psfflux=psfflux,
                                     psffluxerr=psffluxerr,
                                     ra=ra[bpv],
                                     dec=dec[bpv] )
                    srcobjs.append( src )
                    rootdict['src'][f'{bpv}_diasource'].append( src )

                    if sourcemjd < 60050.:
                        srcex = DiaSourceExtra( diasourceid=src.diasourceid,
                                                base_procver_id=src.base_procver_id,
                                                detector=0,
                                                x=1,
                                                y=1 )
                        srcexobjs.append( srcex )
                        rootdict['srcex'][f'{bpv}_diasource'].append( srcex )
                    else:
                        rootdict['srcex'][f'{bpv}_diasource'].append( srcex )

                    if ( bpv != 'bpv2a' ) or ( sourcemjd <= 60025 ):
                        frc = DiaForcedSource( diaforcedsourceid=objtouse.diaobjectid * 1000000 + int(sourcemjd),
                                               base_procver_id=bpvs[f'{bpv}_diaforcedsource'].id,
                                               diaobjectid=objtouse.diaobjectid,
                                               visit=visit,
                                               band=('r' if visit%2==0 else 'i'),
                                               midpointmjdtai=sourcemjd,
                                               psfflux=psfflux,
                                               psffluxerr=psffluxerr,
                                               ra=forcedra[bpv],
                                               dec=forceddec[bpv] )
                        frcobjs.append( frc )
                        rootdict['frc'][f'{bpv}_diaforcedsource'].append( frc )

                        if sourcemjd < 60050.:
                            frcex = DiaForcedSourceExtra( diaforcedsourceid=frc.diaforcedsourceid,
                                                          base_procver_id=frc.base_procver_id,
                                                          detector=0,
                                                          scienceflux=1.1*psfflux,
                                                          sciencefluxerr=psffluxerr,
                                                          timeprocessedmjdtai=60365.,
                                                          timewithdrawnmjdtai=None )
                            frcexobjs.append( frcex )
                            rootdict['frcex'][f'{bpv}_diaforcedsource'].append( frcex )
                        else:
                            rootdict['frcex'][f'{bpv}_diaforcedsource'].append( None )

                    if dopos and ( i < 3 ):
                        pos = DiaObjectPosition( diaobjectid=obj.diaobjectid,
                                                 base_procver_id=bpvs[f'{bpv}_diaobject_position_{postime}'].id,
                                                 ra=ratot[bpv] / npos,
                                                 dec=dectot[bpv] / npos,
                                                 raerr=0.2 / np.sqrt(npos),
                                                 decerr=0.2 / np.sqrt(npos),
                                                 ra_dec_cov=0.05 **2 / npos )
                        posobjs.append( pos )
                        rootdict['pos'][(obj.diaobjectid, f'{bpv}_diaobject_position_{postime}')] = pos

                # first object is in bpv1 and bpv1a
                if i == 0:
                    for bpv in [ 'bpv1', 'bpv1a' ]:
                        if ( ( ( bpv == 'bpv1a' ) and ( sourcemjd > 60015 ) ) or
                             ( ( bpv == 'bpv1' ) and ( sourcemjd > 60030 ) ) ):
                            continue
                        src = DiaSource( diasourceid=obj1.diaobjectid * 1000000 + int(sourcemjd),
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
                        rootdict['src'][f'{bpv}_diasource'].append( src )

                        if sourcemjd < 60050.:
                            srcex = DiaSourceExtra( diasourceid=src.diasourceid,
                                                    base_procver_id=src.base_procver_id,
                                                    detector=0,
                                                    x=1,
                                                    y=1 )
                            srcexobjs.append( srcex )
                            rootdict['srcex'][f'{bpv}_diasource'].append( srcex )
                        else:
                            rootdict['srcex'][f'{bpv}_diasource'].append( None )

                        if ( ( ( bpv == 'bpv1a' ) and ( sourcemjd > 60010 ) ) or
                             ( ( bpv == 'bpv1' ) and ( sourcemjd > 60025 ) ) ):
                            continue
                        frc = DiaForcedSource( diaforcedsourceid=obj1.diaobjectid * 1000000 + int(sourcemjd),
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
                        rootdict['frc'][f'{bpv}_diaforcedsource'].append( frc )

                        if sourcemjd < 60050.:
                            frcex = DiaForcedSourceExtra( diaforcedsourceid=obj1.diaobjectid * 1000000 + int(sourcemjd),
                                                          base_procver_id=frc.base_procver_id,
                                                          detector=0,
                                                          scienceflux=1.1*psfflux,
                                                          sciencefluxerr=psffluxerr,
                                                          timeprocessedmjdtai=60365.,
                                                          timewithdrawnmjdtai=None )
                            frcexobjs.append( frcex )
                            rootdict['frcex'][f'{bpv}_diaforcedsource'].append( frcex )
                        else:
                            rootdict['frcex'][f'{bpv}_diaforcedsource'].append( None )

                        if dopos and ( i < 3 ):
                            pos = DiaObjectPosition( diaobjectid=obj1.diaobjectid,
                                                     base_procver_id=bpvs[f'{bpv}_diaobject_position_{postime}'].id,
                                                     ra=ratot[bpv] / npos,
                                                     dec=dectot[bpv] / npos,
                                                     raerr=0.2 / np.sqrt(npos),
                                                     decerr=0.2 / np.sqrt(npos),
                                                     ra_dec_cov=0.05 **2 / npos )
                            posobjs.append( pos )
                            rootdict['pos'][(obj1.diaobjectid, f'{bpv}_diaobject_position_{postime}')] = pos


            # Nondetections
            mjds1 = np.arange( detrange[i][0] - 10., detrange[i][0], 2.5 )
            mjds2 = np.arange( detrange[i][1] + 2.5, detrange[i][1] + 21., 2.5 )
            mjds = np.concatenate( ( mjds1, mjds2 ) )
            for sourcemjd in mjds:
                visit += 1
                if sourcemjd < tpeak[i]:
                    mag = firstmag[i] + ( detrange[i][0] - sourcemjd ) * ( zeromag - firstmag[i] ) / 10.
                    mag = min( mag, zeromag )
                else:
                    mag = lastmag[i] + ( sourcemjd - detrange[i][0] ) * ( zeromag - lastmag[i] ) / 20.
                    mag = min( mag, zeromag )
                psfflux = flux( mag )
                psffluxerr = 0.5 * psfflux

                if ( i < 3 ) and ( sourcemjd <= 60055. ):
                    frc = DiaForcedSource( diaforcedsourceid=objr.diaobjectid * 1000000 + int(sourcemjd),
                                           base_procver_id=bpvs['realtime_diaforcedsource'].id,
                                           diaobjectid=objr.diaobjectid,
                                           visit=visit,
                                           band=('r' if visit%2==0 else 'i'),
                                           midpointmjdtai=sourcemjd,
                                           psfflux=psfflux,
                                           psffluxerr=psffluxerr,
                                           ra=forcedra['realtime'],
                                           dec=forceddec['realtime'] )
                    frcobjs.append( frc )
                    rootdict['frc']['realtime_diaforcedsource'].append( frc )

                    if sourcemjd < 60050.:
                        frcex = DiaForcedSourceExtra( diaforcedsourceid=objr.diaobjectid * 1000000 + int(sourcemjd),
                                                      base_procver_id=frc.base_procver_id,
                                                      detector=0,
                                                      scienceflux=1.1*psfflux,
                                                      sciencefluxerr=psffluxerr,
                                                      timeprocessedmjdtai=60365.,
                                                      timewithdrawnmjdtai=None )
                        frcexobjs.append( frcex )
                        rootdict['frcex']['realtime_diaforcedsource'].append( frcex )
                    else:
                        rootdict['frcex']['realtime_diaforcedsource'].append( None )

                for bpv in [ 'bpv2', 'bpv2a', 'bpv3' ]:
                    # bpv2a only has sources for [ 60020, 60030 ] and forced sources for [ 60020, 60025 ]
                    if ( bpv == 'bpv2a' ) and ( ( sourcemjd < 60020. ) or ( sourcemjd > 60025. ) ):
                        continue

                    if ( i == 1 ) and ( bpv == 'bpv2' ):
                        # Special case handling for object1 in bpv2, because there are
                        #   two different diaobjects associated with the root object
                        objtouse = obj if sourcemjd == np.floor(sourcemjd) else obj2011
                    else:
                        objtouse = obj

                    frc = DiaForcedSource( diaforcedsourceid=objtouse.diaobjectid * 1000000 + int(sourcemjd),
                                           base_procver_id=bpvs[f'{bpv}_diaforcedsource'].id,
                                           diaobjectid=objtouse.diaobjectid,
                                           visit=visit,
                                           band=('r' if visit%2==0 else 'i'),
                                           midpointmjdtai=sourcemjd,
                                           psfflux=psfflux,
                                           psffluxerr=psffluxerr,
                                           ra=forcedra[bpv],
                                           dec=forceddec[bpv] )
                    frcobjs.append( frc )
                    rootdict['frc'][f'{bpv}_diaforcedsource'].append( frc )

                    if sourcemjd < 60050.:
                        frcex = DiaForcedSourceExtra( diaforcedsourceid=objtouse.diaobjectid * 1000000 + int(sourcemjd),
                                                      base_procver_id=frc.base_procver_id,
                                                      detector=0,
                                                      scienceflux=1.1*psfflux,
                                                      sciencefluxerr=psffluxerr,
                                                      timeprocessedmjdtai=60365.,
                                                      timewithdrawnmjdtai=None )
                        frcexobjs.append( frcex )
                        rootdict['frcex'][f'{bpv}_diaforcedsource'].append( frcex )
                    else:
                        rootdict['frcex'][f'{bpv}_diaforcedsource'].append( None )

                if i == 0:
                    for bpv in [ 'bpv1', 'bpv1a' ]:
                        if ( ( ( bpv == 'bpv1a' ) and ( sourcemjd > 60015. ) ) or
                             ( ( bpv == 'bpv1' ) and ( sourcemjd > 60025. ) ) ):
                            continue
                        frc = DiaForcedSource( diaforcedsourceid=obj.diaobjectid * 1000000 + int(sourcemjd),
                                               base_procver_id=bpvs[f'{bpv}_diaforcedsource'].id,
                                               diaobjectid=obj1.diaobjectid,
                                               visit=visit,
                                               band=('r' if visit%2==0 else 'i'),
                                               midpointmjdtai=sourcemjd,
                                               psfflux=psfflux,
                                               psffluxerr=psffluxerr,
                                               ra=forcedra[bpv],
                                               dec=forceddec[bpv] )
                        frcobjs.append( frc )
                        rootdict['frc'][f'{bpv}_diaforcedsource'].append( frc )

                        if sourcemjd < 60050.:
                            frcex = DiaForcedSourceExtra( diaforcedsourceid=obj.diaobjectid * 1000000 + int(sourcemjd),
                                                          base_procver_id=frc.base_procver_id,
                                                          detector=0,
                                                          scienceflux=1.1*psfflux,
                                                          sciencefluxerr=psffluxerr,
                                                          timeprocessedmjdtai=60365.,
                                                          timewithdrawnmjdtai=None )
                            frcexobjs.append( frcex )
                            rootdict['frcex'][f'{bpv}_diaforcedsource'].append( frcex )
                        else:
                            rootdict['frcex'][f'{bpv}_diaforcedsource'].append( None )

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
        for rootdict in roots:
            for bpv in rootdict['src'].keys():
                assert len( rootdict['src'][bpv] ) == len( rootdict['srcex'][bpv] )
                srces = rootdict['src'][bpv]
                srcexes = rootdict['srcex'][bpv]
                dexen = list( range( len(srces) ) )
                dexen.sort( key=lambda x: srces[x].midpointmjdtai )
                rootdict['src'][bpv] = [ srces[i] for i in dexen ]
                rootdict['srcex'][bpv] = [ srcexes[i] for i in dexen ]

            for bpv in rootdict['frc'].keys():
                assert len( rootdict['frc'][bpv] ) == len( rootdict['frcex'][bpv] )
                frces = rootdict['frc'][bpv]
                frcexes = rootdict['frcex'][bpv]
                dexen = list( range( len(frces) ) )
                dexen.sort( key=lambda x: frces[x].midpointmjdtai )
                rootdict['frc'][bpv] = [ frces[i] for i in dexen ]
                rootdict['frcex'][bpv] = [ frcexes[i] for i in dexen ]

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
        loader = admin.load_snana_fits_ppdb.FITSLoader( nprocs=5, directories=dirs, verbose=True, really_do=True )
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
            nhost = 0
            # cursor.execute( "SELECT COUNT(*) FROM host_galaxy" )
            # nhost = cursor.fetchone()[0]
            # assert nhost == 356
            cursor.execute( "SELECT COUNT(*) FROM root_diaobject" )
            assert cursor.fetchone()[0] == 346

        yield nobj, nsrc, nfrc, nhost

    finally:
        with DBCon() as conn:
            for tab in [ 'root_diaobject', 'host_galaxy', 'diaobject', 'diasource', 'diaforcedsource' ]:
                conn.execute( f"TRUNCATE TABLE {tab} CASCADE" )

            # The loader will also have created a host_galaxy processing version, which as of
            #   this writing is not in the procver_collection fixture so won't cleaned up.
            # WARNING -- if we have host galaxies again, then these deletions here may cause trouble!
            # (Maybe not, since both this and procver_collection are module scope fixtures.)

            conn.execute( "DELETE FROM base_procver_of_procver WHERE procver_id=%(pv)s "
                          "  AND _table IN ('host_galaxy', 'diaobject_host_match')",
                          { 'pv': procver_collection[1]['pv1'].id } )
            conn.execute( "DELETE FROM base_processing_version WHERE description=%(d)s "
                          "  AND _table in ('host_galaxy', 'diaobject_host_match')",
                          { "d": procver_collection[1]['pv1'].description } )

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


@pytest.fixture( scope='module' )
def lightcurve_checker( set_of_lightcurves, procver_collection ):

    def check_ltcv_res( procver, expected_roots, expected_diaobjectids, res, single=False,
                        mjd_now=None, bands=None, which='patch', return_object_info=False,
                        include_object_positions=False,
                        include_base_procver=False, include_source_ids=False, include_source_positions=False,
                        use_weighted_source_positions=False, always_use_weighted_source_positions=False,
                        expect_all_roots=True ):
        # This has a lot of redundancy with test_ltcv.py::compare_ltcv_to_expected
        bpvs, pvs, pvinfo = procver_collection
        roots = set_of_lightcurves
        assert roots is not None

        which = 'patch' if which is None else which
        pvrow = [ p for p in pvinfo if ( ( p['procver'].description == procver ) or
                                         ( procver in pvs and p['procver'].description == pvs[procver].description ) or
                                         ( isinstance(procver, ProcessingVersion) and
                                           procver.id==p['procver'].id ) ) ]
        assert len(pvrow) == 1
        pvrow = pvrow[0]

        expected_root_ids = [ roots[i]['root'].id for i in expected_roots ]
        use_weighted_source_positions = use_weighted_source_positions or always_use_weighted_source_positions

        # ...there are a few different tests that call this that get things wrapped different ways.
        if return_object_info:
            if isinstance( res, tuple ):
                if single:
                    assert isinstance( res[0], dict )
                    ltcvs = [ res[0] ]
                else:
                    assert isinstance( res[0], list )
                    ltcvs = res[0]
                infos = res[1]
            else:
                assert isinstance( res, dict )
                if single:
                    assert set( res.keys() ) == { 'ltcv', 'objinfo' }
                    assert isinstance( res['ltcv'], dict )
                    ltcvs = [ res['ltcv'] ]
                else:
                    assert set( res.keys() ) == { 'ltcvs', 'objinfo' }
                    assert isinstance( res['ltcvs'], list )
                    ltcvs = res['ltcvs']
                assert isinstance( res['objinfo'], dict )
                infos = res[ 'objinfo' ]
        else:
            if single:
                assert isinstance( res, dict )
                ltcvs = [ res ]
            else:
                assert isinstance( res, list )
                ltcvs = res
            infos = None

        if ( len(ltcvs) > 0 ) and isinstance( ltcvs[0]['rootid'], uuid.UUID ):
            rootid_is_uuid = True
            assert all( isinstance( lc['rootid'], uuid.UUID ) for lc in ltcvs )
        else:
            rootid_is_uuid = False
            assert all( isinstance( lc['rootid'], str ) for lc in ltcvs )

        expected_keys = [ 'rootid', 'mjd', 'diasourceid', 'source_diaobjectid',
                          'visit', 'band', 'flux', 'fluxerr', 'isdet' ]
        if which != 'detections':
            expected_keys.extend( [ 'diaforcedsourceid', 'forced_diaobjectid' ] )
        if which == 'patch':
            expected_keys.append( 'ispatch' )
        if include_base_procver:
            expected_keys.append( 'base_procver_s' )
            if which != 'detections':
                expected_keys.append( 'base_procver_f' )
        if include_source_positions:
            expected_keys.extend( [ 'det_ra', 'det_dec', 'det_raerr', 'det_decerr', 'det_ra_dec_cov' ] )

        if not single:
            if expect_all_roots:
                assert len( ltcvs ) == len( expected_roots )
                if rootid_is_uuid:
                    assert set( lc['rootid'] for lc in ltcvs ) == set( expected_root_ids )
                else:
                    assert set( lc['rootid'] for lc in ltcvs ) == set( str(r) for r in expected_root_ids )
            else:
                assert len( set( lc['rootid'] for lc in ltcvs ) ) == len( ltcvs )
                if rootid_is_uuid:
                    assert set( lc['rootid'] for lc in ltcvs ).issubset( expected_root_ids )
                else:
                    assert set( lc['rootid'] for lc in ltcvs ).issubset( set( str(r) for r in expected_root_ids ) )

        datacache = {}
        for rootid in expected_root_ids:
            rdex = None
            for i, root in enumerate( roots ):
                if root['root'].id == rootid:
                    rdex = i
                    break
            if rdex is None:
                raise ValueError( "Failed to find root object {rootid}" )

            if single:
                thisltcv = ltcvs[0]
                assert thisltcv['rootid'] == ( rootid if rootid_is_uuid else str(rootid) )
            elif len(ltcvs) == 0:
                # Logically, if we get here, expect_all_roots must be false
                continue

            dexen = [ i for i, lc in enumerate( ltcvs )
                      if lc['rootid'] == ( rootid if rootid_is_uuid else str(rootid) ) ]
            assert len(dexen) == 1
            thisltcv = ltcvs[ dexen[0] ]

            assert all( len(thisltcv[k]) == len(thisltcv['mjd']) for k in expected_keys
                        if k != 'rootid' )

            # OK, here's the deal.  For each rootid, there are multiple base processing versions that have
            #   data.  We need to extract the highest priority for each.  To do this, rewrangle the
            #   set_of_lightcurves data so that its indexed by base_procver, in descending priority order.

            if mjd_now is not None:
                if bands is not None:
                    conds = lambda s: ( s.midpointmjdtai <= mjd_now ) and ( s.band in bands )
                else:
                    conds = lambda s: s.midpointmjdtai <= mjd_now
            elif bands is not None:
                conds = lambda s: s.band in bands
            else:
                conds = lambda s: True

            src_bpvkeys = [ p[2] for p in pvrow['diasource'] ]
            frc_bpvkeys = [ p[2] for p in pvrow['diaforcedsource'] ]
            reindexed_srces = { k: { s.visit: s for s in roots[rdex]['src'][k] if conds(s) }
                                for k in src_bpvkeys if k in roots[rdex]['src'].keys() }
            reindexed_frced = { k: { f.visit: f for f in roots[rdex]['frc'][k] if conds(f) }
                                for k in frc_bpvkeys if k in roots[rdex]['frc'].keys() }

            # Visit is the thing that lets us decide if a diasource and a diaforcedsource are the same thing.
            # Get the set of visits defined for this object.
            allvisits = set( itertools.chain.from_iterable( list(x.keys()) for x in reindexed_srces.values() ) )
            allvisits = allvisits.union( set( itertools.chain
                                              .from_iterable( list(x.keys())
                                                              for x in reindexed_frced.values() ) ) )

            data = []
            for visit in allvisits:
                thisdata = { 'src': None, 'src_bpv': None, 'frc': None, 'frc_bpv': None }

                srcgotit = False
                for bpvtuple in pvrow['diasource']:
                    bpv, _prio, bpvkey = bpvtuple
                    if bpvkey in reindexed_srces:
                        if visit in reindexed_srces[bpvkey]:
                            thisdata['src'] = reindexed_srces[bpvkey][visit]
                            thisdata['src_bpv'] = bpv
                            srcgotit = True
                            break

                frcgotit = True
                for bpvtuple in pvrow['diaforcedsource']:
                    bpv, _prio, bpvkey = bpvtuple
                    if bpvkey in reindexed_frced:
                        if visit in reindexed_frced[bpvkey]:
                            thisdata['frc'] = reindexed_frced[bpvkey][visit]
                            thisdata['frc_bpv'] = bpv
                            frcgotit = True
                            break

                if srcgotit or frcgotit:
                    data.append( thisdata )

            # Sort data by mjd, since that's how the ltcv returns sort things
            data.sort( key=lambda x: ( x['src'].midpointmjdtai
                                       if x['src'] is not None
                                       else x['frc'].midpointmjdtai ) )

            # Save this as it may be used again below
            datacache[rootid] = data.copy()

            if which == 'detections':
                # If which is detections, throw out only forced sources
                data = [ d for d in data if d['src'] is not None ]
            elif which == 'forced':
                # If which is forced, through out detections for which we don't have forced
                data = [ d for d in data if d['frc'] is not None ]

            # OK, now we have wrangled things to the point where they can be compared

            assert len( data ) == len( thisltcv['mjd'] )
            if which == 'detections':
                assert all( flux == pytest.approx( d['src'].psfflux, rel=1e-6 )
                            for flux, d in zip( thisltcv['flux'], data ) )
                assert all( flux == pytest.approx( d['src'].psffluxerr, rel=1e-6 )
                            for flux, d in zip( thisltcv['fluxerr'], data ) )
                assert all( bool(i) for i in thisltcv['isdet'] )
                assert all( v == d['src'].visit for v, d in zip( thisltcv['visit'], data ) )
                assert all( b == d['src'].band for b, d in zip( thisltcv['band'], data ) )
                assert all( o == d['src'].diaobjectid for o, d in zip( thisltcv['source_diaobjectid'], data ) )
            elif which =='forced':
                assert all( flux == pytest.approx( d['frc'].psfflux, rel=1e-6 )
                            for flux, d in zip( thisltcv['flux'], data ) )
                assert all( flux == pytest.approx( d['frc'].psffluxerr, rel=1e-6 )
                            for flux, d in zip( thisltcv['fluxerr'], data ) )
                assert all( bool(i) == ( d['src'] is not None ) for i, d in zip( thisltcv['isdet'], data ) )
                assert all( v == d['frc'].visit for v, d in zip( thisltcv['visit'], data ) )
                assert all( b == d['frc'].band for b, d in zip( thisltcv['band'], data ) )
                assert all( o == ( d['src'].diaobjectid if d['src'] is not None else None )
                            for o, d in zip( thisltcv['source_diaobjectid'], data ) )
                assert all( o == d['frc'].diaobjectid for o, d in zip( thisltcv['forced_diaobjectid'], data ) )
            else:
                assert all( flux == pytest.approx( ( d['frc'].psfflux if d['frc'] is not None
                                                     else d['src'].psfflux ),
                                                   rel=1e-6 )
                            for flux, d in zip( thisltcv['flux'], data ) )
                assert all( flux == pytest.approx( ( d['frc'].psffluxerr if d['frc'] is not None
                                                     else d['src'].psffluxerr ),
                                                   rel=1e-6 )
                            for flux, d in zip( thisltcv['fluxerr'], data ) )
                assert all( bool(i) == ( d['src'] is not None ) for i, d in zip( thisltcv['isdet'], data ) )
                assert all( bool(i) == ( d['frc'] is None ) for i, d in zip( thisltcv['ispatch'], data ) )
                assert all( v == ( d['frc'].visit if d['frc'] is not None else d['src'].visit )
                            for v, d in zip( thisltcv['visit'], data ) )
                assert all( b == ( d['frc'].band if d['frc'] is not None else d['src'].band )
                            for b, d in zip( thisltcv['band'], data ) )
                assert all( o == ( d['src'].diaobjectid if d['src'] is not None else None )
                            for o, d in zip( thisltcv['source_diaobjectid'], data ) )
                assert all( o == ( d['frc'].diaobjectid if d['frc'] is not None else None )
                            for o, d in zip( thisltcv['forced_diaobjectid'], data ) )

            if include_base_procver:
                assert all( v == ( d['src_bpv'].description if d['src_bpv'] is not None else None )
                            for v, d in zip( thisltcv['base_procver_s'], data ) )
                if which != 'detections':
                    assert all( v == ( d['frc_bpv'].description if d['frc_bpv'] is not None else None )
                                for v, d in zip( thisltcv['base_procver_f'], data ) )

            if include_source_positions:
                for i, d in enumerate( data ):
                    if d['src'] is None:
                        assert all( thisltcv[c][i] is None for c in [ 'det_ra', 'det_dec', 'det_raerr',
                                                                      'det_decerr', 'det_ra_dec_cov' ] )
                    else:
                        assert thisltcv['det_ra'][i] == pytest.approx( d['src'].ra, rel=1e-12 )
                        assert thisltcv['det_dec'][i] == pytest.approx( d['src'].dec, rel=1e-12 )
                        assert thisltcv['det_raerr'][i] == pytest.approx( d['src'].raerr, rel=1e-6 )
                        assert thisltcv['det_decerr'][i] == pytest.approx( d['src'].decerr, rel=1e-6 )
                        # Because the fixutre randomly generaetd ra and dec independently, we expect
                        #  ra_dec_cov to be close to 0, so it's unreasonable to expect full float64 precision
                        assert thisltcv['det_ra_dec_cov'][i] == pytest.approx( d['src'].ra_dec_cov, abs=0.0001/3600. )

        if infos is not None:

            expected_obj_keys = { 'diaobjectid', 'rootid' }
            if include_base_procver:
                expected_obj_keys.add( 'obj_base_procver' )
            if include_object_positions:
                expected_obj_keys = expected_obj_keys.union( { 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' } )
                if include_base_procver:
                    expected_obj_keys.add( 'pos_base_procver' )

            assert set( infos.keys() ) == expected_obj_keys

            if ( len(infos['rootid'])> 0 ) and ( isinstance( infos['rootid'][0], uuid.UUID ) ):
                assert set( infos['rootid'] ) == set( expected_root_ids )
            else:
                assert set( infos['rootid'] ) == set( str(e) for e in expected_root_ids )
            assert len( infos['diaobjectid'] ) == len( expected_diaobjectids )
            assert set( infos['diaobjectid'] ) == set( expected_diaobjectids )

            for r in expected_roots:

                # These next two lines are gratitous, but can be useful for debugging
                dexen = [ i for i, lc in enumerate( ltcvs )
                          if lc['rootid'] == ( roots[r]['root'].id if rootid_is_uuid
                                               else str(roots[r]['root'].id) ) ]
                thisltcv = ltcvs[dexen[0]]

                for diaobjectid in expected_diaobjectids:
                    if diaobjectid not in roots[r]['obj'].keys():
                        # ... is this an error?
                        continue
                    diaobject = roots[r]['obj'][diaobjectid]
                    dex = infos['diaobjectid'].index( diaobjectid )
                    if isinstance( infos['rootid'][dex], uuid.UUID ):
                        assert infos['rootid'][dex] == diaobject.rootid
                    else:
                        assert infos['rootid'][dex] == str( diaobject.rootid )
                    if include_base_procver:
                        # TODO : make sure the *right* processing version was returned!
                        # ...actually... the fixtures only have objects in a single base processing
                        # version for each processing version, so that wouldn't be an interesting test right now.
                        bpv = [ b[0] for b in pvrow['diaobject'] if b[0].description==infos['obj_base_procver'][dex] ]
                        assert len(bpv) == 1
                        bpv = bpv[0]
                        assert diaobject.base_procver_id == bpv.id

                    if include_object_positions:
                        pos = None

                        if always_use_weighted_source_positions:
                            pos = None
                            if include_base_procver and return_object_info:
                                assert infos['pos_base_procver'][dex] is None

                        else:
                            # There are multiple positions, so make sure we got the highest priority one that exists
                            #
                            # OMG this is becoming such an ugly hack of how I store data, this is what
                            #   happens when you just need to get stuff done fast and can't go back
                            #   and refactor.
                            pos = None
                            posbpv = None
                            for proposedposbpv, prio, bpvkey in pvrow['diaobject_position']:
                                if (diaobjectid, bpvkey) in roots[r]['pos'].keys():
                                    pos = roots[r]['pos'][ (diaobjectid, bpvkey) ]
                                    posbpv = proposedposbpv
                                    break

                            if include_base_procver:
                                if ( pos is None ) or always_use_weighted_source_positions:
                                    assert infos['pos_base_procver'][dex] is None
                                else:
                                    assert infos['pos_base_procver'][dex] == posbpv.description

                            if pos is not None:
                                assert infos['ra'][dex] == pytest.approx( pos.ra, rel=1e-12 )
                                assert infos['dec'][dex] == pytest.approx( pos.dec, rel=1e-12 )
                                assert infos['raerr'][dex] == pytest.approx( pos.raerr, rel=1e-6 )
                                assert infos['decerr'][dex] == pytest.approx( pos.decerr, rel=1e-6 )
                                # Fixture didn't put any correlations in but for random variancer
                                assert infos['ra_dec_cov'][dex] == pytest.approx( pos.ra_dec_cov, abs=0.0001/3600. )

                        if use_weighted_source_positions:
                            justsrcs = [ d for d in datacache[roots[r]['root'].id] if d['src'] is not None ]
                            srcra = np.array( [ j['src'].ra for j in justsrcs ] )
                            srcdec = np.array( [ j['src'].dec for j in justsrcs ] )

                            if which == 'detections':
                                sn = np.array( [ j['src'].psfflux / j['src'].psffluxerr for j in justsrcs ] )
                            else:
                                # ... this may be wholly gratuitous because I don't think
                                #     the fixtures set a different forced flux from the source flux
                                # However, this is the equivalent logic server-side.
                                sn = np.array( [ ( j['frc'].psfflux / j['frc'].psffluxerr )
                                                 if j['frc'] is not None
                                                 else ( j['src'].psfflux / j['src'].psffluxerr )
                                                 for j in justsrcs ] )
                            w = np.where( sn > 3 )[0]

                            # ****
                            # ltcvras = np.array( [ thisltcv['det_ra'][i] for i in range(len(thisltcv['isdet']))
                            #                       if thisltcv['isdet'][i] ] )
                            # ltcvdecs = np.array( [ thisltcv['det_dec'][i] for i in range(len(thisltcv['isdet']))
                            #                        if thisltcv['isdet'][i] ] )
                            # ltcvfluxes = np.array( [ thisltcv['flux'][i] for i in range(len(thisltcv['isdet']))
                            #                          if thisltcv['isdet'][i] ] )
                            # ltcvfluxerrs = np.array( [ thisltcv['fluxerr'][i] for i in range(len(thisltcv['isdet']))
                            #                            if thisltcv['isdet'][i] ] )
                            # justfluxes = np.array( [ s['src'].psfflux for s in justsrcs ] )
                            # justfluxerrs = np.array( [ s['src'].psffluxerr for s in justsrcs ] )

                            # FDBLogger.info( f"procver={procver}, which={which}, rootid={roots[r]['root'].id}, "
                            #                 f"diaobjectid={diaobjectid}\n"
                            #                 f"  ( thisltcv.det_ra - srcra ) / srcra = "
                            #                 f"{( ltcvras - srcra ) / srcra}\n"
                            #                 f"  ( thisltcv.det_dec - srcdec ) / srcdec = "
                            #                 f"{( ltcvdecs - srcdec ) / srcdec}\n"
                            #                 f"  ( thisltcv.flux - justfluxes ) / justfluxes = "
                            #                 f"{( ltcvfluxes - justfluxes ) / justfluxes}\n"
                            #                 f"  ( thisltcv.fluxerr - justfluxerrs ) / justfluxerrs = "
                            #                 f"{( ltcvfluxerrs - justfluxerrs ) / justfluxerrs}\n"
                            #                 f"( sn[w] - ltcvfluxes[w]/ltcvfluxerrs[w] ) = "
                            #                 f"{sn[w] - ltcvfluxes[w]/ltcvfluxerrs[w]}\n"
                            #                 f"sn[w] = {sn[w]}\n" )
                            # ****

                            srcra = srcra[w]
                            srcdec = srcdec[w]
                            weight = sn[w] ** 2
                            meanra = ( srcra * weight ).sum() / ( weight.sum() )
                            meandec = ( srcdec * weight ).sum() / ( weight.sum() )
                            raerr = np.sqrt( ( weight * ( srcra - meanra )**2 ).sum() / weight.sum() )
                            decerr = np.sqrt( ( weight * ( srcdec - meandec )**2 ).sum() / weight.sum() )
                            ra_dec_cov = ( weight * ( srcra - meanra ) * ( srcdec - meandec ) ).sum() / weight.sum()

                            if ( pos is None ) or always_use_weighted_source_positions:
                                if pos is not None:
                                    # In this case, the position should *not* match the
                                    #   diaobject_position value too closely
                                    assert not infos['ra'][dex] != pytest.approx( pos.ra, abs=0.01/3600. )
                                    assert not infos['dec'][dex] != pytest.approx( pos.dec, abs=0.01/3600. )
                                # But should be good within numerical precision to the calculated positions (modulo
                                # order of operations and floating roundoff).  (And, in fact, I might be
                                # surprised that it's this good, cause the weights come from fluxes which are
                                # stored as 32-bit floats.  Order of operations here and in the main code
                                # may just be close enough...?)  (Or, just a whole lot of luck that
                                # things worked out that way, except for once?)
                                #
                                # ....*if* there were enough sources to calculate a position from!
                                if len(srcra) > 0:
                                    assert infos['ra'][dex] == pytest.approx( meanra, rel=1e-12 )
                                    assert infos['dec'][dex] == pytest.approx( meandec, rel=1e-12 )
                                    assert infos['raerr'][dex] == pytest.approx( raerr, rel=1e-6 )
                                    assert infos['decerr'][dex] == pytest.approx( decerr, rel=1e-6 )
                                    # Fixture didn't put any correlations in
                                    assert infos['ra_dec_cov'][dex] == pytest.approx( ra_dec_cov, abs=0.0001/3600. )
                                else:
                                    assert all( ( infos[i][dex] is None ) or np.isnan( infos[i][dex] )
                                                for i in ( 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ) )
    return check_ltcv_res
