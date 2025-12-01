import os
import re
import pathlib
import argparse
import logging
import datetime
import uuid
import multiprocessing
import traceback
import functools

import numpy as np
import astropy.table
import astropy.time

from admin.fastdb_loader import FastDBLoader, ColumnMapper
from util import NULLUUID, FDBLogger
from db import ( DB, RootDiaObject, HostGalaxy, DiaObject, DiaSource, DiaForcedSource,
                 PPDBDiaObject, PPDBHostGalaxy, PPDBDiaSource, PPDBDiaForcedSource )


# ======================================================================

class SNANAColumnMapper( ColumnMapper ):
    @classmethod
    def _map_columns( cls, tab, mapper, lcs, manuals={} ):
        yanks = []
        renames = {}
        for col in tab.columns:
            if col in mapper:
                renames[ col ] = mapper[ col ]
            elif col in lcs:
                renames[ col ] = col.lower()
            else:
                yanks.append( col )
                next

        for oldname, newname in renames.items():
            tab.rename_column( oldname, newname )

        for newname, value in manuals.items():
            tab.add_column( value, name=newname )

        for yank in yanks:
            tab.remove_column( yank )

    @classmethod
    def diaobject_map_columns( cls, tab ):
        """Map from the HEAD.FITS.gz files to the diaobject table"""
        mapper = { 'SNID': 'diaobjectid',
                   'HOSTGAL_OBJID': 'nearbyextobj1',
                   'HOSTGAL2_OBJID': 'nearbyextobj2',
                   'HOSTGAL3_OBJID': 'nearbyextobj3',
                   'HOSTGAL_SNSEP': 'nearbyextobj1sep',
                   'HOSTGAL2_SNSEP': 'nearbyextobj2sep',
                   'HOSTGAL3_SNSEP': 'nearbyextobj3sep',
                  }
        lcs = { 'RA', 'DEC' }

        cls._map_columns( tab, mapper, lcs )


    @classmethod
    def hostgalaxy_map_columns( cls, n, tab ):
        """Map from the HEAD.FITS.gz files to the host_galaxy table"""

        n = "" if n == 1 else str(n)

        mapper = { f'HOSTGAL{n}_OBJID': 'objectid',
                   f'HOSTGAL{n}_RA': 'ra',
                   f'HOSTGAL{n}_DEC': 'dec',
                   f'HOSTGAL{n}_PHOTOZ': 'pzmean',
                   f'HOSTGAL{n}_PHOTOZ_ERR': 'pzstd',
                  }
        lcs = {}
        for band in [ 'u', 'g', 'r', 'i', 'z', 'Y' ]:
            mapper[ f'HOSTGAL{n}_MAG_{band}' ] = f'mag_{band.lower()}'
            mapper[ f'HOSTGAL{n}_MAGERR_{band}' ] = f'mag_{band.lower()}_err'
        for quant in range(0, 110, 10):
            mapper[ f'HOSTGAL{n}_ZPHOT_Q{quant:03d}' ] = f'pzquant{quant:03d}'

        cls._map_columns( tab, mapper, lcs )


    @classmethod
    def diasource_map_columns( cls, tab ):
        """Map from the PHOT.FITS.gz files to the diasource table"""
        mapper = { 'MJD': 'midpointmjdtai',
                   'BAND': 'band',
                   'FLUXCAL': 'psfflux',
                   'FLUXCALERR': 'psffluxerr',
                  }
        lcs = { 'PHOTFLAG' }
        cls._map_columns( tab, mapper, lcs )




# ======================================================================

class FITSFileHandler( SNANAColumnMapper ):
    def __init__( self, max_sources_per_object=None, photflag_detect=None, snana_zeropoint=None,
                  base_processing_version=None, processing_version=None,
                  really_do=None, verbose=None, ppdb=None, oneproc=False ):
        super().__init__()

        self.max_sources_per_object = max_sources_per_object
        self.photflag_detect = photflag_detect
        self.snana_zeropoint = snana_zeropoint
        self.base_processing_version = base_processing_version
        self.processing_version = processing_version
        self.really_do = really_do
        self.verbose = verbose
        self.ppdb = ppdb

        if not oneproc:
            FDBLogger.multiprocessing_replace()


    def load_one_file( self, headfile, photfile ):
        try:
            FDBLogger.info( f"PID {os.getpid()} reading {headfile.name}" )

            orig_head = astropy.table.Table.read( headfile )
            # SNID was written as a string, we need it to be a bigint
            orig_head['SNID'] = orig_head['SNID'].astype( np.int64 )
            head = astropy.table.Table( orig_head )

            if len(head) == 0:
                return { 'ok': True, 'headfile': headfile, 'msg': '0-length headfile' }

            phot = astropy.table.Table.read( photfile )

            # Load the host_galaxy and diaobject tables, both built from the head file

            # Build the hostgal table in hostgal
            hostgal1 = astropy.table.Table( orig_head )
            self.hostgalaxy_map_columns( 1, hostgal1 )
            hostgal2 = astropy.table.Table( orig_head )
            self.hostgalaxy_map_columns( 2, hostgal2 )
            hostgal3 = astropy.table.Table( orig_head )
            self.hostgalaxy_map_columns( 3, hostgal3 )
            hostgal = astropy.table.vstack( [ hostgal1, hostgal2, hostgal3 ] )
            hostgal = hostgal[ hostgal[ 'objectid' ] > 0 ]
            hostgal = astropy.table.unique( hostgal, keys='objectid' )
            hostgal.add_column( [ str(uuid.uuid4()) for i in range(len(hostgal)) ], name='id' )

            # Calculate some derived quantities and remove the quantities we don't need
            #   from the table.  Also turn Rick's -99's into None (I think!)
            tp5ln10 = np.log( 10 ) * 2.5
            hostgal.add_column( 10. ** (-0.4 * ( hostgal['mag_r'] - 31.4 ) ), name='petroflux_r' )
            hostgal.add_column( tp5ln10 * hostgal['mag_r_err'] * hostgal['petroflux_r'], name='petroflux_r_err' )
            hostgal[hostgal['mag_r'] < 0]['petroflux_r' ] = None
            hostgal[hostgal['mag_r'] < 0]['petroflux_r_err' ] = None

            bands = [ 'u', 'g', 'r', 'i', 'z', 'y' ]
            for bandi in range( len(bands)-1 ):
                hostgal.add_column( hostgal[f'mag_{bands[bandi]}'] - hostgal[f'mag_{bands[bandi+1]}'],
                                    name=f'stdcolor_{bands[bandi]}_{bands[bandi+1]}' )
                hostgal.add_column( np.sqrt( hostgal[f'mag_{bands[bandi]}']**2 + hostgal[f'mag_{bands[bandi+1]}']**2 ),
                                    name=f'stdcolor_{bands[bandi]}_{bands[bandi+1]}_err' )
                hostgal[ hostgal[f'mag_{bands[bandi]}'] < 0][f'stdcolor_{bands[bandi]}_{bands[bandi+1]}' ] = None
                hostgal[ hostgal[f'mag_{bands[bandi+1]}'] < 0][f'stdcolor_{bands[bandi]}_{bands[bandi+1]}' ] = None
                hostgal[ hostgal[f'mag_{bands[bandi]}'] < 0][f'stdcolor_{bands[bandi]}_{bands[bandi+1]}_err' ] = None
                hostgal[ hostgal[f'mag_{bands[bandi+1]}'] < 0][f'stdcolor_{bands[bandi]}_{bands[bandi+1]}_err' ] = None

            for bandi in bands:
                hostgal.remove_column( f'mag_{bandi}'  )
                hostgal.remove_column( f'mag_{bandi}_err' )

            if not self.ppdb:
                hostgal.add_column( self.base_processing_version, name='base_procver_id' )

            # At this point, hostgal should be ready for feeding to bulk_insert_or_upsert
            #   (onced processed through dict()).

            # Build the diaobject table in head
            self.diaobject_map_columns( head )
            head.add_column( astropy.time.Time( datetime.datetime.now( tz=datetime.UTC ) ).mjd,
                             name='validitystartmjdtai' )
            if not self.ppdb:
                head.add_column( self.base_processing_version, name='base_procver_id' )
                head.add_column( [ str(uuid.uuid4()) for i in range(len(head)) ], name='rootid' )

            head.add_column( str(NULLUUID), name='nearbyextobj1id' )
            head.add_column( str(NULLUUID), name='nearbyextobj2id' )
            if 'nearbyextobj3' in head.columns:
                head.add_column( str(NULLUUID), name='nearbyextobj3id' )

            # GAAAAAAAAAH.
            #
            # OK.  I put in a unique index that made everything break
            #   when the same host galaxy was loaded in more than once.
            #   So, we can't identify the host galaxy IDs right now, but
            #   will have to do it at the end of the whole load process.
            #   Leave them all as null right now.
            #
            # # By construction, in each of the joins below, joint should
            # #   have w rows.  hostgal was selected from all the known
            # #   nearbyextobj* in the HEAD file, and was made unique.
            # # So, when we are done, everything with a nearbyextobj{n}
            # #   that is >=0 should have a non-NULL uuid in
            # #   nearbyextobj{n}id.
            # # The bigger worry is that different HEAD files will use the
            # #   same hostgal more than once.  In that case, the same
            # #   hostgal will show up with different uuids.  The database
            # #   structure sould be OK with that (since there's no unique
            # #   constraint on (objectid, processing_version) in
            # #   host_galaxy), but it would be better to identify the
            # #   same host gal as the same host gal!
            # # For handling actual alerts, we need to be able to do this
            # #   better, as we're already going to need to be able to
            # #   handle repeated reports of the same sources, never mind
            # #   host galaxies.
            # w = np.where( head['nearbyextobj1'] > 0 )[0]
            # if len(w) > 0:
            #     joint = astropy.table.join( head[w], hostgal, keys_left='nearbyextobj1', keys_right=['objectid'] )
            #     head['nearbyextobj1id'][w] = joint['id']
            # w = np.where( head['nearbyextobj2'] > 0 )[0]
            # if len(w) > 0:
            #     joint = astropy.table.join( head[w], hostgal, keys_left='nearbyextobj2', keys_right=['objectid'] )
            #     head['nearbyextobj2id'][w] = joint['id']
            # if 'nearbyextobj3' in head.columns:
            #     w = np.where( head['nearbyextobj3'] > 0 )[0]
            #     if len(w) > 0:
            #         joint = astropy.table.join( head[w], hostgal, keys_left='nearbyextobj3', keys_right=['objectid'] )
            #         head['nearbyextobj3id'][w] = joint['id']

            if self.really_do:
                with DB() as conn:
                    if not self.ppdb:
                        RootDiaObject.bulk_insert_or_upsert( { 'id': list( head['rootid'] ) },
                                                             assume_no_conflict=True, dbcon=conn )

                    cls = PPDBHostGalaxy if self.ppdb else HostGalaxy
                    # NOTE -- we have to leave assume_no_conflict=False
                    #   on host_galaxy, as this process ends up with lots of duplicate
                    #   records.  We left the host galaxy unique constraints in place
                    # nhost = cls.bulk_insert_or_upsert( dict(hostgal), assume_no_conflict=True, dbcon=conn )
                    nhost = cls.bulk_insert_or_upsert( dict(hostgal), assume_no_conflict=False, dbcon=conn )
                    FDBLogger.info( f"PID {os.getpid()} loaded {nhost} host galaxies from {headfile.name}" )

                    cls = PPDBDiaObject if self.ppdb else DiaObject
                    q = cls.bulk_insert_or_upsert( dict(head), assume_no_conflict=True,
                                                   dbcon=conn, nocommit=True )
                    cursor = conn.cursor()
                    cursor.execute( "UPDATE temp_bulk_upsert SET nearbyextobj1=NULL, nearbyextobj1id=NULL, "
                                    "                            nearbyextobj1sep=NULL "
                                    "WHERE nearbyextobj1 <= 0" )
                    cursor.execute( "UPDATE temp_bulk_upsert SET nearbyextobj2=NULL, nearbyextobj2id=NULL, "
                                    "                            nearbyextobj2sep=NULL "
                                    "WHERE nearbyextobj2 <= 0" )
                    if 'nearbyextobj3' in head.columns:
                        cursor.execute( "UPDATE temp_bulk_upsert SET nearbyextobj3=NULL, nearbyextobj3id=NULL, "
                                        "                            nearbyextobj3sep=NULL "
                                        "WHERE nearbyextobj3 <= 0" )
                    # Null out ALL nearbyextids to get rid of the nulluuids we put there before
                    #   because of astropy tables and types and oh my
                    cursor.execute( "UPDATE temp_bulk_upsert SET nearbyextobj1id=NULL, nearbyextobj2id=NULL, "
                                    "  nearbyextobj3id=NULL" )
                    cursor.execute( q )
                    nobj = cursor.rowcount
                    conn.commit()
                    FDBLogger.info( f"PID {os.getpid()} loaded {nhost} hosts and {nobj} objects "
                                    f"from {headfile.name}" )

            else:
                nhost = len(hostgal)
                nobj = len(head)
                FDBLogger.info( f"PID {os.getpid()} would try to load {nobj} objects and {nhost} host galaxies" )

            # Calculate some derived fields we'll need for source and forced sourced tables
            # diasource psfflux is supposed to be in nJY
            # we have flux using self.snana_zeropoint
            # mAB = -2.5 * log10( f/Jy ) + 8.90
            #     = -2.5 * log10( f/nJy * 1e-9 ) + 8.90
            #     = -2.5 * log10( f/nJy ) - ( 2.5 * -9 ) + 8.90
            #     = -2.5 * log10( f/nJY ) + 31.4
            #
            # We want "visit" to be meaningful.  SNANA doesn't have that
            # concept, but practically speaking, two visits can't happen
            # at the same time, so we'll turn MJD into visit.  The
            # biggest MJD we can expect is less than 70000, and exposure
            # times of 15s mean mjds should differ by more than ~0.0002.
            # mjd*20000 should therefore be a safe unique integer, 70000
            # * 20000 < 2^31, and visit is a 32-bit integer (I did 2^31
            # to account for only using positive numbers), so doing a
            # floor of that should work.  (Floating-point roundoff is
            # not a real worry, because what *really* matters for how we
            # use it is that two observations at the same time *of the
            # same object* have the same visit number.  The floating
            # point mjd comes from a single source there, so there's no
            # worry of the same mjd having been calculated two different
            # ways and being off in the last bit or two of precision.)
            # (In the SNANA files, MJD is a double, which has 53 bits in
            # the mantissa, and 53 is way more than 31, so we don't have
            # to worry about our integers not being perfectly
            # represented when we multiply mjd by 20000.)

            phot['FLUXCAL'] *= 10 ** ( ( 31.4 - self.snana_zeropoint ) / 2.5 )
            phot['FLUXCALERR'] *= 10 ** ( ( 31.4 - self.snana_zeropoint ) / 2.5 )

            self.diasource_map_columns( phot )
            phot.add_column( np.int64(-1), name='diaobjectid' )
            phot['band'] = [ i.strip() for i in phot['band'] ]
            phot.add_column( np.int64(-1), name='diaforcedsourceid' )
            if not self.ppdb:
                phot.add_column( self.base_processing_version, name='base_procver_id' )
            phot.add_column( astropy.time.Time( datetime.datetime.now( tz=datetime.UTC ) ).mjd,
                             name='timeprocessedmjdtai' )
            phot.add_column( -1., name='ra' )
            phot.add_column( -100., name='dec' )
            phot.add_column( 0, name='visit' )
            phot.add_column( 0, name='detector' )       # Just something
            phot.add_column( 0., name='x' )             # Just something
            phot.add_column( 0., name='y' )             # Just something
            phot.add_column( 0., name='scienceflux' )
            phot.add_column( 0., name='sciencefluxerr' )

            # Load the DiaForcedSource table

            for obj, headrow in zip( orig_head, head ):
                # All the -1 is because the files are 1-indexed, but astropy is 0-indexed
                pmin = obj['PTROBS_MIN'] -1
                pmax = obj['PTROBS_MAX'] -1
                if ( pmax - pmin + 1 ) > self.max_sources_per_object:
                    FDBLogger.error( f'SNID {obj["SNID"]} in {headfile.name} has {pmax-pmin+1} sources, '
                                     f'which is more than max_sources_per_object={self.max_sources_per_object}' )
                    raise RuntimeError( "Too many sources" )
                phot['diaobjectid'][pmin:pmax+1] = headrow['diaobjectid']
                phot['visit'][pmin:pmax+1] = np.array( np.floor( phot['midpointmjdtai'][pmin:pmax+1] * 20000 ),
                                                       dtype=np.int32 )
                phot['diaforcedsourceid'][pmin:pmax+1] = ( obj['SNID'] + self.max_sources_per_object
                                                           + np.arange( pmax - pmin + 1 ) )
                phot['ra'][pmin:pmax+1] = obj['RA']
                phot['dec'][pmin:pmax+1] = obj['DEC']

            # The phot table has separators, so there will still be some junk data in there I need to purge
            phot = phot[ phot['diaobjectid'] >= 0 ]

            if self.really_do:
                forcedphot = astropy.table.Table( phot )
                forcedphot.remove_column( 'photflag' )
                forcedphot.remove_column( 'x' )
                forcedphot.remove_column( 'y' )
                cls = PPDBDiaForcedSource if self.ppdb else DiaForcedSource
                nfrc = cls.bulk_insert_or_upsert( dict(forcedphot), assume_no_conflict=True )
                FDBLogger.info( f"PID {os.getpid()} loaded {nfrc} forced photometry points from {photfile.name}" )
                del forcedphot
            else:
                nfrc = len(phot)
                FDBLogger.info( f"PID {os.getpid()} would try to load {nfrc} forced photometry points" )

            # Load the DiaSource table
            phot.rename_column( 'diaforcedsourceid', 'diasourceid' )
            phot['snr'] = phot['psfflux'] / phot['psffluxerr']
            phot = phot[ ( phot['photflag'] & self.photflag_detect ) !=0 ]
            phot.remove_column( 'photflag' )

            if self.really_do:
                cls = PPDBDiaSource if self.ppdb else DiaSource
                nsrc = cls.bulk_insert_or_upsert( dict(phot), assume_no_conflict=True )
                FDBLogger.info( f"PID {os.getpid()} loaded {nsrc} sources from {photfile.name}" )
            else:
                nsrc = len(phot)
                FDBLogger.info( f"PID {os.getpid()} would try to load {nsrc} sources" )

            if self.ppdb:
                return { 'ok': True, 'headfile': headfile,
                         'msg': ( f"Loaded {nobj} ppdb objects, {nsrc} ppdb sources, "
                                  f"{nfrc} ppdb forced sources" ) }
            else:
                return { 'ok': True, 'headfile': headfile,
                         'msg': ( f"Loaded {nobj} objects, {nsrc} sources, {nfrc} forced sources" ) }
        except Exception:
            FDBLogger.error( f"Exception loading {headfile}: {traceback.format_exc()}" )
            return { "ok": False, "headfile": headfile, "msg": traceback.format_exc() }


# ======================================================================

# Used in __call__ below by mulitprocessing
def do_loadOneFitsFile( filepair, **kwargs ):
    loader = FITSFileHandler( **kwargs )
    return loader.load_one_file( filepair[0], filepair[1] )


class FITSLoader( FastDBLoader ):
    def __init__( self, nprocs, directories, files=[],
                  max_sources_per_object=100000, photflag_detect=4096, snana_zeropoint=27.5,
                  really_do=False, verbose=False, dont_disable_indexes_fks=False, ppdb=False,
                  **kwargs
                 ):
        super().__init__( **kwargs )
        self.nprocs = nprocs
        self.directories = directories
        self.files = files
        self.max_sources_per_object=max_sources_per_object
        self.photflag_detect = photflag_detect
        self.snana_zeropoint = snana_zeropoint
        self.really_do = really_do
        self.dont_disable_indexes_fks = dont_disable_indexes_fks
        self.verbose = verbose
        self.ppdb = ppdb

        if self.ppdb != ( self.processing_version_name is None ):
            raise ValueError( "Cannot use processing version with ppdb; it's required for non-ppdb." )

        # This is a little bit naughty; we're changing the global
        #   logger object setting.  But, oh well.
        if verbose:
            FDBLogger.set_level( logging.DEBUG )
        else:
            FDBLogger.set_level( logging.INFO )


    def __call__( self ):
        # Make sure all HEAD.FITS.gz and PHOT.FITS.gz files exist, and collect them
        direcheadfiles = {}
        direcphotfiles = {}
        totnfiles = 0
        for directory in self.directories:
            FDBLogger.info( f"Verifying directory {directory}" )
            direc = pathlib.Path( directory )
            if not direc.is_dir():
                raise RuntimeError( f"{str(direc)} isn't a directory" )

            headre = re.compile( r'^(.*)HEAD\.FITS\.gz' )
            if len( self.files ) == 0:
                headfiles = list( direc.glob( '*HEAD.FITS.gz' ) )
            else:
                headfiles = [ direc / h for h in self.files ]
            photfiles = []
            for headfile in headfiles:
                match = headre.search( headfile.name )
                if match is None:
                    raise ValueError( f"Failed to parse {headfile.name} for *.HEAD.FITS.gz" )
                photfile = direc / f"{match.group(1)}PHOT.FITS.gz"
                if not headfile.is_file():
                    raise FileNotFoundError( f"Can't read {headfile}" )
                if not photfile.is_file():
                    raise FileNotFoundError( f"Can't read {photfile}" )
                photfiles.append( photfile )

            direcheadfiles[ direc ] = headfiles
            direcphotfiles[ direc ] = photfiles
            totnfiles += len(headfiles)

        # Get the ids of the processing version
        #  (and load it into the database if it's not there already)
        if not self.ppdb:
            self.make_procver()

        # Be very scary and remove all indexes and foreign key constraints
        #   from the database.  This will make all the bulk inserts
        #   faster, but of course it destroys the database.  It will
        #   write a file load_snana_fits_reconstruct_indexes_constraints.sql
        #   which can be used to manually restore all of that if the process
        #   crashes partway through, and the try / finally doesn't work.

        if not self.dont_disable_indexes_fks:
            self.disable_indexes_and_fks()

        # Do the long stuff
        try:

            loadOneFitsFile = functools.partial( do_loadOneFitsFile,
                                                 **{
                                                     'max_sources_per_object': self.max_sources_per_object,
                                                     'photflag_detect': self.photflag_detect,
                                                     'snana_zeropoint': self.snana_zeropoint,
                                                     'base_processing_version': self.base_processing_version,
                                                     'processing_version': self.processing_version,
                                                     'really_do': self.really_do,
                                                     'verbose': self.verbose,
                                                     'ppdb': self.ppdb,
                                                     'oneproc': self.nprocs==1
                                                 } )


            donefiles = set()
            errorfiles = set()
            horrible_things_have_happened = []

            def callback( msg ):
                if msg['ok']:
                    FDBLogger.info( msg['msg'] )
                    donefiles.add( msg['headfile'] )
                else:
                    FDBLogger.error( msg['msg'] )
                    errorfiles.add( msg['headfile'] )

            def omg( e ):
                FDBLogger.exception( e )
                horrible_things_have_happened.append( e )


            if self.nprocs == 1:
                FDBLogger.info( "Loading db in 1 process." )
                for directory in self.directories:
                    direc = pathlib.Path( directory )
                    headfiles = direcheadfiles[ direc ]
                    photfiles = direcphotfiles[ direc ]

                    for headfile, photfile in zip( headfiles, photfiles ):
                        try:
                            callback( loadOneFitsFile( (headfile, photfile) ) )
                        except Exception as e:
                            omg( e )

            else:

                FDBLogger.info( f'Launching {self.nprocs} processes to load the db.' )
                with multiprocessing.Pool( self.nprocs ) as pool:
                    for directory in self.directories:
                        direc = pathlib.Path( directory )
                        headfiles = direcheadfiles[ direc ]
                        photfiles = direcphotfiles[ direc ]

                        for headfile, photfile in zip( headfiles, photfiles ):
                            pool.apply_async( loadOneFitsFile, [ ( headfile, photfile ), ],
                                              callback=callback, error_callback=omg )


                    pool.close()
                    pool.join()


            if len(horrible_things_have_happened) > 1:
                nl = '\n'
                FDBLogger.error( f"Horrible things have happend:\n"
                                 f"{nl.join(str(i) for i in horrible_things_have_happened)}" )

            FDBLogger.info( f"{len(donefiles)} files succeeded, {len(errorfiles)} errored out, out of {totnfiles}" )

        finally:
            if not self.dont_disable_indexes_fks:
                self.recreate_indexes_and_fks()

            FDBLogger.info( f"Done recreating indices, now filling in host IDs." )
            with DB() as conn:
                cursor = conn.cursor()
                FDBLogger.info( "...nearbyextobj1..." )
                cursor.execute( "UPDATE diaobject o SET nearbyextobj1id=h.id "
                                "  FROM host_galaxy h "
                                "  WHERE o.nearbyextobj1=h.objectid" )
                FDBLogger.info( "...nearbyextobj2..." )
                cursor.execute( "UPDATE diaobject o SET nearbyextobj2id=h.id "
                                "  FROM host_galaxy h "
                                "  WHERE o.nearbyextobj2=h.objectid" )
                FDBLogger.info( "...nearbyextobj2..." )
                cursor.execute( "UPDATE diaobject o SET nearbyextobj3id=h.id "
                                "  FROM host_galaxy h "
                                "  WHERE o.nearbyextobj3=h.objectid" )
                conn.commit()
            FDBLogger.info( "Done." )

# ======================================================================

class ArgFormatter( argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter ):
    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )


def main():
    parser = argparse.ArgumentParser( 'load_snana_fits.py', description="Load fastdb from SNANA fits files",
                                      formatter_class=ArgFormatter,
                                      epilog="""Load FASTDB tables from SNANA fits files.

Loads the tables host_galaxy, diaobject, diasource, diaforcedsource,
Also may add a row to each of processing_version

Does *not* load root_diaobject.
"""
                                      )
    parser.add_argument( '-n', '--nprocs', default=5, type=int,
                         help=( "Number of worker processes to load; make sure that the number of CPUs "
                                "available is at least this many plus one." ) )
    parser.add_argument( '-d', '--directories', default=[], nargs='+', required=True,
                         help="Directories to find the HEAD and PHOT fits files" )
    parser.add_argument( '-f', '--files', default=[], nargs='+',
                         help="Names of HEAD.fits[.[fg]z] files; default is to read all in directory" )
    parser.add_argument( '-v', '--verbose', action='store_true', default=False,
                         help="Set log level to DEBUG (default INFO)" )
    parser.add_argument( '-m', '--max-sources-per-object', default=100000, type=int,
                         help=( "Maximum number of sources for a single object.  Used to generate "
                                "source ids, so make it big enough." ) )
    parser.add_argument( '-p', '--photflag-detect', default=4096, type=int,
                         help=( "The bit (really, 2^the bit) that indicates if a source is detected" ) )
    parser.add_argument( '-z', '--snana-zeropoint', default=27.5, type=float,
                         help="Zeropoint to move all photometry to" )
    parser.add_argument( '--processing-version', '--pv', default=None,
                         help="String value of the processing version to set for all objects" )
    parser.add_argument( '--dont-disable-indexes-fks', action='store_true', default=False,
                         help="Don't temporarily disable indexes and foreign keys (by default will)" )
    parser.add_argument( '--ppdb', action='store_true', default=False,
                         help="Load PPDB tables instead of main tables." )
    parser.add_argument( '--do', action='store_true', default=False,
                         help="Actually do it (otherwise, slowly reads FITS files but doesn't affect db" )

    args = parser.parse_args()

    fitsloader = FITSLoader( args.nprocs,
                             args.directories,
                             files=args.files,
                             max_sources_per_object=args.max_sources_per_object,
                             photflag_detect=args.photflag_detect,
                             snana_zeropoint=args.snana_zeropoint,
                             processing_version=args.processing_version,
                             really_do=args.do,
                             dont_disable_indexes_fks=args.dont_disable_indexes_fks,
                             ppdb=args.ppdb,
                             verbose=args.verbose )

    fitsloader()


# ======================================================================-
if __name__ == "__main__":
    main()
