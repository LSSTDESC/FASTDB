import sys
import os
import re
import pathlib
import argparse
import logging
import datetime
import time
import uuid
import multiprocessing
import queue
import traceback

import numpy as np
import psycopg
import psycopg.rows

import astropy.table

from fastdb_loader import FastDBLoader
from util import NULLUUID
from db import ( DB, HostGalaxy, DiaObject, DiaSource, DiaForcedSource,
                 DiaObjectSnapshot, DiaSourceSnapshot, DiaForcedSourceSnapshot,
                 PPDBDiaObject, PPDBHostGalaxy, PPDBDiaSource,PPDBDiaForcedSource )


# ======================================================================

class ColumnMapper:
    @classmethod
    def diaobject_map_columns( cls, tab ):
        """Map from the HEAD.FITS.gz files to the diaobject table"""
        mapper = { 'SNID': 'diaobjectid',
                   'MJD_TRIGGER': 'radecmjdtai',
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
                   'MJD_TRIGGER': 'psradectai',
                   f'HOSTGAL{n}_RA': 'psra',
                   f'HOSTGAL{n}_DEC': 'psdec',
                   f'HOSTGAL{n}_PHOTOZ': 'pzmean',
                   f'HOSTGAL{n}_PHOTOZ_ERR': 'pzstd',
                  }
        lcs = {}
        for band in [ 'u', 'g', 'r', 'i', 'z', 'Y' ]:
            mapper[ f'HOSTGAL{n}_MAG_{band}' ] = f'stdcolor_{band.lower()}'
            mapper[ f'HOSTGAL{n}_MAGERR_{band}' ] = f'stdcolor_{band.lower()}_err'
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


    @classmethod
    def _map_columns( cls, tab, mapper, lcs ):
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

        for yank in yanks:
            tab.remove_column( yank )


# ======================================================================

class FITSFileHandler( ColumnMapper ):
    def __init__( self, parent, pipe ):
        super().__init__()

        self.pipe = pipe

        # Copy settings from parent
        for attr in [ 'max_sources_per_object', 'photflag_detect',
                      'snana_zeropoint',
                      'processing_version', 'snapshot',
                      'really_do', 'verbose', 'ppdb' ]:
            setattr( self, attr, getattr( parent, attr ) )

        self.logger = logging.getLogger( f"logger {os.getpid()}" )
        self.logger.propagate = False
        loghandler = logging.FileHandler( f'{os.getpid()}.log' )
        self.logger.addHandler( loghandler )
        formatter = logging.Formatter( '[%(asctime)s - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        loghandler.setFormatter( formatter )
        if self.verbose:
            self.logger.setLevel( logging.DEBUG )
        else:
            self.logger.setLevel( logging.INFO )

    def listener( self ):
        done = False
        while not done:
            try:
                msg = self.pipe.recv()
                if msg['command'] == 'die':
                    done = True
                elif msg['command'] == 'do':
                    retval = self.load_one_file( msg['headfile'], msg['photfile'] )
                    self.pipe.send( { 'response': 'done',
                                      'headfile': msg['headfile'],
                                      'photfile': msg['photfile'],
                                      'retval': retval
                                     }
                                   )
            except EOFError:
                done = True

    def load_one_file( self, headfile, photfile ):
        try:
            self.logger.info( f"PID {os.getpid()} reading {headfile.name}" )

            orig_head = astropy.table.Table.read( headfile )
            # SNID was written as a string, we need it to be a bigint
            orig_head['SNID'] = orig_head['SNID'].astype( np.int64 )
            head = astropy.table.Table( orig_head )

            if len(head) == 0:
                return { 'ok': True, 'msg': '0-length headfile' }

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
            if not self.ppdb:
                hostgal.add_column( self.processing_version, name='processing_version' )

            # Build the diaobject table in head
            self.diaobject_map_columns( head )
            if not self.ppdb:
                head.add_column( self.processing_version, name='processing_version' )

            head.add_column( str(NULLUUID), name='nearbyextobj1id' )
            head.add_column( str(NULLUUID), name='nearbyextobj2id' )
            if 'nearbyextobj3' in head.columns:
                head.add_column( str(NULLUUID), name='nearbyextobj3id' )
            # By construction, in each of the joins below, joint should
            #   have w rows.  hostgal was selected from all the known
            #   nearbyextobj* in the HEAD file, and was made unique.
            # So, when we are done, everything with a nearbyextobj{n}
            #   that is >=0 should have a non-NULL uuid in
            #   nearbyextobj{n}id.
            # The bigger worry is that different HEAD files will use the
            #   same hostgal more than once.  In that case, the same
            #   hostgal will show up with different uuids.  The database
            #   structure sould be OK with that (since there's no unique
            #   constraint on (objectid, processing_version) in
            #   host_galaxy), but it would be better to identify the
            #   same host gal as the same host gal!
            # For handling actual alerts, we need to be able to do this
            #   better, as we're already going to need to be able to
            #   handle repeated reports of the same sources, never mind
            #   host galaxies.
            w = np.where( head['nearbyextobj1'] > 0 )[0]
            if len(w) > 0:
                joint = astropy.table.join( head[w], hostgal, keys_left='nearbyextobj1', keys_right=['objectid'] )
                head['nearbyextobj1id'][w] = joint['id']
            w = np.where( head['nearbyextobj2'] > 0 )[0]
            if len(w) > 0:
                joint = astropy.table.join( head[w], hostgal, keys_left='nearbyextobj2', keys_right=['objectid'] )
                head['nearbyextobj2id'][w] = joint['id']
            if 'nearbyextobj3' in head.columns:
                w = np.where( head['nearbyextobj3'] > 0 )[0]
                if len(w) > 0:
                    joint = astropy.table.join( head[w], hostgal, keys_left='nearbyextobj3', keys_right=['objectid'] )
                    head['nearbyextobj3id'][w] = joint['id']

            if self.really_do:
                with DB() as conn:
                    cls = PPDBHostGalaxy if self.ppdb else HostGalaxy
                    nhost = cls.bulk_insert_or_upsert( dict(hostgal), assume_no_conflict=True, dbcon=conn )
                    self.logger.info( f"PID {os.getpid()} loaded {nhost} host galaxies from {headfile.name}" )

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
                    cursor.execute( q )
                    nobj = cursor.rowcount
                    conn.commit()
                    self.logger.info( f"PID {os.getpid()} loaded {nhost} hosts and {nobj} objects "
                                      f"from {headfile.name}" )

            else:
                nhost = len(hostgal)
                nobj = len(head)
                self.logger.info( f"PID {os.getpid()} would try to load {nobj} objects and {nhost} host galaxies" )

            # Load the diaobject_snapshot table
            if not self.ppdb:
                no_ss = 0
                if self.snapshot is not None:
                    o_ss = astropy.table.Table()
                    o_ss['diaobjectid'] = head['diaobjectid']
                    o_ss.add_column( self.processing_version, name='processing_version' )
                    o_ss.add_column( self.snapshot, name='snapshot' )

                    if self.really_do:
                        no_ss = DiaObjectSnapshot.bulk_insert_or_upsert( dict(o_ss), assume_no_conflict=True )
                        self.logger.info( f"PID {os.getpid()} loaded {no_ss} "
                                          f"DiaObjectSnapshot from {headfile.name}" )
                    else:
                        no_ss = len( o_ss )
                        self.logger.info( f"PID {os.getpid()} would try to load {no_ss} rows into diaobject_snapshot" )


            # Calculate some derived fields we'll need for source and forced sourced tables
            # diasource psfflux is supposed to be in nJY
            # we have flux using self.snana_zeropoint
            # mAB = -2.5 * log10( f/Jy ) + 8.90
            #     = -2.5 * log10( f/nJy * 1e-9 ) + 8.90
            #     = -2.5 * log10( f/nJy ) - ( 2.5 * -9 ) + 8.90
            #     = -2.5 * log10( f/nJY ) + 31.4

            phot['FLUXCAL'] *= 10 ** ( ( 31.4 - self.snana_zeropoint ) / 2.5 )
            phot['FLUXCALERR'] *= 10 ** ( ( 31.4 - self.snana_zeropoint ) / 2.5 )

            self.diasource_map_columns( phot )
            phot.add_column( np.int64(-1), name='diaobjectid' )
            if not self.ppdb:
                phot.add_column( -1, name='diaobject_procver' )
            phot['band'] = [ i.strip() for i in phot['band'] ]
            phot.add_column( np.int64(-1), name='diaforcedsourceid' )
            if not self.ppdb:
                phot.add_column( self.processing_version, name='processing_version' )
            phot.add_column( -1., name='ra' )
            phot.add_column( -100., name='dec' )
            phot.add_column( 0, name='visit' )
            phot.add_column( 0, name='detector' )       # Just something
            phot.add_column( 0., name='scienceflux' )
            phot.add_column( 0., name='sciencefluxerr' )

            # Load the DiaForcedSource table

            for obj, headrow in zip( orig_head, head ):
                # All the -1 is because the files are 1-indexed, but astropy is 0-indexed
                pmin = obj['PTROBS_MIN'] -1
                pmax = obj['PTROBS_MAX'] -1
                if ( pmax - pmin + 1 ) > self.max_sources_per_object:
                    self.logger.error( f'SNID {obj["SNID"]} in {headfile.name} has {pmax-pmin+1} sources, '
                                       f'which is more than max_sources_per_object={self.max_sources_per_object}' )
                    raise RuntimeError( "Too many sources" )
                phot['diaobjectid'][pmin:pmax+1] = headrow['diaobjectid']
                if not self.ppdb:
                    phot['diaobject_procver'][pmin:pmax+1] = headrow['processing_version']
                phot['visit'][pmin:pmax+1] = obj['SNID']             # Just something
                phot['diaforcedsourceid'][pmin:pmax+1] = ( obj['SNID'] * self.max_sources_per_object
                                                           + np.arange( pmax - pmin + 1 ) )
                phot['ra'][pmin:pmax+1] = obj['RA']
                phot['dec'][pmin:pmax+1] = obj['DEC']

            # The phot table has separators, so there will still be some junk data in there I need to purge
            phot = phot[ phot['diaobjectid'] >= 0 ]

            if self.really_do:
                forcedphot = astropy.table.Table( phot )
                forcedphot.remove_column( 'photflag' )
                cls = PPDBDiaForcedSource if self.ppdb else DiaForcedSource
                nfrc = cls.bulk_insert_or_upsert( dict(forcedphot), assume_no_conflict=True )
                self.logger.info( f"PID {os.getpid()} loaded {nfrc} forced photometry points from {photfile.name}" )
                del forcedphot
            else:
                nfrc = len(phot)
                self.logger.info( f"PID {os.getpid()} would try to load {nfrc} forced photometry points" )

            # Load the diaforcedsource_snapshot table
            if not self.ppdb:
                nfs_ss = 0
                if self.snapshot is not None:
                    fs_ss = astropy.table.Table()
                    fs_ss['diaforcedsourceid'] = phot['diaforcedsourceid']
                    fs_ss.add_column( self.processing_version, name='processing_version' )
                    fs_ss.add_column( self.snapshot, name='snapshot' )

                    if self.really_do:
                        nfs_ss = DiaForcedSourceSnapshot.bulk_insert_or_upsert( dict(fs_ss), assume_no_conflict=True )
                        self.logger.info( f"PID {os.getpid()} loaded {nfs_ss} "
                                          f"DiaForcedSourceSnapshot from {photfile.name}" )
                    else:
                        nfs_ss = len( fs_ss )
                        self.logger.info( f"PID {os.getpid()} would try to load {nfs_ss} "
                                          f"rows into diaforcedsource_snapshot" )

            # Load the DiaSource table
            phot.rename_column( 'diaforcedsourceid', 'diasourceid' )
            phot['snr'] = phot['psfflux'] / phot['psffluxerr']
            phot = phot[ ( phot['photflag'] & self.photflag_detect ) !=0 ]
            phot.remove_column( 'photflag' )

            if self.really_do:
                cls = PPDBDiaSource if self.ppdb else DiaSource
                nsrc = cls.bulk_insert_or_upsert( dict(phot), assume_no_conflict=True )
                self.logger.info( f"PID {os.getpid()} loaded {nsrc} sources from {photfile.name}" )
            else:
                nsrc = len(phot)
                self.logger.info( f"PID {os.getpid()} would try to load {nsrc} sources" )

            # Load the diasource_snapshot table
            if not self.ppdb:
                ns_ss = 0
                if self.snapshot is not None:
                    s_ss = astropy.table.Table()
                    s_ss['diasourceid'] = phot['diasourceid']
                    s_ss.add_column( self.processing_version, name='processing_version' )
                    s_ss.add_column( self.snapshot, name='snapshot' )

                    if self.really_do:
                        ns_ss = DiaSourceSnapshot.bulk_insert_or_upsert( dict(s_ss), assume_no_conflict=True )
                        self.logger.info( f"PID {os.getpid()} loaded {ns_ss} DiaSourceSnapshot from {photfile.name}" )
                    else:
                        ns_ss = len( s_ss )
                        self.logger.info( f"PID {os.getpid()} would try to load {ns_ss} rows into DStoPVtoSS" )

            if self.ppdb:
                return { 'ok': True, 'msg': ( f"Loaded {nobj} ppdb objects, {nsrc} ppdb sources, "
                                              f"{nfrc} ppdb forced sources" ) }
            else:
                return { 'ok': True, 'msg': ( f"Loaded {nobj} objects, {nsrc} sources, {nfrc} forced, "
                                              f"{no_ss} object_snapshot, {nfs_ss} forced_snapshot, "
                                              f"{ns_ss} source_snapshot" ) }
        except Exception:
            self.logger.error( f"Exception loading {headfile}: {traceback.format_exc()}" )
            return { "ok": False, "msg": traceback.format_exc() }


# ======================================================================

class FITSLoader( FastDBLoader ):
    def __init__( self, nprocs, directories, files=[],
                  max_sources_per_object=100000, photflag_detect=4096,
                  snana_zeropoint=27.5,
                  processing_version=None, snapshot=None,
                  really_do=False, verbose=False, dont_disable_indexes_fks=False,
                  ppdb=False,
                  logger=logging.getLogger( "load_snana_fits") ):
        super().__init__()
        self.nprocs = nprocs
        self.directories = directories
        self.files = files
        self.max_sources_per_object=max_sources_per_object
        self.photflag_detect = photflag_detect
        self.snana_zeropoint = snana_zeropoint
        self.processing_version = None
        self.processing_version_name = processing_version
        self.snapshot = None
        self.snapshot_name = snapshot
        self.really_do = really_do
        self.logger = logger
        self.sublogger = None
        self.verbose = verbose
        self.dont_disable_indexes_fks = dont_disable_indexes_fks
        self.ppdb = ppdb


    def make_procver_and_snapshot( self ):
        with DB() as conn:
            try:
                cursor = conn.cursor( row_factory=psycopg.rows.dict_row )
                cursor.execute( "LOCK TABLE processing_version" )
                cursor.execute( "SELECT * FROM processing_version WHERE description=%(pv)s",
                                { 'pv': self.processing_version_name } )
                rows = cursor.fetchall()
                if len(rows) >= 1:
                    self.processing_version = rows[0]['id']
                else:
                    cursor.execute( "SELECT MAX(id) AS maxid FROM processing_version" )
                    row = cursor.fetchone()
                    self.processing_version = row['maxid'] + 1 if row['maxid'] is not None else 0
                    cursor.execute( "INSERT INTO processing_version(id,description,validity_start) "
                                    "VALUES (%(id)s, %(pv)s, %(now)s)",
                                    { 'id': self.processing_version, 'pv': self.processing_version_name,
                                      'now': datetime.datetime.now(tz=datetime.UTC) } )
                    conn.commit()
            finally:
                conn.rollback()

            try:
                cursor = conn.cursor( row_factory=psycopg.rows.dict_row )
                cursor.execute( "LOCK TABLE snapshot ")
                cursor.execute( "SELECT * FROM snapshot WHERE description=%(ss)s",
                                { 'ss': self.snapshot_name } )
                rows = cursor.fetchall()
                if len(rows) >= 1:
                    self.snapshot = rows[0]['id']
                else:
                    cursor.execute( "SELECT MAX(id) AS maxid FROM snapshot" )
                    row = cursor.fetchone()
                    self.snapshot = row['maxid'] + 1 if row['maxid'] is not None else 0
                    cursor.execute( "INSERT INTO snapshot(id,description) VALUES (%(id)s, %(ss)s)",
                                    { 'id': self.snapshot, 'ss': self.snapshot_name } )
                    conn.commit()
            finally:
                conn.rollback()


    def __call__( self ):
        # Make sure all HEAD.FITS.gz and PHOT.FITS.gz files exist
        direcheadfiles = {}
        direcphotfiles = {}
        for directory in self.directories:
            self.logger.info( f"Verifying directory {directory}" )
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

        # Get the ids of the processing version and snapshot
        #  (and load them into the database if they're not there already)
        if not self.ppdb:
            self.make_procver_and_snapshot()


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

            self.logger.info( f'Launching {self.nprocs} processes to load the db.' )

            def launchFITSFileHandler( pipe ):
                hndlr = FITSFileHandler( self, pipe )
                hndlr.listener()

            freeprocs = set()
            busyprocs = set()
            procinfo = {}
            for i in range(self.nprocs):
                parentconn, childconn = multiprocessing.Pipe()
                proc = multiprocessing.Process( target=lambda: launchFITSFileHandler( childconn ) )
                proc.start()
                procinfo[ proc.pid ] = { 'proc': proc,
                                         'parentconn': parentconn,
                                         'childconn': childconn }
                freeprocs.add( proc.pid )

            # Go through the directories and load everything
            # Note: I might be able to refactor this to use multiprocesing.pool
            #   and make it all much simpler.
            for directory in self.directories:
                self.logger.info( f"Loading files in {directory}" )
                direc = pathlib.Path( directory )
                headfiles = direcheadfiles[ direc ]
                photfiles = direcphotfiles[ direc ]
                donefiles = set()
                errfiles = set()

                fileptr = 0
                done = False
                while not done:
                    # Tell any free processes what to do:
                    while ( len(freeprocs) > 0 ) and ( fileptr < len(headfiles) ):
                        pid = freeprocs.pop()
                        busyprocs.add( pid )
                        procinfo[pid]['parentconn'].send( { 'command': "do",
                                                            'headfile': headfiles[fileptr],
                                                            'photfile': photfiles[fileptr]
                                                           } )
                        fileptr += 1

                    # Check for response from busy processes
                    doneprocs = set()
                    for pid in busyprocs:
                        try:
                            # ROB TODO : recv() blocks.
                            # Use poll()
                            msg = procinfo[pid]['parentconn'].recv()
                        except queue.Empty:
                            continue
                        if msg['response'] != 'done':
                            raise ValueError( f"Unexpected response from child process: {msg}" )
                        if msg['headfile'] in donefiles:
                            raise RuntimeError( f"{msg['headfile']} got processed twice" )
                        donefiles.add( msg['headfile'] )
                        if msg['retval']['ok']:
                            self.logger.info( f"{msg['headfile']} done: {msg['retval']['msg']}" )
                        else:
                            errfiles.add( msg['headfile'] )
                            self.logger.error( f"{msg['headfile']} failed {msg['retval']['msg']}" )
                        doneprocs.add( pid )

                    for pid in doneprocs:
                        busyprocs.remove( pid )
                        freeprocs.add( pid )

                    if ( len(busyprocs) == 0 ) and ( fileptr >= len(headfiles) ):
                        # Everything has been submitted, we're waiting for
                        # no processes, so we're done.
                        done = True
                    else:
                        # Sleep before polling if there's nothing ready to submit
                        # and we're just waiting for responses from busy processes
                        if ( len(freeprocs) == 0 ) or ( fileptr >= len(headfiles) ):
                            time.sleep( 1 )

                if len(donefiles) != len(headfiles):
                    raise RuntimeError( f"Something bad has happened; there are {len(headfiles)} headfiles, "
                                        f"but only {len(donefiles)} donefiles!" )

            # Close all the processes we started

            for pid, info in procinfo.items():
                info['parentconn'].send( { 'command': 'die' } )
            time.sleep( 1 )
            for pid, info in procinfo.items():
                info['proc'].close()

        finally:
            if not self.dont_disable_indexes_fks:
                self.recreate_indexes_and_fks()


# ======================================================================

class ArgFormatter( argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter ):
    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )


def main():
    logger = logging.getLogger( "load_snana_fits" )
    logout = logging.StreamHandler( sys.stderr )
    logger.addHandler( logout )
    formatter = logging.Formatter( '[%(asctime)s - %(message)s',
                                   datefmt='%Y-%m-%d %H:%M:%S' )
    logout.setFormatter( formatter )
    logger.setLevel( logging.INFO )

    parser = argparse.ArgumentParser( 'load_snana_fits.py', description="Load fastdb from SNANA fits files",
                                      formatter_class=ArgFormatter,
                                      epilog="""Load FASTDB tables from SNANA fits files.

Loads the tables host_galaxy, diaobject, diasource, diaforcedsource,
diaobject_snapshot, diasource_snapshot, and diaforcedsource_snapshot.
Also may add a row to each of processing_version and snapshot.

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
    parser.add_argument( '-s', '--snapshot', default=None,
                         help="If given, create this snapshot and put all loaded sources/forced sources in it" )
    parser.add_argument( '--dont-disable-indexes-fks', action='store_true', default=False,
                         help="Don't temporarily disable indexes and foreign keys (by default will)" )
    parser.add_argument( '--ppdb', action='store_true', default=False,
                         help="Load PPDB tables instead of main tables." )
    parser.add_argument( '--do', action='store_true', default=False,
                         help="Actually do it (otherwise, slowly reads FITS files but doesn't affect db" )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel( logging.DEBUG )

    if args.ppdb:
        if ( args.snapshot is not None ) or ( args.processing_version is not None ):
            logger.warning( "processing_version and snapshot are ignored when loading the ppdb" )
        else:
            if args.processing_version is None:
                logger.error( "processing_version is required" )
            if args.snapshot is None:
                logger.warning( "No snapshot specified, snapshot tables will not be loaded" )

    fitsloader = FITSLoader( args.nprocs,
                             args.directories,
                             files=args.files,
                             max_sources_per_object=args.max_sources_per_object,
                             photflag_detect=args.photflag_detect,
                             snana_zeropoint=args.snana_zeropoint,
                             processing_version=args.processing_version,
                             snapshot=args.snapshot,
                             really_do=args.do,
                             dont_disable_indexes_fks=args.dont_disable_indexes_fks,
                             ppdb=args.ppdb,
                             verbose=args.verbose,
                             logger=logger )

    fitsloader()


# ======================================================================-
if __name__ == "__main__":
    main()
