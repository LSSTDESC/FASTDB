import sys
import os
import re
import time
import pathlib
import multiprocessing
import logging
import argparse
import traceback

import nested_pandas

from fastdb_loader import FastDBLoader, ColumnMapper
from db import ( DiaObject, DiaSource, DiaForcedSource, #, HostGalaxy
                 PPDBDiaObject, PPDBDiaSource,PPDBDiaForcedSource ) # PPDBHostGalaxy,



class DP1ColumnMapper( ColumnMapper ):
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

        tab.rename( renames, axis='columns', inplace=True )
        tab.drop( yanks, axis='columns', inplace=True )


    @classmethod
    def diaobject_map_columns( cls, tab ):
        mapper = {}
        lcs = { 'diaObjectId', 'radecMjdTai', 'ra', 'dec' }

        cls._map_columns( tab, mapper, lcs )


    @classmethod
    def hostgalaxy_map_columns( cls, n, tab ):
        raise NotImplementedError( "I don't do host galaxies yet" )


    @classmethod
    def diasource_map_columns( cls, tab ):
        mapper = {}
        # TODO : flags, pixelflags (outside column mapper)
        lcs = { 'diaObjectId', 'ssObjectId', 'visit', 'detector',
                'x', 'y', 'xErr', 'yErr', 'band', 'midpointMjdTai',
                'ra', 'dec', 'raErr', 'decErr', 'ra_dec_Cov',
                'psfFlux', 'psfFluxErr', 'psfNdata', 'snr',
                'scienceFlux', 'scienceFluxErr',
                'extendedness', 'reliability', 'ixx', 'iyy', 'ixy',
                'ixxPSF', 'ixyPSF', 'iyyPSF' }
        cls._map_columns( tab, mapper, lcs )

    @classmethod
    def diaforcedsource_map_columns( cls, tab ):
        mapper = { 'coord_ra': 'ra',
                   'coord_dec': 'dec',
                   'psfDiffFlux': 'psfflux',
                   'psfDiffFluxErr': 'psffluxerr',
                   'psfFlux': 'scienceflux',
                   'psfFluxErr': 'sciencefluxerr',
                  }
        # TODO : pixelflags
        lcs = { 'diaObjectId', 'visit', 'detector', 'midpointMjdTai',
                'band' }
        cls._map_columns( tab, mapper, lcs )



# ======================================================================

class ParquetFileHandler:
    def __init__( self, parent, pipe ):
        self.pipe = pipe

        # Copy settings from parent
        for attr in [ 'really_do', 'verbose', 'ppdb', 'processing_version' ]:
            setattr( self, attr, getattr( parent ,attr ) )

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
                    retval = self.load_one_file( msg['file'] )
                    self.pipe.send( { 'response': 'done',
                                      'file': msg['file'],
                                      'retval': retval
                                      }
                                   )

            except EOFError:
                done = True

    def load_one_file( self, filepath ):
        try:
            self.logger.info( f"PID {os.getpid()} reading {filepath.name}" )
            df = nested_pandas.read_parquet( filepath )

            # There must be a faster way to do this.  This is really slow
            self.logger.info( f"Concatenating sources from {len(df)} objects..." )
            sourcedf = df.diaSource.nest.to_flat().join( df.diaObjectId )
            self.logger.info( f"...concatenated to {len(sourcedf)} sources" )

            self.logger.info( f"Concatenating forcedsources from {len(df)} objects..." )
            forceddf = df.diaObjectForcedSource.nest.to_flat().join( df.diaObjectId )
            self.logger.info( f"...concatenated to {len(forceddf)} foreced sources" )

            DP1ColumnMapper.diaobject_map_columns( df )
            DP1ColumnMapper.diasource_map_columns( sourcedf )
            DP1ColumnMapper.diaforcedsource_map_columns( forceddf )

            if not self.ppdb:
                df['processing_version'] = self.processing_version
                sourcedf['processing_version'] = self.processing_version
                forceddf['processing_version'] = self.processing_version
                sourcedf['diaobject_procver'] = self.processing_version
                forceddf['diaobject_procver'] = self.processing_version

            # Not sure where the index came from.  There was lots of
            #   redundancy.  This may be something nested_pandas did?
            #   Made the indesx of the thing I extracted the same
            #   as the index of the parent dataframe?  If so,
            #   that might be a hint to speeding up the
            #   extraction above....  Hurm.  Really not sure.
            df.reset_index( inplace=True )
            df.drop( ['index'], axis='columns', inplace=True )
            sourcedf.reset_index( inplace=True )
            sourcedf.drop( ['index'], axis='columns', inplace=True )
            forceddf.reset_index( inplace=True )
            forceddf.drop( ['index'], axis='columns', inplace=True )

            # TODO snapshot table

            self.logger.info( f"Going to try to load {len(df)} objects, {len(sourcedf)} sources, "
                              f"{len(forceddf)} forced" )
            if self.really_do:
                loaded = "Loaded"
                if self.ppdb:
                    nobj = PPDBDiaObject.bulk_insert_or_upsert( df.to_dict(), assume_no_conflict=True )
                    nsrc = PPDBDiaSource.bulk_insert_or_upsert( sourcedf.to_dict(), assume_no_conflict=True )
                    nfrc = PPDBDiaForcedSource.bulk_insert_or_upsert( forceddf.to_dict(), assume_no_conflict=True )
                else:
                    nobj = DiaObject.bulk_insert_or_upsert( df.to_dict(), assume_no_conflict=True )
                    nsrc = DiaSource.bulk_insert_or_upsert( sourcedf.to_dict(), assume_no_conflict=True )
                    nfrc = DiaForcedSource.bulk_insert_or_upsert( forceddf.to_dict(), assume_no_conflict=True )
            else:
                loaded = "Would load"
                nobj = len(df)
                nsrc = len(sourcedf)
                nfrc = len(forceddf)

            ppdb = "ppdb " if self.ppdb else ""
            self.logger.info( f"{loaded} {nobj} {ppdb}objects, {nsrc} {ppdb}sources, {nfrc} {ppdb}forced" )
            return { 'ok': True, 'msg': ( f"{loaded} {nobj} {ppdb}objects, {nsrc} {ppdb}sources, "
                                          f"{nfrc} {ppdb}forced sources" ) }

        except Exception:
            self.logger.error( f"Exception loading {filepath}: {traceback.format_exc()}" )
            return { "ok": False, "msg": traceback.format_exc() }


# ======================================================================

class DP1ParquetLoader( FastDBLoader ):
    def __init__( self, nprocs, basedir, really_do=False, verbose=False,
                  dont_disable_indexes_fks=False, ppdb=False,
                  logger=logging.getLogger( "load_dp1_parquet" ),
                  **kwargs ):
        super().__init__( **kwargs )

        self.nprocs = nprocs
        self.basedir = basedir
        self.really_do = really_do
        self.verbose = verbose
        self.dont_disable_indexes_fks = dont_disable_indexes_fks
        self.ppdb = ppdb
        self.logger = logger

        if self.snapshot is not None:
            raise NotImplementedError( "Loading snapshots not yet implemented" )

    def recursive_find_files( self, direc , files=[] ):
        filematch = re.compile( r"^Npix=\d+\.parquet$" )
        if not direc.is_dir():
            if filematch.search( direc.name ):
                return files + [ direc.resolve() ]
            else:
                return files
        for f in direc.glob( "*" ):
            return self.recursive_find_files( f, files )

    def __call__( self ):
        # Find all the parquet files to laod
        files = self.recursive_find_files( pathlib.Path( self.basedir ) )

        # Get the ids of the processing version and snapshot
        #  (and load them into the database if they're not there already)
        if ( not self.ppdb ) and ( self.really_do ):
            self.make_procver_and_snapshot()

        # Strip all indexes and fks for insert efficiency.
        # Writes a file load_snana_fits_reconstruct_indexes_constraints.sql
        # which you can feed to psql to rebuild the indices if there
        # is a crash.

        if not self.dont_disable_indexes_fks:
            self.disable_indexes_and_fks()

        # Do the long stuff
        try:
            self.logger.info( f"Launching {self.nprocs} procesess to load {len(files)} parquet files" )

            def launchDP1FileHandler( pipe ):
                hndlr = ParquetFileHandler( self, pipe )
                hndlr.listener()

            donefiles = set()
            errfiles = set()

            freeprocs = set()
            busyprocs = set()
            procinfo = {}
            for i in range( self.nprocs ):
                parentconn, childconn = multiprocessing.Pipe()
                proc = multiprocessing.Process( target=lambda: launchDP1FileHandler( childconn ) )
                proc.start()
                procinfo[ proc.pid ] = { 'proc': proc,
                                         'parentconn': parentconn,
                                         'childconn': childconn }
                freeprocs.add( proc.pid )

            fileptr = 0
            done = False
            while not done:
                while ( len(freeprocs) > 0 ) and ( fileptr < len(files) ):
                    pid = freeprocs.pop()
                    busyprocs.add( pid )
                    procinfo[pid]['parentconn'].send( { 'command': 'do',
                                                        'file': files[fileptr] } )
                    fileptr += 1

                doneprocs = set()
                for pid in busyprocs:
                    if procinfo[pid]['parentconn'].poll():
                        msg = procinfo[pid]['parentconn'].recv()
                        if msg['response'] != 'done':
                            raise ValueError( f"Unexpected response from child process: {msg}" )
                        if msg['file'] in donefiles:
                            raise RuntimeError( f"{msg['file']} got processed twice" )
                        donefiles.add( msg['file'] )
                        if msg['retval']['ok']:
                            self.logger.info( f"{msg['file']} done: {msg['retval']['msg']}" )
                        else:
                            errfiles.add( msg['file'] )
                            self.logger.error( f"{msg['file']} failed: {msg['retval']['msg']}" )
                        doneprocs.add( pid )

                for pid in doneprocs:
                    busyprocs.remove( pid )
                    freeprocs.add( pid )

                if ( len(busyprocs) == 0 ) and ( fileptr >= len(files) ):
                    done = True
                else:
                    if ( len(freeprocs) == 0 ) or ( fileptr >= len(files) ):
                        time.sleep( 1 )

            if len(donefiles) != len(files):
                raise RuntimeError( f"Something bad has happened; there are {len(files)} files, "
                                    f"but only {len(donefiles)} donefiles!" )

            for pid, info in procinfo.items():
                info['parentconn'].send( { 'command': 'die' } )
            # TODO : actually wait on the processes with join or something
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
    logger = logging.getLogger( "load_dp1_parquet" )
    logout = logging.StreamHandler( sys.stderr )
    logger.addHandler( logout )

    formatter = logging.Formatter( '[%(asctime)s - %(message)s',
                                   datefmt='%Y-%m-%d %H:%M:%S' )
    logout.setFormatter( formatter )
    logger.setLevel( logging.INFO )

    parser = argparse.ArgumentParser( 'load_dp1_parquet.py', description="Load fastdb from DP1 parquet files",
                                      formatter_class=ArgFormatter,
                                      epilog="""Load FASTDB tables from DP1 parquet files.

Trolls the given directory for all files named "Npix=<number>.parqet".  Loads
diaobject, diasource, diaforcedsource, or, if --ppdb is given, ppdb_diaobject,
ppdb_diasource, and ppdb_diaforcedsource.  Does not currently support snapshots.
May add a processing_version row.

Does *not* load root_diaobject!
"""
                                     )
    parser.add_argument( 'dir', help="Top level directory under which to look for parquet files" )
    parser.add_argument( '-n', '--nprocs', default=5, type=int,
                         help=( "Number of worker processes to load; make sure that the number of CPUs "
                                "available is at least this many plus one." ) )
    parser.add_argument( '--processing-version', '--pv', default=None,
                         help="String value of the processing version to set for all objects" )
    parser.add_argument( '--dont-disable-indexes-fks', action='store_true', default=False,
                         help="Don't temporarily disable indexes and foreign keys (by default will)" )
    parser.add_argument( '--ppdb', action='store_true', default=False,
                         help="Load PPDB tables instead of main tables." )
    parser.add_argument( '--do', action='store_true', default=False,
                         help="Actually do it (otherwise, slowly reads FITS files but doesn't affect db" )
    parser.add_argument( '-v', '--verbose', action='store_true', default=False,
                         help="Set log level to DEBUG (default INFO)" )

    args = parser.parse_args()

    if args.verbose:
        logger.setLevel( logging.DEBUG )

    if args.ppdb:
        if args.processing_version is not None:
            logger.warning( "processing_version is ignored when loading the ppdb" )

    loader = DP1ParquetLoader( args.nprocs,
                               args.dir,
                               really_do=args.do,
                               verbose=args.verbose,
                               dont_disable_indexes_fks=args.dont_disable_indexes_fks,
                               processing_version=args.processing_version )
    loader()


# ======================================================================
if __name__ == "__main__":
    main()
