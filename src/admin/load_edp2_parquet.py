import argparse
import pathlib
import uuid
import textwrap
import functools
import multiprocessing
from concurrent.futures import ProcessPoolExecutor


import numpy as np
from psycopg import sql
import nested_pandas

import db
from db import ( DBCon, RootDiaObject, BaseProcessingVersion, DiaObject, DiaObjectPosition,
                 DiaSource, DiaSourceExtra, DiaForcedSource, DiaForcedSourceExtra )
from util import FDBLogger, asUUID


class EDP2Loader:
    def __init__( self, processing_version, create_pv=False, test_only=False, slow_test=False, stop_after_n_files=0 ):
        self.processing_version_passed = processing_version
        self._test_only = test_only or slow_test
        self._slow_test = slow_test
        self._stop_after_n_files = stop_after_n_files
        with DBCon( dictcursor=True ) as pgdb:
            try:
                self.pvid = None
                try:
                    self.pvid = asUUID( processing_version )
                    processing_version = self.pvid
                    col = 'id'
                except Exception:
                    col = 'description'
                if self._slow_test or ( not self._test_only ):
                    pgdb.execute( "LOCK TABLE processing_version" )
                rows = pgdb.execute( sql.SQL( "SELECT * FROM processing_version WHERE {col}={pv}" )
                                     .format( col=sql.Identifier(col), pv=processing_version ) )
                if len(rows) > 0:
                    # ... should we verify that len(rows) is not >1?  By construction it shouldn't be
                    self.pvid = rows[0]['id']
                else:
                    rows = pgdb.execute( sql.SQL( "SELECT * FROM processing_version_alias WHERE description={pv}" )
                                         .format( pv=processing_version ) )
                    if len(rows) > 0:
                        self.pvid = rows[0]['procver_id']

                if self.pvid is None:
                    if create_pv:
                        self.pvid = uuid.uuid4()
                        if self._test_only and ( not self._slow_test ):
                            FDBLogger.info( "Would create processing version {pid}, but test_only is True." )
                        else:
                            FDBLogger.info( f"Creating processing version {processing_version} ({self.pvid})" )
                            pgdb.execute_nofetch(
                                sql.SQL( "INSERT INTO processing_version(id, description) "
                                         "VALUES( {pid}, {desc} )" )
                                .format( pid=self.pvid, desc=processing_version ) )
                    else:
                        raise ValueError( f"Unknown processing version {processing_version}" )
                else:
                    FDBLogger.info( f"Found processing version {processing_version} {(self.pvid)}" )

                rows = pgdb.execute( sql.SQL( textwrap.dedent(
                    """\
                    SELECT b.*
                    FROM (
                      SELECT DISTINCT ON(_table) base_procver_id, _table
                      FROM base_procver_of_procver
                      WHERE procver_id={pv}
                        AND _table IN ('diaobject', 'diaobject_position', 'diasource', 'diaforcedsource')
                      ORDER BY _table, priority DESC
                    ) bpop
                    INNER JOIN base_processing_version b ON b.id=bpop.base_procver_id
                    """
                ) ).format( pv=self.pvid ) )
                rowdict = { row['_table'] : row for row in rows }

                missing = { 'diaobject', 'diaobject_position', 'diasource', 'diaforcedsource' } - set( rowdict.keys() )
                if len(missing) > 0:
                    if create_pv:
                        for table in missing:
                            rowdict[table] = { 'id': uuid.uuid4(),
                                               'description': processing_version,
                                               '_table': table }
                            if self._test_only and ( not self._slow_test ):
                                FDBLogger.info( f"Would create base processing version {rowdict[table]['id']} "
                                                f"for table {table}, but test_only is True" )
                            else:
                                FDBLogger.info( f"Creating base processing version {rowdict[table]['id']} "
                                                f"for table {table}." )
                                pgdb.execute( sql.SQL( textwrap.dedent(
                                    """\
                                    INSERT INTO base_processing_version(id,description,_table)
                                    VALUES({id},{description},{_table})
                                    """
                                ) ).format( **(rowdict[table]) ) )
                                pgdb.execute( sql.SQL( textwrap.dedent(
                                    """\
                                    INSERT INTO base_procver_of_procver(procver_id, base_procver_id, priority, _table)
                                    VALUES ({pvid}, {bpvid}, 0, {table})
                                    """
                                ) ).format( pvid=self.pvid, bpvid=rowdict[table]['id'], table=table ) )
                    else:
                        raise RuntimeError( f"Can't find base provenance id for tables: {missing}" )

                if self._slow_test or ( not self._test_only ):
                    pgdb.commit()

            finally:
                # Make sure to release the lock on processing_version
                pgdb.rollback()

        self.objbpv = BaseProcessingVersion( **(rowdict['diaobject']) )
        self.posbpv = BaseProcessingVersion( **(rowdict['diaobject_position']) )
        self.srcbpv = BaseProcessingVersion( **(rowdict['diasource']) )
        self.frcbpv = BaseProcessingVersion( **(rowdict['diaforcedsource']) )


    def _mp_do_one_file( self, *args, **kwargs ):
        FDBLogger.multiprocessing_replace()
        return self.do_one_file( *args, **kwargs )

    def do_one_file( self, filepath ):
        FDBLogger.info( f"Loading from file {filepath}..." )

        try:
            df = nested_pandas.read_parquet( filepath )

            # Verify that df.diaObjectId is unique... I really hope it is
            if ( len( df.diaObjectId ) != len( set( df.diaObjectId ) ) ):
                raise RuntimeError( f"diaObjectIds are duplicated within file {filepath}" )

            # Find root objects, create new ones as necessary
            with DBCon() as dbcon:
                # Do the root creation in a try / finally because we want to lock the root table
                #   (unless we're doing a test), and the finally makes sure we unlock.
                try:
                    FDBLogger.info( f"Searching for existing root_diaobject for {len(df)} diaobjects..." )
                    knownobjects = { o: { 'ra': ra, 'dec': dec, 'base_procver_id': self.objbpv.id }
                                      for o, ra, dec in zip( df.diaObjectId, df.ra, df.dec ) }
                    if not self._test_only:
                        # My goal: to make sure nobody else inserts into
                        #   the root_diaobject table between when I read
                        #   it and write to it, so I can be sure that
                        #   what I'm writing isn't redundant with
                        #   something already there.  (But, also, anbody
                        #   else who is trying to do the same thing
                        #   should have their read locked until I'm done
                        #   here... which by default won't happen, so
                        #   any other function, e.g. source importer,
                        #   that mucks with root_diaobject would also
                        #   need to use a consistent lock mode!)
                        #
                        # Cf: https://www.postgresql.org/docs/current/explicit-locking.html
                        #
                        # Tried straight-up locking (which defaults to
                        #   ACCESS EXCLUSIVE), and EXCLUSIVE locking.
                        #   In both cases, I saw processes sitting on
                        #   the root acquire lock, where other processes
                        #   were down on the INSERT into the data tables
                        #   (i.e. outside the block where I want the
                        #   lock to apply).  I think the issue is the
                        #   root_diaobject foreign key on diaobject,
                        #   which causes an implicit ROW SHARE lock on
                        #   root_diaobject when we're in a transaction
                        #   that INSERTs into diaobject.
                        #
                        # Trying SHARE ROW EXCLUSIVE because that conflicts
                        #   with itself (so this block will only have one
                        #   process at a time using it), but does not conflict
                        #   with ROW SHARE.  TODO : this needs to be consistent
                        #   with source_importer if this and source_importer
                        #   are going to run at the same time!
                        #
                        # ...that seemed to have worked, but I'm not convinced
                        #   that the overall process is *that* much faster.  I'd
                        #   have to do lots of stress testing to really know.
                        #   Whatevs.  Leaving it running.
                        dbcon.execute_nofetch( "LOCK TABLE root_diaobject IN SHARE ROW EXCLUSIVE MODE" )
                    dbcon.execute( "CREATE TEMP TABLE temp_ra_dec(diaobjectid bigint, "
                                   "ra double precision, dec double precision)" )
                    with dbcon.cursor.copy( "COPY temp_ra_dec(diaobjectid, ra, dec) FROM STDIN" ) as copier:
                        for oid, row in knownobjects.items():
                            copier.write_row( [ oid, row['ra'], row['dec'] ] )
                    rows, _cols = dbcon.execute( "SELECT t.diaobjectid, r.id FROM temp_ra_dec t "
                                                 "INNER JOIN root_diaobject r "
                                                 "  ON q3c_join(t.ra, t.dec, r.ra, r.dec, 1./3600.)" )
                    oldroots = { r[0]: r[1] for r in rows }
                    currentobjs = set( [ int(i) for i in df.diaObjectId ] )
                    missings = set( currentobjs ) - set( oldroots.keys() )
                    FDBLogger.info( f"...found {len(oldroots)} existing root ids, making {len(missings)} new ones..." )

                    newroots = { m: uuid.uuid4() for m in missings }
                    # ...since dictionaries aren't guaranteed sorted, let's not assume that
                    #   you even get the keys back in the same order more than once.
                    #   (So I don't want "for k in newroots.keys() on more than one line below.)
                    objkeys = list( newroots.keys() )
                    newrootdata = { 'id': [ newroots[k] for k in objkeys ],
                                    'ra': [ knownobjects[k]['ra'] for k in objkeys ],
                                    'dec': [ knownobjects[k]['dec'] for k in objkeys ] }
                    if self._test_only:
                        if self._slow_test:
                            RootDiaObject.bulk_insert_or_upsert( newrootdata, dbcon=dbcon, nocommit=True,
                                                                 execute_even_if_nocommit=True )
                        else:
                            FDBLogger.info( "(Not actually inserting new root objects, test_only is True.)" )
                    else:
                        RootDiaObject.bulk_insert_or_upsert( newrootdata, dbcon=dbcon )
                    FDBLogger.info( "...done with root ids." )

                except Exception as ex:
                    FDBLogger.exception( f"Exception finding/creating root objects; rolling back: {ex}" )
                    dbcon.rollback()
                    raise

                finally:
                    dbcon.execute_nofetch( "DROP TABLE IF EXISTS temp_ra_dec" )
                    if not self._test_only:
                        dbcon.commit()
                        # Make sure to release that lock!  ...though I think the
                        #   dbcon.commit() I just did already released it...
                        # If _test_only was true, then we never locked, AND we want
                        #   to keep the transaction for the rest of the test.
                        dbcon.rollback()

                # Figure out all the other stuff to be inserted

                FDBLogger.info( "Building tables to insert objects, sources, forced sources" )

                # diaobject
                objdata = {
                    'diaobjectid': df.diaObjectId.to_numpy(),
                    'rootid': [ oldroots[o] if o in oldroots else newroots[o] for o in df.diaObjectId ],
                    'base_procver_id': np.full( (len(df),), self.objbpv.id )
                }

                # diaobject_position
                posdata = {
                    'diaobjectid': df.diaObjectId.to_numpy(),
                    'ra': df.ra.to_numpy(),
                    'dec': df.dec.to_numpy(),
                    'base_procver_id': np.full( (len(df),), self.posbpv.id )
                }

                # diasource
                sourcedf = ( df.loc[ :, ['diaObjectId', 'diaSource'] ]
                             .set_index( 'diaObjectId' )
                             .explode( 'diaSource' )
                             .reset_index() )
                cols = sourcedf.columns.to_list()
                sourcecols = []
                sourceextracols = []
                sourceflagscols = []
                sourcepixelflagscols = []
                unknown = set()
                source_ignore_cols = {
                    # These are things we don't keep
                    'dipoleFitAttempted', 'dipoleChi2',
                    'dipoleFluxDiff', 'dipoleFluxDiffErr', 'dipoleMeanFlux', 'dipoleMeanFluxErr',
                    'dipoleNdata', 'dipoleAngle', 'dipoleLength',
                    'trailRa', 'trailDec', 'trailLength', 'trailFlux', 'trailFluxErr', 'trailAngle',
                    'coord_ra', 'coord_dec', 'dd_Dec', 'ssObjectId', 'tract',
                    # ...the following columns aren't even show in
                    #   https://sdm-schemas.lsst.io/v/EDP2-deploy-v3/dp2.html#DiaSource
                    # !!!!!!!!!!!  What am I supposed to think?
                    'scienceMag', 'scienceMagErr', 'psfMagErr', 'psfMag'
                }
                # Some fields in edp2 don't mean the same thing as in alerts!  YIKES.  Translate.
                #  (...seems only to be the case in diaForcedSoure)
                source_map_cols = {}
                for col in cols:
                    if col in source_ignore_cols:
                        continue
                    mappedcol = source_map_cols[col] if col in source_map_cols else col
                    whatisthis = True
                    if mappedcol in DiaSourceExtra._flags_bits.values():
                        sourceflagscols.append( col )
                        whatisthis = False
                    elif mappedcol in DiaSourceExtra._pixelflags_bits.values():
                        sourcepixelflagscols.append( col )
                        whatisthis = False
                    else:
                        # Can't just do more elifs because some columns are duplicated
                        #   between diasource and diasource_extra
                        if mappedcol.lower() in DiaSource.tablemeta().keys():
                            sourcecols.append( col )
                            whatisthis = False
                        if mappedcol.lower() in DiaSourceExtra.tablemeta().keys():
                            sourceextracols.append( col )
                            whatisthis = False

                    if whatisthis:
                        unknown.add( col )
                    else:
                        if col not in source_map_cols:
                            source_map_cols[col] = mappedcol

                if len(unknown) != 0:
                    raise ValueError( f"Unknown diaSource columns {unknown}" )

                sourcedata = { source_map_cols[c].lower(): getattr(sourcedf, c).to_numpy() for c in sourcecols }
                sourcedata['base_procver_id'] = np.full( (len(sourcedf),), self.srcbpv.id )
                sourceextradata = { source_map_cols[c].lower(): getattr(sourcedf, c).to_numpy()
                                    for c in sourceextracols }
                sourceextradata['base_procver_id'] = np.full( (len(sourcedf),), self.srcbpv.id )
                sourceextradata['flags'] = np.full( (len(sourcedf),), 0, dtype=np.int32 )
                sourceextradata['pixelflags'] = np.full( (len(sourcedf),), 0, dtype=np.int32 )
                for mask, col in DiaSourceExtra._flags_bits.items():
                    if ( col in source_map_cols ) and ( source_map_cols[col] in sourceflagscols ):
                        sourceextradata['flags'] = np.bitwise_and( sourceextradata['flags'],
                                                                   sourcedf[col].to_numpy() * mask )
                for mask, col in DiaSourceExtra._pixelflags_bits.items():
                    if ( col in source_map_cols ) and ( source_map_cols[col] in sourcepixelflagscols ):
                        sourceextradata['pixelflags'] = np.bitwise_and( sourceextradata['pixelflags'],
                                                                        sourcedf[col].to_numpy() * mask )

                # diaforcedsource
                forceddf = ( df.loc[ :, ['diaObjectId', 'diaObjectForcedSource'] ]
                             .set_index( 'diaObjectId' )
                             .explode( 'diaObjectForcedSource' )
                             .reset_index() )
                cols = forceddf.columns.to_list()
                forcedcols = []
                forcedextracols = []
                forcedflagscols = []
                forcedpixelflagscols = []
                unknown = set()
                forced_ignore_cols = {
                    # This first set conflicts with mapped columns!
                    'psfFlux', 'psfFluxErr', 'psfFlux_flag',
                    # THe rest we just don't keep
                    'coord_ra', 'coord_dec', 'tract', 'patch', 'parentObjectId',
                    # ...the following columns aren't even show in
                    #   https://sdm-schemas.lsst.io/v/EDP2-deploy-v3/dp2.html#ForcedSourceOnDiaObject
                    # !!!!!!!!!!!  What am I supposed to think?
                    'psfMag', 'psfMagErr', 'psfMagErr_corrected',
                    'psfFluxErr_corrected', 'psfFluxErr_corrected_flag',
                    'psfDiffFluxErr_corrected', 'psfDiffFluxErr_corrected_flag'
                }
                # Some fields in edp2 don't mean the same thing as in alerts!  YIKES.  Translate.
                forced_map_cols = {
                    'psfDiffFlux': 'psfFlux',
                    'psfDiffFluxErr': 'psfFluxErr',
                    'psfDiffFlux_flag': 'psfFlux_flag'
                }
                for col in cols:
                    if col in forced_ignore_cols:
                        continue
                    mappedcol = forced_map_cols[col] if col in forced_map_cols else col
                    whatisthis = True
                    if mappedcol in DiaForcedSourceExtra._flags_bits.values():
                        forcedflagscols.append( col )
                        whatisthis = False
                    elif mappedcol in DiaForcedSourceExtra._pixelflags_bits.values():
                        forcedpixelflagscols.append( col )
                        whatisthis = False
                    else:
                        # Can't just do more elifs because some columns are duplicated
                        #   between diaforcedsource and diaforcedsource_extra
                        if mappedcol.lower() in DiaForcedSource.tablemeta().keys():
                            forcedcols.append( col )
                            whatisthis = False
                        if mappedcol.lower() in DiaForcedSourceExtra.tablemeta().keys():
                            forcedextracols.append( col )
                            whatisthis = False

                    if whatisthis:
                        unknown.add( col )
                    else:
                        if col not in forced_map_cols:
                            forced_map_cols[col] = mappedcol

                if len(unknown) != 0:
                    raise ValueError( f"Unknown diaObjectForcedSource columns {unknown}" )

                forceddata = { forced_map_cols[c].lower(): getattr(forceddf, c).to_numpy() for c in forcedcols }
                forceddata['base_procver_id'] = np.full( (len(forceddf),), self.frcbpv.id )
                forcedextradata = { forced_map_cols[c].lower(): getattr(forceddf, c).to_numpy()
                                    for c in forcedextracols }
                forcedextradata['base_procver_id'] = np.full( (len(forceddf),), self.frcbpv.id )
                forcedextradata['flags'] = np.full( (len(forceddf),), 0, dtype=np.int32 )
                forcedextradata['pixelflags'] = np.full( (len(forceddf),), 0, dtype=np.int32 )
                for mask, col in DiaForcedSourceExtra._flags_bits.items():
                    if ( col in forced_map_cols ) and ( forced_map_cols[col] in forcedflagscols ):
                        forcedextradata['flags'] = np.bitwise_and( forcedextradata['flags'],
                                                                   forceddf[col].to_numpy() * mask )
                for mask, col in DiaForcedSourceExtra._pixelflags_bits.items():
                    if ( col in forced_map_cols ) and ( forced_map_cols[col] in forcedpixelflagscols ):
                        forcedextradata['pixelflags'] = np.bitwise_and( forcedextradata['pixelflags'],
                                                                   forceddf[col].to_numpy() * mask )

                #  Actually insert.  Do it all in one transaction.
                FDBLogger.info( f"Inserting {len(objdata['diaobjectid'])} diaobjects, "
                                f"{len(posdata['diaobjectid'])} positions, "
                                f"{len(sourcedata['diasourceid'])} sources, "
                                f"{len(forceddata['diaobjectid'])} forced sources." )
                if self._slow_test or ( not self._test_only ):
                    DiaObject.bulk_insert_or_upsert( objdata, dbcon=dbcon, nocommit=True,
                                                     execute_even_if_nocommit=True )
                    DiaObjectPosition.bulk_insert_or_upsert( posdata, dbcon=dbcon, nocommit=True,
                                                             execute_even_if_nocommit=True )
                    DiaSource.bulk_insert_or_upsert( sourcedata, dbcon=dbcon, nocommit=True,
                                                     execute_even_if_nocommit=True )
                    DiaSourceExtra.bulk_insert_or_upsert( sourceextradata, dbcon=dbcon, nocommit=True,
                                                          execute_even_if_nocommit=True )
                    DiaForcedSource.bulk_insert_or_upsert( forceddata, dbcon=dbcon, nocommit=True,
                                                           execute_even_if_nocommit=True )
                    DiaForcedSourceExtra.bulk_insert_or_upsert( forcedextradata, dbcon=dbcon, nocommit=True,
                                                                execute_even_if_nocommit=True )

                    if self._slow_test:
                        FDBLogger.info( "(Not commtting inserts, test_only is true, rolling back.)" )
                        dbcon.rollback()
                    else:
                        dbcon.commit()
                else:
                    FDBLogger.info( "(Not actually inserting, test_only is True.)" )

            FDBLogger.info( f"....done {'scanning' if self._test_only else 'loading'} file {filepath}" )
            return ( True, filepath, None )

        except Exception as ex:
            FDBLogger.exception( f"Exception processing file {filepath}: {ex}" )
            return ( False, filepath, str(ex) )


    def troll_directory( self, direc ):
        direc = pathlib.Path( direc ).resolve()
        FDBLogger.info( f"Looking for parquet files in {direc}..."  )

        allfiles = []
        direc = pathlib.Path( direc )
        subdirs = []
        for item in direc.iterdir():
            if item.is_dir():
                subdirs.append( item.resolve() )
            elif ( len(item.name) >= 8 ) and ( item.name[-8:] == '.parquet' ):
                allfiles.append( item.resolve() )

        for item in subdirs:
            allfiles.extend( self.troll_directory( item ) )

        return allfiles


    def run_many_files( self, files, numprocs=1 ):
        successes = []
        failures = []
        loading = "Loading" if not self._test_only else "Scanning (but not loading, test_only is True)"
        if ( self._stop_after_n_files > 0 ) and ( len(files) > self._stop_after_n_files ):
            FDBLogger.warning( f"Found {len(files)} files, but only procesing {self._stop_after_n_files} "
                               f"files as requested." )
            files = files[:self._stop_after_n_files]

        if numprocs <= 1:
            FDBLogger.info( f"{loading} {len(files)} parquet files serially" )
            for pqfile in files:
                result = self.do_one_file( pqfile )
                if result[0]:
                    successes.append( str(pqfile) )
                else:
                    failures.append( str(pqfile) )
                    FDBLogger.error( f"Failure running {pqfile}: {result[2]}" )
        else:
            FDBLogger.info( f"{loading} {len(files)} parquet files in {numprocs} processes" )
            executor = ProcessPoolExecutor( max_workers=numprocs,
                                            mp_context=multiprocessing.get_context("fork") )
            doer = functools.partial( self.__class__._mp_do_one_file, self )
            for result in executor.map( doer, files ):
                if result[0]:
                    successes.append( str(result[1]) )
                else:
                    failures.append( str(result[1]) )
                    FDBLogger.error( f"Failure running {result[1]}: {result[2]}" )

        nl = "\n    "
        FDBLogger.info( f"Succeeded on:{nl}{nl.join(successes)}" )
        FDBLogger.info( f"Failed on:{nl}{nl.join(failures)}" )
        FDBLogger.info( f"EDP2Loader.run_many_files done, {len(successes)} succeeded, {len(failures)} failed." )
        if self._test_only:
            FDBLogger.warning( "test_only was True, didn't actually load anything!" )


    def __call__( self, direc, numprocs=1 ):
        FDBLogger.info( "**********************************************************************" )
        FDBLogger.info( f"Looking for parquet files to load into processing version "
                        f"{self.processing_version_passed} ({self.pvid})" )
        files = self.troll_directory( direc )
        FDBLogger.info( "**********************************************************************" )
        self.run_many_files( files, numprocs=numprocs )


# ======================================================================

def main():
    parser = argparse.ArgumentParser( 'load_edp2_parquet.py',
                                      description="Load EDP2 parquest files underneath a directory." )
    parser.add_argument( "direc", help="Top level directory to search for .parquet files (recurses)" )
    parser.add_argument( "-p", "--processing-version", required=True,
                         help="Description (or uuid) of the processing version to import these to." )
    parser.add_argument( "-c", '--create-processing-version', default=False, action='store_true',
                         help=( "Normally, the processing version must already exist.  Set this to create it "
                                "if it doesn't.  Do not use this if you pass a uuid to -p!" ) )
    parser.add_argument( "-n", "--numprocs", default=1, type=int,
                         help=( "Number of subprocesses to run.  If 1, no subprocesses, run everything "
                                "serially in the main procss." ) )
    parser.add_argument( "--do", action="store_true", default=False,
                         help=( "Actually load the database; otherwise, only reads all the parquet files.  "
                                "Note: *will* create some temp tables in the database even without --do!  "
                                "--do is ignored if --slow-test is given" ) )
    parser.add_argument( "--slow-test", action="store_true", default=False,
                         help=( "Normally, without --do, just read all parquet files.  "
                                "With --slow-test, will do all of the database command, just won't commit.  Note "
                                "That this is still not quite the same as the full thing, because root objects "
                                "found in one file will not be present when run for a subsequent file.  "
                                "WARNING: processing versions *will* be committed to the database in this case!" ) )
    parser.add_argument( "--stop-after-n-files", default=0, type=int,
                         help="If >0, stop after finding this many files.  For testing purposes." )
    parser.add_argument( "-v", "--verbose", action='store_true', default=False,
                         help="Log at DEBUG (default INFO)" )
    args = parser.parse_args()

    FDBLogger.setLevel( "DEBUG" if args.verbose else "INFO" )

    # HACK ALERT
    # I want debug messages, but echo queries is too much.  Turn it off even if the default is on
    db._echoqueries = False

    loader = EDP2Loader( args.processing_version,
                         create_pv=args.create_processing_version,
                         test_only=not args.do,
                         slow_test=args.slow_test,
                         stop_after_n_files=args.stop_after_n_files )
    loader( args.direc, numprocs=args.numprocs )


# ======================================================================
if __name__ == "__main__":
    main()
