# FEAR AND LOATHING
#
# You really need the native timezone of your postgres server to be UTC.
#
# mongodb just stores everything UTC
#
# We try to convert for this.  However, there are some cases where the conversion
#   doesn't happen.  Our postgres tables are all timezone-aware, but they will
#   be getting imports from timezoneless mongodb.  This means that if the native
#   timezone of the postgres server is not UTC, chaos will rule.
#
# See function util.datetime_to_utc

import io
import sys
import datetime
import argparse
import simplejson
import textwrap
import logging
import traceback
# ****
import pprint
import numpy
import numbers
# ****

import psycopg.sql as sql
import db
import util
from util import FDBLogger


class SourceImporter:
    """Import sources from mongo into postgres.

    Instantiate the object with the processing version (the key into the
    processing_version table).  Then call .import(), passing it the
    MongoDB collection (see db.py::get_mongo_collection) to import from.
    """

    diaobject_fields = [ 'diaobjectid', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]

    diasource_fields = [ 'diasourceid', 'diaobjectid', 'visit', 'band', 'midpointmjdtai',
                         'psfflux', 'psffluxerr', 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]

    diasource_extra_fields = [ 'diasourceid', 'detector', 'x', 'y', 'xerr', 'yerr', 'x_y_cov',
                               'psflnl', 'psfchi2', 'psfndata', 'snr',
                               'scienceflux', 'sciencefluxerr', 'templateflux', 'templatefluxerr',
                               'extendedness', 'reliability', 'ixx', 'iyy', 'ixy', 'ixxpsf', 'iyypsf', 'ixypsf',
                               'flags', 'pixelflags', 'apflux', 'apfluxerr', 'bboxsize',
                               'timeprocessedmjdtai', 'timewithdrawnmjdtai', 'parentdiasourceid' ]

    diaforcedsource_fields = [ 'diaforcedsourceid', 'diaobjectid', 'visit', 'band', 'midpointmjdtai',
                               'psfflux', 'psffluxerr', 'ra', 'dec' ]

    diaforcedsource_extra_fields = [ 'diaforcedsourceid', 'detector', 'scienceflux', 'sciencefluxerr',
                                     'timeprocessedmjdtai', 'timewithdrawnmjdtai' ]


    @classmethod
    def build_flags( cls, flagmap, row ):
        val = 0
        for mask, field in flagmap.items():
            if row[field]:
                val |= mask
        return mask


    def __init__( self, object_base_processing_version=None,
                  object_position_base_processing_version=None,
                  source_base_processing_version=None,
                  forcedsource_base_processing_version=None,
                  host_base_processing_version=None,
                  collection_base_name=None,
                  object_match_radius=1.,
                  debug_just_read_mongo=False ):
        """Create a SourceImporter.

        Parameters
        ----------
          object_base_processing_version : UUID or str
            The processing version for diaobject.  This must be a valid entry (id or
            description) in the base_processing_version table.

          object_position_base_processing_version : UUID or str
            The processing version for diaobject_position.  This must be
            a valid entry (id or description) in the
            base_processing_version table.

          source_base_processing_version : UUID or str
            The processing version for diasource.  This must be a valid entry (id or
            description) in the base_processing_version table.

          forcedsource_base_processing_version : UUID or str
            The processing version for diaforcedsource.  This must be a valid entry (id or
            description) in the base_processing_version table.

          host_base_processing_verson : UUID or str
            The processing version for hsot_galaxy.  This must be a valid entry (id or
            description) in the base_processing_version table.

          collection_base_name: str
            Mongo collections are this plus suffixes.

          object_match_radius : float, default 1.
            Objects within this many arcsec of an existing object will be considered
            the same root_diaobject.

          debug_just_read_mongo : bool, default False
            This is for timing debugging.  If this is given, then nothing will be
            written to postgres (not even writing temporary tables), instead just
            all the results will be read from mongo and then discarded.


        """
        if any( i is None for i in [ object_base_processing_version, object_position_base_processing_version,
                                     source_base_processing_version, forcedsource_base_processing_version ] ):
            raise ValueError( "base processing versions are required" )

        if ( not isinstance( collection_base_name, str ) ) or ( len( collection_base_name ) == 0 ):
            raise ValueError( "collection_base_name must be a non-empty string" )
        self.collection_base_name = collection_base_name

        self.object_base_processing_version = util.base_procver_id( object_base_processing_version, 'diaobject' )
        self.object_position_base_processing_version = util.base_procver_id( object_position_base_processing_version,
                                                                             'diaobject_position' )
        self.source_base_processing_version = util.base_procver_id( source_base_processing_version, 'diasource' )
        self.forcedsource_base_processing_version = util.base_procver_id( forcedsource_base_processing_version,
                                                                          'diaforcedsource' )
        # self.host_base_processing_version = util.base_procver_id( host_base_processing_version )
        self.object_match_radius = float( object_match_radius )

        self.debug_just_read_mongo = bool( debug_just_read_mongo )


    @classmethod
    def _add_mongo_time_limits_to_pipeline( cls, pipeline, t0, t1 ):
        t0 = util.datetime_to_utc( t0, with_tz=False, now_on_none=False )
        t1 = util.datetime_to_utc( t1, with_tz=False, now_on_none=False )

        if t0 is not None:
            if t1 is not None:
                pipeline.append( { "$match": { "$and": [ { "savetime": { "$gt": t0 } },
                                                         { "savetime": { "$lte": t1 } } ] } } )
            else:
                pipeline.append( { "$match": { "savetime": { "$gt": t0 } } } )
        elif t1 is not None:
            pipeline.append( { "$match": { "savetime": { "$lte": t1 } } } )


    def read_mongo_objects( self, dbcon, t0=None, t1=None, batchsize=10000 ):
        """Read all diaobject records from a mongo collection and stick them a temp table.

        Populates temp tables temp_diaobject_import.  It will only live
        as long as the dbcon session is open.

        Parameters
        ----------
          dbcon : db.DBCon

          t0, t1 : datetime.datetime or None
            Time limits.  Will import all objects with t0 < savetime ≤ t1
            If either is None, that limit won't be included.

          batchsize : int, default 10000
            Read rows from the mongodb and copy them tothe postgres temp
            table in batches of this size.  Here so that memory doesn't
            have to get out of hand.

        """

        if not self.debug_just_read_mongo:
            q = sql.SQL( textwrap.dedent(
                """
                CREATE TEMP TABLE temp_diaobject_import (
                  diaobjectid bigint NOT NULL,
                  rootid uuid,
                  base_procver_id uuid,
                  base_pos_procver_id uuid,
                  ra double precision,
                  dec double precision,
                  raerr real,
                  decerr real,
                  ra_dec_cov real )
                """ ) )
            dbcon.execute( q )

        pipeline = []
        self._add_mongo_time_limits_to_pipeline( pipeline, t0, t1 )
        # OK... scary.  Going to first sort on diaobject id, so that we
        #   can use that index, then project to add a field that says if
        #   position is null or not, then sort *again* on diaobjectid
        #   and posisnull to make sure non-null positions come first.
        #   If I understand mongodb correctly, if I project first, then
        #   the sort won't be able ot use the index for sorting
        #   diaobjectid.
        # (ASIDE: I'm not really clear on mongodb and what's the right
        #   thing to do with sorting and grouping.)
        pipeline.extend( [ { "$sort": { 'diaobjectid': 1 } },
                           { "$project": { "diaobjectid": 1,
                                           "diaobjectposition": 1,
                                           "posisnull": {
                                                "$cond": {
                                                    "if": { "$eq": [ 'diaobjectposition', None ] },
                                                    "then": 1,
                                                    "else": 0
                                                }
                                           }
                                          }
                            },
                           { "$sort": { 'diaobjectid': 1, 'posisnull': 1 } },
                           { "$group": { "_id": "$diaobjectid",
                                         "diaobjectid": { "$first": "$diaobjectid" },
                                         "diaobjectposition": { "$first": "$diaobjectposition" }
                                        }
                            } ] )
        with db.MGCon() as mg:
            collection = mg.collection( f"{self.collection_base_name}_diaobject" )
            mongocursor = collection.aggregate( pipeline )
            n = 0

            if self.debug_just_read_mongo:
                for row in mongocursor:
                    # Just to make sure some optimizer doesn't decide it doesn't even have to read
                    n += 1 if 'diaobjectid' in row else 0
                FDBLogger.debug( f"      ...read {n} rows from mongo" )

            else:
                with ( dbcon.cursor.copy( "COPY temp_diaobject_import(diaobjectid, rootid, base_procver_id,\n"
                                          "                           base_pos_procver_id, ra, dec, raerr,\n"
                                          "                           decerr, ra_dec_cov) FROM STDIN" )
                       as pgcopy ):
                    for row in mongocursor:
                        data = [ str(row['diaobjectid']), None, str(self.object_base_processing_version) ]
                        if row['diaobjectposition'] is None:
                            data.extend( [ None, None, None, None, None, None ] )
                        else:
                            data.append( str(self.object_position_base_processing_version) ),
                            for f in [ 'ra', 'dec', 'raerr', 'decerr', 'ra_dec_cov' ]:
                                data.append( None if row['diaobjectposition'][f] is None
                                             else row['diaobjectposition'][f] )
                        pgcopy.write_row( tuple( data ) )
                        n += 1

                FDBLogger.debug( f"      ...wrote {n} rows to temp_diaobject_import" )


    def _read_mongo_fields( self, dbcon, collection, pipeline, fields,
                            temptable, liketable, batchsize=10000,
                            base_procver_id=None ):

        if not self.debug_just_read_mongo:
            q = sql.SQL( "CREATE TEMP TABLE IF NOT EXISTS {temptable} (LIKE {liketable})"
                        ).format( temptable=sql.Identifier(temptable), liketable=sql.Identifier(liketable) )
            dbcon.execute( q )

        # ****
        # strio = io.StringIO()
        # strio.write( "mongo pipeline is:\n" )
        # pprint.pp( pipeline, strio )
        # FDBLogger.debug( strio.getvalue() )
        # ****

        mongocursor = collection.aggregate( pipeline )
        writefields = fields.copy()
        if base_procver_id is not None:
            writefields.append( 'base_procver_id' )
        n = 0

        if self.debug_just_read_mongo:
            gratuitous = 0
            for row in mongocursor:
                gratuitous += 1
            FDBLogger.debug( f"      ...read {gratuitous} rows from mongo" )

        else:
            with dbcon.cursor.copy( f"COPY {temptable}({','.join(writefields)}) FROM STDIN" ) as pgcopy:
                for row in mongocursor:
                    # This is probably inefficient.  Generator to list to tuple.  python makes
                    #   writing this easy, but it's probably doing multiple gratuitous memory copies
                    # ****
                    if 'psfflux' in fields:
                        if ( ( row['psfflux'] is None ) or
                             ( isinstance( row['psfflux'], numbers.Real ) and
                               ( numpy.isnan( row['psfflux'] ) or numpy.isinf( row['psfflux'] ) )
                              )
                            ):
                            strio = io.StringIO()
                            strio.write( "====================== row:\n" )
                            pprint.pp( row, strio )
                            jsondump = simplejson.dumps( row['psfflux'], allow_nan=True )
                            strio.write( f"simplejson: {jsondump}" )
                    # ****
                    data = [ None if row[f] is None
                             else simplejson.dumps(row[f], ignore_nan=True) if isinstance( row[f], dict )
                             else row[f]
                             for f in fields ]
                    if base_procver_id is not None:
                        data.append( base_procver_id )
                    pgcopy.write_row( tuple( data ) )
                    n += 1

            FDBLogger.debug( f"      ...wrote {n} rows to {temptable}" )


    def read_mongo_sources( self, dbcon, t0=None, t1=None, batchsize=10000 ):
        """Read all top-level diaSource records from a mongo collection and stick them in temp tables.

        Populates temp tables temp_diasource_import and
        temp_diasource_extra_import.  They will only live as long as the
        dbcon session is open.

        Parmeters are the same as read_mongo_objects.

        """

        with db.MGCon() as mg:
            group = { "_id": "$diasourceid"}
            group.update( { k: { "$first": f"${k}" } for k in self.diasource_fields } )
            pipeline = []
            self._add_mongo_time_limits_to_pipeline( pipeline, t0, t1 )
            pipeline.append( { "$group": group } )
            collection = mg.collection( f"{self.collection_base_name}_diasource" )
            self._read_mongo_fields( dbcon, collection, pipeline, self.diasource_fields,
                                     "temp_diasource_import", "diasource",
                                     batchsize=batchsize, base_procver_id=self.source_base_processing_version )

            group = { "_id": "$diasourceid" }
            group.update( { k: { "$first": f"${k}" } for k in self.diasource_extra_fields } )
            pipeline = []
            self._add_mongo_time_limits_to_pipeline( pipeline, t0, t1 )
            pipeline.append( { "$group": group } )
            collection = mg.collection( f"{self.collection_base_name}_diasource_extra" )
            self._read_mongo_fields( dbcon, collection, pipeline, self.diasource_extra_fields,
                                     "temp_diasource_extra_import", "diasource_extra",
                                     batchsize=batchsize, base_procver_id=self.source_base_processing_version )


    def read_mongo_prvforcedsources( self, dbcon, t0=None, t1=None, batchsize=10000 ):
        """Read all prvForcedDiaSource records from a mongo collection and stick them in temp tables.

        Gets all prvForcedDiaSources from all sources in the time range.
        Deduplicates.  Populates temp_prvdiaforcedsource_import and
        temp_prvdiaforcedsource_extra_import, which will only live as
        long as the dbcon session is open.

        Parameters are the same as read_mongo_objects.

        """

        with db.MGCon() as mg:
            pipeline = []
            self._add_mongo_time_limits_to_pipeline( pipeline, t0, t1 )
            group = { "_id": "$diaforcedsourceid" }
            group.update( { k: { "$first": f"${k}" } for k in self.diaforcedsource_fields } )
            pipeline.append( { "$group": group } )
            collection = mg.collection( f"{self.collection_base_name}_diaforcedsource" )
            self._read_mongo_fields( dbcon, collection, pipeline, self.diaforcedsource_fields,
                                     "temp_prvdiaforcedsource_import", "diaforcedsource",
                                     batchsize=batchsize, base_procver_id=self.forcedsource_base_processing_version )

            pipeline = []
            self._add_mongo_time_limits_to_pipeline( pipeline, t0, t1 )
            group = { "_id": "$diaforcedsourceid" }
            group.update( { k: { "$first": f"${k}" } for k in self.diaforcedsource_extra_fields } )
            pipeline.append( { "$group": group } )
            collection = mg.collection( f"{self.collection_base_name}_diaforcedsource_extra" )
            self._read_mongo_fields( dbcon, collection, pipeline, self.diaforcedsource_extra_fields,
                                     "temp_prvdiaforcedsource_extra_import", "diaforcedsource_extra",
                                     batchsize=batchsize, base_procver_id=self.forcedsource_base_processing_version )


    def read_mongo_brokerinfo( self, dbcon, t0=None, t1=None, batchsize=1000 ):
        now = datetime.datetime.now( tz=datetime.UTC ).isoformat()

        with db.MGCon() as mg:
            pipeline = []
            self._add_mongo_time_limits_to_pipeline( pipeline, t0, t1 )

            group = { "_id": { "brokername": "$brokername", "topic": "$topic",
                               "diasourceid": "$diasourceid" },
                      "brokername": { "$first": "$brokername" },
                      "topic": { "$first": "$topic" },
                      "diasourceid": { "$first": "$diasourceid" },
                      "diaobjectid": { "$first": "$diaobjectid" },
                      "prv_diasourceid": { "$first": "$prv_diasourceid" },
                      "prv_diaforcedsourceid": { "$first": "$prv_diaforcedsourceid" },
                      "msgtime": { "$first": "$timestamp" },
                      "receivedtime": { "$first": "$savetime" },
                      "importtime": { "$first": now },
                      "info": { "$first": "$info" }
                     }
            pipeline.append( { "$group": group } )
            collection = mg.collection( f"{self.collection_base_name}_brokerinfo" )
            self._read_mongo_fields( dbcon, collection, pipeline, [ "brokername", "topic", "diasourceid",
                                                                    "diaobjectid", "prv_diasourceid",
                                                                    "prv_diaforcedsourceid",
                                                                    "msgtime", "receivedtime", "importtime",
                                                                    "info" ],
                                     "temp_diasource_brokerinfo_import", "diasource_brokerinfo",
                                     batchsize=batchsize, base_procver_id=self.source_base_processing_version )


    def import_objects( self, t0=None, t1=None, batchsize=10000, dbcon=None, commit=True ):
        """Write docs.

        Do.
        """
        with db.DBCon( dbcon ) as dbcon:
            FDBLogger.debug( f"  ...reading mongo to temp table from {t0} to {t1}..." )
            self.read_mongo_objects( dbcon, t0=t0, t1=t1, batchsize=batchsize )

            if self.debug_just_read_mongo:
                return 0, 0, 0

            # Filter the temp table to just new objects
            dbcon.execute( "DROP TABLE IF EXISTS temp_new_diaobject" )
            dbcon.execute( "CREATE TEMP TABLE temp_new_diaobject AS "
                           "( SELECT tdi.* FROM temp_diaobject_import tdi "
                           "  LEFT JOIN diaobject o ON "
                           "    o.diaobjectid=tdi.diaobjectid AND o.base_procver_id=tdi.base_procver_id "
                           "  WHERE o.diaobjectid IS NULL )" )

            # Link new objects to existing root objects
            # TODO : test this with multiple processing versions and multiple
            #   objects that match!!!
            FDBLogger.debug( "   ...linking to existing root diaobjects..." )
            dbcon.execute( "UPDATE temp_new_diaobject tno SET rootid=r.id\n"
                           "FROM root_diaobject r\n"
                           "WHERE q3c_radial_query( r.ra, r.dec, tno.ra, tno.dec, %(rad)s)",
                           { 'rad': self.object_match_radius/3600. } )

            # Create new root objects
            FDBLogger.debug( "   ...creating new root diaobjects..." )
            dbcon.execute( "CREATE TEMP TABLE temp_new_root_obj (id UUID, ra double precision, dec double precision)" )
            dbcon.execute( "INSERT INTO temp_new_root_obj(id, ra, dec) "
                           "( SELECT gen_random_uuid(), ra, dec FROM temp_new_diaobject "
                           "  WHERE rootid IS NULL )" )
            # This next one is byzantine.  I'm trying to say, "hey, there are n
            # rows in temp_new_diaobject that have NULL rootid, and I've just
            # created temp_new_root_obj with n rows, now just fill those n NULL rootids
            # from the n rows in temp_new_root_obj".  There must be a less byzantine
            # way to do this.
            FDBLogger.debug( "   ...filling new rootid into temp table..." )
            dbcon.execute( "UPDATE temp_new_diaobject tno SET rootid=r.id "
                           "FROM ( ( SELECT id, ROW_NUMBER() OVER () AS n FROM temp_new_root_obj ) tnro "
                           "       INNER JOIN "
                           "       ( SELECT diaobjectid, rootid, ROW_NUMBER() OVER () AS n FROM "
                           "         ( SELECT diaobjectid, rootid FROM temp_new_diaobject WHERE rootid IS NULL ) subq "
                           "       ) tnd "
                           "       ON tnro.n=tnd.n ) r "
                        "WHERE r.diaobjectid=tno.diaobjectid" )

            # Add the new root diaobjects
            FDBLogger.debug( "   ...inserting new root objects into root_diaobject tables..." )
            dbcon.execute( "INSERT INTO root_diaobject(id, ra, dec) ( SELECT id, ra, dec FROM temp_new_root_obj )" )
            nroot = dbcon.cursor.rowcount
            FDBLogger.debug( f"      ...inserted {nroot} objects" )

            # Add the new objects.
            FDBLogger.debug( "   ...inserting new diaobjects into diaobject table..." )
            dbcon.execute( "INSERT INTO diaobject(diaobjectid, rootid, base_procver_id)\n"
                           "( SELECT diaobjectid, rootid, base_procver_id FROM temp_new_diaobject )" )
            nobjs = dbcon.cursor.rowcount
            FDBLogger.debug( f"      ...inserted {nobjs} objects" )

            # For diaobject position, it's simpler, we can just do an import and ignore conflicts.

            FDBLogger.debug( "   ...inserting unknown positions into diaobject table..." )
            dbcon.execute( "INSERT INTO diaobject_position(diaobjectid, base_procver_id,\n"
                           "                               ra, dec, raerr, decerr, ra_dec_cov)\n"
                            "( SELECT diaobjectid, base_pos_procver_id, ra, dec, raerr, decerr, ra_dec_cov\n"
                            "  FROM temp_new_diaobject\n"
                            "  WHERE base_pos_procver_id IS NOT NULL )\n"
                            "ON CONFLICT DO NOTHING" )
            npos = dbcon.cursor.rowcount

            if commit:
                FDBLogger.debug("   ...commiting objects" )
                dbcon.commit()

            return nobjs, nroot, npos


    def import_sources( self, t0=None, t1=None, batchsize=10000, dbcon=None, commit=True ):
        """write docs

        Assumes all objects are already imported.

        """

        with db.DBCon( dbcon ) as dbcon:
            # dbcon.execute( "SET CONSTRAINTS fk_diasource_extra_diasource DEFERRED" )

            FDBLogger.debug( f"   ...reading mongo sources to temp table from {t0} to {t1}" )
            self.read_mongo_sources( dbcon, t0=t0, t1=t1, batchsize=batchsize )

            if self.debug_just_read_mongo:
                return 0

            FDBLogger.debug( f"   ...inserting new sources to temp table from {t0} to {t1}" )
            dbcon.execute( "INSERT INTO diasource( SELECT * FROM temp_diasource_import ) ON CONFLICT DO NOTHING" )
            nsrc = dbcon.cursor.rowcount
            FDBLogger.debug( f"      ...inserted {nsrc} sources" )

            # For diasource extra, we want to update fields that are null, just in case some broker
            #   gave us information that a previous broker didn't.
            FDBLogger.debug( "   ...upserting into diasource_extra" )
            q = sql.SQL( "INSERT INTO diasource_extra ( SELECT * FROM temp_diasource_extra_import )\n"
                         "ON CONFLICT (diasourceid, base_procver_id) DO UPDATE SET (\n" )
            first = True
            for f in self.diasource_extra_fields:
                if first:
                    first = False
                else:
                    q += sql.SQL( "," )
                q += sql.Identifier( f )
            q += sql.SQL( ") = (" )
            first = True
            for f in self.diasource_extra_fields:
                if first:
                    first = False
                    q += sql.SQL( "\n  " )
                else:
                    q += sql.SQL( ",\n  " )
                q += sql.SQL( "COALESCE(diasource_extra.{f}, EXCLUDED.{f})" ).format( f=sql.Identifier(f) )
            q += sql.SQL( "\n)" )

            dbcon.execute( q )
            nextra = dbcon.cursor.rowcount
            FDBLogger.debug( f"      ...hit {nextra} rows, but I'm not 100% sure what that means" )

            if commit:
                FDBLogger.debug( "   ...comitting sources" )
                dbcon.commit()

            return nsrc

    def import_forcedsources( self, t0=None, t1=None, batchsize=10000, dbcon=None, commit=True ):
        """Write docs.

        Do.

        """

        with db.DBCon( dbcon ) as dbcon:
            # dbcon.execute( "SET CONSTRAINTS fk_diaforcedsource_extra_diaforcedsource DEFERRED" )

            FDBLogger.debug( f"   ....reading mongo to temp table from {t0} to {t1}" )
            self.read_mongo_prvforcedsources( dbcon, t0=t0, t1=t1, batchsize=batchsize )

            if self.debug_just_read_mongo:
                return 0

            FDBLogger.debug( "   ...inserting new forcedsources" )
            dbcon.execute( "INSERT INTO diaforcedsource "
                           "( SELECT * FROM temp_prvdiaforcedsource_import ) "
                           "ON CONFLICT DO NOTHING" )
            nfrc = dbcon.cursor.rowcount
            FDBLogger.debug( f"      ...inserted {nfrc} rows" )

            # As with sources, for the diaforcedsource_extra table we want to update in case a
            #  broker gives us something that a previous broker didn't.
            FDBLogger.debug( "   ...upserting into diaforcedsource_extra" )
            q = sql.SQL( "INSERT INTO diaforcedsource_extra ( SELECT * FROM temp_prvdiaforcedsource_extra_import )\n"
                          "ON CONFLICT (diaforcedsourceid, base_procver_id) DO UPDATE SET (\n" )
            first = True
            for f in self.diaforcedsource_extra_fields:
                if first:
                    first = False
                else:
                    q += sql.SQL( "," )
                q += sql.Identifier( f )
            q += sql.SQL( ") = (" )
            first = True
            for f in self.diaforcedsource_extra_fields:
                if first:
                    first = False
                    q += sql.SQL( "\n  " )
                else:
                    q += sql.SQL( ",\n  " )
                q += sql.SQL( "COALESCE(diaforcedsource_extra.{f}, EXCLUDED.{f})" ).format( f=sql.Identifier(f) )
            q += sql.SQL( "\n)" )

            dbcon.execute( q )
            nextra = dbcon.cursor.rowcount
            FDBLogger.debug( f"      ...hit {nextra} rows but I'm not 100% sure what that means" )

            if commit:
                FDBLogger.debug( "   ...comitting forcedsources" )
                dbcon.commit()

            return nfrc


    def import_brokerinfo( self, t0=None, t1=None, batchsize=10000, dbcon=None, commit=True ):
        with db.DBCon( dbcon ) as dbcon:
            # dbcon.execute( "SET CONSTRAINTS fk_diasource_brokerinfo_diasource DEFERRED" )

            FDBLogger.debug( f"   ...reading mongo to temp table from {t0} to {t1}" )
            self.read_mongo_brokerinfo( dbcon, t0=t0, t1=t1, batchsize=batchsize )

            if self.debug_just_read_mongo:
                return 0

            FDBLogger.debug( "   ...inserting new brokerinfos" )
            dbcon.execute( "INSERT INTO diasource_brokerinfo "
                           "( SELECT * FROM temp_diasource_brokerinfo_import ) "
                           "ON CONFLICT DO NOTHING" )
            ninfo = dbcon.cursor.rowcount
            FDBLogger.debug( f"   ...inserted {ninfo} brokerinfos" )

            if commit:
                FDBLogger.debug( "   ...comitting brokerinfos" )
                dbcon.commit()

            return ninfo


    def import_cutouts( self, mg, t0=None, t1=None, commit=True ):
        if self.debug_just_read_mongo:
            return None

        collection = mg.collection( f'{self.collection_base_name}_thumbnails' )
        session = mg.client.start_session()
        session.start_transaction()

        if t0 is not None:
            if ( t1 is not None ):
                pipeline = [ { "$match": { "$and": [ { "cutoutdifference": { "$ne": None } },
                                                     { "savetime": { "$gt": t0 } },
                                                     { "savetime": { "$lte": t1 } } ] } } ]
            else:
                pipeline = [ { "$match": { "$and": [ { "cutoutdifference": { "$ne": None } },
                                                     { "savetime": { "$gt": t0 } } ] } } ]
        elif t1 is not None:
            pipeline = [ { "$match": { "$and": [ { "cutoutdifference": { "$ne": None } },
                                                 { "savetime": { "$lte": t1 } } ] } } ]
        else:
            pipeline = [ { "$match": { "cutoutdifference": { "$ne": None } } } ]


        # Going to use cutoutDifference as the canary
        pipeline.extend( [ { "$group": { "_id": "$diasourceid",
                                         "diasourceid": { "$first": "$diasourceid" },
                                         "base_procver_id": { "$first": str( self.source_base_processing_version ) },
                                         "cutoutdifference": { "$first": "$cutoutdifference" },
                                         "cutoutscience": { "$first": "$cutoutscience" },
                                         "cutouttemplate": { "$first": "$cutouttemplate" }
                                        } },
                           { "$merge": { "into": "source_thumbnails",
                                         "on": [ "diasourceid", "base_procver_id" ],
                                         "whenMatched": "keepExisting"
                                        } }
                          ] )
        FDBLogger.debug( "   ...aggregating cutouts to mongo source_thumbnails collection" )
        collection.aggregate( pipeline )

        if commit:
            FDBLogger.debug( "   ...committing to mongo source_thumbnails collection" )
            session.commit_transaction()
            session.end_session()
            return None
        else:
            return session


    # **********************************************************************
    # This is the main method to call from outside
    #
    # It seems that python won't let you name a method "import"

    def import_from_mongo( self, t1=None ):
        """Import data from the mongodb database to PostgreSQL tables.

        Will find all broker alerts saved to the collections between
        when the last time this function ran and the current time.  Will
        impport all diaobject, diasource, and diaforcedsource rows that
        are in the mongodb collection but not yet in PostgreSQL.

        Parameters
        ----------
          t1 : datetime.datetime, default None
            Only import alerts that were saved to the mongo database
            through this time.  If None, will use now.

        Returns
        -------
          nobj, nsrc, nfrc

          Number of objects, sources, and forced sources added to the PostgreSQL database.

        """

        try:
            # Everything happens in one transaction, until the commit() at the end
            #   of this block.  Make sure that none of the functions called
            #   end the transaction in dbcon.
            with db.DBCon() as dbcon:
                timestampexists = False
                t0 = None
                rows, _cols = dbcon.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                                             { 'col': self.collection_base_name } )
                if len(rows) > 0:
                    timestampexists = True
                    t0 = util.datetime_to_utc( rows[0][0], with_tz=True, now_on_none=False )

                t1 = util.datetime_to_utc( t1, with_tz=True, now_on_none=True )

                # Make sure foreign key constraints aren't goign to trip us up
                #   below, but that they're only checked at the end of the transaction.
                if not self.debug_just_read_mongo:
                    dbcon.execute( "SET CONSTRAINTS fk_diasource_diaobject DEFERRED" )
                    dbcon.execute( "SET CONSTRAINTS fk_diaforcedsource_diaobject DEFERRED" )

                FDBLogger.debug( "Importing objects..." )
                nobj, nroot, npos = self.import_objects( t0, t1, dbcon=dbcon, commit=False )
                FDBLogger.debug( "Importing sources..." )
                nsrc = self.import_sources( t0, t1, dbcon=dbcon, commit=False )
                FDBLogger.debug( "Importing forcedsources..." )
                nfrc = self.import_forcedsources( t0, t1, dbcon=dbcon, commit=False )
                FDBLogger.debug( "Importing brokerinfos..." )
                ninfo = self.import_brokerinfo( t0, t1, dbcon=dbcon, commit=False )


                with db.MGCon() as mg:
                    FDBLogger.debug( "Importing cutouts..." )
                    mongosession = self.import_cutouts( mg, t0, t1, commit=False )

                    if not self.debug_just_read_mongo:
                        FDBLogger.debug( "Updating diasource_import_time..." )
                        if timestampexists:
                            dbcon.execute( "UPDATE diasource_import_time SET t=%(t)s WHERE collection=%(col)s",
                                           { 't': t1, 'col': self.collection_base_name } )
                        else:
                            dbcon.execute( "INSERT INTO diasource_import_time(collection,t) "
                                           "VALUES(%(col)s,%(t)s)",
                                           { 't': t1, 'col': self.collection_base_name } )

                    # Only commit once at the end.  That way, if anything goes wrong,
                    #   the database will be rolled back.  No objects or sources will
                    #   have been saved, and the timestamp will not have been updated.
                    # The timestamp will be updated if and only if everything imported.
                    if not self.debug_just_read_mongo:
                        FDBLogger.debug( "Committing postgres..." )
                        dbcon.commit()
                        FDBLogger.debug( "Committing mongo..." )
                        mongosession.commit_transaction()
                        mongosession.end_session()

                FDBLogger.debug( "Done." )

            return nobj, nroot, npos, nsrc, nfrc, ninfo
        except Exception:
            strio = io.StringIO()
            traceback.print_exc( file=strio )
            # This is just so we get the timestamp in the log
            FDBLogger.error( f"Exception:\n{strio.getvalue()}" )
            raise



# ======================================================================

def main():
    parser = argparse.ArgumentParser( 'source_importer.py', description='Import sources from mongo to postgres',
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "-c", "--collection", required=True, nargs='+',
                         help="MongoDB collections to import from" )
    parser.add_argument( "-o", "--object-base-processing-version", default=None,
                         help="Base processing version (uuid or text) to tag imported objects with." )
    parser.add_argument( "-p", "--object-position-base-processing-version", default=None,
                         help="Base processing version (uuid or text) to tag imported object positions with." )
    parser.add_argument( "-s", "--source-base-processing-version", required=True,
                         help="Base processing version (uuid or text) to tag imported sources with." )
    parser.add_argument( "-f", "--forcedsource-base-processing-version", required=True,
                         help="Base processing version (uuid or text) to tag imported forced sources with." )
    parser.add_argument( "-H", "--host-base-processing-version", default=None,
                         help=( "Base processing verson (uuid or text) to tag imported hosts with.  "
                                "Not currently used." ) )
    parser.add_argument( "--t1", default=None, help="Only load alerts received through this time (UTC) (ISO format)" )
    parser.add_argument( "-d", "--debug-just-read-mongo", default=False, action='store_true',
                         help="Don't write to postgres (even temporary tables), just read mongo for timing." )
    parser.add_argument( "-v", "--verbose", action='store_true', default=False,
                         help="Show debug log messages" )
    args = parser.parse_args()

    if args.verbose:
        FDBLogger.setLevel( logging.DEBUG )
    else:
        FDBLogger.setLevel( logging.INFO )

    t1 = util.datetime_to_utc( args.t1, with_tz=True, now_on_none=False )

    totnobj = 0
    totnroot = 0
    totnpos = 0
    totnsrc = 0
    totnfrc = 0
    totninfo = 0

    for collection_name in args.collection:

        FDBLogger.info( f"Importing from {collection_name}*..." )

        si = SourceImporter( object_base_processing_version=args.object_base_processing_version,
                             object_position_base_processing_version=args.object_position_base_processing_version,
                             source_base_processing_version=args.source_base_processing_version,
                             forcedsource_base_processing_version=args.forcedsource_base_processing_version,
                             host_base_processing_version=args.host_base_processing_version,
                             collection_base_name=collection_name,
                             debug_just_read_mongo=args.debug_just_read_mongo )

        try:
            nobj, nroot, npos, nsrc, nfrc, ninfo = si.import_from_mongo( t1=t1 )
        except Exception:
            # The traceback will have been printed in import_from_collection
            FDBLogger.error( "Fail." )
            sys.exit( 1 )

        FDBLogger.info( f"...imported {nobj} objects, {nroot} root objects, {npos} object positions, "
                        f"{nsrc} sources, {nfrc} forced sources, {ninfo} broker infos from {collection_name}" )
        totnobj += nobj
        totnroot += nroot
        totnpos += npos
        totnsrc += nsrc
        totnfrc += nfrc
        totninfo += ninfo

    FDBLogger.info( f"Overall, imported {totnobj} objects, {totnroot} root objects, {totnpos} object positions, "
                    f"{totnsrc} sources, {totnfrc} forced sources, {totninfo} broker infos." )


# ======================================================================
if __name__ == "__main__":
    main()
