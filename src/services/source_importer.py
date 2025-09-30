import re
import datetime
import argparse

import db

import util


class SourceImporter:
    """Import sources from mongo into postgres.

    Instantiate the object with the processing version (the key into the
    processing_version table).  Then call .import(), passing it the
    MongoDB collection (see db.py::get_mongo_collection) to import from.
    """


    object_lcfields = [ 'diaObjectId', 'radecMjdTai', 'validityStartMjdTai',
                        'ra', 'raErr', 'dec', 'decErr', 'ra_dec_Cov' ]

    object_funcfields = None

    # The following fields may eventually be there, but aren't in the lsst 9.0 alerts
    #                    'nearbyExtObj1', 'nearbyExtObj1Sep', 'nearbyExtObj2', 'nearbyExtObj2Sep',
    #                      'nearbyExtObj3', 'nearbyExtObj3Sep', 'nearbyLowzGal', 'nearbyLowzGalSep',


    source_lcfields = [ 'diaSourceId', 'visit', 'detector', 'diaOjbectId', 'ssObjectId',
                        'parentDiaSourceId', 'midpointMjdTai', 'ra', 'raErr', 'dec', 'decErr', 'ra_dec_Cov',
                        'x', 'xErr', 'y', 'yErr', 'apFlux', 'apFluxErr', 'snr',
                        'psfFlux', 'psfFluxErr', 'psfLnL', 'psfChi2', 'psfNdata',
                        'scienceFlux', 'scienceFluxErr', 'templateFlux', 'templateFluxErr',
                        'ixx', 'iyy', 'ixy', 'ixxPSF', 'iyyPSF', 'ixyPSF',
                        'extendedness', 'reliability', 'band',
                        'timeProcessedMjdTai','timeWithdrawnMjdTai', 'bboxSize' ]

    source_funcfields = { 'flags': lambda row: SourceImporter.build_flags( db.DiaSource._flags_bits, row ),
                          'pixelflags': lambda row: SourceImporter.build_flags( db.DiaSource._pixelflags_bits, row )
                         }

    forcedsource_lcfields = [ 'diaForcedSourceId', 'diaObjectId', 'ra', 'dec', 'visit', 'detector',
                              'psfFlux', 'psfFluxErr', 'midpointMjdTai',
                              'scienceFlux', 'scienceFluxErr', 'band'
                              'timeProcessedMjdTai', 'timeWithdrawnMjdTai' ]

    forcedsource_funcfields = None


    @classmethod
    def build_flags( cls, flagmap, row ):
        val = 0
        for mask, field in flagmap:
            if row[field]:
                val |= mask
        return mask


    def __init__( self, base_processing_version, object_base_processing_version, object_match_radius=1. ):
        """Create a SourceImporter.

        Parameters
        ----------
          base_processing_version : UUID or str
            The processing version.  This must be a valid entry (id or
            description) in the base_processing_version table.

          object_match_radius : float, default 1.
            Objects within this many arcsec of an existing object will be considered
            the same root_diaobject.

        """
        self.base_processing_version = util.base_procver_id( base_processing_version )
        self.object_base_processing_version = util.base_procver_id( object_base_processing_version )
        self.object_match_radius = float( object_match_radius )


    def _read_mongo_fields( self, pqconn, collection, pipeline, lcfields, funcfields,
                            temptable, liketable, t0=None, t1=None, batchsize=10000,
                            procver_fields=['base_procver_id'],
                            isobj=False ):
        if not re.search( "^[a-zA-Z0-9_]+$", temptable ):
            raise ValueError( f"Invalid temp table name {temptable}" )
        if not re.search( "^[a-zA-Z0-9_]+$", liketable ):
            raise ValueError( f"Invalid temp table name {liketable}" )
        pqcursor = pqconn.cursor()
        pqcursor.execute( f"CREATE TEMP TABLE {temptable} (LIKE {liketable})" )
        # Special case hack alert : in the case of diaobject, we have to make
        # the root object nullable in the temp table.  (This is just because
        # when we import objects, they start that way, and only get root ids
        # after we figure out which ones need them and they've been created.)
        if liketable == 'diaobject':
            pqcursor.execute( f"ALTER TABLE {temptable} ALTER COLUMN rootid DROP NOT NULL" )

        if ( t0 is not None ) or ( t1 is not None ):
            if ( t0 is not None ) and ( t1 is not None ):
                pipeline.insert( 0, { "$match": { "$and": [ { "savetime": { "$gt": t0 } },
                                                            { "savetime": { "$lte": t1 } } ] } } )
            elif t0 is not None:
                pipeline.insert( 0, { "$match": { "savetime": { "$gt": t0 } } } )
            else:
                pipeline.insert( 0, { "$match": { "savetime": { "$lte": t1 } } } )

        mongocursor = collection.aggregate( pipeline )
        writefields = [ str(f).lower() for f in lcfields ]
        if funcfields is not None:
            writefields.extend( [ str(f).lower() for f in funcfields ] )
        writefields.extend( procver_fields )
        bpv = self.object_base_processing_version if isobj else self.base_processing_version
        procverextend = [ bpv for i in procver_fields ]
        with pqcursor.copy( f"COPY {temptable}({','.join(writefields)}) FROM STDIN" ) as pgcopy:
            for row in mongocursor:
                # This is probably inefficient.  Generator to list to tuple.  python makes
                #   writing this easy, but it's probably doing multiple gratuitous memory copies
                data = [ None if row[f] is None else str(row[f]) for f in lcfields ]
                for field, func  in funcfields.items():
                    data.append( func(row) )
                data.extend( procverextend )
                pgcopy.write_row( tuple( data ) )


    def read_mongo_objects( self, pqconn, collection, t0=None, t1=None, batchsize=10000 ):
        """Read all diaObject records from a mongo collection and stick them in a temp table.

        Populates temp table temp_diaobject_import.  It will only live
        as long as the pqconn session is open.

        Parameters
        ----------
          pqconn : psycopg.Connection

          collection : pymongo.collection
            The PyMongo collection we're pulling from.

          t0, t1 : datetime.datetime or None
            Time limits.  Will import all objects with t0 < savetime ≤ t1
            If either is None, that limit won't be included.

          batchsize : int, default 10000
            Read rows from the mongodb and copy them tothe postgres temp
            table in batches of this size.  Here so that memory doesn't
            have to get out of hand.

        """

        lcfields = self.object_lcfields
        funcfields = None
        allfields = lcfields if funcfields is None else lcfields + funcfields
        group = { "_id": "$msg.diaObject.diaObjectId" }
        group.update( { k: { "$first": f"$msg.diaObject.{k}" } for k in allfields } )
        pipeline = [ { "$group": group } ]

        self._read_mongo_fields( pqconn, collection, pipeline, lcfields, funcfields,
                                 "temp_diaobject_import", "diaobject",
                                 t0=t0, t1=t1, batchsize=batchsize, isobj=True )


    def read_mongo_sources( self, pqconn, collection, t0=None, t1=None, batchsize=10000 ):
        """Read all top-level diaSource records from a mongo collection and stick them in a temp table.

        Populates temp table temp_diasource_import.  It will only live
        as long as the pqconn session is open.

        Parmeters are the same as read_mongo_objects.

        """

        lcfields = self.source_lcfields
        funcfields = self.source_funcfields
        allfields = lcfields if funcfields is None else lcfields + funcfields
        group = { "_id": { "diaObjectId": "$msg.diaSource.diaObjectId", "visit": "$msg.diaSource.visit" } }
        group.update( { k: { "$first": f"$msg.diaSource.{k}" } for k in allfields } )
        pipeline = [ { "$group": group } ]

        self._read_mongo_fields( pqconn, collection, pipeline, lcfields, funcfields,
                                 "temp_diasource_import", "diasource",
                                 t0=t0, t1=t1, batchsize=batchsize,
                                 procver_fields=[ 'base_procver_id' ] )


    def read_mongo_prvsources( self, pqconn, collection, t0=None, t1=None, batchsize=10000 ):
        """Read all prvDiaSource records from a mongo collection and stick them in a temp table.

        Gets all prvDiaSources from all sources in the time range.
        Deduplicates.  Populates temp_prvdiasource_import, which will
        only live as long as the pqconn session is open.

        Parameters are the same as read_mongo_objects.

        """

        lcfields = self.source_lcfields
        funcfields = self.source_funcfields
        allfields = lcfields if funcfields is None else lcfields + funcfields
        group = { "_id": { "diaObjectId": "$msg.prvDiaSources.diaObjectId", "visit": "$msg.prvDiaSources.visit" } }
        group.update( { k: { "$first": f"$msg.prvDiaSources.{k}" } for k in allfields } )
        pipeline = [ { "$unwind": "$msg.prvDiaSources" },
                     { "$group": group } ]

        self._read_mongo_fields( pqconn, collection, pipeline, lcfields, funcfields,
                                 "temp_prvdiasource_import", "diasource",
                                 t0=t0, t1=t1, batchsize=batchsize,
                                 procver_fields=[ 'base_procver_id' ] )


    def read_mongo_prvforcedsources( self, pqconn, collection, t0=None, t1=None, batchsize=10000 ):
        """Read all prvForcedDiaSource records from a mongo collection and stick them in a temp table.

        Gets all prvForcedDiaSources from all sources in the time range.
        Deduplicates.  Populates temp_prvdiaforcedsource_import, which will
        only live as long as the pqconn session is open.

        Parameters are the same as read_mongo_objects.

        """

        lcfields = self.forcedsource_lcfields
        funcfields = self.forcedsource_funcfields
        allfields = lcfields if funcfields is None else lcfields + funcfields
        group = { "_id": { "diaObjectId": "$msg.prvDiaForcedSources.diaObjectId",
                           "visit": "$msg.prvDiaForcedSources.visit" } }
        group.update( { k: { "$first": f"$msg.prvDiaForcedSources.{k}" } for k in allfields } )
        pipeline = [ { "$unwind": "$msg.prvDiaForcedSources" },
                     { "$group": group } ]

        self._read_mongo_fields( pqconn, collection, pipeline, lcfields, funcfields,
                                 "temp_prvdiaforcedsource_import", "diaforcedsource",
                                 t0=t0, t1=t1, batchsize=batchsize,
                                 procver_fields=[ 'base_procver_id' ] )


    def import_objects_from_collection( self, collection, t0=None, t1=None, batchsize=10000,
                                        conn=None, commit=True ):
        """Write docs.

        Do.
        """
        with db.DB( conn ) as pqconn:
            self.read_mongo_objects( pqconn, collection, t0=t0, t1=t1, batchsize=batchsize )

            cursor = pqconn.cursor()

            # Filter the temp table to just new objects
            cursor.execute( "DROP TABLE IF EXISTS temp_new_diaobject" )
            cursor.execute( "CREATE TEMP TABLE temp_new_diaobject AS "
                            "( SELECT tdi.* FROM temp_diaobject_import tdi "
                            "  LEFT JOIN diaobject o ON "
                            "    o.diaobjectid=tdi.diaobjectid AND o.base_procver_id=tdi.base_procver_id "
                            "  WHERE o.diaobjectid IS NULL )" )

            # Link new objects to existing root objects
            # TODO : test this with multiple processing versions and multiple#
            #   objects that match!!!
            cursor.execute( "UPDATE temp_new_diaobject tno SET rootid=o.rootid "
                            "FROM diaobject o "
                            "WHERE o.base_procver_id=tno.base_procver_id "
                            " AND q3c_radial_query(o.ra, o.dec, tno.ra, tno.dec, %(rad)s)",
                            { 'rad': self.object_match_radius/3600. } )

            # Create new root objects
            cursor.execute( "CREATE TEMP TABLE temp_new_root_obj (id UUID)" )
            cursor.execute( "INSERT INTO temp_new_root_obj "
                            "( SELECT gen_random_uuid() FROM temp_new_diaobject "
                            "  WHERE rootid IS NULL )" )
            # This next one is byzantine.  I'm trying to say, "hey, there are n
            # rows in tmp_new_diaobject that have NULL rootid, and I've just
            # created tmp_new_root_obj with n rows, now just fill those n NULL rootids
            # from the n rows in tmp_new_root_obj".  There must be a less byzantine
            # way to do this.
            cursor.execute( "UPDATE temp_new_diaobject tno SET rootid=r.id "
                            "FROM ( ( SELECT id, ROW_NUMBER() OVER () AS n FROM temp_new_root_obj ) tnro "
                            "       INNER JOIN "
                            "       ( SELECT diaobjectid, rootid, ROW_NUMBER() OVER () AS n FROM "
                            "         ( SELECT diaobjectid, rootid FROM temp_new_diaobject WHERE rootid IS NULL ) subq "
                            "       ) tnd "
                            "       ON tnro.n=tnd.n ) r "
                            "WHERE r.diaobjectid=tno.diaobjectid" )

            # Add the new root diaobjects
            cursor.execute( "INSERT INTO root_diaobject ( SELECT * FROM temp_new_root_obj )" )
            nroot = cursor.rowcount

            # Add the new objects.
            cursor.execute( "INSERT INTO diaobject ( SELECT * FROM temp_new_diaobject )" )
            nobjs = cursor.rowcount

            if commit:
                pqconn.commit()

            return nobjs, nroot


    def import_sources_from_collection( self, collection, t0=None, t1=None, batchsize=10000,
                                        conn=None, commit=True ):
        """write docs

        Assumes all objects are already imported.

        """
        with db.DB( conn ) as pqconn:
            self.read_mongo_sources( pqconn, collection, t0=t0, t1=t1, batchsize=batchsize )

            cursor = pqconn.cursor()
            cursor.execute( "INSERT INTO diasource ( SELECT * FROM temp_diasource_import ) ON CONFLICT DO NOTHING" )
            if commit:
                pqconn.commit()

            return cursor.rowcount


    def import_prvsources_from_collection( self, collection, t0=None, t1=None, batchsize=10000,
                                           conn=None, commit=True ):
        """Write docs.

        Do.

        """
        with db.DB( conn ) as pqconn:
            self.read_mongo_prvsources( pqconn, collection, t0=t0, t1=t1, batchsize=batchsize )

            cursor = pqconn.cursor()
            cursor.execute( "INSERT INTO diasource ( SELECT * FROM temp_prvdiasource_import ) ON CONFLICT DO NOTHING" )
            if commit:
                pqconn.commit()

            return cursor.rowcount


    def import_prvforcedsources_from_collection( self, collection, t0=None, t1=None, batchsize=10000,
                                                 conn=None, commit=True ):
        """Write docs.

        Do.

        """
        with db.DB( conn ) as pqconn:
            self.read_mongo_prvforcedsources( pqconn, collection, t0=t0, t1=t1, batchsize=batchsize )

            cursor = pqconn.cursor()
            cursor.execute( "INSERT INTO diaforcedsource "
                            "( SELECT * FROM temp_prvdiaforcedsource_import ) "
                            "ON CONFLICT DO NOTHING" )
            if commit:
                pqconn.commit()

            return cursor.rowcount


    # **********************************************************************
    # This is the main method to call from outside
    #
    # It seems that python won't let you name a method "import"

    def import_from_mongo( self, collection ):
        """Import data from the mongodb database to PostgreSQL tables.

        Will look at the desired collection.  Will find all broker
        alerts saved to the collection between when the last time this
        function ran and the current time.  Will impport all diaobject,
        diasource, and diaforcedsource rows that are in the mongodb
        collection but not yet in PostgreSQL.

        Parameters
        ----------
          collection : pymongo.collection
            You can get this with:
                import db
                with db.MG() as mgc:
                    collection = db.get_mongo_collection( mgc, collection_name )
            where collection_name is the name of the collection you want.  Make
            sure to call the SoruceImporter object's .import method within the
            same "with db.MG()" block.

        Returns
        -------
          nobj, nsrc, nfrc

          Number of objects, sources, and forced sources added to the PostgreSQL database.

        """

        # Everything happens in one transaction, until the commit() at the end
        #   of this block.  Make sure that none of the functions called
        #   end the transaction in pqconn.
        with db.DB() as pqconn:
            cursor = pqconn.cursor()
            timestampexists = False
            cursor.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                            { 'col': collection.name } )
            rows = cursor.fetchall()
            if len(rows) == 0:
                t0 = datetime.datetime( 1970, 1, 1, 0, 0, 0, tzinfo=datetime.UTC )
            else:
                timestampexists = True
                t0 = rows[0][0]

            t1 = datetime.datetime.now( tz=datetime.UTC )

            # Make sure foreign key constraints aren't goign to trip us up
            #   below, but that they're only checked at the end of the transaction.
            cursor.execute( "SET CONSTRAINTS fk_diasource_diaobjectid DEFERRED" )
            cursor.execute( "SET CONSTRAINTS fk_diaforcedsource_diaobjectid DEFERRED" )

            nobj, nroot = self.import_objects_from_collection( collection, t0, t1, conn=pqconn, commit=False )
            nsrc = self.import_sources_from_collection( collection, t0, t1, conn=pqconn, commit=False )
            nprvsrc = self.import_prvsources_from_collection( collection, t0, t1, conn=pqconn, commit=False )
            nprvfrc = self.import_prvforcedsources_from_collection( collection, t0, t1, conn=pqconn, commit=False )

            if timestampexists:
                cursor.execute( "UPDATE diasource_import_time SET t=%(t)s WHERE collection=%(col)s",
                                { 't': t1, 'col': collection.name } )
            else:
                cursor.execute( "INSERT INTO diasource_import_time(collection,t) "
                                "VALUES(%(col)s,%(t)s)",
                                { 't': t1, 'col': collection.name } )

            # Only commit once at the end.  That way, if anything goes wrong,
            #   the database will be rolled back.  No objects or sources will
            #   have been saved, and the timestamp will not have been updated.
            # The timestamp will be updated if and only if everything imported.
            pqconn.commit()

        return nobj, nroot, nsrc + nprvsrc, nprvfrc


# ======================================================================

def main():
    parser = argparse.ArgumentParser( 'source_importer.py', description='Import sources from mongo to postgres',
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "-p", "--base-processing-version", required=True,
                         help="Base processing version (uuid or text) to tag imported sources with." )
    parser.add_argument( "-o", "--object-base-processing-version", default=None,
                         help=( "Base processing version (uuid or text) to tag imported objects with.  "
                                "Defaults to the same as --base-processing-version" ) )
    parser.add_argument( "-c", "--collection", required=True,
                         help="MongoDB collection to import from" )
    args = parser.parse_args()

    objpv = ( args.base_processing_version
              if args.object_base_processing_version is None
              else args.object_base_processing_version )

    si = SourceImporter( args.base_processing_version, objpv )
    with db.MG() as mg:
        collection = db.get_mongo_collection( mg, args.collection )
        nobj, nsrc, nfrc = si.import_from_mongo( collection )

    print( f"Imported {nobj} objects, {nsrc} sources, {nfrc} forced sources" )


# ======================================================================
if __name__ == "__main__":
    main()
