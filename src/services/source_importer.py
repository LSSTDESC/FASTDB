import re
import datetime
import argparse

import db
import psycopg.rows


class SourceImporter:
    """Import sources from mongo into postgres.

    Instantiate the object with the processing version (the key into the
    processing_version table).  Then call .import(), passing it the
    MongoDB collection (see db.py::get_mongo_collection) to import from.

    """

    object_lcfields = [ 'diaObjectId', 'radecMjdTai', 'validityStart', 'validityEnd',
                        'ra', 'raErr', 'dec', 'decErr', 'ra_dec_Cov',
                        'nearbyExtObj1', 'nearbyExtObj1Sep', 'nearbyExtObj2', 'nearbyExtObj2Sep',
                        'nearbyExtObj3', 'nearbyExtObj3Sep', 'nearbyLowzGal', 'nearbyLowzGalSep',
                        'parallax', 'parallaxErr', 'pmRa', 'pmRaErr', 'pmRa_parallax_Cov',
                        'pmDec', 'pmDecErr', 'pmDec_parallax_Cov', 'pmRa_pmDec_Cov' ]

    # TODO : flags!
    source_lcfields = [ 'diaSourceId', 'diaObjectId', 'ssObjectId', 'visit', 'detector',
                        'x', 'y', 'xErr', 'yErr', 'x_y_Cov',
                        'band', 'midpointMjdTai', 'ra', 'raErr', 'dec', 'decErr', 'ra_dec_Cov',
                        'psfFlux', 'psfFluxErr', 'psfRa', 'psfDec', 'psfRaErr', 'psfDecErr',
                        'psfra_psfdec_Cov', 'psfFlux_psfRa_Cov', 'psfFlux_psfDec_Cov',
                        'psfLnL', 'psfChi2', 'psfNdata', 'snr',
                        'sciencEFlux', 'scienceFluxErr', 'fpBkgd', 'fpBkgdErr',
                        'parentDiaSourceId', 'extendedness', 'reliability',
                        'ixx', 'ixxErr', 'iyy', 'iyyErr', 'ixy', 'ixyErr',
                        'ixx_ixy_Cov', 'ixx_iyy_Cov', 'iyy_ixy_Cov',
                        'ixxPsf', 'iyyPsf', 'ixyPsf' ]

    forcedsource_lcfields = [ 'diaForcedSourceId', 'diaObjectId', 'visit', 'detector',
                              'midpointMjdTai', 'band', 'ra', 'dec', 'psfFlux', 'psfFluxErr',
                              'scienceFlux', 'scienceFluxErr', 'time_processed', 'time_withdrawn' ]


    def __init__( self, processing_version ):
        """Create a SourceImporter.

        Parameters
        ----------
          processing_version : int
            The processing version.  This must be the key of a valid
            entry in the processing_version table.

        """
        self.processing_version = processing_version


    def _read_mongo_fields( self, pqconn, collection, pipeline, fields, temptable, liketable,
                            t0=None, t1=None, batchsize=10000, procver_fields=['processing_version'] ):
        if not re.search( "^[a-zA-Z0-9_]+$", temptable ):
            raise ValueError( f"Invalid temp table name {temptable}" )
        if not re.search( "^[a-zA-Z0-9_]+$", liketable ):
            raise ValueError( f"Invalid temp table name {liketable}" )
        pqcursor = pqconn.cursor()
        pqcursor.execute( f"CREATE TEMP TABLE {temptable} (LIKE {liketable})" )

        if ( t0 is not None ) or ( t1 is not None ):
            if ( t0 is not None ) and ( t1 is not None ):
                pipeline.insert( 0, { "$match": { "$and": [ { "savetime": { "$gt": t0 } },
                                                            { "savetime": { "$lte": t1 } } ] } } )
            elif t0 is not None:
                pipeline.insert( 0, { "$match": { "savetime": { "$gt": t0 } } } )
            else:
                pipeline.insert( 0, { "$match": { "savetime": { "$lte": t1 } } } )

        mongocursor = collection.aggregate( pipeline )
        writefields = list( fields )
        writefields.extend( procver_fields )
        procverextend = [ self.processing_version for i in procver_fields ]
        with pqcursor.copy( f"COPY {temptable}({','.join(writefields)}) FROM STDIN" ) as pgcopy:
            for row in mongocursor:
                # This is probably inefficient.  Generator to list to tuple.  python makes
                #   writing this easy, but it's probably doing multiple gratuitous memory copies
                data = [ None if row[f] is None else str(row[f]) for f in fields ]
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

        fields = self.object_lcfields
        group = { "_id": "$msg.diaObject.diaObjectId" }
        group.update( { k: { "$first": f"$msg.diaObject.{k}" } for k in fields } )
        pipeline = [ { "$group": group } ]

        self._read_mongo_fields( pqconn, collection, pipeline, fields, "temp_diaobject_import", "diaobject",
                                 t0=t0, t1=t1, batchsize=batchsize )


    def read_mongo_sources( self, pqconn, collection, t0=None, t1=None, batchsize=10000 ):
        """Read all top-level diaSource records from a mongo collection and stick them in a temp table.

        Populates temp table temp_diasource_import.  It will only live
        as long as the pqconn session is open.

        Parmeters are the same as read_mongo_objects.

        """

        fields = self.source_lcfields
        group = { "_id": "$msg.diaSource.diaSourceId" }
        group.update( { k: { "$first": f"$msg.diaSource.{k}" } for k in fields } )
        pipeline = [ { "$group": group } ]

        self._read_mongo_fields( pqconn, collection, pipeline, fields, "temp_diasource_import", "diasource",
                                 t0=t0, t1=t1, batchsize=batchsize,
                                 procver_fields=[ 'processing_version', 'diaobject_procver' ] )


    def read_mongo_prvsources( self, pqconn, collection, t0=None, t1=None, batchsize=10000 ):
        """Read all prvDiaSource records from a mongo collection and stick them in a temp table.

        Gets all prvDiaSources from all sources in the time range.
        Deduplicates.  Populates temp_prvdiasource_import, which will
        only live as long as the pqconn session is open.

        Parameters are the same as read_mongo_objects.

        """

        fields = self.source_lcfields
        group = { "_id": "$msg.prvDiaSources.diaSourceId" }
        group.update( { k: { "$first": f"$msg.prvDiaSources.{k}" } for k in fields } )
        pipeline = [ { "$unwind": "$msg.prvDiaSources" },
                     { "$group": group } ]

        self._read_mongo_fields( pqconn, collection, pipeline, fields, "temp_prvdiasource_import", "diasource",
                                 t0=t0, t1=t1, batchsize=batchsize,
                                 procver_fields=[ 'processing_version', 'diaobject_procver' ] )


    def read_mongo_prvforcedsources( self, pqconn, collection, t0=None, t1=None, batchsize=10000 ):
        """Read all prvForcedDiaSource records from a mongo collection and stick them in a temp table.

        Gets all prvForcedDiaSources from all sources in the time range.
        Deduplicates.  Populates temp_prvdiaforcedsource_import, which will
        only live as long as the pqconn session is open.

        Parameters are the same as read_mongo_objects.

        """

        fields = self.forcedsource_lcfields
        group = { "_id": "$msg.prvDiaForcedSources.diaForcedSourceId" }
        group.update( { k: { "$first": f"$msg.prvDiaForcedSources.{k}" } for k in fields } )
        pipeline = [ { "$unwind": "$msg.prvDiaForcedSources" },
                     { "$group": group } ]

        self._read_mongo_fields( pqconn, collection, pipeline, fields, "temp_prvdiaforcedsource_import",
                                 "diaforcedsource", t0=t0, t1=t1, batchsize=batchsize,
                                 procver_fields=[ 'processing_version', 'diaobject_procver' ] )


    def import_objects_from_collection( self, collection, t0=None, t1=None, batchsize=10000,
                                        conn=None, commit=True ):
        """Write docs.

        Do.
        """
        with db.DB( conn ) as pqconn:
            self.read_mongo_objects( pqconn, collection, t0=t0, t1=t1, batchsize=batchsize )

            cursor = pqconn.cursor()
            cursor.execute( "INSERT INTO diaobject ( SELECT * FROM temp_diaobject_import ) ON CONFLICT DO NOTHING" )
            if commit:
                pqconn.commit()

            return cursor.rowcount


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
            cursor.execute( "SET CONSTRAINTS fk_diasource_diaobject DEFERRED" )
            cursor.execute( "SET CONSTRAINTS fk_diaforcedsource_diaobject DEFERRED" )

            nobj = self.import_objects_from_collection( collection, t0, t1, conn=pqconn, commit=False )
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

        return nobj, nsrc + nprvsrc, nprvfrc


# ======================================================================

def main():
    parser = argparse.ArgumentParser( 'source_importer.py', description='Import sources from mongo to postgres',
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "-p", "--processing-version", required=True,
                         help="Processing version (number or text) to tag imported sources with" )
    parser.add_argument( "-c", "--collection", required=True,
                         help="MongoDB collection to import from" )
    args = parser.parse_args()

    with db.DB() as con:
        cursor = con.cursor( row_factory=psycopg.rows.dict_row )
        subdict = {}
        q = "SELECT * FROM processing_version WHERE "
        pv = args.processing_version
        q += "description=%(pv)s"
        subdict['pv'] = pv
        try:
            ipv = int( pv )
            q += " OR id=%(ipv)s"
            subdict['ipv'] = ipv
        except ValueError:
            pass
        # TODO : validity dates?
        cursor.execute( q, subdict )
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise ValueError( f"Could not find processing version {pv}" )
        if len(rows) > 1:
            raise ValueError( f"More than one processing version matches {pv}, you're probably screwed." )

        ipv = rows[0]['id']

    si = SourceImporter( ipv )
    with db.MG() as mg:
        collection = db.get_mongo_collection( mg, args.collection )
        nobj, nsrc, nfrc = si.import_from_mongo( collection )

    print( f"Imported {nobj} objects, {nsrc} sources, {nfrc} forced sources" )


# ======================================================================
if __name__ == "__main__":
    main()
