import io
import re

import db


class SourceImporter:
    """Import sources from mongo into postgres."""

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

        def flush_to_db( sourceio ):
            sourceio.seek( 0 )
            columns = [ i.lower() for i in fields ]
            columns.extend( procver_fields )
            pqcursor.copy_from( sourceio, temptable, size=65536, columns=columns )

        cursize = 0
        strio = None
        for row in mongocursor:
            if strio is None:
                strio = io.StringIO()
            # ... WORRY.  What if there's a TAB in one of the broker messages?
            strio.write( "\t".join( ( r'\N' if row[f] is None else str(row[f]) ) for f in fields ) )
            if len( procver_fields ) > 0:
                strio.write( "\t" )
                strprocver = str( self.processing_version )
                strio.write( "\t".join( strprocver for f in procver_fields ) )
            strio.write( "\n" )
            cursize += 1
            if cursize >= batchsize:
                flush_to_db( strio )
                strio = None
                cursize = 0

        if cursize > 0:
            flush_to_db( strio )


    def read_mongo_objects( self, pqconn, collection, t0=None, t1=None, batchsize=10000 ):
        """Read all diaObject records from a mongo collection and stick them in a temp table.

        Populates temp table temp_diaobject_import.  It will only live
        as long as the pqconn session is open.

        Parameters
        ----------
          pqconn : psycopg2.Connection

          collection : pymongo.collection
            You can get this with get_collection()

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


    def import_objects_from_collection( self, collection, t0=None, t1=None, batchsize=10000 ):
        with db.DB() as pqconn:
            self.read_mongo_objects( pqconn, collection, t0=t0, t1=t1, batchsize=batchsize )

            cursor = pqconn.cursor()
            cursor.execute( "INSERT INTO diaobject ( SELECT * FROM temp_diaobject_import ) ON CONFLICT DO NOTHING" )
            pqconn.commit()


    def import_sources_from_collection( self, collection, t0=None, t1=None, batchsize=10000 ):
        """write docs

        Assumes all objects are already imported.

        """
        with db.DB() as pqconn:
            self.read_mongo_sources( pqconn, collection, t0=t0, t1=t1, batchsize=batchsize )

            cursor = pqconn.cursor()
            cursor.execute( "INSERT INTO diasource ( SELECT * FROM temp_diasource_import ) ON CONFLICT DO NOTHING" )
            pqconn.commit()


    def import_prvsources_from_collection( self, collection, t0=None, t1=None, batchsize=10000 ):
        """Write docs.

        Do.

        """
        with db.DB() as pqconn:
            self.read_mongo_prvsources( pqconn, collection, t0=t0, t1=t1, batchsize=batchsize )

            cursor = pqconn.cursor()
            cursor.execute( "INSERT INTO diasource ( SELECT * FROM temp_prvdiasource_import ) ON CONFLICT DO NOTHING" )
            pqconn.commit()


    def import_prvforcedsources_from_collection( self, collection, t0=None, t1=None, batchsize=10000 ):
        """Write docs.

        Do.

        """
        with db.DB() as pqconn:
            self.read_mongo_prvforcedsources( pqconn, collection, t0=t0, t1=t1, batchsize=batchsize )

            cursor = pqconn.cursor()
            cursor.execute( "INSERT INTO diaforcedsource "
                            "( SELECT * FROM temp_prvdiaforcedsource_import ) "
                            "ON CONFLICT DO NOTHING" )
            pqconn.commit()
