import io
import re


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

    def __init__( self, processing_version ):
        """Create a SourceImporter.

        Parameters
        ----------
          processing_version : int
            The processing version.  This must be the key of a valid
            entry in the processing_version table.

        """
        self.processing_version = processing_version


    def _read_mongo_fields( self, pqconn, collection, savetimecut,
                            temptable, liketable, fields, msgsub, idfield, batchsize=10000,
                            procver_fields=['processing_version'] ):
        if not re.search( "^[a-zA-Z0-9_]+$", temptable ):
            raise ValueError( f"Invalid temp table name {temptable}" )
        if not re.search( "^[a-zA-Z0-9_]+$", liketable ):
            raise ValueError( f"Invalid temp table name {liketable}" )
        pqcursor = pqconn.cursor()
        pqcursor.execute( f"CREATE TEMP TABLE {temptable} (LIKE {liketable})" )

        group = { "_id": f"${idfield}" }
        group.update( { k: { "$first": f"${msgsub}.{k}" } for k in fields } )
        mongocursor = collection.aggregate( [ { "$match": { "savetime": { "$lt": savetimecut } } },
                                              { "$group": group } ] )

        cursize = 0
        strio = None

        def flush_to_db( sourceio ):
            sourceio.seek( 0 )
            columns = [ i.lower() for i in fields ]
            columns.extend( procver_fields )
            pqcursor.copy_from( sourceio, temptable, size=65536, columns=columns )

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


    def read_mongo_objects( self, pqconn, collection, savetimecut, batchsize=10000 ):
        """Read all diaObject records from a mongo collection and stick them in a temp table.

        Parameters
        ----------
          pqconn : psycopg2.Connection

          collection : pymongo.collection
            You can get this with get_collection()

          savetimecut : datetime.datetime
            Import all objects whose savetime is *before* this time.

          batchsize : int, default 10000
            Read rows from the mongodb and copy them tothe postgres temp
            table in batches of this size.  Here so that memory doesn't
            have to get out of hand.

        """
        self._read_mongo_fields( pqconn, collection, savetimecut,
                                 'temp_diaobject_import', 'diaobject', self.object_lcfields,
                                 'msg.diaObject','msg.diaObject.diaObjectId', batchsize=batchsize )


    def read_mongo_sources(  self, pqconn, collection, savetimecut, batchsize=10000 ):
        """Read all top-level diaSource records from a mongo collection and stick them in a temp table.

        Parmeters
        ---------
          pqconn : psycopg2.Connection

          collection : pymongo.collection
            You can get this with get_collection()

          savetimecut : datetime.datetime
            Import all objects whose savetime is *before* this time.

          batchsize : int, default 10000
            Read rows from the mongodb and copy them tothe postgres temp
            table in batches of this size.  Here so that memory doesn't
            have to get out of hand.

        """
        self._read_mongo_fields( pqconn, collection, savetimecut,
                                 'temp_diasource_import', 'diasource', self.source_lcfields,
                                 'msg.diaSource', 'msg.diaSource.diaSourceId', batchsize=batchsize,
                                 procver_fields=[ 'processing_version', 'diaobject_procver' ] )
