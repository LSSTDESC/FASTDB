import sys
import pathlib
import logging
import fastavro
import json
import multiprocessing
import alerts.models
from django.core.management.base import BaseCommand, CommandError
import signal
import datetime
import time
from psycopg2.extras import execute_values
from psycopg2 import sql
import psycopg2
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import pprint
import urllib.parse


_rundir = pathlib.Path(__file__).parent
print(_rundir)
sys.path.insert(0, str(_rundir) )


class Command(BaseCommand):
    help = 'Store alerts in FASTDB'

    def __init__( self, *args, **kwargs ):
        super().__init__( *args, **kwargs )
        self.logger = logging.getLogger( "FASTDB log" )
        self.logger.propagate = False
        logout = logging.FileHandler( _rundir.parent.parent.parent / f"logs/fastdb.log" )
        self.logger.addHandler( logout )
        formatter = logging.Formatter( f'[%(asctime)s - fastdb - %(levelname)s] - %(message)s',
                                       datefmt='%Y-%m-%d %H:%M:%S' )
        logout.setFormatter( formatter )
        self.logger.setLevel( logging.DEBUG )

    def add_arguments( self, parser ):
        parser.add_argument( '--season', default=1, help="Observing season" )
        parser.add_argument( '--brokers', nargs="*", help="List of brokers" )
        parser.add_argument( '--snapshot', help="Snapshot name" )
        parser.add_argument( '--tag', help="Snapshot Tag" )

    def handle( self, *args, **options ):
        self.logger.info( "********fastdb starting ***********" )

        season = options['season']
        snapshot = options['snapshot']
        
        username = urllib.parse.quote_plus('alert_writer')
        password = urllib.parse.quote_plus(os.environ['MONGODB_PASSWORD'])

        client = MongoClient("mongodb://%s:%s@fastdb-mongodb:27017/alerts" % (username,password))
        db = client.alerts
        collection = db.messages


        # Connect to the PPDB
            
        # Get password

        secret = os.environ['PPDB_READER_PASSWORD']
        conn_string = "host='fastdb-ppdb-psql' dbname='ppdb' user='ppdb_reader' password='%s'" % secret.strip()
        conn = psycopg2.connect(conn_string)
        
        cursor = conn.cursor()
        self.logger.info("Connected to PPDB")


        # Get last update time

        last_update_time = LastUpdateTime.objects.all()
        current_datetime = datetime.datetime.now(tz=datetime.timezone.utc)

        # get ProcessingVersions

        pv = ProcessingVersions.objects.filter(validity_start__gte=current_datetime)

        # get all the alerts that pass at least one of the SN criteria with probability > 0.7 since last_update_time
        # Loop over the brokers that were passed in via the argument list

        brokerstodo = options['brokers']

        list_diaSourceId = []
        
        for name in brokerstodo:
            results = collection.find({"$and":[{"msg.brokerName":name},{"timestamp":{'$gte':last_update_time}},{"msg.classifications":{'$elemMatch':{'$and':[{"classId":{'$in':[2221,2222,2223,2224,2225,2226]}},{"probability":{'$gte':0.7}}]}}}]})
 
            for r in results:

                diaSource_id = r['msg']['diaSourceId']
                alert_id = r['msg']['alert_id']

                bc = BrokerClassification.get_or_create(alert_id=alert_id)
                bc.dia_source = r['msg']['diaSourceId']
                bc.topic_name = r['topic']
                bc.desc_ingest_timestamp =  datetime.datetime.now(tz=datetime.timezone.utc)
                bc.broker_ingest_timestamp = r['msg']['brokerIngestTimestamp']
                broker_version = r['msg']['brokerVersion']
                broker_classifier = BrokerClassifier.objects.get(broker_name=name, broker_version=broker_version]
                bc.classifier = broker_classifier.classifier_id  # Local copy of classifier to circumvent Django Foreign key rules
                bc.classifications = r['msg']['classifications']

                bc.save()
                
                list_diaSourceId.append(diaSource_id)

        # Get unique set of source Ids across all broker alerts

        uniqueSourceId = set(list_diaSourceId)

        # Look for DiaSourceIds in the PPDB DiaSource table

        #columns = diaSourceId,diaObjectId,psFlux,psFluxSigma,midPointTai,ra,decl,snr,filterName,observeDate

        query = sql.SQL( "SELECT * FROM {}  where {} = %s").format(sql.Identifier('DiaSource'),sql.Identifier('diaSourceId'))
        self.logger.info(query.as_string(conn))

        for d in uniqueSourceId:

            cursor.execute(query,(d,))
            if cursor.rowcount != 0:
                result = cursor.fetchone()

                # Store this new Source in the FASTDB

                ds = DiaSource.get_or_create(dia_source=result[0])
                ds.season = season
                ds.filter_name = result[8]
                ds.ra = result[5]
                ds.decl = result[6]
                ds.ps_flux = result[2]
                ds.ps_flux_err = result[3]
                ds.snr = result[7]
                ds.mid_point_tai = result[4]
                
                # Count how many brokers alerted on this Source Id
                ds.broker_count = list_diaSourceId.count(d)
                ds.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)

                diaObjectId = result[1]

                # Now look to see whether we already have this DiaObject in FASTDB
                
                try:
                    do = DiaObject.objects.get(pk=diaObjectId)
                    
                    # Update number of observations
                    
                    do.nobs +=1
                    do.save()
                    
                except DoesNotExist:
                        
                    self.logger.info("DiaObject not in FASTDB. Create new entry.")
                    
                    # Fetch the DiaObject from the PPDB
                    
                    q = sql.SQL("SELECT * from {} where {} = %s").format(sql.Identifier('DiaObject'),sql.Identifier('diaObjectId'))
                    
                    cursor.execute(query,(diaObjectId))
                    if cursor.rowcount != 0:
                        result = cursor.fetchone()
                        do = DiaObject.get_or_create(dia_object=diaObjectId)
                        do.validity_start = result[1]
                        do.season = season
                        do.ra = result[3]
                        do.decl = result[4]
                        do.nobs = 1
                        
                        # locate Host Galaxies in Data release DB Object table
                        # There is information in the PPDB for the 3 closest objects. Is this good enough?
                        # Where to get them in season 1?
                        
                        
                        do.save()


                # Store Foreign key to DiaObject, fake_id, season in DiaSource table

                do = DiaObject.objects.get(pk=diaObjectId)
                ds.dia_object = do
                ds.fake_id = do.fake_id
                ds.season = do.season
                ds.processing_version = pv.version

                ds.save()

                dspvss = DStoPVtoSS.get_or_create(dia_source=d)
                dspvss.processing_version = pv.version
                dspvss.snapshot_name = snapshot
                dspvss.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)

                dspvss.save()
                
                # Look to see if there any ForcedSource entries for this object

                # diaForcedSourceId,diaObjectId,psFlux,psFluxSigma,filterName,observeDate
                q = sql.SQL("SELECT * from {} where {} = %s").format(sql.Identifier('DiaForcedSource'),sql.Identifier('diaObjectId'))
                cursor.execute(query,(diaObjectId))
                if cursor.rowcount != 0:
                    results = cursor.fetchall()
                    for r in results:
                        dfs = DiaForcedSource.get_or_create(dia_forced_source=result[0])
                        dfs.dia_force_source = result[0]
                        dfs.dia_object = do
                        dfs.filter_name = result[4]
                        dfs.ps_flux = result[2]
                        dfs.ps_flux_err = result[3]
                        dfs.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)
                        dfs.processing_version = pv.version

                        dfs.save()

                        dfspvss = DFStoPVtoSS.get_or_create(dia_forced_source=result[0])
                        dfspvss.processing_version = pv.version
                        dfspvss.snapshot_name = snapshot
                        dfspvss.insert_time =  datetime.datetime.now(tz=datetime.timezone.utc)

                        dfspvss.save()
                
  
                    
        cursor.close()
        conn.close()
        
        



        return
