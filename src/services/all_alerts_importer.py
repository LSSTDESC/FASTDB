import argparse
import logging

import util
from util import FDBLogger
import db


class AllAlertsImporter:
    """Import alert information from a broker cache to the all_alerts mongo tables."""

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

    brokerinfo_fields = [ 'brokername', 'topic', 'diasourceid', 'diaobjectid',
                          'prv_diasourceid', 'prv_diaforcedsourceid', 'msgtime', 'info' ]

    thumbnails_fields = [ 'diasourceid', 'cutoutdifference', 'cutoutscience', 'cutouttemplate' ]


    def __init__( self, collection_base_name=None ):
        self.collection_base_name = collection_base_name

    def __call__( self, t1=None, commit=True ):
        FDBLogger.info( f"Updating all_alerts_* from {self.collection_base_name}*" )

        collections = { 'diaobject': ( "diaobjectid", ),
                        'diasource': ( "diasourceid", ),
                        'diasource_extra': ( "diasourceid", ),
                        'diaforcedsource': ( "diaforcedsourceid", ),
                        'diaforcedsource_extra': ( "diaforcedsourceid", ),
                        'brokerinfo': ( "diasourceid", "brokername", "topic" ),
                        'thumbnails': ( "diasourceid", )
                       }

        with db.DBCon() as dbcon:
            timestampexists= False
            t0 = None
            rows, _cols = dbcon.execute( "SELECT t FROM all_alerts_import_time" )
            if len(rows) > 0:
                timestampexists = True
                t0 = util.datetime_to_utc( rows[0][0], with_tz=True, now_on_none=False )

            t1 = util.datetime_to_utc( t1, with_tz=True, now_on_none=True )

            FDBLogger.info( f"Importing data from {t0} to {t1}" )

            mongosession = None
            with db.MGCon() as mg:
                mongosession = mg.client.start_session()
                mongosession.start_transaction()

                for colsuffix, indices in collections.items():
                    inputcollection = mg.collection( f'{self.collection_base_name}_{colsuffix}' )

                    if t0 is not None:
                        if t1 is not None:
                            pipeline = [ { "$match": { "$and": [ { "savetime": { "$gt": t0 } },
                                                                 { "savetime": { "$lte": t1 } } ] } } ]
                        else:
                            pipeline = [ { "$match": { "savetime": { "$gt": t0 } } } ]
                    elif t1 is not None:
                        pipeline = [ { "$match": { "savetime": { "$lte": t1 } } } ]
                    else:
                        pipeline = []

                    group = { "_id": { c: f"${c}" for c in indices } }
                    group.update( { f: { "$first": f"${f}" } for f in getattr( self, f'{colsuffix}_fields' ) } )
                    group.update( { "savetime": { "$first": "$savetime" } } )

                    pipeline.extend( [ { "$group": group, },
                                       { "$merge": { "into": f"all_alerts_{colsuffix}",
                                                     "on": list(indices),
                                                     "whenMatched": "keepExisting" } },
                                      ] )

                    FDBLogger.info( f"Aggregating {colsuffix} from {self.collection_base_name} to all_alerts..." )
                    inputcollection.aggregate( pipeline )
                    FDBLogger.info( f"...done aggregating {colsuffix}" )

                if timestampexists:
                    dbcon.execute( "UPDATE all_alerts_import_time SET t=%(t)s", {'t': t1} )
                else:
                    dbcon.execute( "INSERT INTO all_alerts_import_time(t) VALUES (%(t)s)", {'t': t1} )

                if commit:
                    mongosession.commit_transaction()
                    dbcon.commit()

                mongosession.end_session()

        FDBLogger.info( "Done updating all_alerts_*" )


# ======================================================================

def main():
    parser = argparse.ArgumentParser( 'all_alerts_importer.py',
                                      description="Import sources to the all_alerts* mongo collections",
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "-c", "--collection", required=True, help="Base name of input collections to import from" )
    parser.add_argument( "--commit", action='store_true', default=False, help="Commit to databases." )
    parser.add_argument( "-v", "--verbose", action='store_true', default=False, help="Show debug log" )
    args = parser.parse_args()

    if args.verbose:
        FDBLogger.set_level( logging.DEBUG )
    else:
        FDBLogger.set_level( logging.INFO )

    importer = AllAlertsImporter( args.collection )
    importer( commit=args.commit )
