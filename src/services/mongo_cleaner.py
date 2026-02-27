import argparse

import db
from util import FDBLogger


class MongoCleaner:
    def __init__( self ):
        pass

    def clean( self, collection ):
        with db.DBCon() as conn:
            rows = conn.execute( "SELECT t FROM diasource_import_time WHERE collection=%(col)s",
                                 { 'col': collection } )
            if len(rows) == 0:
                FDBLogger.warning( f"Not cleaning anything out, there's no row for {collection} in "
                                   f"diasource_import_time." )
                return
            tmax = rows[0][0]

        import pdb; pdb.set_trace()
        with db.MG() as mongoconn:
            coll = db.get_mongo_collection( mongoconn, collection )
            ndel = coll.delete_many( { "savetime": { "$lte": tmax } } )
            FDBLogger.info( f"Deleted {ndel} documents from collection {collection}" )


def main():
    parser = argparse.ArgumentParser( 'mongo_cleaner.py', description='Remove older messages from mongo cache',
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter )
    parser.add_argument( "collections", required=True, nargs="+",
                         help="names of collections to clean out" )
    parser.add_argument( "--do", action='store_true', default=False,
                         help="Actually do, otherwise just count what would be done." )
    args = parser.parse.args()

    cleaner = MongoCleaner()
    for collection in args.collections:
        cleaner.clean( collection )
