# Not going to use BaseTestDB with this because it's a special case

import datetime
import uuid

from db import QueryQueue, DB
from util import asUUID


# This also tests "get"
def test_insert( test_user, src1, src1_pv2 ):
    queryid = uuid.uuid4()
    try:
        qq = QueryQueue(
            queryid = queryid,
            userid = test_user.id,
            submitted = datetime.datetime.now( tz=datetime.UTC ),
            queries = [ 'SELECT * FROM diasource WHERE diaobjectid=%(id)s AND visit=%(visit)s' ],
            subdicts = [ { 'id': 42, 'visit': 64 } ]
        )
        qq.insert()

        with DB() as con:
            cursor = con.cursor()
            cursor.execute( "SELECT * FROM query_queue WHERE queryid=%(id)s", { 'id': str(queryid) } )
            columns = { d.name: i for i, d in enumerate(cursor.description) }
            rows = cursor.fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert asUUID( row[columns['queryid']] ) == qq.queryid
        assert asUUID( row[columns['userid']] ) == test_user.id
        assert row[columns['queries']] == qq.queries
        assert row[columns['subdicts']] == qq.subdicts

        newqq = qq.get( queryid )
        assert all( getattr( newqq, att ) == getattr( qq, att ) for att in [ 'queryid', 'userid', 'submitted',
                                                                             'queries', 'subdicts' ] )

    finally:
        with DB() as con:
            cursor = con.cursor()
            cursor.execute( "DELETE FROM query_queue WHERE queryid=%(id)s", { 'id': str(queryid) } )
            con.commit()
