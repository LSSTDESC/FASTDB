import pytest
import datetime

import db
from services.all_alerts_importer import AllAlertsImporter


def extract_info_from_mongo( mg, mint=None ):
    query = {} if mint is None else { "savetime": { "$gte": mint } }

    acsrcids = set( x['diasourceid'] for x in ( mg.collection( 'fastdb_alertcycle_test_diasource' )
                                                .find( query, projection={ 'diasourceid': 1 } ) ) )
    acsrcexids = set( x['diasourceid'] for x in ( mg.collection( 'fastdb_alertcycle_test_diasource_extra' )
                                                  .find( query, projection={ 'diasourceid': 1 } ) ) )
    acbiids = set( ( x['diasourceid'] , x['brokername'], x['topic'] )
                   for x in ( mg.collection( 'fastdb_alertcycle_test_brokerinfo' )
                              .find( query, projection={ 'diasourceid': 1,
                                                      'brokername': 1,
                                                      'topic': 1 } ) ) )
    acfrcids = set( x['diaforcedsourceid'] for x in ( mg.collection( 'fastdb_alertcycle_test_diaforcedsource' )
                                                      .find( query, projection={ 'diaforcedsourceid': 1 } ) ) )
    acfrcexids = set( x['diaforcedsourceid']
                      for x in ( mg.collection( 'fastdb_alertcycle_test_diaforcedsource_extra' )
                                 .find( query, projection={ 'diaforcedsourceid': 1 } ) ) )
    acobjids = set( x['diaobjectid'] for x in ( mg.collection( 'fastdb_alertcycle_test_diaobject' )
                                                .find( query, projection={ 'diaobjectid': 1 } ) ) )
    acthumbids = set( x['diasourceid'] for x in ( mg.collection( 'fastdb_alertcycle_test_thumbnails' )
                                                  .find( query, projection={ 'diasourceid': 1 } ) ) )

    allsrcids = set( x['diasourceid'] for x in ( mg.collection( 'all_alerts_diasource' )
                                                 .find( query, projection={ 'diasourceid': 1 } ) ) )
    allsrcexids = set( x['diasourceid'] for x in ( mg.collection( 'all_alerts_diasource_extra' )
                                                   .find( query, projection={ 'diasourceid': 1 } ) ) )
    allbiids = set( ( x['diasourceid'] , x['brokername'], x['topic'] )
                    for x in ( mg.collection( 'all_alerts_brokerinfo' )
                               .find( query, projection={ 'diasourceid': 1,
                                                       'brokername': 1,
                                                       'topic': 1 } ) ) )
    allfrcids = set( x['diaforcedsourceid'] for x in ( mg.collection( 'all_alerts_diaforcedsource' )
                                                            .find( query, projection={ 'diaforcedsourceid': 1 } ) ) )
    allfrcexids = set( x['diaforcedsourceid']
                       for x in ( mg.collection( 'all_alerts_diaforcedsource_extra' )
                                  .find( query, projection={ 'diaforcedsourceid': 1 } ) ) )
    allobjids = set( x['diaobjectid'] for x in ( mg.collection( 'all_alerts_diaobject' )
                                                 .find( query, projection={ 'diaobjectid': 1 } ) ) )
    allthumbids = set( x['diasourceid'] for x in ( mg.collection( 'all_alerts_thumbnails' )
                                                   .find( query, projection={ 'diasourceid': 1 } ) ) )

    full_allsrcids = set( x['diasourceid'] for x in ( mg.collection( 'all_alerts_diasource' )
                                                      .find( {}, projection={ 'diasourceid': 1 } ) ) )
    full_allsrcexids = set( x['diasourceid'] for x in ( mg.collection( 'all_alerts_diasource_extra' )
                                                        .find( {}, projection={ 'diasourceid': 1 } ) ) )
    full_allbiids = set( ( x['diasourceid'] , x['brokername'], x['topic'] )
                         for x in ( mg.collection( 'all_alerts_brokerinfo' )
                                    .find( {}, projection={ 'diasourceid': 1,
                                                               'brokername': 1,
                                                               'topic': 1 } ) ) )
    full_allfrcids = set( x['diaforcedsourceid'] for x in ( mg.collection( 'all_alerts_diaforcedsource' )
                                                            .find( {}, projection={ 'diaforcedsourceid': 1 } ) ) )
    full_allfrcexids = set( x['diaforcedsourceid']
                            for x in ( mg.collection( 'all_alerts_diaforcedsource_extra' )
                                       .find( {}, projection={ 'diaforcedsourceid': 1 } ) ) )
    full_allobjids = set( x['diaobjectid'] for x in ( mg.collection( 'all_alerts_diaobject' )
                                                      .find( {}, projection={ 'diaobjectid': 1 } ) ) )
    full_allthumbids = set( x['diasourceid'] for x in ( mg.collection( 'all_alerts_thumbnails' )
                                                        .find( {}, projection={ 'diasourceid': 1 } ) ) )

    return { 'acsrcids': acsrcids,
             'acsrcexids': acsrcexids,
             'acfrcids': acfrcids,
             'acfrcexids': acfrcexids,
             'acobjids': acobjids,
             'acthumbids': acthumbids,
             'acbiids': acbiids,
             'allsrcids': allsrcids,
             'allsrcexids': allsrcexids,
             'allfrcids': allfrcids,
             'allfrcexids': allfrcexids,
             'allobjids': allobjids,
             'allthumbids': allthumbids,
             'allbiids': allbiids,
             'full_allsrcids': full_allsrcids,
             'full_allsrcexids': full_allsrcexids,
             'full_allfrcids': full_allfrcids,
             'full_allfrcexids': full_allfrcexids,
             'full_allobjids': full_allobjids,
             'full_allthumbids': full_allthumbids,
             'full_allbiids': full_allbiids,
             }


@pytest.fixture( scope='module' )
def all_alerts_imported_30days( alerts_30days_sent_and_brokermessage_consumed ):
    try:
        with db.DBCon() as con:
            rows, _cols = con.execute( "SELECT MAX(senttime) FROM ppdb_alerts_sent" )
            tsent = rows[0][0]
            assert tsent < datetime.datetime.now( tz=datetime.UTC )
            rows, _cols = con.execute( "SELECT s.diasourceid\n"
                                       "FROM ppdb_alerts_sent a\n"
                                       "INNER JOIN ppdb_diasource s ON a.diaobjectid=s.diaobjectid\n"
                                       "                           AND a.visit=s.visit\n" )
            alertids = set( r[0] for r in rows )

        importer = AllAlertsImporter( 'fastdb_alertcycle_test' )
        importer()

        yield { 'tsent': tsent,
                'tdone': datetime.datetime.now( tz=datetime.UTC ),
                'alertids': alertids
               }
    finally:
        tables =  [ 'diaobject', 'diasource', 'diasource_extra',
                    'diaforcedsource', 'diaforcedsource_extra',
                    'thumbnails', 'brokerinfo' ]
        allalertcollections = [ f'all_alerts_{s}' for s in tables ]
        with db.MGCon() as mg:
            for col in allalertcollections:
                mg.collection( col ).delete_many( {} )
        with db.DBCon() as con:
            con.execute( "DELETE FROM all_alerts_import_time" )
            con.commit()


@pytest.fixture( scope='module' )
def all_alerts_imported_next60days( all_alerts_imported_30days,
                                    alerts_60moredays_sent_and_brokermessage_consumed ):
    with db.DBCon() as con:
        rows, _cols = con.execute( "SELECT MAX(senttime) FROM ppdb_alerts_sent" )
        tsent = rows[0][0]
        assert tsent < datetime.datetime.now( tz=datetime.UTC )
        rows, _cols = con.execute( "SELECT s.diasourceid\n"
                                   "FROM ppdb_alerts_sent a\n"
                                   "INNER JOIN ppdb_diasource s ON a.diaobjectid=s.diaobjectid\n"
                                   "                           AND a.visit=s.visit\n" )
        allalertids = set( r[0] for r in rows )
        rows, _cols = con.execute( "SELECT s.diasourceid\n"
                                   "FROM ppdb_alerts_sent a\n"
                                   "INNER JOIN ppdb_diasource s ON a.diaobjectid=s.diaobjectid\n"
                                   "                           AND a.visit=s.visit\n"
                                   "WHERE senttime>%(t)s",
                                   { 't': all_alerts_imported_30days['tsent'] }
                                  )
        alertids = set( r[0] for r in rows )

    importer = AllAlertsImporter( 'fastdb_alertcycle_test' )
    importer()

    # The all_alerts_imported_30days fixture handles our cleanup
    return { 'tsent': tsent,
             'tdone': datetime.datetime.now( tz=datetime.UTC ),
             'alertids': alertids,
             'allalertids': allalertids
            }


def test_all_alerts_importer_30days( all_alerts_imported_30days ):
    tsent = all_alerts_imported_30days['tsent']
    tdone = all_alerts_imported_30days['tdone']
    firstsentids = all_alerts_imported_30days['alertids']

    with db.DBCon() as con:
        rows, _cols = con.execute( "SELECT t FROM all_alerts_import_time" )
        tfirstimport = rows[0][0]
        assert tfirstimport > tsent
        assert tfirstimport < tdone
        assert tdone < datetime.datetime.now( tz=datetime.UTC )

    with db.MGCon() as mg:
        mess = extract_info_from_mongo( mg )
        assert len(mess['acbiids']) == 2 * len(firstsentids)
        assert set( x[0] for x in mess['acbiids'] ) == firstsentids
        assert len(mess['allbiids']) == len(mess['acbiids'])
        assert mess['allbiids'] == mess['acbiids']
        assert len(mess['allsrcids']) == len(mess['acsrcids'])
        assert mess['allsrcids'] == mess['acsrcids']
        assert mess['acsrcexids'] == mess['acsrcids']
        assert mess['allsrcexids'] == mess['allsrcids']
        assert len(mess['allfrcids']) == len(mess['acfrcids'])
        assert mess['allfrcids'] == mess['acfrcids']
        assert mess['acfrcexids'] == mess['acfrcids']
        assert mess['allfrcexids'] == mess['allfrcids']
        assert len(mess['acthumbids']) == len(firstsentids)
        assert len(mess['allthumbids']) == len(mess['acthumbids'])
        assert mess['acthumbids'] == firstsentids
        assert mess['allthumbids'] == firstsentids
        assert len(mess['allobjids']) == len(mess['acobjids'])
        assert mess['allobjids'] == mess['allobjids']


def test_all_alerts_importer_next60days( all_alerts_imported_30days, all_alerts_imported_next60days ):
    alertids30 = all_alerts_imported_30days['alertids']
    tdone30 = all_alerts_imported_30days['tdone']
    tsent60 = all_alerts_imported_next60days['tsent']
    tdone60 = all_alerts_imported_next60days['tdone']
    alertids60 = all_alerts_imported_next60days['alertids']
    allalertids = all_alerts_imported_next60days['allalertids']

    with db.DBCon() as con:
        rows, _cols = con.execute( "SELECT t FROM all_alerts_import_time" )
        timport = rows[0][0]
        assert tdone30 < tsent60
        assert timport > tsent60
        assert timport < tdone60
        assert tdone60 < datetime.datetime.now( tz=datetime.UTC )

   # These next couple should be true by constructon
    assert len(alertids30) + len(alertids60) == len(allalertids)
    assert allalertids == alertids30.union( alertids60 )

    with db.MGCon() as mg:
        # We only want alerts whose savetime is late enough to only include the
        #   second batch of alerts.  Those were all sent after time tdone30.
        mess = extract_info_from_mongo( mg, mint=tdone30 )
        assert len(mess['acbiids']) == 2 * len(alertids60)
        assert set( x[0] for x in mess['acbiids'] )  == alertids60
        assert len(mess['allbiids']) == len(mess['acbiids'])
        assert mess['allbiids'] == mess['acbiids']
        assert len(mess['allsrcids']) < len(mess['acsrcids'])  # < because there were repeats in prvDiaSources
        assert mess['allsrcids'].issubset( mess['acsrcids'] )
        assert mess['acsrcexids'] == mess['acsrcids']
        assert mess['allsrcexids'] == mess['allsrcids']
        assert len(mess['allfrcids']) < len(mess['acfrcids']) # again, repeats in prvDiaForcedSources
        assert mess['allfrcids'].issubset( mess['acfrcids'] )
        assert mess['acfrcexids'] == mess['acfrcids']
        assert mess['allfrcexids'] == mess['allfrcids']
        assert len(mess['acthumbids']) == len(alertids60)
        assert len(mess['allthumbids']) == len(mess['acthumbids'])
        assert mess['acthumbids'] == alertids60
        assert mess['allthumbids'] == alertids60
        assert len(mess['allobjids']) < len(mess['acobjids'])  # some objects were already known
        assert mess['allobjids'].issubset( mess['acobjids'])

        assert mess['full_allthumbids'] == allalertids
        assert set( x[0] for x in mess['full_allbiids'] ) == allalertids
        assert mess['acsrcids'].issubset( mess['full_allsrcids'] )
        assert mess['acfrcids'].issubset( mess['full_allfrcids'] )
        assert mess['acobjids'].issubset( mess['full_allobjids'] )


# Right now, I'm anticipating that we're going to do our "save all the alerts" from
#   Pitt-Google, so here's a test based on that.
# ...right now the loop filter is sending the same alert over and over again,
#    so this test doesn't fully work
# @pytest.mark.skipif( not env_as_bool('RUN_PITTGOOGLE_TESTS'), reason='RUN_PITTGOOGLE_TESTS is not set' )
# def test_pittgoogle_all_alerts_importer():
#     barf = "".join( random.choices( 'abcdefghijklmnopqrstuvwxyz', k=6 ) )
#     brokertopic = 'loop'
#     groupid = f'fastdb-test-{barf}'
#     extraconfig = { 'survey': 'lsst',
#                     'name': brokertopic
#                    }
#     os.environ['GOOGLE_CLOUD_PROJECT'] = 'fastdb-test-20251103'
#     os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/secrets/fastdb-test-20251103-5a0f5182da01.json'

#     tables =  [ 'diaobject', 'diasource', 'diasource_extra',
#                 'diaforcedsource', 'diaforcedsource_extra',
#                 'thumbnails', 'brokerinfo', 'alertcache' ]
#     expectedcollections = [ f'fastdb_test_pittgoogle_{s}' for s in tables ]
#     allalertcollections = [ f'all_alerts_{s}' for s in tables if s != 'alertcache' ]

#     try:
#         importer = AllAlertsImporter( collection_base_name='fastdb_test_pittgogole' )
#         pgb = PittGoogleConsumer( groupid=groupid, max_workers=2, batch_maxn=10, batch_maxwait=5, cache_alerts=True,
#                                   schemafile='/fastdb/share/avsc/lsst.v10_0.alert.avsc',
#                                   mongodb_collection_base='fastdb_test_pittgoogle', extraconfig=extraconfig )

#         # First poll
#         FDBLogger.info( "Running PittGoogleBroker.poll() for 10s...." )
#         pgb.poll( restart_time=datetime.timedelta( seconds=10 ), max_restarts=0 )
#         nfirstpoll = pgb.tot_n_messages_consumed
#         FDBLogger.info( f"...got {nfirstpoll} messages.  Importing to all_alerts*" )
#         t0 = datetime.datetime.now( tz=datetime.UTC )
#         importer()
#         t1 = datetime.datetime.now( tz=datetime.UTC )
#         with db.DBCon() as con:
#             rows, _cols = con.execute( "SELECT t FROM all_alerts_import_time" )
#             tfirstimport = rows[0][0]
#             assert tfirstimport > t0
#             assert tfirstimport < t1
#         import pdb; pdb.set_trace()

#         def _extract( mg ):
#             pgsrcids = set( x['diasourceid'] for x in ( mg.collection( 'fastdb_test_pittgoogle_diasource' )
#                                                         .find( {}, projection={ 'diasourceid': 1 } ) ) )
#             pgsrcexids = set( x['diasourceid'] for x in ( mg.collection( 'fastdb_test_pittgoogle_diasource_extra' )
#                                                           .find( {}, projection={ 'diasourceid': 1 } ) ) )
#             pgbiids = set( ( x['diasourceid'] , x['brokername'], x['topic'] )
#                            for x in ( mg.collection( 'fastdb_test_pittgoogle_brokerinfo' )
#                                       .find( {}, projection={ 'diasourceid': 1,
#                                                               'brokername': 1,
#                                                               'topic': 1 } ) ) )
#             pgfrcids = set( x['diaforcedsourceid'] for x in (mg.collection('fastdb_test_pittgoogle_diaforcedsource')
#                                                              .find( {}, projection={ 'diaforcedsourceid': 1 } ) ) )
#             pgfrcexids = set( x['diaforcedsourceid']
#                               for x in ( mg.collection( 'fastdb_test_pittgoogle_diaforcedsource_extra' )
#                                          .find( {}, projection={ 'diaforcedsourceid': 1 } ) ) )
#             pgobjids = set( x['diaobjectid'] for x in ( mg.collection( 'fastdb_test_pittgoogle_diaobject' )
#                                                         .find( {}, projection={ 'diaobjectid': 1 } ) ) )
#             pgthumbids = set( x['diasourceid'] for x in ( mg.collection( 'fastdb_test_pittgoogle_thumbnails' )
#                                                           .find( {}, projection={ 'diasourceid': 1 } ) ) )

#             allsrcids = set( x['diasourceid'] for x in ( mg.collection( 'all_alerts_diasource' )
#                                                          .find( {}, projection={ 'diasourceid': 1 } ) ) )
#             allsrcexids = set( x['diasourceid'] for x in ( mg.collection( 'all_alerts_diasource_extra' )
#                                                            .find( {}, projection={ 'diasourceid': 1 } ) ) )
#             allbiids = set( ( x['diasourceid'] , x['brokername'], x['topic'] )
#                             for x in ( mg.collection( 'all_alerts_brokerinfo' )
#                                        .find( {}, projection={ 'diasourceid': 1,
#                                                                'brokername': 1,
#                                                                'topic': 1 } ) ) )
#             allfrcids = set( x['diaforcedsourceid'] for x in ( mg.collection( 'all_alerts_diaforcedsource' )
#                                                                .find( {}, projection={ 'diaforcedsourceid': 1 } ) ) )
#             allfrcexids = set( x['diaforcedsourceid']
#                                for x in ( mg.collection( 'all_alerts_diaforcedsource_extra' )
#                                           .find( {}, projection={ 'diaforcedsourceid': 1 } ) ) )
#             allobjids = set( x['diaobjectid'] for x in ( mg.collection( 'all_alerts_diaobject' )
#                                                          .find( {}, projection={ 'diaobjectid': 1 } ) ) )
#             allthumbids = set( x['diasourceid'] for x in ( mg.collection( 'all_alerts_thumbnails' )
#                                                            .find( {}, projection={ 'diasourceid': 1 } ) ) )

#             return { 'pgsrcids': pgsrcids,
#                      'pgsrcexids': pgsrcexids,
#                      'pgfrcids': pgfrcids,
#                      'pgfrcexids': pgfrcexids,
#                      'pgobjids': pgobjids,
#                      'pgthumbids': pgthumbids,
#                      'pgbiids': pgbiids,
#                      'allsrcids': allsrcids,
#                      'allsrcexids': allsrcexids,
#                      'allfrcids': allfrcids,
#                      'allfrcexids': allfrcexids,
#                      'allobjids': allobjids,
#                      'allthumbids': allthumbids,
#                      'allbiids': allbiids }

#         with db.MGCon() as mg:
#             mess = _extract( mg )
#             assert mess['allobjids'] == mess['pgobjids']
#             assert mess['allsrcids'] == mess['pgsrcids']
#             assert mess['pgsrcexids'] == mess['pgsrcids']
#             assert mess['allsrcexids'] == mess['pgsrcids']
#             assert mess['allfrcids'] == mess['pgfrcids']
#             assert mess['pgfrcexids'] == mess['pgfrcids']
#             assert mess['allfrcexids'] == mess['pgfrcids']
#             assert mess['pgthumbids'].issubset( mess['pgsrcids'] )
#             assert len( mess['pgthumbids'] ) == nfirstpoll
#             assert mess['allthumbids'] == mess['pgthumbids']
#             assert len( mess['pgbiids'] ) == len( mess['pgsrcids'] )
#             assert mess['allbiids'] == mess['pgbiids']



#     finally:
#         with db.MGCon() as mg:
#             for col in expectedcollections:
#                 mg.collection( col ).drop()
#             for col in allalertcollections:
#                 mg.collection( col ).delete_many( {} )
#         with db.DBCon() as con:
#             con.execute( "DELETE FROM all_alerts_import_time" )
#             con.commit()
