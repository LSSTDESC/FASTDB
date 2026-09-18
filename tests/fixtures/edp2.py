import pytest
import textwrap

from psycopg import sql

import db
import ltcv
from admin.load_edp2_parquet import EDP2Loader


@pytest.fixture( scope='module' )
def edp2_loaded_module():
    try:
        loader = EDP2Loader( processing_version='test_load_edp2', create_pv=True )
        loader( "test_data/edp2" )

        yield True

    finally:
        with db.DBCon() as dbcon:
            rows, _cols = dbcon.execute( textwrap.dedent(
                """\
                SELECT b.base_procver_id, b._table FROM base_procver_of_procver b
                INNER JOIN processing_version p ON b.procver_id=p.id
                WHERE p.description='test_load_edp2'
                  AND b._table IN ('diaobject', 'diaobject_position', 'diasource', 'diaforcedsource' )
                """
            ) )

            bpvlist = [ row[0] for row in rows ]
            baseprocvers = { row[1]: row[0] for row in rows }
            if 'diasource' in bpvlist:
                baseprocvers[ 'diasource_extra'] = baseprocvers['diasource']
            if 'diaforcedsource' in bpvlist:
                baseprocvers[ 'diaforcedsource_extra'] = baseprocvers['diaforcedsource']

            for tab in ( 'diaforcedsource_extra', 'diaforcedsource', 'diasource_extra', 'diasource',
                         'diaobject_position', 'diaobject' ):
                if ( tab in baseprocvers ) and ( baseprocvers[tab] in bpvlist ):
                    dbcon.execute( sql.SQL( "DELETE FROM {tab} WHERE base_procver_id={bpv}" )
                                   .format( tab=sql.Identifier(tab), bpv=baseprocvers[tab] ) )

            if len(bpvlist) > 0:
                dbcon.execute( sql.SQL( "DELETE FROM base_procver_of_procver WHERE base_procver_id=ANY(ARRAY[{bpvs}])" )
                               .format( bpvs=sql.SQL(",").join(bpvlist) ) )
                dbcon.execute( sql.SQL( "DELETE FROM base_processing_version WHERE id=ANY(ARRAY[{bpvs}])" )
                               .format( bpvs=sql.SQL(",").join(bpvlist) ) )
            dbcon.execute( "DELETE FROM processing_version WHERE description='test_load_edp2'" )

            dbcon.commit()


@pytest.fixture( scope='module' )
def edp2_materialized_view( edp2_loaded_module ):
    try:
        ltcv.create_object_stats_materialized_view( 'test_load_edp2' )
        yield True
    finally:
        with db.DBCon() as dbcon:
            dbcon.execute( "DROP MATERIALIZED VIEW objstatscomb_test_load_edp2" )
            dbcon.execute( "DROP MATERIALIZED VIEW objstats_test_load_edp2" )
            dbcon.commit()
