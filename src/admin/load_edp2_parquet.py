from db import DB, RootDiaObject
import uuid

import nested_pandas



def do_one_file( filepath, objbpv, posbpv, srcbpv, frcbpv  ):
    df = nested_pandas.read_parquet( filepath )

    knownobjects = { o: { 'ra': ra, 'dec': dec, 'base_procver_id': objbpv
                          for o, ra, dec in zip( df.diaObjectId, df.ra, df.dec ) } }
        
    with DB() as dbcon:
        # Find root objects, create new ones as necessary
        con.execute( "CREATE TABLE temp_ra_dec(diaobjectid bigint, ra double precision, dec double precision)" )
        with con.cursor.copy( "COPY temp_ra_dec(diaobjectid, ra, dec) FROM STDIN" ) as copier:
            for oid, row in knownobjects.items():
                copier.write_row( [ o, row['ra'], row['dec'] ] )
        rows, _cols = con.execute( "SELECT t.diaobjectid, r.id FROM temp_ra_dec t "
                                   "INNER JOIN root_diaobject r "
                                   "  ON q3c_join(t.ra, t.dec, o.ra, o.dec, 1./3600.)" )
        db.con.execute( "DROP TABLE temp_ra_dec" )
        oldroots = { r[0]: r[1] for r in rows }
        missings = set( objdata['diaobjectid'] ) - set( oldroots.keys() )
        newroots = { m: uuid.uuid4() for m in missings }

        # ...since dictionaries aren't guaranteed sorted, let's not assume that
        #   you even get the keys back in the same order more than once.
        objkeys = list( newroots.keys() )
        newrootdata = { 'rootid': [ newroots[k] for k in objkeys ]
                        'ra': [ knownobjects[k]['ra'] for k in objkeys ]
                        'dec': [ knownobjects[k]['dec'] for k in objkeys ] }

        # diaobject
        objdata = {
            'diaobjectid': df.diaObjectId.to_numpy(),
            'rootid': [ oldroots[o] if o in oldroot else newroots[o] for o in df.diaObjectId ],
            'base_procver_id': np.full( len(df), objbpv )
        }

        # diaobject_position
        posdata = {
            'diaobjectid': df.diaObjectId.to_numpy(),
            'ra': df.ra.to_numpy(),
            'dec': df.dec.to_numpy(),
            'base_procver_id': np.full( len(df), posbpv )
        }

        # diasource
        cols = df[0]['diaSource'].columns.to_list()
        sourcecols = []
        extracols = []
        unknown = set()
        for col in cols:
            if ( ( col in DiaSourceExtra._flags_bits.values() ) or
                 ( col in DiaSourceExtra._pixelflags_bits.values() ) ):
                continue
            elif col.lower() in DiaSource.tablemeta.keys():
                sourcecols.append( col )
            elif col.lower() in DiaSourceExtra.tablemeta.keys():
                extracols.append( col )
            else:
                unknown.add( col )
        if len(unknown) != 0:
            raise ValueError( f"Unknown diaSource columns {unknown}" )

        sourcedf = ( df.iloc[ :, ['diaObjectId', 'diaSource'] ]
                     .set_index( 'diaObjectId' )
                     .explode( 'diaSource' )
                     .reset_index() )
        sourcedata = { c: getattr(sourcedf, c).to_numpy()
                       for c in sourcecols }
        extracols = { c: getattr(sourcedf, c).to_numpy()
                      for c in extracols }


        ROB YOU ARE HERE
        


        RootDiaObject.bulk_insert_or_upsert( newrootdata, dbcon=dbcon, nocommit=True )
        DiaObject.bulk_insert_or_upsert( objdata, dbcon=dbcon, nocommit=True )
        DiaObjectPosition.bulk_insert_or_upsert( objdata, dbcon=dbcon, nocommit=True )
        
