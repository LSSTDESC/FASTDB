import io
from pprint import pp  # noqa: F401

import db

with db.MG( readonly=True ) as mg:
    col = db.get_mongo_collection( mg, 'fink_alerts' )

    pipeline = [ { "$unwind": "$prvdiasources" },
                 { "$group": { "_id": "$prvdiasources.diasourceid",
                               "prv_diasourceid": { "$first": "$prvdiasources.diasourceid" },
                               "spec1": { "$addToSet": { "diaobjectid": "$diaobjectid",
                                                         "visit": "$prvdiasources.visit" }
                                         },
                               "spec2": { "$addToSet": { "prv_diaobjectid": "$prvdiasources.diaobjectid",
                                                         "visit": "$prvdiasources.visit" } },

                              }
                  },
                 { "$match": { "$or": [ { "spec1.1": { "$exists": True } },
                                        { "spec2.1": { "$exists": True } } ] } },
                 # { "$limit": 3 },
                ]

    findresults = list( col.aggregate( pipeline ) )
    for r in findresults:
        del r[ '_id' ]
    import pdb; pdb.set_trace()

    searchspec1 = []
    searchspec2 = []
    for r in findresults:
        for s in r['spec1']:
            searchspec1.append( { 'prv_diasourceid': r['prv_diasourceid'],
                                  'diaobjectid': s['diaobjectid'],
                                  'visit': s['visit'] } )
        for s in r['spec2']:
            searchspec2.append( { 'prv_diasourceid': r['prv_diasourceid'],
                                  'prv_diaobjectid': s['prv_diaobjectid'],
                                  'visit': s['visit'] } )
    pipeline = [ { "$unwind": "$prvdiasources" },
                 { "$addFields": { "spec1": { 'prv_diasourceid': "$prvdiasources.diasourceid",
                                              'diaobjectid': "$diaobjectid",
                                              'visit': "$prvdiasources.visit" },
                                   "spec2": { 'prv_diasourceid': "$prvdiasources.diasourceid",
                                              'prv_diaobjectid': "$prvdiasources.diaobjectid",
                                              'visit': "$prvdiasources.visit" }
                                  }
                  },
                 { "$match": { "$or": [ { "$expr": { "$in": [ "$spec1", searchspec1 ] } },
                                        { "$expr": { "$in": [ "$spec2", searchspec2 ] } }
                                       ]
                              }
                  },
                 { "$group": { "_id": "$prvdiasources.diasourceid",
                               "prv_diasourceid": { "$first": "$prvdiasources.diasourceid" },
                               "diasourceid": { "$push": "$diasource.diasourceid" },
                               "prv_diaobjectid": { "$push": "$prvdiasources.diaobjectid" },
                               "diaobjectid": { "$push": "$diaobjectid" },
                               "visit": { "$push": "$prvdiasources.visit" },
                              }
                  }
                ]
    results = list( col.aggregate( pipeline ) )
    import pdb; pdb.set_trace()

    strio = io.StringIO()
    strio.write( f"{'previous diaSourceId':>20s} {'diaSourceId':>20s} {'diaObjectId':>20s} "
                 f"{'prv diaObjectId':>20s} {'visit':>20s}\n" )
    strio.write( f"{'-'*20} {'-'*20} {'-'*20} {'-'*20} {'-'*20}\n" )
    lastprv = None
    first = True
    for r in results:
        for s, o, po, v in zip( r['diasourceid'], r['diaobjectid'], r['prv_diaobjectid'], r['visit'] ):
            if lastprv != r['prv_diasourceid']:
                if first:
                    first = False
                else:
                    strio.write("\n")
                lastprv = r['prv_diasourceid']
                strio.write( f"{r['prv_diasourceid']:20d} " )
            else:
                strio.write( f"{' '*20} " )
            strio.write( f"{s:20d} {o:20d} {po:20d} {v:20d}\n" )

    with open( "mongoprob1.out", "w" ) as ofp:
        ofp.write( strio.getvalue() )
