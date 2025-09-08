def test_first_alerts( alerts_30days_sent ):
    # TODO
    pass


def test_first_classified( alerts_30days_sent_and_classified ):
    # TODO
    pass


def test_first_consumed( alerts_30days_sent_and_brokermessage_consumed ):
    # TODO
    pass


def test_60days_more_alerts( alerts_60moredays_sent ):
    # TOOD
    pass


def test_60days_more_consumed( alerts_60moredays_sent_and_brokermessage_consumed ):
    # TODO
    pass


# NOTE : to really test this next one, look at the fixture in
#  fixtures/alertcycle.py.  Notice there are two versions, one commented
#  out.  To really test that everything is working, comment out the
#  version that just loads the database, and uncomment the version that
#  actually does stuff.  The database-loader version is there so that
#  other tests that want the full elasticc2 data in there for test purposes
#  can get it there quickly.
def test_full90days( alerts_90days_sent_received_and_imported ):
    # TODO
    pass
