import pytest

import psycopg

import db


# TODO : write things that capture the log output
#   to make sure that echoqueries and alwaysexplain
#   works.  (Or, don't bother testing that.  Testing
#   that is really too anal.  Just look at the output
#   and feel good about yourself.)
def test_DBCon( test_user ):
    # Test basic access
    dbcon = db.DBCon()

    rows, cols = dbcon.execute( "SELECT * FROM authuser" )
    assert set(cols) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
    assert len(rows) == 1
    coldex = { cols[i]: i for i in range(len(cols)) }
    assert rows[0][coldex['username']] == 'test'

    # Make sure the connection closes
    dbcon.close()
    with pytest.raises( psycopg.OperationalError, match="the connection is closed" ):
        dbcon.execute( "SELECT * FROM authuser" )

    # Test a dictionary cursor
    dbcon = db.DBCon( dictcursor=True )
    rows = dbcon.execute( "SELECT * FROM authuser" )
    assert len(rows) == 1
    assert isinstance( rows[0], dict )
    assert set( rows[0].keys() ) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
    assert rows[0]['username'] == 'test'

    # Try a different test that the connection closed
    dbcon.close()
    with pytest.raises( psycopg.OperationalError, match="the connection is closed" ):
        dbcon.remake_cursor()

    # Try remaking the cursor
    dbcon = db.DBCon()
    rows, cols = dbcon.execute( "SELECT * FROM authuser" )
    assert set(cols) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
    assert len(rows) == 1
    coldex = { cols[i]: i for i in range(len(cols)) }
    assert rows[0][coldex['username']] == 'test'
    dbcon.remake_cursor()
    rows, cols = dbcon.execute( "SELECT * FROM authuser" )
    assert set(cols) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
    assert len(rows) == 1
    coldex = { cols[i]: i for i in range(len(cols)) }
    assert rows[0][coldex['username']] == 'test'
    dbcon.remake_cursor( dictcursor=True )
    rows = dbcon.execute( "SELECT * FROM authuser" )
    assert len(rows) == 1
    assert isinstance( rows[0], dict )
    assert set( rows[0].keys() ) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
    assert rows[0]['username'] == 'test'
    dbcon.remake_cursor()
    rows, cols = dbcon.execute( "SELECT * FROM authuser" )
    assert set(cols) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
    assert len(rows) == 1
    coldex = { cols[i]: i for i in range(len(cols)) }
    assert rows[0][coldex['username']] == 'test'
    dbcon.close()

    dbcon = db.DBCon( dictcursor=True )
    rows = dbcon.execute( "SELECT * FROM authuser" )
    assert len(rows) == 1
    assert isinstance( rows[0], dict )
    assert set( rows[0].keys() ) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
    assert rows[0]['username'] == 'test'
    dbcon.remake_cursor( dictcursor=False )
    rows, cols = dbcon.execute( "SELECT * FROM authuser" )
    assert set(cols) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
    assert len(rows) == 1
    coldex = { cols[i]: i for i in range(len(cols)) }
    assert rows[0][coldex['username']] == 'test'
    dbcon.close()


    # Test context manager
    with db.DBCon() as dbcon:
        rows, cols = dbcon.execute( "SELECT * FROM authuser" )
        assert set(cols) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
        assert len(rows) == 1
        coldex = { cols[i]: i for i in range(len(cols)) }
        assert rows[0][coldex['username']] == 'test'
        dbcon.remake_cursor()
        rows, cols = dbcon.execute( "SELECT * FROM authuser" )
        assert set(cols) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
        assert len(rows) == 1
        coldex = { cols[i]: i for i in range(len(cols)) }
        assert rows[0][coldex['username']] == 'test'
        dbcon.remake_cursor( dictcursor=True )
        rows = dbcon.execute( "SELECT * FROM authuser" )
        assert len(rows) == 1
        assert isinstance( rows[0], dict )
        assert set( rows[0].keys() ) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
        assert rows[0]['username'] == 'test'
        dbcon.remake_cursor()
        rows, cols = dbcon.execute( "SELECT * FROM authuser" )
        assert set(cols) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
        assert len(rows) == 1
        coldex = { cols[i]: i for i in range(len(cols)) }
        assert rows[0][coldex['username']] == 'test'
    # TODO : somehow verify that there is no connection to the database
    # (ask the database for connection statistics?)


    with db.DBCon( dictcursor=True ) as dbcon:
        rows = dbcon.execute( "SELECT * FROM authuser" )
        assert len(rows) == 1
        assert isinstance( rows[0], dict )
        assert set( rows[0].keys() ) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
        assert rows[0]['username'] == 'test'
        dbcon.remake_cursor( dictcursor=False )
        rows, cols = dbcon.execute( "SELECT * FROM authuser" )
        assert set(cols) == { 'id', 'username', 'displayname', 'email', 'pubkey', 'privkey' }
        assert len(rows) == 1
        coldex = { cols[i]: i for i in range(len(cols)) }
        assert rows[0][coldex['username']] == 'test'
    # TODO : somehow verify that there is no connection to the database
