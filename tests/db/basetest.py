import pytest
import uuid

import psycopg

from db import DB


class BaseTestDB:
    # Derived classes must define a fixture basetest_setup which defines the following:
    #     self.cls : The class we're testing (a subclass of DBBase)
    #     self.columns : Set, the names of the columns in the class
    #     self.safe_to_modify : list of columns that aren't part of a unique, foreign key, or primary key constraint
    #                           They should have different values in all of dict1, dict2, dict3
    #     self.uniques : list of columns that have a solo unique constraint but arent pk or fk
    #     self.obj1 : An object of the class, built manually, not inserted
    #     self.dict1 : A dictionary with key: value corresponding to the table, for self.obj1
    #     self.obj2 : Another object like self.obj1 but with diferent values, not inserted
    #     self.dict2 : self.dict1:self.obj1::self.dict2:selfobj2
    #     self.dict3 : Like self.dict1, but different values from dict1 and dict2
    #
    # They will then have access to two additional fixtures,
    #   obj1_inserted and obj2_inserted

    @pytest.fixture
    def obj1_inserted( self, basetest_setup ):
        try:
            self.obj1.insert()
            yield True
        finally:
            with DB() as dbcon:
                cursor = dbcon.cursor()
                q, subdict = self.obj1._construct_pk_query_where( me=self.obj1 )
                cursor.execute( f"DELETE FROM {self.cls.__tablename__} {q}", subdict )
                dbcon.commit()

    @pytest.fixture
    def obj2_inserted( self, basetest_setup ):
        try:
            self.obj2.insert()
            yield True
        finally:
            with DB() as dbcon:
                cursor = dbcon.cursor()
                q, subdict = self.obj2._construct_pk_query_where( me=self.obj2 )
                cursor.execute( f"DELETE FROM {self.cls.__tablename__} {q}", subdict )
                dbcon.commit()


    def test_table_meta( self, basetest_setup ):
        obj1 = self.cls()
        obj2 = self.cls()

        assert set( obj1.tablemeta.keys() ) == self.columns
        assert obj2._tablemeta is not None
        assert obj2._tablemeta == obj1.tablemeta

    def test_instantiate( self, basetest_setup ):
        # Test basic instantiation
        obj = self.cls( **self.dict1 )
        assert all( getattr( obj, k ) is not None for k in obj._pk )
        for k, v in self.dict1.items():
            assert getattr( obj, k ) == v

        # Test missing column
        with pytest.raises( RuntimeError, match="Unknown columns" ):
            _ = self.cls( this_column_will_never_exist_in_any_table=42 )


    def test_insert( self, obj1_inserted ):
        with DB() as dbcon:
            cursor = dbcon.cursor()
            q, subdict = self.obj1._construct_pk_query_where( me=self.obj1 )
            cursor.execute( f"SELECT * FROM {self.cls.__tablename__} {q}", subdict )
            assert cursor.rowcount == 1

    def test_full_udpate( self, obj1_inserted, obj2_inserted ):
        if len( self.columns ) == len( self.cls._pk ):
            # All columns are part of the primary key, so updating is meaningless
            return

        origpk1 = self.obj1.pks
        origpk2 = self.obj2.pks
        obj1 = self.cls.get( *origpk1 )
        obj2 = self.cls.get( *origpk2 )
        assert obj1.pks != obj2.pks

        for k, v in self.dict3.items():
            if k in self.obj1._pk:
                continue
            setattr( obj1, k, v )
        obj1.update()

        # Make sure the update took
        assert obj1.pks == origpk1
        reobj1 = self.cls.get( *origpk1 )
        for k, v in self.dict3.items():
            if k in self.obj1._pk:
                continue
            assert getattr( reobj1, k ) == v, f"After updated, expected {k}={v}, but it was {getattr(reobj1,k)}"

        # Make sure the other object didn't get munged
        reobj2 = self.cls.get( *origpk2 )
        for col in self.columns:
            assert getattr( reobj2, col ) == getattr( obj2, col )


    def test_some_update( self, obj1_inserted, obj2_inserted ):
        if len( self.safe_to_modify ) == 0:
            # Can't run this test, so just pass
            return

        toupdate = self.safe_to_modify[:1]
        tonotupdate = [] if len(self.safe_to_modify) == 1 else self.safe_to_modify[1:]

        obj1 = self.cls.get( *self.obj1.pks )

        for k in toupdate:
            setattr( obj1, k, self.dict3[k] )

        obj1.update()

        reobj1 = self.cls.get( *obj1.pks )
        for k in toupdate:
            assert getattr( reobj1, k ) == self.dict3[k]
            assert getattr( reobj1, k ) != getattr( self.obj1, k )
        for k in tonotupdate:
            assert getattr( reobj1, k ) == getattr( self.obj1, k )


    def test_refresh( self, obj1_inserted ):
        modkws = [ k for k in self.safe_to_modify if k in self.dict3 ]
        if len( modkws ) == 0:
            # Can't run this test, so just pass
            return

        obj1 = self.cls.get( *self.obj1.pks )
        mungedobj1 = self.cls.get( *self.obj1.pks )

        for k in modkws:
            v = self.dict3[k]
            setattr( mungedobj1, k, v )

        for k in modkws:
            assert getattr( mungedobj1, k )  != getattr( obj1, k )

        mungedobj1.refresh()

        for k in self.dict3.keys():
            assert getattr( mungedobj1, k ) == getattr( obj1, k )


    def test_get_batch( self, obj1_inserted, obj2_inserted ):
        # Make sure we get both
        them = self.cls.get_batch( [ self.obj1.pks, self.obj2.pks ] )
        assert len(them) == 2
        assert sorted( [ i.pks for i in them ] ) == sorted( [ self.obj1.pks, self.obj2.pks ] )

        # Make sure we only get one if we ask for one plus a non-existent id
        bs = {
            'uuid': uuid.uuid4(),
            'smallint': -1,
            'integer': -1,
            'bigint': -1,
            'text': 'does_not_exist',
            'jsonb': { 'does_not_exist': 'nope' },
            'real': -666.,
            'dobule precision': -666.
        }
        missing = [ bs[ self.obj1.tablemeta[k]['data_type'] ] for k in self.obj1._pk ]
        them = self.cls.get_batch( [ self.obj1.pks, missing ] )
        assert len(them) == 1
        assert them[0].pks == self.obj1.pks

        # Make sure we get an empty list if we ask for non-existing stuff
        them = self.cls.get_batch( [ missing ] )
        assert them == []


    def test_get_by_attrs( self, obj1_inserted, obj2_inserted ):
        if len( self.safe_to_modify ) == 0:
            # Can't run this test, so just pass
            return

        tomod = [ k for k in self.safe_to_modify if k in self.dict3 ]
        k0 = tomod[0]

        # Get one
        kwargs = { k0: self.dict1[k0] }
        gotten = self.cls.getbyattrs( **kwargs )
        assert len(gotten) == 1
        assert gotten[0].pks == self.obj1.pks

        # Make sure we get nothing if nothing should match
        # (dict3 isn't inserted)
        kwargs = { k0: self.dict3[k0] }
        gotten = self.cls.getbyattrs( **kwargs )
        assert len(gotten) == 0

        # The rest of these tests require there to be
        #   two modifiable attributes that exist
        #   also in dict3

        if len( tomod ) > 1:
            k1 = tomod[1]

            # Get two
            kwargs = { k0: self.dict1[k0], k1: self.dict1[k1] }
            gotten = self.cls.getbyattrs( **kwargs )
            assert len(gotten) == 1
            assert gotten[0].pks == self.obj1.pks

            # Make sure that we get nothing if nothing should match
            #  (dict3 will have a different k1 from dict1/obj1)
            kwargs = { k0: self.dict1[k0], k1: self.dict3[k1] }
            gotten = self.cls.getbyattrs( **kwargs )
            assert len(gotten) == 0

            # Make sure that we get multiple of multiple should match
            kwargs = { k1: getattr( self.obj1, k1 ) }
            gotten = self.cls.getbyattrs( **kwargs )
            assert len(gotten) == 1
            assert gotten[0].pks == self.obj1.pks

            obj2 = self.cls.get( *self.obj2.pks )
            setattr( obj2, k1, getattr( self.obj1, k1 ) )
            obj2.update()
            gotten = self.cls.getbyattrs( **kwargs )
            assert len(gotten) == 2
            assert sorted( [ i.pks for i in gotten ] ) == sorted( [ self.obj1.pks, self.obj2.pks ] )

    def test_unique( self, obj1_inserted ):
        if len( self.uniques ) == 0:
            # Can't run this test, so just pass
            return

        for unique in self.uniques:
            d = dict( self.dict3 )
            d[unique] = self.dict1[unique]
            obj = self.cls( **d )
            with pytest.raises( psycopg.errors.UniqueViolation,
                                match="duplicate key value violates unique constraint" ):
                obj.insert()

    def test_delete( self, basetest_setup, obj1_inserted, obj2_inserted ):
        objs = self.cls.get_batch( [ self.obj1.pks, self.obj2.pks ] )
        assert len(objs) == 2

        self.obj1.delete_from_db()
        objs = self.cls.get_batch( [ self.obj1.pks, self.obj2.pks ] )
        assert len(objs) == 1
        assert [ getattr( objs[0], k ) for k in self.cls._pk ] == self.obj2.pks


    # This just tests functionality, not performance
    def test_bulk_upsert( self, basetest_setup ):
        try:
            objs = self.cls.get_batch( [ self.obj1.pks, self.obj2.pks ] )
            assert len(objs) == 0

            self.cls.load_table_meta()
            jsoncols = [ c for c in self.cls._tablemeta if self.cls._tablemeta[c]['data_type'] == 'jsonb' ]

            # First : list of objects
            # We know this won't work if there are any json columns
            if len( jsoncols ) == 0:
                n = self.cls.bulk_insert_or_upsert( [ self.obj1, self.obj2 ] )
                assert n == 2

                objs = self.cls.get_batch( [ self.obj1.pks, self.obj2.pks ] )
                assert len(objs) == 2
                assert ( sorted( [ [ getattr(o, k) for k in self.cls._pk ] for o in objs ] ) ==
                         sorted( [ self.obj1.pks, self.obj2.pks ] ) )
                for obj in [ self.obj1, self.obj2 ]:
                    which = [ o for o in objs if [ getattr(o, k) for k in self.cls._pk ] == obj.pks ]
                    which = which[0]
                    assert all( getattr( which, k ) == getattr( obj, k ) for k in self.columns )

                self.obj1.delete_from_db()
                self.obj2.delete_from_db()

            # Next : list of dictionaries
            # Strip out the json columns
            dicts = [ o._build_subdict() for o in [ self.obj1, self.obj2 ] ]
            dicts = [ { k: v for k, v in d.items() if k not in jsoncols } for d in dicts ]
            n = self.cls.bulk_insert_or_upsert( dicts )
            assert n == 2
            objs = self.cls.get_batch( [ self.obj1.pks, self.obj2.pks ] )
            assert len( objs ) == 2
            assert ( sorted( [ [ getattr(o, k) for k in self.cls._pk ] for o in objs ] )
                     == sorted( [ self.obj1.pks, self.obj2.pks ] ) )
            for obj in [ self.obj1, self.obj2 ]:
                which = [ o for o in objs if [ getattr(o, k) for k in self.cls._pk ] == obj.pks ]
                which = which[0]
                assert all( getattr( which, k ) == getattr( obj, k ) for k in self.columns if k not in jsoncols )
            self.obj1.delete_from_db()
            self.obj2.delete_from_db()

            # Next : dictionary of lists
            dicts = [ o._build_subdict() for o in [ self.obj1, self.obj2 ] ]
            dicts = [ { k: v for k, v in d.items() if k not in jsoncols } for d in dicts ]
            dictoflists = { k: [ d[k] for d in dicts ] for k in dicts[0].keys() }
            n = self.cls.bulk_insert_or_upsert( dictoflists )
            assert n == 2
            objs = self.cls.get_batch( [ self.obj1.pks, self.obj2.pks ] )
            assert len( objs ) == 2
            assert ( sorted( [ [ getattr(o, k) for k in self.cls._pk ] for o in objs ] )
                     == sorted( [ self.obj1.pks, self.obj2.pks ] ) )
            for obj in [ self.obj1, self.obj2 ]:
                which = [ o for o in objs if [ getattr(o, k) for k in self.cls._pk ] == obj.pks ]
                which = which[0]
                assert all( getattr( which, k ) == getattr( obj, k ) for k in self.columns if k not in jsoncols )
            self.obj1.delete_from_db()
            self.obj2.delete_from_db()


            # TODO : test updating, conflicts, etc.

        finally:
            self.obj1.delete_from_db()
            self.obj2.delete_from_db()
