import uuid


def asUUID( id ):
    if isinstance( id, uuid.UUID ):
        return id
    elif isinstance( id, str ):
        return uuid.UUID( id )
    else:
        raise ValueError( f"Don't know how to turn {id} into UUID." )


_NULLUUID = asUUID( '00000000-0000-0000-0000-000000000000' )
