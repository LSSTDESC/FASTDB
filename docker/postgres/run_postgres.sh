#!/bin/bash
if [ ! -f $POSTGRES_DATA_DIR/PG_VERSION ]; then
    echo "Running initdb in $POSTGRES_DATA_DIR"
    /usr/lib/postgresql/17/bin/initdb -U postgres --pwfile=/secrets/pgpasswd $POSTGRES_DATA_DIR
    /usr/lib/postgresql/17/bin/pg_ctl -o "-c listen_addresses=''" -D $POSTGRES_DATA_DIR start
    psql --command "CREATE DATABASE fastdb OWNER postgres"
    psql --command "CREATE EXTENSION q3c" fastdb
    psql --command "CREATE EXTENSION pgcrypto" fastdb
    psql --command "CREATE EXTENSION pg_hint_plan" fastdb
    psql --command "CREATE EXTENSION pg_parquet" fastdb
    ropasswd=`cat /secrets/postgres_ro_password`
    psql --command "CREATE USER postgres_ro PASSWORD '${ropasswd}' LOGIN"
    psql --command "GRANT CONNECT ON DATABASE fastdb TO postgres_ro"
    psql --command "GRANT USAGE ON SCHEMA public TO postgres_ro" fastdb
    psql --command "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO postgres_ro" fastdb
    
    /usr/lib/postgresql/17/bin/pg_ctl -D $POSTGRES_DATA_DIR stop

    # pgBackRest stanza creation and initial base backup (first init only)
    echo "current $FASTDB_ARCHIVE_ENABLED"
    if [ "$FASTDB_ARCHIVE_ENABLED" = "true" ]; then
        /usr/lib/postgresql/17/bin/pg_ctl -o "-c listen_addresses='' -c wal_level=replica -c archive_mode=on -c archive_timeout=300 -c archive_command='pgbackrest --stanza=fastdb archive-push \"%p\"'" -D $POSTGRES_DATA_DIR start
        # Wait for S3 buckets to be ready (created by create-buckets job)
        if [ "$SETUP_BACKUP" = "true" ]; then
            echo "Waiting for S3 buckets to be ready..."
            for i in $(seq 1 60); do
                if pgbackrest --stanza=fastdb stanza-create 2>&1; then
                    echo "Stanza created successfully"
                    break
                fi
                echo "Stanza create failed, retry $i/60 (buckets may not exist yet)..."
                sleep 5
            done
        
            # Initiate full backup
            pgbackrest --stanza=fastdb --repo=1 --type=full backup
            pgbackrest --stanza=fastdb --repo=2 --type=full backup
        fi
        echo "Initial base backup complete"
        /usr/lib/postgresql/17/bin/pg_ctl -D $POSTGRES_DATA_DIR stop
    fi
fi

# RKNOP 2025-11-05 : commented this out, confusion is reigning
# Make sure the temporary tablespace directory is properly created
# /usr/lib/postgresql/17/bin/pg_ctl -D $POSTGRES_DATA_DIR -o "-c listen_addresses=''" start
# psql --command "DROP TABLESPACE IF EXISTS postgres_temp" fastdb
# psql --command "CREATE TABLESPACE postgres_temp LOCATION '/tmp/postgres_temp'" fastdb
# /usr/lib/postgresql/17/bin/pg_ctl -D $POSTGRES_DATA_DIR stop

if [ "$FASTDB_ARCHIVE_ENABLED" = "true" ]; then
    exec /usr/lib/postgresql/17/bin/postgres \
        -c config_file=/etc/postgresql/17/main/postgresql.conf \
        -c wal_level=replica \
        -c archive_mode=on \
        -c archive_command='pgbackrest --stanza=fastdb archive-push "%p"'
        # Not sure if we need an archive timeout
        #-c archive_timeout=300
else
    exec /usr/lib/postgresql/17/bin/postgres \
        -c config_file=/etc/postgresql/17/main/postgresql.conf
fi

