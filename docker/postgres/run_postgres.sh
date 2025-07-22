#!/bin/bash

if [ ! -f $POSTGRES_DATA_DIR/PG_VERSION ]; then
    echo "Running initdb in $POSTGRES_DATA_DIR"
    /usr/lib/postgresql/15/bin/initdb -U postgres --pwfile=/secrets/pgpasswd $POSTGRES_DATA_DIR
    /usr/lib/postgresql/15/bin/pg_ctl -D $POSTGRES_DATA_DIR start
    psql --command "CREATE DATABASE fastdb OWNER postgres"
    psql --command "CREATE EXTENSION q3c" fastdb
    psql --command "CREATE EXTENSION pgcrypto" fastdb
    psql --command "CREATE EXTENSION pg_hint_plan" fastdb
    psql --command "CREATE EXTENSION pg_parquet" fastdb
    ropasswd=`cat /secrets/postgres_ro_password`
    psql --command "CREATE USER postgres_ro PASSWORD '${ropasswd}'"
    psql --command "GRANT CONNECT ON DATABASE fastdb TO postgres_ro"
    psql --command "GRANT USAGE ON SCHEMA public TO postgres_ro" fastdb
    psql --command "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO postgres_ro" fastdb
    /usr/lib/postgresql/15/bin/pg_ctl -D $POSTGRES_DATA_DIR stop
fi
exec /usr/lib/postgresql/15/bin/postgres -c config_file=/etc/postgresql/15/main/postgresql.conf
