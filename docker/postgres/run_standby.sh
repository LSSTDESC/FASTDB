#!/usr/bin/env bash
set -euo pipefail

STANZA="${PGBACKREST_STANZA:-fastdb}"
WAIT_FOR_PRIMARY="${FASTDB_STANDBY_WAIT_FOR_PRIMARY:-false}"
REMOTE_RESTORE="${FASTDB_STANDBY_REMOTE_RESTORE:-false}"

echo "PGDATA=${PGDATA}"
echo "STANZA=$STANZA"
echo "FASTDB_STANDBY_WAIT_FOR_PRIMARY=$WAIT_FOR_PRIMARY"
echo "FASTDB_STANDBY_REMOTE_RESTORE=$REMOTE_RESTORE"

if [ "$WAIT_FOR_PRIMARY" = "true" ] && [ "$REMOTE_RESTORE" = "true" ]; then
    echo "ERROR: FASTDB_STANDBY_WAIT_FOR_PRIMARY and FASTDB_STANDBY_REMOTE_RESTORE cannot both be true"
    exit 1
fi

mkdir -p "$PGDATA"
#chmod 700 "$PGDATA"

if [ "$REMOTE_RESTORE" = "true" ]; then
    echo "Running in remote-restore standby mode"

    if [ ! -f "$PGDATA/PG_VERSION" ]; then
        echo "No PG_VERSION found; restoring standby from pgBackRest repo"
        find "$PGDATA" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
        pgbackrest --stanza="$STANZA" --delta --type=standby restore
    else
        echo "Existing PGDATA found; skipping restore"
    fi
else
    echo "Running in legacy/local standby mode"

    # Keep this close to the old behavior.
    pgbackrest --stanza="$STANZA" restore --delta || true
fi

touch "$PGDATA/standby.signal"

cat >> "$PGDATA/postgresql.auto.conf" <<EOF
restore_command = 'pgbackrest --stanza=${STANZA} archive-get %f %p'
recovery_target_timeline = 'latest'
EOF

exec /usr/lib/postgresql/15/bin/postgres \
    -D "$PGDATA" \
    -c config_file=/etc/postgresql/15/main/postgresql.conf