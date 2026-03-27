#!/bin/bash

# Restore base backup from object store
pgbackrest --stanza=fastdb restore --delta

# Create standby signal file
touch /var/lib/postgresql/data/standby.signal

# Configure restore_command
cat >> /var/lib/postgresql/data/postgresql.auto.conf <<EOF
restore_command = 'pgbackrest --stanza=fastdb archive-get %f %p'
recovery_target_timeline = 'latest'
EOF

# Start postgres in standby mode
exec /usr/lib/postgresql/15/bin/postgres \
    -c config_file=/etc/postgresql/15/main/postgresql.conf