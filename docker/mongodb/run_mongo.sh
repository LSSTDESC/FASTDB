#!/bin/bash

# Make sure we have needed env vars
if [ -z "$MONGODB_DBNAME" ] || [ -z "$MONGODB_ADMIN_USER" ] || [ -z "$MONGODB_ADMIN_PASSWD" ] || [ -z "$MONGODB_ALERT_READER_USER" ] || [ -z "$MONGODB_ALERT_READER_PASSWD" ] || [ -z "$MONGODB_ALERT_WRITER_USER" ] || [ -z "$MONGODB_ALERT_WRITER_PASSWD" ]; then
    echo "Must set all of MONGODB_DBNAME and MONGODB_(ADMIN|ALERT_READER|ALERT_WRITER)_(USER|PASSWD)"
    exit 1
fi

# Run once without auth, listening only on localhost
echo
echo "*** Starting mongod listening locally, no auth; logging to /var/log/mongodb/init_mongodb.log ***"
echo
mongod -f /etc/mongod.conf --bind_ip 127.0.0.1 --fork --logpath /var/log/mongodb/init_mongodb.log

# See if the admin user exists; create it if it doesn't
# (We're assuming that if admin exists, then all the others do too....)
if [ `mongosh --eval "use admin" --eval "db.system.users.find({user:'$MONGODB_ADMIN_USER'}).count()"` -eq 0 ]; then
    echo
    echo "*** Creating mongo users ***"
    echo
    mongosh --eval "use admin" \
            --eval "db.createUser({user:'$MONGODB_ADMIN_USER', pwd:'$MONGODB_ADMIN_PASSWD', roles: [{role:'userAdminAnyDatabase', db: 'admin'}, 'readWriteAnyDatabase']})" \
            --eval "use $MONGODB_DBNAME" \
            --eval "db.createUser({user:'$MONGODB_ALERT_READER_USER', pwd:'$MONGODB_ALERT_READER_PASSWD', roles: [{role:'read', db: '$MONGODB_DBNAME'}]})" \
            --eval "db.createUser({user:'$MONGODB_ALERT_WRITER_USER', pwd:'$MONGODB_ALERT_WRITER_PASSWD', roles: [{role:'readWrite', db: '$MONGODB_DBNAME'}]})" \
    echo
    echo "*** Done creating mongo users ***"
    echo
else
    echo
    echo "*** Mongo admin user exists***"
    echo
fi

# Kill the running mongodb
# (This seems to leave behind a defunct process.  I'm not happy about that, but things seem to be working...?)
# (...or not.  When I did it interactively, it left behind a zombie process, but when running for real it doesn't.)
echo
echo "*** Killing initial mongod ***"
echo
mongod --shutdown

# Run mongodb requiring auth
echo
echo "*** Starting final mongod with --auth ***"
echo
mongod -f /etc/mongod.conf --auth
