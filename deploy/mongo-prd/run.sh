#!/bin/bash
set -m

db_path=/data/db
lockfile=$db_path/mongod.lock

cmd="mongod --replSet krakken --journal --smallfiles --dbpath $db_path"

if [ ! -f $lockfile ]; then
    exec $cmd
else
    rm $lockfile
    exec $cmd
fi

RET=1
while [[ RET -ne 0 ]]; do
    echo "=> Waiting for confirmation of MongoDB service startup"
    sleep 5
    mongo admin --eval "help" >/dev/null 2>&1
    RET=$?
done

# To be done manually
# mongo <<- IOSQL
#   rs.initiate()
# IOSQL

while [ 1 ]; do fg 2> /dev/null; [ $? == 1 ] && break; done
