#!/bin/bash
set -m

echo "Initalizing MongoDB replica set - $RS"

sudo mongod --replSet "$RS" --port $PORT &

sleep 10s

hosts=`cat /etc/hosts`
echo "Hosts file ->"
echo ""
echo "$hosts"
replica=`gethostip -d $LINK`
echo "Replica ip address is $replica"

mongo --port $PORT <<- IOSQL
  rs.initiate()
IOSQL

mongo --port $PORT <<- IOSQL
  rs.add("$replica")
IOSQL

while [ 1 ]; do fg 2> /dev/null; [ $? == 1 ] && break; done
