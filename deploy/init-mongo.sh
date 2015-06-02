#!/bin/bash

# osx only -- returns mongo host
#host=$DOCKER_HOST
hostIn="$DOCKER_HOST"
arrHost=(${hostIn//:/ })
host=${arrHost[1]///}
primaryport=27017
secondaryport=27018

docker stop rs0 rs1 amb

# Clean containers
docker rm $(docker ps -a -q)

# Clean images
docker rmi $(docker images | grep "^<none>" | awk "{print $3}")

# Boot ambassador
docker run -d -v /var/run/docker.sock:/var/run/docker.sock --name amb cpuguy83/docker-grand-ambassador -name rs0 -name rs1

# Boot replicaSet
docker run -d -e "RS=rs1" -e "LINK=rs0" -e "PORT=$primaryport" --name rs1 -p $primaryport:$primaryport --link amb:rs0 ernestrc/mongodb:latest
docker run -d -e "RS=rs0" -e "LINK=rs1" -e "PORT=$secondaryport" --name rs0 -p $secondaryport:$secondaryport --link amb:rs1 ernestrc/mongodb:latest
echo ""
echo "Replica set was deployed! Connect to primary with:"
echo "   mongo --port $primaryport --host $host"
echo "Or connect to secondary with:"
echo "   mongo --port $secondaryport --host $host"
