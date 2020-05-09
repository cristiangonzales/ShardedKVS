#!/bin/sh
# Emulate a Docker network with 7 nodes

cd ..
docker build -t shardedkvs .

docker run -p 8086:8080 --net=mynet --ip=10.0.0.26 -e ip_port="10.0.0.26:8080"  shardedkvs
