#!/bin/sh
# Emulate a Docker network with 7 nodes

cd ..
docker build -t shardedkvs .

docker run -p 8082:8080 --net=mynet --ip=10.0.0.22 -e K=2 -e VIEW="10.0.0.20:8080,10.0.0.21:8080,10.0.0.22:8080,10.0.0.23:8080,10.0.0.24:8080,10.0.0.25:8080" -e ip_port="10.0.0.22:8080" shardedkvs
