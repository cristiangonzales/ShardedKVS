#!/bin/sh
# Kill the Docker network 'mynet'

echo Removing the Docker network...
docker network rm mynet > /dev/null 2>&1

echo Killing all Docker containers...
docker kill $(docker ps -q) > /dev/null 2>&1

echo Killing the Docker network...
docker network rm mynet > /dev/null 2>&1
