#!/bin/sh
# Create a Docker network called 'mynet' and build Docker container

docker network create --subnet 10.0.0.0/16 mynet
