#!/bin/sh
# Completely rid local storage of anything Docker

docker container stop $(docker container ls -a -q) && docker system prune -a -f --volumes
