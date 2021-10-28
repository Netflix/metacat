#!/usr/bin/env bash

set -x

COMPOSE_FILE=$1

docker-compose --file $COMPOSE_FILE down --remove-orphans
if [ $? -ne 0 ]; then
    echo "Unable to bring down docker-compose"
    exit 9
fi
