#!/usr/bin/env bash

set -x

echo PATH is $PATH
echo DOCKER env is $(env | grep DOCKER)

COMPOSE_FILE=$1

docker compose --file ${COMPOSE_FILE} up storage-barrier
if [ $? -ne 0 ]; then
    echo "Unable to start crdb-barrier container"
    exit 9
fi

docker compose --file ${COMPOSE_FILE} up -d polaris-aurora
if [ $? -ne 0 ]; then
    echo "Unable to start crdb container"
    exit 10
fi
