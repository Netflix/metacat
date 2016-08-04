#!/usr/bin/env bash

set -x

echo PATH is $PATH
echo DOCKER env is $(env | grep DOCKER)

# Usage: ./startCluster.sh

docker-compose up storage-barrier #>> build/docker_compose.log 2>&1
if [ $? -ne 0 ]; then
    echo "Unable to start storage containers"
    exit 9
fi

docker-compose up service-barrier #>> build/docker_compose.log 2>&1
if [ $? -ne 0 ]; then
    echo "Unable to start service containers"
    exit 10
fi

docker-compose up metacat-barrier #>> build/docker_compose.log 2>&1
if [ $? -ne 0 ]; then
    echo "Unable to start metacat service container"
    exit 11
fi
