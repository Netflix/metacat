#!/usr/bin/env bash

set -x

# Usage: ./stopCluster.sh docker-compose.yml
COMPOSE_FILE=$1

docker-compose --file $COMPOSE_FILE stop -t 30
if [ $? -ne 0 ]; then
    echo "Unable to stop docker-compose"
    exit 9
fi

docker-compose --file $COMPOSE_FILE rm -f
if [ $? -ne 0 ]; then
    echo "Unable to remove docker compose containers"
    exit 10
fi

TEST_CONTAINERS=$(docker ps -a -q --filter "label=com.netflix.metacat.oss.test=true")
if [[ "$TEST_CONTAINER" != "" ]]; then
    docker rm -f $TEST_CONTAINERS
    if [ $? -ne 0 ]; then
        echo "Unable to remove metacat-test docker containers"
        exit 11
    fi
fi
