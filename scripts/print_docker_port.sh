#!/usr/bin/env bash

DOCKER_FILTER=$1
DOCKER_PORT_NUM=$2

DOCKER_CONTAINER=$(docker ps -a -q --filter $DOCKER_FILTER 2>/dev/null)
if [ ! "$DOCKER_CONTAINER" ]; then
    echo "Unable to find container matching $DOCKER_FILTER"
    exit 9
fi

MAPPED_PORT=$(docker inspect $DOCKER_CONTAINER 2>/dev/null | jq ".[].NetworkSettings .Ports[\"$DOCKER_PORT_NUM/tcp\"][].HostPort" | tr -d '"')
if [ ! "$MAPPED_PORT" ]; then
    echo "Container matching '$DOCKER_FILTER' does not export port $DOCKER_PORT_NUM"
    exit 10
fi
echo "$MAPPED_PORT"
