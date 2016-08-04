#!/usr/bin/env bash

set -x

if [[ ( $# -eq 0 ) || ( -z "$1" ) || ( -z "$2" ) || ( -z "$3" ) ]]; then
    echo "Usage: ./printDockerPort.sh [docker_host_ip] [docker_container_filter] [container_port_number]"
    exit 1
fi

# Adding /usr/local/bin to the path if it is not already present
if [ -d "/usr/local/bin" ] && [[ ":$PATH:" != *":/usr/local/bin:"* ]]; then
    PATH="${PATH:+"$PATH:"}/usr/local/bin"
fi

DOCKER_IP=$1
DOCKER_FILTER=$2
DOCKER_PORT_NUM=$3

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
