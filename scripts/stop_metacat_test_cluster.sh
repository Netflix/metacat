#!/usr/bin/env bash

set -x

# Usage: ./stopCluster.sh docker-compose.yml
COMPOSE_FILE=$1

docker-compose --file $COMPOSE_FILE down
if [ $? -ne 0 ]; then
    echo "Unable to bring down docker-compose"
    exit 9
fi
