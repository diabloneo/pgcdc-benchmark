#!/bin/bash

set -e

BASE_PATH=$(dirname $(readlink -f ${BASH_SOURCE[0]}))
echo ${BASE_PATH}

image="postgres:13.2"
container=cdc-test-pg
echo "Stop docker container ${container}"
docker rm -v -f ${container}
echo "Start docker container ${container}"

docker run -d -p 127.0.0.1:5432:5432 --name ${container} \
	-v ${BASE_PATH}/postgres/postgresql.conf:/etc/postgresql/postgresql.conf \
	-e POSTGRES_PASSWORD=password \
	${image} \
	-c 'config_file=/etc/postgresql/postgresql.conf'

echo "Wait postgres to start"
sleep 10

echo "Create database"
docker exec -it -u postgres ${container} psql -c 'CREATE DATABASE "cdc" OWNER=postgres;'

echo "Done"
