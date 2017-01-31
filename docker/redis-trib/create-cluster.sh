#!/usr/bin/env sh
COMPOSE_ARGS="-p rediscluster"

docker-compose ${COMPOSE_ARGS} stop
docker-compose ${COMPOSE_ARGS} rm --force
docker-compose ${COMPOSE_ARGS} up -d
docker-compose ${COMPOSE_ARGS} scale redis=3

NODE1=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' rediscluster_redis_1)
NODE2=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' rediscluster_redis_2)
NODE3=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' rediscluster_redis_3)

COMMAND="create --replicas 0 ${NODE1}:6379 ${NODE2}:6379 ${NODE3}:6379"

docker run --rm -t -i gavinmroy/redis-trib:latest ${COMMAND}
