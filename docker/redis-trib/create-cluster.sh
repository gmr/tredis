#!/usr/bin/env sh
COMPOSE_ARGS="-p redis"

docker-compose ${COMPOSE_ARGS} stop
docker-compose ${COMPOSE_ARGS} rm --force
docker-compose ${COMPOSE_ARGS} up -d

NODE1=$(docker inspect --format '{{ .NetworkSettings.Networks.redis_default.IPAddress }}' redis_node1_1)
NODE2=$(docker inspect --format '{{ .NetworkSettings.Networks.redis_default.IPAddress }}' redis_node2_1)
NODE3=$(docker inspect --format '{{ .NetworkSettings.Networks.redis_default.IPAddress }}' redis_node3_1)
NODE4=$(docker inspect --format '{{ .NetworkSettings.Networks.redis_default.IPAddress }}' redis_node4_1)
NODE5=$(docker inspect --format '{{ .NetworkSettings.Networks.redis_default.IPAddress }}' redis_node5_1)
NODE6=$(docker inspect --format '{{ .NetworkSettings.Networks.redis_default.IPAddress }}' redis_node6_1)

echo "create --replicas 1 ${NODE1}:6700 ${NODE2}:6701 ${NODE3}:6702 ${NODE4}:6703 ${NODE5}:6704 ${NODE6}:6705"

COMMAND="create --replicas 1 ${NODE1}:6700 ${NODE2}:6701 ${NODE3}:6702 ${NODE4}:6703 ${NODE5}:6704 ${NODE6}:6705"

docker run --network redis_default --rm -t -i gavinmroy/redis-trib:latest ${COMMAND}
