#!/bin/bash
get_exposed_port() {
  docker-compose port --index=$3 $1 $2 | cut -d: -f2
}

DOCKER_IP=${DOCKER_IP:-$1}
if [ -z "${DOCKER_IP}" ]
then
  if test -e /var/run/docker.sock
  then
    DOCKER_IP=127.0.0.1
  else
    docker-machine status tredis 2>/dev/null
    RESULT=$?
    if [ ${RESULT} -ne 0 ]
    then
      echo "docker-machine is not running, run bootstrap first"
      exit 2
    fi
    eval $(docker-machine env tredis 2>/dev/null) || {
      echo "Failed to initialize docker environment"
      exit 2
    }
    DOCKER_IP=$(docker-machine ip tredis)
  fi
fi

docker-compose ps 2>/dev/null
RESULT=$?
if [ ${RESULT} -ne 0 ]
then
  echo "Docker environment is not running, run bootstrap first"
  exit 2
fi

export REDIS_HOST=${DOCKER_IP}
export REDIS_PORT=$(get_exposed_port redis 6379 1)
export REDIS2_PORT=$(get_exposed_port redis 6379 2)
export REDIS2_HOST=$(docker inspect --format '{{ .NetworkSettings.IPAddress }}' tredis_redis_2)
nosetests
