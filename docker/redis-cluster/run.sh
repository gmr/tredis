#!/usr/bin/env sh
REDIS_PORT=${REDIS_PORT:-6379}
sed -i 's/^# cluster-enabled yes/cluster-enabled yes/' /etc/redis.conf
sed -i 's/protected-mode yes/protected-mode no/' /etc/redis.conf
sed -i 's/^\(bind .*\)$/# \1/' /etc/redis.conf
sed -i 's/^\(daemonize .*\)$/# \1/' /etc/redis.conf
sed -i 's/^\(dir .*\)$/# \1\ndir \/data/' /etc/redis.conf
sed -i 's/^\(logfile .*\)$/# \1/' /etc/redis.conf
COMMAND="s/port 6379/port ${REDIS_PORT}/"
sed -i "${COMMAND}" /etc/redis.conf
/usr/bin/redis-server /etc/redis.conf $@
