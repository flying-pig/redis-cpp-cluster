#!/bin/sh

#cd 7000
#redis-server ./redis.conf > /var/log/redis-cluster/redis-7000.log 2>&1 &
#cd ../7001/
#redis-server ./redis.conf > /var/log/redis-cluster/redis-7001.log 2>&1 &
#cd ../7002
#redis-server ./redis.conf > /var/log/redis-cluster/redis-7002.log 2>&1 &
#cd ../7003/
#redis-server ./redis.conf > /var/log/redis-cluster/redis-7003.log 2>&1 &
#cd ../7004
#redis-server ./redis.conf > /var/log/redis-cluster/redis-7004.log 2>&1 &
#cd ../7005/
#redis-server ./redis.conf > /var/log/redis-cluster/redis-7005.log 2>&1 &

# if redis-trib.rb in /usr/local/bin
redis-trib.rb create --replicas 1 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005
