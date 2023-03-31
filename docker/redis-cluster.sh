#!/bin/sh

IP="127.0.0.1"
PORTS="5000 5001 5002 5003 5004 5005"
TIMEOUT=5000
NODES=""

mkdir cluster-test
cd cluster-test || exit

echo "Redis cluster is starting"
for port in $PORTS
do
echo "Starting $port"
redis-server --port $port --cluster-enabled yes --cluster-config-file nodes-${port}.conf --cluster-node-timeout $TIMEOUT --appendonly yes --appendfilename appendonly-${port}.aof --logfile ${port}.log --daemonize yes
NODES="${NODES} ${IP}:${port}"
done
echo "All nodes for redis cluster have been started ${NODES}"

redis-cli --cluster create ${NODES} --cluster-replicas 1 --cluster-yes
echo "Redis cluster has been created"
sleep 10000
