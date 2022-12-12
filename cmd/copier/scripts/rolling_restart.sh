#!/bin/bash --

set -x

CLUSTER="bryan-rolling-restart-test"

fn() {
    echo $1
    roachprod run $CLUSTER":$1" -- "./cockroach node drain --certs-dir=/home/ubuntu/certs --host=:26257 $1"
    sleep 10
    roachprod run $CLUSTER":$1" -- "sudo systemctl restart cockroach"
    sleep 45
}

while true;
do
    start_time=$(date)
    echo "start rolling restart - ${start_time}"
    for ((node_id=1; node_id <=6; node_id++>)); do fn $node_id ; done
    end_time=$(date)
    echo "rolling restart complete - ${end_time}"
done