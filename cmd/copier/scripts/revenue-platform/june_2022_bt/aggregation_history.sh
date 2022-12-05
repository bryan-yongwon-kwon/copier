#!/bin/bash --

set -x

# IMPORTANT NOTE: bump the max read message size on both sides to 512MiB otherwise this workload won't complete.
# SET CLUSTER SETTING sql.conn.max_read_buffer_message_size='512MiB';

EXCLUDE_DOMAINS_FILE="aggregation_history.domains"
grep 'beginning to read from domain' aggregation_history.[4-5].log | grep lower-bound | awk '{print $9}' | cut -d = -f2 > "${EXCLUDE_DOMAINS_FILE}"
grep 'finished copying domain' aggregation_history.6.log | grep lower-bound | awk '{print $7}' | cut -d= -f2 >> "${EXCLUDE_DOMAINS_FILE}"

SRC_HOME="/home/ubuntu/crdb/certs/revenue-platform"
DST_HOME="/home/ubuntu/crdb/certs/revenue-workflow-uat"
THREADS=512
SRC_QUERY_LIMIT=100
DST_QUERY_LIMIT=48

env COPIER_CONFIG=aggregation_history.ini copier \
        --exclude-domains-file="${EXCLUDE_DOMAINS_FILE}" \
        --threads="${THREADS}" \
        --dst-query-limit=${DST_QUERY_LIMIT} \
        --src-query-limit=${SRC_QUERY_LIMIT} \
        \
        --src-ca-name="${SRC_HOME}/ca.crt" \
        --src-cert-name="${SRC_HOME}/client.root.crt" \
        --src-key-name="${SRC_HOME}/client.root.key" \
        \
        --dst-ca-name="${DST_HOME}/ca.crt" \
        --dst-cert-name="${DST_HOME}/client.root.crt" \
        --dst-key-name="${DST_HOME}/client.root.key" \
