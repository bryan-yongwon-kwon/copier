#!/bin/bash --

set -x

# IMPORTANT NOTE: bump the max read message size on both sides to 512MiB otherwise this workload won't complete.
# SET CLUSTER SETTING sql.conn.max_read_buffer_message_size='512MiB';

EXCLUDE_DOMAINS_FILE="aggregation_history.domains"
grep 'beginning to read from domain' aggregation_history.[4-5].log | grep lower-bound | awk '{print $9}' | cut -d = -f2 > "${EXCLUDE_DOMAINS_FILE}"
grep 'finished copying domain' aggregation_history.6.log | grep lower-bound | awk '{print $7}' | cut -d= -f2 >> "${EXCLUDE_DOMAINS_FILE}"

SRC_HOME="/home/ubuntu/crdb/certs/revenue-platform"
DST_HOME="/home/ubuntu/crdb/certs/revenue-workflow-uat"
THREADS=1024
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


# root@revenue-platform-crdb.us-west-2.aws.ddnw.net:26257/revenue_platform_prod> SHOW CREATE aggregation_history;
#       table_name      |                                                                                   create_statement
# ----------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#   aggregation_history | CREATE TABLE public.aggregation_history (
#                       |     faux_id INT8 NOT NULL DEFAULT unique_rowid(),
#                       |     aggregation_id UUID NOT NULL,
#                       |     entity_type STRING NOT NULL,
#                       |     entity_id STRING NOT NULL,
#                       |     created_at TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
#                       |     data JSONB NULL,
#                       |     version DECIMAL NOT NULL DEFAULT 0:::DECIMAL,
#                       |     change_type STRING NULL,
#                       |     change_reference STRING NULL,
#                       |     ehid STRING NULL AS (md5(concat(entity_id, entity_type))) STORED,
#                       |     crdb_internal_created_at_shard_40 INT4 NOT VISIBLE NOT NULL AS (mod(fnv32(COALESCE(CAST(created_at AS STRING), '':::STRING)), 40:::INT8)) STORED,
#                       |     updated_at TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
#                       |     CONSTRAINT "primary" PRIMARY KEY (aggregation_id ASC, entity_id ASC, entity_type ASC, version ASC, faux_id ASC),
#                       |     INDEX aggregation_history_type_id_version (ehid ASC, version DESC),
#                       |     INDEX idx_created_at (created_at ASC) USING HASH WITH BUCKET_COUNT = 40,
#                       |     FAMILY "primary" (faux_id, aggregation_id, entity_type, entity_id, created_at, data, version, change_type, change_reference, ehid, crdb_internal_created_at_shard_40, updated_at)
#                       | );
#                       | ALTER TABLE revenue_platform_prod.public.aggregation_history CONFIGURE ZONE USING
#                       |
# (1 row)
