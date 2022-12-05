#!/bin/bash --

set -x

SRC_HOME="/home/ubuntu/crdb/certs/revenue-platform"
DST_HOME="/home/ubuntu/crdb/certs/revenue-workflow-uat"

copier \
	--threads=512 \
	--src-dsn='postgresql://root@revenue-platform-crdb.us-west-2.aws.ddnw.net:26257/revenue_platform_prod?application_name=copier' \
	--src-max-db-conns=2048 \
	--src-ca-name="${SRC_HOME}/ca.crt" \
	--src-cert-name="${SRC_HOME}/client.root.crt" \
	--src-key-name="${SRC_HOME}/client.root.key" \
	--src-table-name=errors \
        --copy-sql="SELECT {{.CopyColumns}} FROM {{.SrcTableName}} WHERE created_at < '2021-06-01 00:00:00' AND {{.DomainIDName}} >= \$1 AND {{.DomainIDName}} <= \$2{{if .CursorPredicate}} AND ({{.PrimaryKeys}}) >= ({{.PrimaryKeyPlaceholders}}){{end}} ORDER BY {{.PrimaryKeysAsc}} ASC LIMIT {{.QueryLimit}}" \
	\
	--dst-dsn='postgresql://root@revenue-workflow-uat-crdb.us-west-2.aws.ddnw.net:26257/revenue_workflow_uat_prod?application_name=copier' \
	--dst-max-db-conns=2048 \
	--dst-ca-name="${DST_HOME}/ca.crt" \
	--dst-cert-name="${DST_HOME}/client.root.crt" \
	--dst-key-name="${DST_HOME}/client.root.key" \
	--dst-table-name=errors_base \
	\
	--column=id \
	--column=event_id \
	--column=aggregator_id \
	--column=is_resolved \
	--column=type \
	--column=data \
	--column=entity_id \
	--column=entity_type \
	--column=category \
	--column=created_at \
	--column=updated_at \
	\
	--domain-id-name=id \
	--primary-key-name=id \
	--query-limit=15

# root@revenue-platform-crdb.us-west-2.aws.ddnw.net:26257/revenue_platform_prod> SHOW CREATE errors;
#   table_name |                                                                                              create_statement
# -------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#   errors     | CREATE TABLE public.errors (
#              |     id INT8 NOT NULL DEFAULT unique_rowid(),
#              |     event_id UUID NULL,
#              |     aggregator_id STRING NULL,
#              |     is_resolved BOOL NULL DEFAULT false,
#              |     type STRING NOT NULL,
#              |     data STRING NULL,
#              |     entity_id STRING NULL,
#              |     entity_type STRING NULL,
#              |     category STRING NULL DEFAULT 'error':::STRING,
#              |     created_at TIMESTAMP NULL DEFAULT now():::TIMESTAMP,
#              |     updated_at TIMESTAMP NULL DEFAULT now():::TIMESTAMP,
#              |     crdb_internal_created_at_shard_64 INT4 NOT VISIBLE NOT NULL AS (mod(fnv32(COALESCE(CAST(created_at AS STRING), '':::STRING)), 64:::INT8)) STORED,
#              |     crdb_internal_updated_at_shard_64 INT4 NOT VISIBLE NOT NULL AS (mod(fnv32(COALESCE(CAST(updated_at AS STRING), '':::STRING)), 64:::INT8)) STORED,
#              |     CONSTRAINT event_errors_pkey PRIMARY KEY (id ASC),
#              |     INDEX errors_event_index (event_id ASC),
#              |     INDEX errors_create_time_index (created_at ASC) USING HASH WITH BUCKET_COUNT = 64,
#              |     INDEX errors_aggregator_idx (aggregator_id ASC),
#              |     INDEX errors_update_date (updated_at DESC) USING HASH WITH BUCKET_COUNT = 64,
#              |     INDEX error_entity_id (entity_type ASC, entity_id ASC),
#              |     FAMILY "primary" (id, event_id, aggregator_id, is_resolved, type, data, entity_id, entity_type, category, created_at, updated_at, crdb_internal_created_at_shard_64, crdb_internal_updated_at_shard_64)
#              | );
#              | COMMENT ON TABLE public.errors IS 'Collects errors';
#              | ALTER TABLE revenue_platform_prod.public.errors CONFIGURE ZONE USING
#              |
# (1 row)
