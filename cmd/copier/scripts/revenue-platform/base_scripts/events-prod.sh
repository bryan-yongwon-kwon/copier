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
	--src-table-name=events \
        --copy-sql="SELECT {{.CopyColumns}} FROM {{.SrcTableName}} WHERE ingestion_time < '2021-06-01 00:00:00' AND {{.DomainIDName}} >= \$1 AND {{.DomainIDName}} <= \$2{{if .CursorPredicate}} AND ({{.PrimaryKeys}}) >= ({{.PrimaryKeyPlaceholders}}){{end}} ORDER BY {{.PrimaryKeysAsc}} ASC LIMIT {{.QueryLimit}}" \
	\
	--dst-dsn='postgresql://root@revenue-workflow-uat-crdb.us-west-2.aws.ddnw.net:26257/revenue_workflow_uat_prod?application_name=copier' \
	--dst-max-db-conns=2048 \
	--dst-ca-name="${DST_HOME}/ca.crt" \
	--dst-cert-name="${DST_HOME}/client.root.crt" \
	--dst-key-name="${DST_HOME}/client.root.key" \
	--dst-table-name=events_base \
	\
	--column=id \
	--column=type \
	--column=source_time \
	--column=ingestion_time \
	--column=source \
	--column=name \
	--column=data \
	--column=entity_id \
	--column=entity_type \
	--column=entity_change_time \
	--column=checksum \
	--column=source_event_id \
	--column=is_duplicate \
	--column=is_correction \
	--column=is_reprocessed \
	\
	--domain-id-name=id \
	--primary-key-name=id \
	--query-limit=15

# root@revenue-platform-crdb.us-west-2.aws.ddnw.net:26257/revenue_platform_prod> SHOW CREATE events;
#   table_name |                                                                                                                                             create_statement
# -------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#   events     | CREATE TABLE public.events (
#              |     id UUID NOT NULL,
#              |     type STRING NOT NULL,
#              |     source_time TIMESTAMP NOT NULL,
#              |     ingestion_time TIMESTAMP NOT NULL,
#              |     source STRING NOT NULL DEFAULT 'UNKNOWN':::STRING,
#              |     name STRING NOT NULL,
#              |     data JSONB NULL,
#              |     entity_id STRING NULL,
#              |     entity_type STRING NULL,
#              |     entity_change_time TIMESTAMP NULL,
#              |     checksum STRING NOT NULL,
#              |     source_event_id STRING NOT NULL,
#              |     is_duplicate BOOL NULL,
#              |     is_correction BOOL NULL,
#              |     is_reprocessed BOOL NULL,
#              |     crdb_internal_ingestion_time_name_type_shard_64 INT4 NOT VISIBLE NOT NULL AS (mod(fnv32(COALESCE(CAST(ingestion_time AS STRING), '':::STRING)) + (fnv32(COALESCE(name, '':::STRING)) + fnv32(COALESCE(type, '':::STRING))), 64:::INT8)) STORED,
#              |     crdb_internal_source_event_id_shard_64 INT4 NOT VISIBLE NOT NULL AS (mod(fnv32(COALESCE(source_event_id, '':::STRING)), 64:::INT8)) STORED,
#              |     updated_at TIMESTAMP NULL DEFAULT now():::TIMESTAMP,
#              |     CONSTRAINT "primary" PRIMARY KEY (id ASC),
#              |     INDEX events_ingestion_time2 (ingestion_time ASC, name ASC, type ASC) USING HASH WITH BUCKET_COUNT = 64,
#              |     INDEX events_entity_info2 (entity_id ASC, entity_type ASC),
#              |     INDEX events_source_event_id_idx3 (source_event_id ASC) USING HASH WITH BUCKET_COUNT = 64,
#              |     INDEX idx_events_updated_at_null (id ASC) WHERE updated_at IS NULL,
#              |     FAMILY "primary" (id, type, source_time, ingestion_time, source, name, data, entity_id, entity_type, entity_change_time, checksum, source_event_id, is_duplicate, is_correction, is_reprocessed, crdb_internal_ingestion_time_name_type_shard_64, crdb_internal_source_event_id_shard_64, updated_at)
#              | );
#              | COMMENT ON TABLE public.events IS 'Store for consumable events';
#              | ALTER TABLE revenue_platform_prod.public.events CONFIGURE ZONE USING
#              |
# (1 row)
