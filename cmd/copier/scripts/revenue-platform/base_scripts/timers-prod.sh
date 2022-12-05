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
	--src-table-name=timers \
	--copy-sql="SELECT {{.CopyColumns}} FROM {{.SrcTableName}} WHERE ((fires_at <= '2021-08-02 00:00:00' AND fires_at >= '2021-08-01 00:00:00') OR (fires_at > '2021-08-02 00:00:00' AND fires_at < '2021-09-01 00:00:00' AND name = 'SubscriptionMonthlyTimer')) AND {{.DomainIDName}} >= \$1 AND {{.DomainIDName}} <= \$2{{if .CursorPredicate}} AND ({{.PrimaryKeys}}) >= ({{.PrimaryKeyPlaceholders}}){{end}} ORDER BY {{.PrimaryKeysAsc}} LIMIT {{.QueryLimit}}" \
	\
	--dst-dsn='postgresql://root@revenue-workflow-uat-crdb.us-west-2.aws.ddnw.net:26257/revenue_workflow_uat_prod?application_name=copier' \
	--dst-max-db-conns=2048 \
	--dst-ca-name="${DST_HOME}/ca.crt" \
	--dst-cert-name="${DST_HOME}/client.root.crt" \
	--dst-key-name="${DST_HOME}/client.root.key" \
	--dst-table-name=timers_backtesting_oct_2021 \
	\
	--column=id \
	--column=name \
	--column=type \
	--column=aggregator_type \
	--column=aggregator_id \
	--column=status \
	--column=fires_at \
	--column=created_at \
	--column=updated_at \
	\
	--domain-id-name=id \
	--primary-key-name=id \
	--query-limit=15

# root@revenue-platform-crdb.us-west-2.aws.ddnw.net:26257/revenue_platform_prod> SHOW CREATE timers;
#   table_name |                                                                                          create_statement
# -------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#   timers     | CREATE TABLE public.timers (
#              |     id UUID NOT NULL,
#              |     name STRING NULL,
#              |     type STRING NULL,
#              |     aggregator_type STRING NOT NULL,
#              |     aggregator_id STRING NOT NULL,
#              |     status STRING NOT NULL,
#              |     fires_at TIMESTAMP NOT NULL,
#              |     created_at TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
#              |     updated_at TIMESTAMP NOT NULL DEFAULT now():::TIMESTAMP,
#              |     crdb_internal_status_updated_at_shard_64 INT4 NOT VISIBLE NOT NULL AS (mod(fnv32(COALESCE(status, '':::STRING)) + fnv32(COALESCE(CAST(updated_at AS STRING), '':::STRING)), 64:::INT8)) STORED,
#              |     CONSTRAINT "primary" PRIMARY KEY (id ASC),
#              |     INDEX timers_by_date_status (updated_at ASC, status ASC) USING HASH WITH BUCKET_COUNT = 64,
#              |     UNIQUE INDEX timers_by_aggregator_type_name_fires_at (aggregator_type ASC, aggregator_id ASC, name ASC, fires_at ASC),
#              |     FAMILY "primary" (id, name, type, aggregator_type, aggregator_id, status, fires_at, created_at, updated_at, crdb_internal_status_updated_at_shard_64)
#              | );
#              | ALTER TABLE revenue_platform_prod.public.timers CONFIGURE ZONE USING
#              |
# (1 row)
