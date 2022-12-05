#!/bin/bash --

set -x

SRC_HOME="/home/ubuntu/crdb/certs/revenue-workflow-beta"
DST_HOME="/home/ubuntu/crdb/certs/revenue-workflow-uat"

copier \
        --threads=512 \
        --src-dsn='postgresql://root@revenue-workflow-beta-crdb.us-west-2.aws.ddnw.net:26257/revenue_workflow_beta_prod?application_name=copier' \
        --src-max-db-conns=2048 \
        --src-ca-name="${SRC_HOME}/ca.crt" \
        --src-cert-name="${SRC_HOME}/client.root.crt" \
        --src-key-name="${SRC_HOME}/client.root.key" \
        --src-table-name=timers \
        --copy-sql="SELECT {{.CopyColumns}} FROM {{.SrcTableName}} WHERE fires_at >='2022-01-01 00:00:00' and fires_at < '2022-02-01 00:00:00' 
        AND name='SubscriptionMonthlyTimer' AND {{.DomainIDName}} >= \$1 AND {{.DomainIDName}} <= \$2{{if .CursorPredicate}} 
                AND ({{.PrimaryKeys}}) >= ({{.PrimaryKeyPlaceholders}}){{end}} ORDER BY {{.PrimaryKeysAsc}} LIMIT {{.SrcQueryLimit}}" \
        \
        --dst-dsn='postgresql://root@revenue-workflow-uat-crdb.us-west-2.aws.ddnw.net:26257/revenue_workflow_uat_prod?application_name=copier' \
        --dst-max-db-conns=2048 \
        --dst-ca-name="${DST_HOME}/ca.crt" \
        --dst-cert-name="${DST_HOME}/client.root.crt" \
        --dst-key-name="${DST_HOME}/client.root.key" \
        --dst-table-name=timers_backtesting_beta_mar_2022 \
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
        --src-query-limit=100 \
        --dst-query-limit=50