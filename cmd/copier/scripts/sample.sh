#!/bin/bash --

set -x

SRC_HOME="/home/ubuntu/certs"
DST_HOME="/home/ubuntu/certs"

copier \
        --threads=16 \
        --src-dsn='postgresql://root@test-crdb.us-west-2.aws.ddnw.net:26257/defaultdb?application_name=copier' \
        --src-max-db-conns=16 \
        --src-ca-name="${SRC_HOME}/ca.crt" \
        --src-cert-name="${SRC_HOME}/client.root.crt" \
        --src-key-name="${SRC_HOME}/client.root.key" \
        --src-table-name=measure \
        --copy-sql="SELECT {{.CopyColumns}} FROM {{.SrcTableName}} WHERE created_at >= '2022-07-01 00:00:00' AND {{.DomainIDName}} >= \$1 AND {{.DomainIDName}} <= \$2{{if .CursorPredicate}} AND ({{.PrimaryKeys}}) >= ({{.PrimaryKeyPlaceholders}}){{end}} ORDER BY {{.PrimaryKeysAsc}} LIMIT {{.SrcQueryLimit}}" \
        \
        --dst-dsn='postgresql://root@test-crdb.us-west-2.aws.ddnw.net:26257/defaultdb?application_name=copier' \
        --dst-max-db-conns=16 \
        --dst-ca-name="${DST_HOME}/ca.crt" \
        --dst-cert-name="${DST_HOME}/client.root.crt" \
        --dst-key-name="${DST_HOME}/client.root.key" \
        --dst-table-name=measure_dst \
        \
        --column=id \
        --column=created_at \
        --column=updated_at \
        --column=update_count \
        --column=device_id \
        --column=customer_id \
        --column=measured_value \
        --column=valid \
        --column=measure_notes \
        --column=measured_data \
        \
        --domain-id-name=id \
        --primary-key-name=id \
        --src-query-limit=20 \
        --dst-query-limit=20