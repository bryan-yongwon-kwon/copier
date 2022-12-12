#!/bin/bash --

set -x

SRC_HOME="/home/ubuntu/certs"
DST_HOME="/home/ubuntu/certs"

copier \
        --threads=16 \
        --src-dsn='postgresql://root@localhost:26257/tpcc?application_name=copier&sslmode=require' \
        --src-max-db-conns=16 \
        --src-ca-name="${SRC_HOME}/ca.crt" \
        --src-cert-name="${SRC_HOME}/client.root.crt" \
        --src-key-name="${SRC_HOME}/client.root.key" \
        --src-table-name=order_line \
        --copy-sql="SELECT {{.CopyColumns}} FROM {{.SrcTableName}} WHERE new_id >= \$1 AND new_id <= \$2{{if .CursorPredicate}} AND ({{.PrimaryKeys}}) >= ({{.PrimaryKeyPlaceholders}}){{end}} ORDER BY new_id ASC LIMIT {{.SrcQueryLimit}}" \
        \
        --dst-dsn='postgresql://root@localhost:26257/tpcc?application_name=copier&sslmode=require' \
        --dst-max-db-conns=16 \
        --dst-ca-name="${DST_HOME}/ca.crt" \
        --dst-cert-name="${DST_HOME}/client.root.crt" \
        --dst-key-name="${DST_HOME}/client.root.key" \
        --dst-table-name=order_line_dst \
        \
        --column=new_id \
        --column=ol_o_id \
        --column=ol_d_id \
        --column=ol_w_id \
        --column=ol_number \
        --column=ol_i_id \
        --column=ol_supply_w_id \
        --column=ol_delivery_d \
        --column=ol_quantity \
        --column=ol_amount \
        --column=ol_dist_info \
        \
        --domain-id-name=new_id \
        --primary-key-name=new_id \
        --src-query-limit=20 \
        --dst-query-limit=20
