# `backfiller`

## Installation and Usage

```sh
env CGO_ENABLED=0 go install
```

## Usage

```sh
backfiller --table-name=aggregation_history --primary-key-name=aggregation_id --threads=2048 --num-domains=4096 --max-db-conns=5000 --dsn='postgresql://root@revenue-platform-crdb.us-west-2.aws.ddnw.net:26257/revenue_platform_prod?sslmode=verify-ca&application_name=backfiller-aggregation_history' --pprof-port=6062
```

## Help

```sh
Usage:
  backfiller [OPTIONS]

Application Options:
      --log-no-color                      Disable use of colors when using the console logger
      --pprof-port=PPROF-PORT             PProf Port (default: 6060)
      --max-proc-ttl=MAX-PROC-TTL         Max process time to live for this process (default: 23.5h)
      --dsn=DSN                           DSN [$COCKROACH_URL]
      --ca-name=CA-CERT-FILENAME          CA Certificate filename for destination database (default: ca.crt)
      --cert-name=CLIENT-CERT-FILENAME    Client Certificate filename for destination database (default: client.root.crt)
      --key-name=CLIENT-KEY-FILENAME      Client Key filename for destination database (default: client.root.key)
  -A, --max-db-age=MAX-DB-CONN-AGE        Max DB connection age for destination database (default: 60s)
      --max-db-conns=MAX-DB-CONNS         Max number of DB connections for destination database (default: 8)
      --threads=THREADS                   Max number of destination threads (default: 4)
  -d, --num-domains=NUM-DOMAINS           Number of Domains (default: 16)
      --primary-key-name=ID               Name of the primary key (default: id)
      --updated-column-name=UPDATED-AT    Name of the updated_at column (default: updated_at)
      --query-limit=NUM-ROWS              Number of rows to update in a single query (default: 1)
      --table-name=TABLE-NAME             Name of the table to backfill

Help Options:
  -h, --help                              Show this help message
```
