# `copier`

## Installation and Usage

```sh
env CGO_ENABLED=0 go install
```

## Usage

See the `scripts/` directory for invoation examples.

### Important Usage Notes

Due to programmer laziness, it critical that the every `--primary-key-name` also
be grouped together as the first flags to `--column`.  For example:

```
# Correct usage
copier --column=pk1 --column=pk2 --column=col3 --column=col5 --column=col4 --primary-key-name=pk1 --primary-key-name=pk2

# Incorrect:
copier --column=col3 --column=pk1 --column=col3 --column=col5 --column=col4 --primary-key-name=pk1 --primary-key-name=pk2
```

Failure to create a parallel array between the first few entries of `--column`
and `--primary-key-name` will, at best, result in a runtime error.  At worst, it
could silently not copy portions of data to the destination table.

### Config Files

`copier` supports loading its data from an ini configuration file.  The configuration
filename can be set with the `COPIER_CONFIG` environment variable.

```sh
env COPIER_CONFIG=aggregation_history.ini copier
```

If you have an existing configuration specified with command-line arguments, use
the `--config-rewrite` option to have `copier` write out a config file and exit.
For example:

```
# Write out the config
env COPIER_CONFIG=example.ini copier --column=pk1 --column=pk2 --column=col3 --column=col5 --column=col4 --primary-key-name=pk1 --primary-key-name=pk2 --config-rewrite

# Examine the config file
cat example.ini

# Execute copier using the new ini file
env COPIER_CONFIG=example.ini copier
```

## Help

```sh
Usage:
  copier [OPTIONS]

Application Options:
  -f, --config=CONFIG-FILE                           Configuration file for copier (default: copier.ini) [$COPIER_CONFIG]
      --log-no-color                                 Disable use of colors when using the console logger
      --pprof-port=PPROF-PORT                        PProf Port (default: 6060)
      --max-proc-ttl=MAX-PROC-TTL                    Max process time to live for this process (default: 23.5h)
      --dst-dsn=DST-DSN                              DST-DSN [$DST_COCKROACH_URL]
      --dst-ca-name=DST-CA-CERT-FILENAME             CA Certificate filename for destination database (default: dst-ca.crt)
      --dst-cert-name=DST-CLIENT-CERT-FILENAME       Client Certificate filename for destination database (default: dst-db.crt)
      --dst-key-name=DST-CLIENT-KEY-FILENAME         Client Key filename for destination database (default: dst-db.key)
      --dst-max-db-age=DST-MAX-DB-CONN-AGE           Max DB connection age for destination database (default: 300s)
      --dst-max-db-conns=DST-MAX-DB-CONNS            Max number of DB connections for destination database (default: 512)
      --dst-max-proc-ttl=DST-MAX-PROC-TTL            Max process time to live for destination database (default: 23.5h)
      --dst-table-name=TABLE-NAME                    Name of the table to copy data from
      --src-dsn=SRC-DSN                              SRC-DSN [$SRC_COCKROACH_URL]
      --src-ca-name=SRC-CA-CERT-FILENAME             CA Certificate filename for source database (default: src-ca.crt)
      --src-cert-name=SRC-CLIENT-CERT-FILENAME       Client Certificate filename for source database (default: src-db.crt)
      --src-key-name=SRC-CLIENT-KEY-FILENAME         Client Key filename for source database (default: src-db.key)
      --src-max-db-age=SRC-MAX-DB-CONN-AGE           Max DB connection age for source database (default: 300s)
      --src-max-db-conns=SRC-MAX-DB-CONNS            Max number of DB connections for source database (default: 512)
      --src-max-proc-ttl=SRC-MAX-PROC-TTL            Max process time to live for source database (default: 23.5h)
      --src-table-name=TABLE-NAME                    Name of the table to copy data from
      --threads=THREADS                              Max number of destination threads (default: 256)
  -d, --num-domains=NUM-DOMAINS                      Number of Domains (default: 1048576)
      --domain-dispatch-delay=DISPATCH-DELAY         Delay between every domain (default: 10ms)
      --state=STATE-DB                               Name of database used to checkpoint progress (default: copier.state)
      --exclude-domains-file=DOMAINS-EXCLUDE-FILE    Filename of domains (lower-bound) to exclude
      --copy-sql=COPY-SQL                            SQL to used to copy data (default: ${COPY_SQL})
      --column=COLUMN-NAME                           Column(s) to include in copy
      --domain-id-name=DOMAIN-ID-NAME                Name of the domain ID column (default: id)
      --primary-key-name=ID                          Name of the primary key(s) (default: id)
      --src-query-limit=READ-ROWS                    Number of rows to return from a single query (default: 1000)
      --dst-query-limit=WRITE-ROWS                   Number of rows to write in a single batch (default: 1)

Help Options:
  -h, --help                                         Show this help message
```
