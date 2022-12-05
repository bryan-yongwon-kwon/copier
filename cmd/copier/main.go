package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	stdlog "log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/doordash/crdb-operator/pkg/uuid_mask"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jessevdk/go-flags"
	"github.com/lib/pq"
	"github.com/mattn/go-isatty"
	"github.com/openhistogram/circonusllhist"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sean-/sysexits"
	"github.com/sethvargo/go-signalcontext"
)

// TODO(seanc): Write state/progress information to disk so the job can be
// resumable.

// TODO(seanc): Add an option to write log files to a directory (instead of
// stdout).  Automatically generate the filename based on the timestamp.

// TODO(seanc): Add the ability to interactively observe the progress of the
// backfiller.  Create a `ui` option that lets you attach to a running program
// to see its progress.  A running program exposes its state as a server that
// the `ui` client polls (or maybe just make `ui` an expvar client?).

// TODO(seanc): redirect pgx logs to a different file than backfiller's default
// logger.

// TODO(seanc): emit pgx stats

// TODO(seanc): pass a logger around via a context so that we can debug log the
// rendered SQL query in the backfiller without dirtying up the API

// TODO(seanc): increase the number of error conditions that we retry

// TODO(seanc): break apart parseArgs into different CLI arg groups.  Currently
// everything is under single "application options" struct and hasn't been
// teased apart into distinct groups.

// TODO(seanc): move from `go build` to `plz`

const (
	_BinName = "copier"

	_LogTimeFormat = "2006-01-02T15:04:05.000000000Z07:00"
)

type _DBType string

const (
	_DBTypeCRDB _DBType = "crdb"
	_DBTypePG   _DBType = "pg"
)

type _DBPool struct {
	P *pgxpool.Pool
	L zerolog.Logger
	H *circonusllhist.Histogram
}

type _Args struct {
	ConfigFile    string `long:"config" short:"f" default:"copier.ini" description:"Configuration file for copier" env:"COPIER_CONFIG" value-name:"CONFIG-FILE"`
	RewriteConfig bool   `long:"config-rewrite" hidden:"true" description:"Rewrite the config file"`

	// LogFormat  *string `short:"L" long:"log-format" default:"AUTO" description:"Set the log format" value-name:"FORMAT" choice:"AUTO" choice:"JSON" choice:"CONSOLE"`                                         // nolint:lll
	// LogLevel   *string `short:"l" long:"log-level" default:"info" description:"Set the log level" value-name:"LEVEL" choice:"debug" choice:"info" choice:"warn" choice:"error" choice:"fatal" choice:"panic"` // nolint:lll
	LogNoColor bool          `long:"log-no-color" description:"Disable use of colors when using the console logger"` // nolint:lll
	PProfPort  uint16        `long:"pprof-port" default:"6060" description:"PProf Port" value-name:"PPROF-PORT"`     // nolint:lll
	MaxProcTTL time.Duration `default:"23.5h" long:"max-proc-ttl" description:"Max process time to live for this process" value-name:"MAX-PROC-TTL"`

	DstDSN        string        `default:"" long:"dst-dsn" description:"DST-DSN" env:"DST_COCKROACH_URL" value-name:"DST-DSN"`
	DstCACert     string        `default:"dst-ca.crt" long:"dst-ca-name" description:"CA Certificate filename for destination database" value-name:"DST-CA-CERT-FILENAME"`
	DstCertName   string        `default:"dst-db.crt" long:"dst-cert-name" description:"Client Certificate filename for destination database" value-name:"DST-CLIENT-CERT-FILENAME"`
	DstKeyName    string        `default:"dst-db.key" long:"dst-key-name" description:"Client Key filename for destination database" value-name:"DST-CLIENT-KEY-FILENAME"`
	DstDBConnAge  time.Duration `default:"300s" long:"dst-max-db-age" description:"Max DB connection age for destination database" value-name:"DST-MAX-DB-CONN-AGE"`
	DstMaxDBConns uint          `default:"512" long:"dst-max-db-conns" description:"Max number of DB connections for destination database" value-name:"DST-MAX-DB-CONNS"`
	DstMaxProcTTL time.Duration `default:"23.5h" long:"dst-max-proc-ttl" description:"Max process time to live for destination database" value-name:"DST-MAX-PROC-TTL"`
	DstTableName  string        `long:"dst-table-name" required:"true" description:"Name of the table to copy data from" value-name:"TABLE-NAME"`

	SrcDSN        string        `default:"" long:"src-dsn" description:"SRC-DSN" env:"SRC_COCKROACH_URL" value-name:"SRC-DSN"`
	SrcCACert     string        `default:"src-ca.crt" long:"src-ca-name" description:"CA Certificate filename for source database" value-name:"SRC-CA-CERT-FILENAME"`
	SrcCertName   string        `default:"src-db.crt" long:"src-cert-name" description:"Client Certificate filename for source database" value-name:"SRC-CLIENT-CERT-FILENAME"`
	SrcKeyName    string        `default:"src-db.key" long:"src-key-name" description:"Client Key filename for source database" value-name:"SRC-CLIENT-KEY-FILENAME"`
	SrcDBConnAge  time.Duration `default:"300s" long:"src-max-db-age" description:"Max DB connection age for source database" value-name:"SRC-MAX-DB-CONN-AGE"`
	SrcMaxDBConns uint          `default:"512" long:"src-max-db-conns" description:"Max number of DB connections for source database" value-name:"SRC-MAX-DB-CONNS"`
	SrcMaxProcTTL time.Duration `default:"23.5h" long:"src-max-proc-ttl" description:"Max process time to live for source database" value-name:"SRC-MAX-PROC-TTL"`
	SrcTableName  string        `long:"src-table-name" required:"true" description:"Name of the table to copy data from" value-name:"TABLE-NAME"`

	MaxThreads          uint          `default:"256" long:"threads" description:"Max number of destination threads" value-name:"THREADS"`
	NumDomains          uint          `short:"d" default:"1048576" long:"num-domains" description:"Number of Domains" value-name:"NUM-DOMAINS"`
	DomainDispatchDelay time.Duration `default:"10ms" long:"domain-dispatch-delay" description:"Delay between every domain" value-name:"DISPATCH-DELAY"`
	StateDBName         string        `default:"copier.state" long:"state" description:"Name of database used to checkpoint progress" value-name:"STATE-DB"`

	DomainsExcludeFile string `long:"exclude-domains-file" description:"Filename of domains (lower-bound) to exclude" value-name:"DOMAINS-EXCLUDE-FILE"`

	// NOTE: Read CopySQL is intended to be read from an .ini file so copy jobs
	// are read from file that can be reviewed and checked into github.
	CopySQL string `long:"copy-sql" default:"SELECT {{.CopyColumns}} FROM {{.SrcTableName}} WHERE {{.DomainIDName}} >= $1 AND {{.DomainIDName}} <= $2{{if .CursorPredicate}} AND ({{.PrimaryKeys}}) >= ({{.PrimaryKeyPlaceholders}}){{end}} ORDER BY {{.PrimaryKeysAsc}} ASC LIMIT {{.SrcQueryLimit}}" default-mask:"${COPY_SQL}" description:"SQL to used to copy data" value-name:"COPY-SQL"`

	CopyColumns   []string `long:"column" required:"true" description:"Column(s) to include in copy" value-name:"COLUMN-NAME"`
	DomainIDName  string   `long:"domain-id-name" required:"true" default:"id" description:"Name of the domain ID column" value-name:"DOMAIN-ID-NAME"`
	PrimaryKeys   []string `long:"primary-key-name" required:"true" default:"id" description:"Name of the primary key(s)" value-name:"ID"`
	SrcQueryLimit uint     `long:"src-query-limit" default:"1000" description:"Number of rows to return from a single query" value-name:"READ-ROWS"`
	DstQueryLimit uint     `long:"dst-query-limit" default:"1" description:"Number of rows to write in a single batch" value-name:"WRITE-ROWS"`
}

func main() {
	// 0: Parse args
	opts, err := parseArgs(os.Args[1:])
	if err != nil {
		if xerr, ok := err.(*flags.Error); ok {
			switch xerr.Type {
			case flags.ErrHelp:
				os.Exit(sysexits.OK)
			default:
				os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
				os.Exit(sysexits.Usage)
			}
		}

		os.Stderr.WriteString(fmt.Sprintf("%v\n", err))
		os.Exit(sysexits.Usage)
	}

	// 0.1: Initialize signal handlers
	sigIntCtx, _ := signalcontext.Wrap(context.Background(), syscall.SIGINT)
	sigTermCtx, _ := signalcontext.Wrap(sigIntCtx, syscall.SIGTERM)

	ctx, cancel := context.WithTimeout(sigTermCtx, opts.MaxProcTTL)
	defer cancel()

	// 0.2: create logger
	var useColor bool
	if opts.LogNoColor {
		useColor = false
	} else {
		var isTerminal bool
		if isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd()) {
			isTerminal = true
		}

		useColor = isTerminal
	}
	var w io.Writer
	w = zerolog.ConsoleWriter{
		Out:        os.Stdout,
		NoColor:    !useColor,
		TimeFormat: _LogTimeFormat,
	}

	zlog := zerolog.New(zerolog.SyncWriter(w)).With().Timestamp().Logger()
	zerolog.DurationFieldUnit = time.Microsecond
	zerolog.DurationFieldInteger = true
	zerolog.TimeFieldFormat = _LogTimeFormat
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	log.Logger = zlog

	stdlog.SetFlags(0)
	stdlog.SetOutput(zlog)

	// 0.3: Create pprof endpoint
	go func() {
		log.Debug().Err(http.ListenAndServe(fmt.Sprintf("localhost:%d", opts.PProfPort), nil)).Msg("")
	}()

	// 1.0: Call main
	if err := realMain(ctx, cancel, zlog, opts); err == nil {
		os.Exit(sysexits.OK)
	} else {
		log.Error().Err(err).Msg("exiting")
		os.Exit(sysexits.Software)
	}
}

func realMain(ctx context.Context, cancelFunc context.CancelFunc, l zerolog.Logger, opts *_Args) error {
	defer cancelFunc()

	// FIXME(seanc@): Fix jank UUID bucketing scheme
	if opts.NumDomains%16 != 0 {
		return errors.Errorf("--domains must be divisible by 16")
	}
	if opts.DstQueryLimit < 1 || opts.SrcQueryLimit < 1 {
		return errors.Errorf("--dst-query-limit and --src-query-limit must be greater than zero")
	}
	if opts.DstMaxDBConns <= 0 || opts.SrcMaxDBConns <= 0 {
		return errors.Errorf("--dst-max-db-conns and --src-max-db-conns MUST be greater than 0")
	}
	if opts.DstMaxDBConns < opts.MaxThreads {
		return errors.Errorf("--dst-max-db-conns MUST be greater than or equal to --threads")
	}
	if opts.SrcMaxDBConns < opts.MaxThreads {
		return errors.Errorf("--src-max-db-conns MUST be greater than or equal to --threads")
	}

	// 1.0: create DB connection pools
	dbPoolDst, err := makePool(_DBPoolConfig{
		Context: ctx,
		DSN:     opts.DstDSN,
		// L: l.Sample( // FIXME(seanc): use a different, dedicated logger for SQL
		// 	zerolog.LevelSampler{
		// 		DebugSampler: &zerolog.BurstSampler{ // FIXME(seanc@) make configurable
		// 			Burst:       5,
		// 			Period:      10 * time.Minute,
		// 			NextSampler: &zerolog.BasicSampler{N: 100000},
		// 		},
		// 		InfoSampler: &zerolog.BurstSampler{ // FIXME(seanc@) make configurable
		// 			Burst:       5,
		// 			Period:      10 * time.Minute,
		// 			NextSampler: &zerolog.BasicSampler{N: 100000},
		// 		},
		// 	},
		// ).With().Logger(),
		MaxDBConns: int32(opts.DstMaxDBConns),
		MaxConnAge: opts.DstDBConnAge,
		CACert:     opts.DstCACert,
		CertName:   opts.DstCertName,
		KeyName:    opts.DstKeyName,
	})
	if err != nil {
		l.Error().Err(err).Msg("unable to make dst pool")
		return err
	}
	defer dbPoolDst.LogStats()

	dbPoolSrc, err := makePool(_DBPoolConfig{
		Context: ctx,
		DSN:     opts.SrcDSN,
		// L: l.Sample( // FIXME(seanc): use a different, dedicated logger for SQL
		// 	zerolog.LevelSampler{
		// 		DebugSampler: &zerolog.BurstSampler{ // FIXME(seanc@) make configurable
		// 			Burst:       5,
		// 			Period:      1 * time.Minute,
		// 			NextSampler: &zerolog.BasicSampler{N: 10000},
		// 		},
		// 		InfoSampler: &zerolog.BurstSampler{ // FIXME(seanc@) make configurable
		// 			Burst:       5,
		// 			Period:      1 * time.Minute,
		// 			NextSampler: &zerolog.BasicSampler{N: 10000},
		// 		},
		// 	},
		// ).With().Logger(),
		MaxDBConns: int32(opts.SrcMaxDBConns),
		MaxConnAge: opts.SrcDBConnAge,
		CACert:     opts.SrcCACert,
		CertName:   opts.SrcCertName,
		KeyName:    opts.SrcKeyName,
	})
	if err != nil {
		l.Error().Err(err).Msg("unable to make src pool")
		return err
	}
	defer dbPoolSrc.LogStats()

	// 1.1: Get the DB type
	if ok, err := dbPoolDst.IsCRDB(ctx); err != nil || !ok {
		errors.Errorf("dst db must be of type %q", _DBTypeCRDB)
	}
	if ok, err := dbPoolSrc.IsCRDB(ctx); err != nil || !ok {
		errors.Errorf("src db must be of type %q", _DBTypeCRDB)
	}

	// // 1.2: Create state file to checkpoint progress
	// stateDB, err := NewStateDB(opts.StateDBName)
	// if err != nil {
	// 	return errors.Wrapf(err, "unable to open state database")
	// }
	// defer func() {
	// 	if err := stateDB.Close(); err != nil {
	// 		l.Error().Err(err).Msg("unable to close state database")
	// 	}
	// }()

	// 2.0: create worker pool
	var wg sync.WaitGroup
	workQueue := make(WorkQueue)

	sr, err := NewCRDBTableReader(
		CRDBTableReaderOpts{
			DstDBPool:     dbPoolDst,
			SrcDBPool:     dbPoolSrc,
			DstTableName:  opts.DstTableName,
			SrcTableName:  opts.SrcTableName,
			DstQueryLimit: opts.DstQueryLimit,
			SrcQueryLimit: opts.SrcQueryLimit,

			CopySQL:      opts.CopySQL,
			CopyColumns:  opts.CopyColumns,
			DomainIDName: opts.DomainIDName,
			PrimaryKeys:  opts.PrimaryKeys,
		})
	if err != nil {
		l.Error().Err(err).Msg("unable to create new source CRDB Table Reader")
		return errors.Wrapf(err, "unable to create new source CRDB Table Reader")
	}

	// 2.9: Split destination table
	mask := uuid_mask.New(int(opts.NumDomains))
	if false { // FIXME(seanc@): turn into tunable
		if err := SplitEnable(ctx, dbPoolDst, opts.DstTableName, mask); err != nil {
			return errors.Wrapf(err, "unable to split dst table %q", opts.DstTableName)
		}
	}

	// 3.0: Launch workers
	for i := 0; i < int(opts.MaxThreads); i++ {
		i := i
		wg.Add(1)
		go func(sr *CRDBTableReader /*DBTableReader*/) {
			defer wg.Done()
			l := l.With().Int("thread-id", i).Logger()
			l.Debug().Msg("starting worker")
			defer l.Debug().Msg("finished worker")

			jobHist := circonusllhist.New()
			defer func() {
				pNs, err := jobHist.ApproxQuantile(_NumQuantiles)
				if err != nil {
					l.Warn().Err(err).Msg("no query stats: unable to get approx quantiles")
				} else {
					l.Info().
						Uint64("query-count", jobHist.Count()).
						Strs("pN-quantiles", _StrQuantiles).
						Floats64("pN-latencies", pNs).
						Msg("finished (shutting down)")
				}
			}()

			jobNum := 0
			var totalRowsCopied uint64
			for {
				select {
				case <-ctx.Done():
					l.Debug().Int("num-jobs", jobNum).Msg("finished")
					return
				case job, ok := <-workQueue:
					if !ok {
						l.Warn().Msg("exiting, no more work")
						return
					}

					jobNum++
					l.Debug().Int("job-num", jobNum).
						Str("lower-bound", job.LowerBound()).Str("upper-bound", job.UpperBound()).
						Msg("beginning to read from domain")
					rowsCopied, err := retryDomainCopy(ctx, job, l, sr)
					totalRowsCopied += rowsCopied
					if err != nil {
						l.Error().Err(err).Msgf("unable to copy domain")
						cancelFunc()
						return
					}

					pNs, err := job.h.ApproxQuantile(_NumQuantiles)
					if err != nil {
						l.Warn().Err(err).
							Int("job-num", jobNum).
							Str("lower-bound", job.LowerBound()).Str("upper-bound", job.UpperBound()).
							Uint64("job-rows", rowsCopied).
							Uint64("total-rows-copied", totalRowsCopied).
							Str("lower-bound", job.LowerBound()).Str("upper-bound", job.UpperBound()).
							Msg("no query stats: unable to get approx quantiles")
					} else {
						l.Debug().
							Uint64("job-rows", rowsCopied).
							Str("lower-bound", job.LowerBound()).Str("upper-bound", job.UpperBound()).
							Uint64("total-rows-copied", totalRowsCopied).
							Uint64("query-count", job.h.Count()).
							Strs("pN-quantiles", _StrQuantiles).
							Floats64("pN-latencies", pNs).
							Msg("finished copying domain")
					}
					jobHist.Merge(job.h)
				}
			}
		}(sr)
	}

	// 4.0: Launch dispatcher to fill the work queue
	wg.Add(1)
	go func() {
		l := l.With().Int("dispatcher-id", 0).Logger()
		l.Debug().Msg("starting dispatcher")
		domainHist := circonusllhist.New()

		defer func() {
			pNs, err := domainHist.ApproxQuantile(_NumQuantiles)
			if err != nil {
				l.Error().Err(err).Msgf("unable to get approx quantiles from domain histogram")
			} else {
				l.Info().
					Uint64("domain-count", domainHist.Count()).
					Strs("pN-quantiles", _StrQuantiles).
					Floats64("pN-latencies", pNs).
					Msg("domain stats")
			}

			close(workQueue)
			wg.Done()

			l.Debug().Msg("shutdown")
		}()

		// Create an array of domains so we can shuffle the values before sending
		// them to workers.  The intention here is to fan out the work and prevent
		// contention between adjacent domains that happen to land on the same crdb
		// range.
		domains := make([]uuid_mask.Domain, 0, opts.NumDomains)
		{
			domainsExclude := make(map[uuid_mask.UUID]struct{}, opts.NumDomains)
			if opts.DomainsExcludeFile != "" {
				if err := loadDomainsExclude(ctx, opts.DomainsExcludeFile, domainsExclude); err != nil {
					l.Error().Err(err).Str("exclude-domains-file", opts.DomainsExcludeFile).Msg("unable to read exclude-domains-file file")
					return
				}
			}
			l.Debug().Str("exclude-domains-file", opts.DomainsExcludeFile).Int("excluded-domains", len(domainsExclude)).Msg("exclude-domains stats")
			for i := 0; i < int(opts.NumDomains); i++ {
				if _, found := domainsExclude[mask.LowerBound(uuid_mask.Domain(i))]; !found {
					domains = append(domains, uuid_mask.Domain(i))
				}
			}
			rand.Shuffle(len(domains), func(i, j int) {
				domains[i], domains[j] = domains[j], domains[i]
			})
		}

		completedDomains := 0
		remainingDomains := len(domains)
		totalDomains := len(domains)
		t1 := time.Now()
		startTime := time.Now()
		for n := range domains {
			job := NewWork(mask, domains[n])
			select {
			case workQueue <- job:
				dur := time.Now().Sub(t1)
				completedDomains++
				remainingDomains--

				if n > int(opts.MaxThreads) {
					domainHist.RecordDuration(dur)
					remainingDomains := totalDomains - n
					processingMean := time.Duration(domainHist.ApproxMean() * float64(time.Second))
					l.Debug().
						Str("progress", fmt.Sprintf("%d/%d", completedDomains, totalDomains)).
						Str("progress-pct", fmt.Sprintf("%0.02f", 100.0*(float64(completedDomains)/float64(totalDomains)))).
						Str("lower-bound", job.LowerBound()).Str("upper-bound", job.UpperBound()).
						Str("queue-time", dur.String()).
						Str("domain-mean", processingMean.String()).
						Str("running-time", time.Now().Sub(startTime).String()).
						Str("remaining-time", time.Duration(int64(processingMean)*int64(remainingDomains)).String()).
						Int("remaining-domains", remainingDomains).Int("completed-domains", completedDomains).
						Time("estimated-completion", time.Now().Add(time.Duration(int64(processingMean)*int64(remainingDomains)))).
						Msg("added domain to work queue")
				}
				t1 = time.Now()

				// Ramp up injecting domains into the worker pool
				time.Sleep(opts.DomainDispatchDelay)
			case <-ctx.Done():
				// l.Debug().Msg("shutting down")
			}
		}

		l.Debug().Int("num-domains", totalDomains).Msg("finished dispatcher")
	}()

	// 5.0: Block until all in-process work has completed
	wg.Wait()
	l.Info().Msg("shutdown")

	return nil
}

var (
	retryFirstSuccess bool = true // FIXME(seanc@): turn into tunable
)

// retryDomainCopy handles retrying copies for a given piece of work
func retryDomainCopy(ctx context.Context, job *Work, l zerolog.Logger, sr *CRDBTableReader) (uint64, error) {
	const _MaxDomainRetries = 300               // FIXME(seanc): make tunable
	const _RetrySleepDuration = 1 * time.Second // FIXME(seanc): make tunable

	var err error
	var retryRowsCopied, rowsCopied uint64
	for i := 1; i <= _MaxDomainRetries; i++ {
		retryRowsCopied, err = sr.CopyDomain(ctx, job)
		rowsCopied += retryRowsCopied
		if err == nil {
			return rowsCopied, nil
		}
		select {
		case <-ctx.Done():
			return rowsCopied, ctx.Err()
		default:
			l.Warn().Err(err).Dur("sleeping", _RetrySleepDuration).Int("attempt", i).Msg("retrying")
			time.Sleep(_RetrySleepDuration)
		}

		// switch {
		// case err == nil:
		// 	retryFirstSuccess = true
		// 	return rowsCopied, nil
		// case ctx.Err() != nil:
		// 	return rowsCopied, errors.Wrapf(ctx.Err(), "shutting down")
		// case strings.Contains(err.Error(), "unable to acquire destination connection from db pool"):
		// 	fallthrough
		// case strings.Contains(err.Error(), "unable to acquire source connection from db pool"):
		// 	fallthrough
		// case strings.Contains(err.Error(), "read: connection reset by peer"):
		// 	fallthrough
		// case strings.Contains(err.Error(), "read: connection timed out"):
		// 	fallthrough
		// case strings.Contains(err.Error(), "write: connection reset by peer"):
		// 	fallthrough
		// case strings.Contains(err.Error(), "write: connection timed out"):
		// 	fallthrough
		// case strings.Contains(err.Error(), "result is ambiguous (unable to determine whether command was applied via snapshot)"):
		// 	fallthrough
		// case strings.Contains(err.Error(), "server is shutting down"):
		// 	fallthrough
		// case strings.Contains(err.Error(), "result is ambiguous"):
		// 	fallthrough
		// case strings.Contains(err.Error(), "unexpected EOF"):
		// 	if !retryFirstSuccess {
		// 		return rowsCopied, err
		// 	}
		// 	l.Warn().Err(err).Dur("sleeping", _RetrySleepDuration).Int("attempt", i).Msg("retrying")
		// 	time.Sleep(_RetrySleepDuration)
		// 	continue
		// default:
		// 	return rowsCopied, errors.Wrapf(err, "unrecoverable crdb error")
		// }
	}

	l.Error().Err(err).Int("max-retries", _MaxDomainRetries).Msg("per-domain retry limit exceeded, giving up")
	return rowsCopied, errors.Wrapf(err, "unrecoverable crdb error")
}

// getDBType returns pg or crdb
func getDBType(ctx context.Context, p *_DBPool) (_DBType, error) {
	conn, err := p.P.Acquire(ctx)
	if err != nil {
		return "", errors.Wrapf(err, "unable to acquire connection from db pool")
	}
	defer conn.Release()

	const versionSQL = "SELECT VERSION()"
	t1 := time.Now()
	rows, err := conn.Query(ctx, versionSQL)
	if err != nil {
		return "", errors.Wrapf(err, "unable to get DB version")
	}
	defer rows.Close()
	p.H.RecordDuration(time.Now().Sub(t1))

	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return "", errors.Wrapf(err, "unable to get values from DB version query")
		}

		if len(vals) != 1 {
			return "", errors.New("jagged rows from DB version query")
		}

		var dbVersion string
		var ok bool
		if dbVersion, ok = vals[0].(string); !ok {
			return "", errors.New("invalid DB version")
		}

		switch {
		case strings.HasPrefix(dbVersion, "CockroachDB"):
			return _DBTypeCRDB, nil
		case strings.HasPrefix(dbVersion, "PostgreSQL"):
			return _DBTypePG, nil
		default:
			return "", errors.Errorf("DB version %q unsupported", dbVersion)
		}
	}

	return "", errors.New("DB version failed")
}

// enableSeqScans is a pgxpool callback handler that is run after every
// connection is established in order to ensure sequential scans are enabled for
// the newly established connection.
func enableSeqScans(ctx context.Context, conn *pgx.Conn, p *_DBPool) error {
	const enableSeqScan = `SET disallow_full_table_scans=false`
	t1 := time.Now()
	_, err := conn.Exec(ctx, enableSeqScan)
	if err != nil {
		return errors.Wrapf(err, "unable to enable seq scans")
	}
	p.H.RecordDuration(time.Now().Sub(t1))

	return nil
}

type _DBPoolConfig struct {
	Context        context.Context
	DSN            string
	L              zerolog.Logger
	MaxDBConns     int32
	MaxConnAge     time.Duration
	CACert         string
	CertName       string
	KeyName        string
	EnableSeqScans bool
}

// makePool creates a new database connection pool
func makePool(cfg _DBPoolConfig) (*_DBPool, error) {
	p := &_DBPool{
		H: circonusllhist.New(),
		L: cfg.L,
	}

	poolConfig, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse dsn %q", cfg.DSN)
	}

	poolConfig.ConnConfig.Logger = zerologadapter.NewLogger(cfg.L)
	poolConfig.LazyConnect = true
	poolConfig.MaxConns = cfg.MaxDBConns
	poolConfig.MinConns = 4 // FIXME(seanc): make tunable
	poolConfig.MaxConnLifetime = cfg.MaxConnAge
	poolConfig.ConnConfig.PreferSimpleProtocol = false
	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// Don't decode JSONB as JSON, instead, treat JSONB as BYTEA.
		conn.ConnInfo().RegisterDataType(pgtype.DataType{Value: &pgtype.Bytea{}, Name: "jsonb", OID: pgtype.JSONBOID})

		if cfg.EnableSeqScans {
			if err := enableSeqScans(ctx, conn, p); err != nil {
				return errors.Wrapf(err, "unable to enable sequential scans")
			}
		}

		return nil
	}

	rootCAs := x509.NewCertPool()
	caCerts, err := ioutil.ReadFile(cfg.CACert)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load %q", cfg.CACert)
	}

	if ok := rootCAs.AppendCertsFromPEM(caCerts); !ok {
		return nil, errors.Wrapf(err, "unable to use %q", cfg.CertName)
	}

	certPair, err := tls.LoadX509KeyPair(cfg.CertName, cfg.KeyName)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to load certs from cert (%q) and key (%q)", cfg.CertName, cfg.KeyName)
	}

	tlsConfig := tls.Config{
		ServerName:   poolConfig.ConnConfig.Host,
		Certificates: []tls.Certificate{certPair},
		RootCAs:      rootCAs,
	}
	poolConfig.ConnConfig.TLSConfig = &tlsConfig

	poolConfig.ConnConfig.RuntimeParams["application_name"] = _BinName
	poolConfig.ConnConfig.ConnectTimeout = 5 * time.Second

	dbPool, err := pgxpool.ConnectConfig(cfg.Context, poolConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to DB: %q", cfg.DSN)
	}
	p.P = dbPool

	return p, nil
}

// parseArgs wires up the CLI
func parseArgs(args []string) (*_Args, error) {
	p := flags.NewNamedParser(_BinName, flags.HelpFlag|flags.PassDoubleDash)
	opts := &_Args{}
	if _, err := p.AddGroup("Application Options", "", opts); err != nil {
		return nil, errors.Wrapf(err, "unable to register options")
	}

	inip := flags.NewIniParser(p)
	if val, found := os.LookupEnv("COPIER_CONFIG"); found {
		opts.ConfigFile = val
	} else {
		opts.ConfigFile = _BinName + `.ini`
	}
	if _, err := os.Stat(opts.ConfigFile); err == nil {
		if err := inip.ParseFile(opts.ConfigFile); err != nil {
			return nil, errors.Wrapf(err, "unable to parse file %q", opts.ConfigFile)
		}
	}

	extraArgs, err := p.ParseArgs(args)
	if err != nil {
		if xerr, ok := err.(*flags.Error); ok {
			switch xerr.Type {
			case flags.ErrHelp:
				p.WriteHelp(os.Stdout)
				return nil, err
			case flags.ErrCommandRequired:
				p.WriteHelp(os.Stderr)
				return nil, xerr
			default:
				return nil, errors.Wrapf(err, "%v", xerr.Type)
			}
		}

		return nil, err
	}
	if len(extraArgs) > 0 {
		return nil, errors.Errorf("extra args ignored: %+v", extraArgs)
	}

	if opts.RewriteConfig {
		const iniOpts = flags.IniCommentDefaults | flags.IniIncludeComments | flags.IniIncludeDefaults
		if err := inip.WriteFile(opts.ConfigFile, iniOpts); err != nil {
			return nil, errors.Wrapf(err, "unable to write config file %q", opts.ConfigFile)
		}
		os.Exit(0)
	}

	return opts, nil
}

// LogStats is a helper function to emit stats from a pool
func (p *_DBPool) IsCRDB(ctx context.Context) (bool, error) {
	dbType, err := getDBType(ctx, p)
	if err != nil {
		return false, errors.Wrapf(err, "unable to get db type")
	}

	if dbType != _DBTypeCRDB {
		// NOTE: enableSeqScans() only works on crdb
		return false, nil
	}

	return true, nil
}

func (p *_DBPool) LogStats() {
	pNs, err := p.H.ApproxQuantile(_NumQuantiles)
	if err != nil {
		p.L.Error().Err(err).Msgf("unable to get approx quantiles")
	} else {
		p.L.Info().
			Uint64("query-count", p.H.Count()).
			Strs("pN-quantiles", _StrQuantiles).
			Floats64("pN-latencies", pNs).
			Msg("query stats")
	}
}

func SplitEnable(ctx context.Context, p *_DBPool, tableName string, m uuid_mask.UUIDMask) error {
	conn, err := p.P.Acquire(ctx)
	if err != nil {
		return errors.Wrapf(err, "unable to acquire connection from db pool")
	}
	defer conn.Release()

	baseSQL := fmt.Sprintf(`ALTER TABLE %s SPLIT AT VALUES `, pq.QuoteIdentifier(tableName))

	// FIXME(seanc@): Magic number. Should be tunable, but not the default number
	// of domains of 1M.
	m = uuid_mask.New(256)
	splits := make([]string, 0, m.NumDomains())
	for i := 0; i < m.NumDomains(); i++ {
		splits = append(splits, `(`+pq.QuoteLiteral(m.LowerBound(uuid_mask.Domain(i)).String())+`)`)
	}

	_, err = conn.Exec(ctx, baseSQL+strings.Join(splits, ",")+` WITH EXPIRATION NOW() + '15 minutes'::INTERVAL`)
	if err != nil {
		return errors.Wrapf(err, "unable to enable table splits on %q", tableName)
	}

	return nil
}

func loadDomainsExclude(ctx context.Context, filename string, excludeMap map[uuid_mask.UUID]struct{}) error {
	f, err := os.Open(filename)
	if err != nil {
		return errors.Wrapf(err, "unable to open exclude-domains-file %q", filename)
	}

	s := bufio.NewScanner(f)
	for s.Scan() {
		tok := s.Text()
		if strings.HasPrefix(tok, "#") {
			continue
		}
		u, err := uuid.Parse(tok)
		if err != nil {
			return errors.Wrapf(err, "invalid UUID %q", tok)
		}
		excludeMap[uuid_mask.UUID(u)] = struct{}{}
	}
	if err := s.Err(); err != nil {
		return errors.Wrapf(err, "error reading exclude-domains-file %q", filename)
	}

	return nil
}
