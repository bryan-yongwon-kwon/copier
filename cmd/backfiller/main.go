package main

import (
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

	"github.com/doordash/backfiller/pkg/uuid_mask"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jessevdk/go-flags"
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
	_BinName = "backfiller"

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
	// LogFormat  *string `short:"L" long:"log-format" default:"AUTO" description:"Set the log format" value-name:"FORMAT" choice:"AUTO" choice:"JSON" choice:"CONSOLE"`                                         // nolint:lll
	// LogLevel   *string `short:"l" long:"log-level" default:"info" description:"Set the log level" value-name:"LEVEL" choice:"debug" choice:"info" choice:"warn" choice:"error" choice:"fatal" choice:"panic"` // nolint:lll
	LogNoColor *bool         `long:"log-no-color" description:"Disable use of colors when using the console logger"` // nolint:lll
	PProfPort  uint16        `long:"pprof-port" default:"6060" description:"PProf Port" value-name:"PPROF-PORT"`     // nolint:lll
	MaxProcTTL time.Duration `default:"23.5h" long:"max-proc-ttl" description:"Max process time to live for this process" value-name:"MAX-PROC-TTL"`

	DSN        string        `default:"" long:"dsn" description:"DSN" env:"COCKROACH_URL" value-name:"DSN"`
	CACert     string        `default:"ca.crt" long:"ca-name" description:"CA Certificate filename for destination database" value-name:"CA-CERT-FILENAME"`
	CertName   string        `default:"client.root.crt" long:"cert-name" description:"Client Certificate filename for destination database" value-name:"CLIENT-CERT-FILENAME"`
	KeyName    string        `default:"client.root.key" long:"key-name" description:"Client Key filename for destination database" value-name:"CLIENT-KEY-FILENAME"`
	DBConnAge  time.Duration `default:"60s" short:"A" long:"max-db-age" description:"Max DB connection age for destination database" value-name:"MAX-DB-CONN-AGE"`
	MaxDBConns uint          `default:"8" long:"max-db-conns" description:"Max number of DB connections for destination database" value-name:"MAX-DB-CONNS"`

	MaxThreads  uint   `default:"256" long:"threads" description:"Max number of destination threads" value-name:"THREADS"`
	NumDomains  uint   `short:"d" default:"1048576" long:"num-domains" description:"Number of Domains" value-name:"NUM-DOMAINS"`
	StateDBName string `default:"backfiller.state" long:"backfill-state" description:"Name of database used to checkpoint backfiller progress" value-name:"STATE-DB"`

	ColumnNameID        string `long:"primary-key-name" required:"true" default:"id" description:"Name of the primary key" value-name:"ID"`
	ColumnNameUpdatedAt string `long:"updated-column-name" required:"true" default:"updated_at" description:"Name of the updated_at column" value-name:"UPDATED-AT"`
	TableName           string `long:"table-name" required:"true" description:"Name of the table to backfill" value-name:"TABLE-NAME"`

	// TODO(seanc@): Move this from a default to a .ini file so backfill jobs are
	// read from files that can be checked into github and reviewed.
	BackfillSQL string `long:"backfill-sql" default:"SELECT {{.ColumnNameID}} FROM [UPDATE {{.TableName}} SET {{.ColumnNameUpdatedAt}} = crdb_internal.approximate_timestamp(crdb_internal_mvcc_timestamp) WHERE {{.ColumnNameUpdatedAt}} IS NULL AND {{.ColumnNameID}} >= $1 AND {{.ColumnNameID}} <= $2{{if .CursorPredicate}} AND {{.ColumnNameID}} >= $3{{end}} ORDER BY {{.ColumnNameID}} ASC LIMIT {{.QueryLimit}} RETURNING {{.ColumnNameID}}] ORDER BY {{.ColumnNameID}} DESC LIMIT 1" default-mask:"${UPDATED_AT_SQL}" description:"SQL to use for the backfill" value-name:"BACKFILL-SQL"`
	QueryLimit  uint   `long:"query-limit" default:"1" description:"Number of rows to update in a single query" value-name:"NUM-ROWS"`
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
	if opts.LogNoColor != nil {
		useColor = !(*opts.LogNoColor)
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
	if opts.QueryLimit < 1 {
		return errors.Errorf("--query-limit must be greater than zero")
	}
	if opts.MaxDBConns == 0 {
		return errors.Errorf("--max-db-conns MUST be greater than 0")
	}
	if opts.MaxThreads == 0 {
		return errors.Errorf("--threads MUST be greater than 0")
	}
	if opts.MaxDBConns < opts.MaxThreads {
		return errors.Errorf("--max-db-conns MUST be greater than or equal to --threads")
	}

	// 1.0: create DB connection pools
	dbPool, err := makePool(_DBPoolConfig{
		Context: ctx,
		DSN:     opts.DSN,
		L: l.Sample( // FIXME(seanc): use a different, dedicated logger for SQL
			zerolog.LevelSampler{
				DebugSampler: &zerolog.BurstSampler{ // FIXME(seanc@) make configurable
					Burst:       5,
					Period:      10 * time.Minute,
					NextSampler: &zerolog.BasicSampler{N: 100000},
				},
				InfoSampler: &zerolog.BurstSampler{ // FIXME(seanc@) make configurable
					Burst:       5,
					Period:      10 * time.Minute,
					NextSampler: &zerolog.BasicSampler{N: 100000},
				},
			},
		).With().Logger(),
		MaxDBConns: int32(opts.MaxDBConns),
		MaxConnAge: opts.DBConnAge,
		CACert:     opts.CACert,
		CertName:   opts.CertName,
		KeyName:    opts.KeyName,
	})
	if err != nil {
		l.Error().Err(err).Msg("unable to make pool")
		return err
	}
	defer dbPool.LogStats()

	// 1.1: Get the DB type
	dbType, err := getDBType(ctx, dbPool)
	if err != nil {
		return errors.Wrapf(err, "unable to get db type")
	}

	if dbType != _DBTypeCRDB {
		// NOTE: enableSeqScans() only works on crdb
		return errors.Errorf("db must be of type %q", _DBTypeCRDB)
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

	bf, err := NewPGBackfiller(
		PGBackfillerOpts{
			DBPool:              dbPool,
			TableName:           opts.TableName,
			QueryLimit:          opts.QueryLimit,
			ColumnNameID:        opts.ColumnNameID,
			ColumnNameUpdatedAt: opts.ColumnNameUpdatedAt,
			BackfillSQL:         opts.BackfillSQL,
		})
	if err != nil {
		l.Error().Err(err).Msg("unable to create new PG backfiller")
		return errors.Wrapf(err, "unable to create new PG backfiller")
	}

	// 3.0: Launch workers
	for i := 0; i < int(opts.MaxThreads); i++ {
		i := i
		wg.Add(1)
		go func(bf *PGBackfiller /*DBBackfiller*/) {
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
						Msg("beginning domain")
					if err := retryBackfillDomain(ctx, job, l, bf); err != nil {
						l.Error().Err(err).Msgf("unable to backfill domain")
						return
					}
					l.Debug().Int("job-num", jobNum).
						Str("lower-bound", job.LowerBound()).Str("upper-bound", job.UpperBound()).
						Msg("finished domain")

					pNs, err := job.h.ApproxQuantile(_NumQuantiles)
					if err != nil {
						l.Warn().Err(err).Msg("no query stats: unable to get approx quantiles")
					} else {
						l.Debug().Uint64("query-count", job.h.Count()).Strs("pN-quantiles", _StrQuantiles).Floats64("pN-latencies", pNs).Msg("finished domain")
					}
					jobHist.Merge(job.h)
				}
			}
		}(bf)
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
		for i := 0; i < int(opts.NumDomains); i++ {
			domains = append(domains, uuid_mask.Domain(i))
		}
		rand.Shuffle(len(domains), func(i, j int) {
			domains[i], domains[j] = domains[j], domains[i]
		})

		mask := uuid_mask.New(int(opts.NumDomains))
		completedDomains := 0
		remainingDomains := len(domains)
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
					remainingDomains := int(opts.NumDomains) - n
					processingMean := time.Duration(domainHist.ApproxMean() * float64(time.Second))
					l.Debug().
						Str("progress", fmt.Sprintf("%d/%d", completedDomains, opts.NumDomains)).
						Str("progress-pct", fmt.Sprintf("%0.02f", 100.0*(float64(completedDomains)/float64(opts.NumDomains)))).
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
			case <-ctx.Done():
				l.Debug().Msg("shutting down")
			}
		}

		l.Debug().Int("num-domains", int(opts.NumDomains)).Msg("finished dispatcher")
	}()

	// 5.0: Block until all in-process work has completed
	wg.Wait()
	l.Info().Msg("shutdown")

	return nil
}

// retryBackfillDomain handles retrying backfills for a given piece of work
func retryBackfillDomain(ctx context.Context, job *Work, l zerolog.Logger, bf *PGBackfiller) error {
	const _MaxDomainRetries = 50                // FIXME(seanc): make tunable
	const _RetrySleepDuration = 1 * time.Minute // FIXME(seanc): make tunable

	var err error
	for i := 1; i <= _MaxDomainRetries; i++ {
		err = bf.BackfillDomain(ctx, job)
		switch {
		case err == nil:
			return nil
		case strings.Contains(err.Error(), "unable to acquire connection from db pool"):
			fallthrough
		case strings.Contains(err.Error(), "unexpected EOF"):
			fallthrough
		case strings.Contains(err.Error(), "read: connection reset by peer"):
			l.Warn().Err(err).Dur("sleeping", _RetrySleepDuration).Int("attempt", i).Msg("retrying")
			time.Sleep(_RetrySleepDuration)
			continue
		default:
			return err
		}
	}

	l.Error().Err(err).Int("max-retries", _MaxDomainRetries).Msg("per-domain retry limit exceeded, giving up")
	return err
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
	poolConfig.MaxConnLifetime = cfg.MaxConnAge
	poolConfig.ConnConfig.PreferSimpleProtocol = false
	if cfg.EnableSeqScans {
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			return enableSeqScans(ctx, conn, p)
		}
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

	return opts, nil
}

// LogStats is a helper function to emit stats from a pool
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
