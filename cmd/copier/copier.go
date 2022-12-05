package main

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/doordash/crdb-operator/pkg/uuid_mask"
)

type DBBackfiller interface {
	BackfillDomain(context.Context, *Work) error
}

type CRDBTableReader struct {
	dstPool       *_DBPool
	srcPool       *_DBPool
	dstTableName  string
	srcTableName  string
	dstQueryLimit uint
	srcQueryLimit uint

	sqlTemplate  *template.Template
	copySQL      string
	copyColumns  []string
	domainIDName string
	primaryKeys  []string
}

type CRDBTableReaderOpts struct {
	DstDBPool     *_DBPool
	SrcDBPool     *_DBPool
	DstTableName  string
	SrcTableName  string
	DstQueryLimit uint
	SrcQueryLimit uint

	CopySQL      string
	CopyColumns  []string
	DomainIDName string
	PrimaryKeys  []string
}

func NewCRDBTableReader(opts CRDBTableReaderOpts) (*CRDBTableReader, error) {
	t, err := template.New(_BinName).Parse(opts.CopySQL)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse copy SQL: %q", opts.CopySQL)
	}

	copyColumns := make([]string, 0, len(opts.CopyColumns))
	for _, col := range opts.CopyColumns {
		copyColumns = append(copyColumns, pq.QuoteIdentifier(col))
	}

	primaryKeys := make([]string, 0, len(opts.PrimaryKeys))
	for _, pk := range opts.PrimaryKeys {
		primaryKeys = append(primaryKeys, pq.QuoteIdentifier(pk))
	}

	sr := &CRDBTableReader{
		dstPool:       opts.DstDBPool,
		srcPool:       opts.SrcDBPool,
		dstTableName:  pq.QuoteIdentifier(opts.DstTableName),
		srcTableName:  pq.QuoteIdentifier(opts.SrcTableName),
		dstQueryLimit: opts.DstQueryLimit,
		srcQueryLimit: opts.SrcQueryLimit,

		sqlTemplate:  t,
		copySQL:      opts.CopySQL,
		copyColumns:  copyColumns,
		domainIDName: pq.QuoteIdentifier(opts.DomainIDName),
		primaryKeys:  primaryKeys,
	}

	return sr, nil
}

func (sr *CRDBTableReader) CopyDomain(ctx context.Context, job *Work) (uint64, error) {
	var rowsCopied uint64

	type SQLInputs struct {
		CopyColumns            string // All column names to be returned
		SrcTableName           string // e.g. "public.aggregation_history_original_2021_06"
		SrcQueryLimit          uint   // e.g. 1 or 128
		DomainIDName           string // "id"
		PrimaryKeys            string // "aggregation_id, faux_id,..." etc
		PrimaryKeysAsc         string // "aggregation_id ASC, faux_id ASC,..." etc
		PrimaryKeyPlaceholders string // "$3" or "$3,$4,..."
		CursorPredicate        bool
	}

	pkPlaceholders := make([]string, 0, len(sr.primaryKeys))
	for i := range sr.primaryKeys {
		pkPlaceholders = append(pkPlaceholders, fmt.Sprintf("$%d", i+3)) // Placeholders start at $3
	}

	input := SQLInputs{
		CopyColumns:            strings.Join(sr.copyColumns, ","),
		SrcTableName:           sr.srcTableName,
		SrcQueryLimit:          sr.srcQueryLimit + 1,
		DomainIDName:           sr.domainIDName,
		PrimaryKeys:            strings.Join(sr.primaryKeys, ","),
		PrimaryKeysAsc:         strings.Join(sr.primaryKeys, " ASC,"),
		PrimaryKeyPlaceholders: strings.Join(pkPlaceholders, ","),
	}
	var buf bytes.Buffer
	if err := sr.sqlTemplate.Execute(&buf, input); err != nil {
		return rowsCopied, errors.Wrapf(err, "unable to render copy SQL")
	}

	copySQL := buf.String()
	//q.Q("first-copy-sql", copySQL, job.LowerBound(), job.UpperBound())
	var pk []interface{}
	var noRows bool
	t1 := time.Now()
	if err := crdb.Execute(func() error {
		srcConn, err := sr.srcPool.P.Acquire(ctx)
		if err != nil {
			return errors.Wrapf(err, "unable to acquire source connection from db pool")
		}
		defer srcConn.Release()

		//q.Q(copySQL)
		rows, err := srcConn.Query(ctx, copySQL,
			pgx.QueryResultFormats{pgx.BinaryFormatCode},
			job.LowerBound(), job.UpperBound())
		job.h.RecordDuration(time.Now().Sub(t1))
		//q.Q("first-query, err", err)
		if err != nil {
			if err == pgx.ErrNoRows {
				noRows = true
				return nil
			}
			return errors.Wrapf(err, "unable to read first batch of rows: %q", copySQL)
		}

		// 1) Scan over data and write row data to src.dstPool
		bw := _NewBatchWriter(rows, uint(len(sr.copyColumns)), sr.srcQueryLimit, sr.dstQueryLimit)
		n, err := sr.writeRows(ctx, bw)
		rowsCopied += n
		if err != nil {
			return errors.Wrapf(err, "unable to write rows[1]")
		}

		// 2) Exit loop w/ one row left to create cursor
		if !bw.PaginationRow() {
			if bw.Err() == nil || bw.Err() == pgx.ErrNoRows {
				noRows = true
				return nil
			}
			return errors.Wrapf(bw.Err(), "unable to get next row")
		}

		// last row is used for pagination
		v, err := bw.PaginationValue()
		if err != nil {
			return errors.Wrapf(err, "unable to get primary key values for pagination")
		}
		pk = make([]interface{}, len(sr.primaryKeys))

		// NOTE,FIXME(seanc): deadly assumption assume primaryKeys are the first
		// values in the copyColumns
		copy(pk, v[:len(sr.primaryKeys)])

		return nil
	}); err != nil {
		return rowsCopied, errors.Wrapf(err, "crdb query error")
	}
	if noRows {
		return rowsCopied, nil
	}

	buf.Reset()
	input.CursorPredicate = true
	if err := sr.sqlTemplate.Execute(&buf, input); err != nil {
		return rowsCopied, errors.Wrapf(err, "unable to render backfill SQL")
	}
	copySQL = buf.String()
	//q.Q("second-copy-sql", copySQL, job.LowerBound(), job.UpperBound(), uuid_mask.UUID(pk[0].([16]uint8)).String())
	var finishedDomain bool
	var retryCount int
	for !finishedDomain {
		t1 = time.Now()
		if err := crdb.Execute(func() error {
			retryCount++
			srcConn, err := sr.srcPool.P.Acquire(ctx)
			if err != nil {
				return errors.Wrapf(err, "unable to acquire source connection from db pool")
			}
			defer srcConn.Release()

			args := make([]interface{}, 0, 3+len(pk))
			args = append(args, pgx.QueryResultFormats{pgx.BinaryFormatCode})
			args = append(args, job.LowerBound())
			args = append(args, job.UpperBound())
			args = append(args, pk...)
			rows, err := srcConn.Query(ctx, copySQL, args...)
			//q.Q("post-query2", job.LowerBound(), job.UpperBound(), uuid_mask.UUID(pk[0].([16]uint8)).String(), retryCount, rows.Err(), err, rowsCopied, finishedDomain)
			job.h.RecordDuration(time.Now().Sub(t1))
			if rows.Err() != nil {
				//q.Q("post-query2 with error", rows.Err(), err, rowsCopied, finishedDomain)
				if rows.Err() == pgx.ErrNoRows {
					finishedDomain = true
					return nil
				}
				//q.Q("unable to execute copy SELECT", finishedDomain)
				return errors.Wrapf(err, "unable to execute copy SELECT: %q", copySQL)
			}

			// TODO(seanc@): Figure out how to report out the progress of this cursor
			// in its domain.

			// 1) Scan over data and write row data to src.dstPool
			bw := _NewBatchWriter(rows, uint(len(sr.copyColumns)), sr.srcQueryLimit, sr.dstQueryLimit)
			n, err := sr.writeRows(ctx, bw)
			rowsCopied += n
			if err != nil {
				//q.Q("err2", retryCount, job.LowerBound(), job.UpperBound(), uuid_mask.UUID(pk[0].([16]uint8)).String(), rows.Err(), err, rowsCopied, finishedDomain)
				return errors.Wrapf(err, "unable to write rows[2]: %q/%q/%q", job.LowerBound(), job.UpperBound(), uuid_mask.UUID(pk[0].([16]uint8)).String())
			}

			// 2) Exit loop if there isn't one row left for a pagination cursor
			if !bw.PaginationRow() {
				if bw.Err() == nil || bw.Err() == pgx.ErrNoRows {
					//q.Q("no next, no rows as error", rows.Err(), finishedDomain)
					finishedDomain = true
					return nil
				}
				//q.Q("no next, rando error", rows.Err(), finishedDomain)
				return errors.Wrapf(bw.Err(), "unable to get next row")
			}

			// last row is used for pagination
			v, err := bw.PaginationValue()
			if err != nil {
				//q.Q("no primary key available", finishedDomain)
				return errors.Wrapf(err, "unable to get primary key values for pagination")
			}
			pk = make([]interface{}, len(sr.primaryKeys))

			// NOTE,FIXME(seanc): deadly assumption assume primaryKeys are the first
			// values in the copyColumns
			copy(pk, v[:len(sr.primaryKeys)])

			//q.Q("success", retryCount, job.LowerBound(), job.UpperBound(), uuid_mask.UUID(pk[0].([16]uint8)).String(), rows.Err(), err, rowsCopied, finishedDomain)
			return nil
		}); err != nil {
			return rowsCopied, errors.Wrapf(err, "crdb backfill error")
		}
		//q.Q("resetting retryCount to 0", finishedDomain)
		retryCount = 0
	}

	return rowsCopied, nil
}

func (sr *CRDBTableReader) writeRows(ctx context.Context, bw *_BatchWriter) (uint64, error) {
	dst, err := sr.dstPool.P.Acquire(ctx)
	if err != nil {
		return 0, errors.Wrapf(err, "unable to acquire destination connection from db pool")
	}
	defer dst.Release()

	var copyCount int64
	const useCopy = false // NOTE(seanc): The COPY protocol leaves us operationally blind atm.  Use INSERTs/UPSERTs instead.
	if useCopy {
		// NOTE: No +1 is used for the sr.srcQueryLimit.  Pagination is enabled by using
		// the _LimitRows struct.
		for bw.NextBatch() {
			n, err := dst.CopyFrom(ctx, pgx.Identifier{sr.dstTableName}, sr.copyColumns, bw)
			copyCount += n
			if err != nil {
				return uint64(copyCount), errors.Wrapf(err, "unable to copy data (%d/%d)", copyCount, sr.srcQueryLimit)
			}
			if copyCount != int64(sr.srcQueryLimit) {
				return uint64(copyCount), errors.Wrapf(err, "short-write (%d/%d)", copyCount, sr.srcQueryLimit)
			}
			if bw.Err() != nil {
				return uint64(copyCount), errors.Wrapf(bw.Err(), "error while copying data")
			}
		}
	} else {
		// UPSERT () VALUES ($1,$2,$3),($4,$5,$6),...
		// 1) Create UPSERT
		// 2) Accumulate multi-row UPSERT values
		// 3) Execute

		var sqlBuf bytes.Buffer
		const insertVerb = "UPSERT" // or "INSERT"
		sqlBuf.WriteString(insertVerb)
		sqlBuf.WriteString(` INTO `)
		sqlBuf.WriteString(sr.dstTableName)
		sqlBuf.WriteString(` (`)
		for i, col := range sr.copyColumns {
			sqlBuf.WriteString(col)
			if i != len(sr.copyColumns)-1 {
				sqlBuf.WriteByte(',')
			}
		}
		sqlBuf.WriteString(") VALUES ")
		baseInsertSQL := sqlBuf.String()

		for bw.NextBatch() {
			values, err := bw.Values()
			if err != nil {
				return uint64(copyCount), errors.Wrapf(err, "error creating batch")
			}
			if len(values) == 0 {
				break
			}
			insertSQL := baseInsertSQL + bw.Bindings(values)

			if err := crdb.Execute(func() error {
				//q.Q(insertSQL, len(values))
				commandTag, err := dst.Exec(ctx, insertSQL, values...)
				if err != nil {
					return err
				}
				copyCount += commandTag.RowsAffected()
				return nil
			}); err != nil {
				return uint64(copyCount), errors.Wrapf(err, "unable to INSERT %q", sr.dstTableName)
			}
		}
	}

	return uint64(copyCount), nil
}

// _BatchWriter short-circuits calls to Next() to aid in pagination and batching
// of writes.
type _BatchWriter struct {
	rows      pgx.Rows
	rowWidth  uint // width of an individual row
	readLimit uint // expected number of rows
	batchSize uint // size of an individual batch

	batchIndex uint // write index within a batch
	writeIndex uint // write cursor
}

func _NewBatchWriter(rows pgx.Rows, rowWidth uint, readLimit uint, batchSize uint) *_BatchWriter {
	return &_BatchWriter{
		rows:      rows,
		rowWidth:  rowWidth,
		readLimit: readLimit,
		batchSize: batchSize,
	}
}

func (bw *_BatchWriter) Err() error {
	return bw.rows.Err()
}

func (bw *_BatchWriter) Next() bool {
	bw.writeIndex++
	bw.batchIndex++
	if bw.writeIndex <= bw.readLimit && bw.batchIndex <= bw.batchSize {
		return bw.rows.Next()
	}

	return false
}

func (bw *_BatchWriter) NextBatch() bool {
	bw.batchIndex = 0
	return bw.writeIndex < bw.readLimit
}

// Bindings returns a string representing the bindings (e.g. "($1,$2),($3,$4)".
func (bw *_BatchWriter) Bindings(values []interface{}) string {
	var bindingBuf bytes.Buffer

	batchSize := bw.batchSize
	if uint(len(values))/bw.rowWidth < batchSize {
		batchSize = uint(len(values)) / bw.rowWidth
	}

	bindingPos := 1
	for i := uint(0); i < batchSize; i++ {
		bindingBuf.WriteByte('(')
		for j := uint(0); j < bw.rowWidth; j++ {
			bindingBuf.WriteByte('$')
			bindingBuf.WriteString(strconv.Itoa(bindingPos))
			bindingPos++
			if j != bw.rowWidth-1 {
				bindingBuf.WriteByte(',')
			}
		}

		bindingBuf.WriteByte(')')
		if i != batchSize-1 {
			bindingBuf.WriteByte(',')
		}
	}

	return bindingBuf.String()
}

// PaginationRow returns true if there is one row remaining in _BatchWriter.rows
func (bw *_BatchWriter) PaginationRow() bool {
	return bw.rows.Next()
}

func (bw *_BatchWriter) PaginationValue() ([]interface{}, error) {
	row, err := bw.rows.Values()
	if err != nil {
		return []interface{}{}, errors.Wrapf(err, "error with pagination value")
	}

	values := make([]interface{}, 0, len(row))
	for i := range row {
		values = append(values, row[i])
	}

	return values, nil
}

// Values returns a batch of values appropriate to the number of bindings used
// in the INSERT statement.
func (bw *_BatchWriter) Values() ([]interface{}, error) {
	values := make([]interface{}, 0, int(bw.batchSize*bw.rowWidth))

	for bw.Next() {
		row, err := bw.rows.Values()
		if err != nil {
			return []interface{}{}, errors.Wrapf(bw.Err(), "unable to accumulate values")
		}
		for i := range row {
			values = append(values, row[i])
		}
	}

	if bw.Err() != nil {
		return []interface{}{}, errors.Wrapf(bw.Err(), "unable to accumulate rows")
	}

	return values, nil
}
