package main

import (
	"bytes"
	"context"
	"text/template"
	"time"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/doordash/backfiller/pkg/uuid_mask"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

type DBBackfiller interface {
	BackfillDomain(context.Context, *Work) error
}

type PGBackfiller struct {
	dbPool  *_DBPool
	domains uuid_mask.Domains

	columnNameID        string
	columnNameUpdatedAt string
	queryLimit          uint
	tableName           string
	backfillSQL         string
	backfillSQLTemplate *template.Template
}

type PGBackfillerOpts struct {
	DBPool              *_DBPool
	TableName           string
	QueryLimit          uint
	ColumnNameID        string
	ColumnNameUpdatedAt string
	BackfillSQL         string
}

func NewPGBackfiller(opts PGBackfillerOpts) (*PGBackfiller, error) {
	t, err := template.New(_BinName).Parse(opts.BackfillSQL)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse backfill SQL: %q", opts.BackfillSQL)
	}

	bf := &PGBackfiller{
		dbPool:              opts.DBPool,
		columnNameID:        opts.ColumnNameID,
		columnNameUpdatedAt: opts.ColumnNameUpdatedAt,
		queryLimit:          opts.QueryLimit,
		tableName:           opts.TableName,
		backfillSQL:         opts.BackfillSQL,
		backfillSQLTemplate: t,
	}

	return bf, nil
}

func (bf *PGBackfiller) BackfillDomain(ctx context.Context, job *Work) error {
	conn, err := bf.dbPool.P.Acquire(ctx)
	if err != nil {
		return errors.Wrapf(err, "unable to acquire connection from db pool")
	}
	defer func() {
		conn.Release()
	}()

	type SQLInputs struct {
		ColumnNameID        string // e.g. "id" or "aggregation_id"
		ColumnNameUpdatedAt string // e.g. "updated_at"
		CursorPredicate     bool
		QueryLimit          uint   // e.g. 1 or 128
		TableName           string // e.g. "public.aggregation_history_original_2021_06"
	}
	input := SQLInputs{
		ColumnNameID:        bf.columnNameID,
		ColumnNameUpdatedAt: bf.columnNameUpdatedAt,
		QueryLimit:          bf.queryLimit,
		TableName:           bf.tableName,
	}
	var buf bytes.Buffer
	if err := bf.backfillSQLTemplate.Execute(&buf, input); err != nil {
		return errors.Wrapf(err, "unable to render backfill SQL")
	}

	backfillSQL := buf.String()
	var cursor uuid_mask.UUID
	var noRows bool
	t1 := time.Now()
	if err := crdb.Execute(func() error {
		row := conn.QueryRow(ctx, backfillSQL,
			pgx.QueryResultFormats{pgx.BinaryFormatCode},
			job.LowerBound(), job.UpperBound())
		job.h.RecordDuration(time.Now().Sub(t1))
		if err := row.Scan(&cursor); err != nil {
			if err == pgx.ErrNoRows {
				noRows = true
				return nil
			}
			return errors.Wrapf(err, "unable to scan ID into cursor: %q", backfillSQL)
		}

		return nil
	}); err != nil {
		return errors.Wrapf(err, "unrecoverable crdb query error")
	}
	if noRows {
		return nil
	}
	conn.Release()

	buf.Reset()
	input.CursorPredicate = true
	if err := bf.backfillSQLTemplate.Execute(&buf, input); err != nil {
		return errors.Wrapf(err, "unable to render backfill SQL")
	}
	backfillSQL = buf.String()
	var finishedDomain bool
	for !finishedDomain {
		t1 = time.Now()
		conn, err = bf.dbPool.P.Acquire(ctx)
		if err != nil {
			return errors.Wrapf(err, "unable to acquire connection from db pool")
		}
		if err := crdb.Execute(func() error {
			row := conn.QueryRow(ctx, backfillSQL,
				pgx.QueryResultFormats{pgx.BinaryFormatCode},
				job.LowerBound(), job.UpperBound(), cursor.String())
			job.h.RecordDuration(time.Now().Sub(t1))
			if err := row.Scan(&cursor); err != nil {
				if err == pgx.ErrNoRows {
					finishedDomain = true
					return nil
				}
				return errors.Wrapf(err, "unable to execute batch: %q", backfillSQL)
			}

			// TODO(seanc@): Figure out how to report out the progress of this cursor
			// in its domain.
			return nil
		}); err != nil {
			return errors.Wrapf(err, "unrecoverable crdb backfill error")
		}

		conn.Release()
	}

	return nil
}
