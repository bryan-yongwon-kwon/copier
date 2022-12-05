package main

import (
	"github.com/doordash/backfiller/pkg/uuid_mask"
	"github.com/openhistogram/circonusllhist"
	"github.com/pkg/errors"
)

var (
	_NumQuantiles = []float64{0.5, 0.90, 0.95, 0.99}     // Keep in sync with _StrQuantiles
	_StrQuantiles = []string{"p50", "p90", "p95", "p99"} // Keep in sync with _NumQuantiles
)

type WorkQueue chan *Work

type Work struct {
	h      *circonusllhist.Histogram
	mask   uuid_mask.UUIDMask
	domain uuid_mask.Domain
}

func NewWork(mask uuid_mask.UUIDMask, domain uuid_mask.Domain) *Work {
	return &Work{
		h:      circonusllhist.New(),
		mask:   mask,
		domain: domain,
	}
}

func (w Work) LowerBound() string {
	return w.mask.LowerBound(w.domain).String()
}

func (w Work) UpperBound() string {
	return w.mask.UpperBound(w.domain).String()
}

func (w Work) Quantiles() ([]string, []float64, error) {
	pNs, err := w.h.ApproxQuantile(_NumQuantiles)
	if err != nil {
		return []string{}, []float64{}, errors.Wrapf(err, "unable to get approx quantiles")
	}
	return _StrQuantiles, pNs, nil
}
