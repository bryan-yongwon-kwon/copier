package uuid_mask_test

import (
	"testing"

	"github.com/doordash/crdb-operator/pkg/uuid_mask"
	"github.com/stretchr/testify/require"
)

func TestUUIDMask(t *testing.T) {
	cases := []struct {
		numDomains int
		domain     uuid_mask.Domain
		expectedLB string
		expectedUB string
	}{
		// FIXME(seanc@): fix half-baked UUID partitioning strategy
		{16, 0, "00000000-0000-0000-0000-000000000000", "0fffffff-ffff-ffff-ffff-ffffffffffff"},
		{16, 1, "10000000-0000-0000-0000-000000000000", "1fffffff-ffff-ffff-ffff-ffffffffffff"},
		{16, 2, "20000000-0000-0000-0000-000000000000", "2fffffff-ffff-ffff-ffff-ffffffffffff"},
		{16, 3, "30000000-0000-0000-0000-000000000000", "3fffffff-ffff-ffff-ffff-ffffffffffff"},
		{16, 14, "e0000000-0000-0000-0000-000000000000", "efffffff-ffff-ffff-ffff-ffffffffffff"},
		{16, 15, "f0000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"},
		{256, 0, "00000000-0000-0000-0000-000000000000", "00ffffff-ffff-ffff-ffff-ffffffffffff"},
		{256, 1, "01000000-0000-0000-0000-000000000000", "01ffffff-ffff-ffff-ffff-ffffffffffff"},
		{256, 254, "fe000000-0000-0000-0000-000000000000", "feffffff-ffff-ffff-ffff-ffffffffffff"},
		{256, 255, "ff000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"},
		{4096, 0, "00000000-0000-0000-0000-000000000000", "000fffff-ffff-ffff-ffff-ffffffffffff"},
		{4096, 1, "00100000-0000-0000-0000-000000000000", "001fffff-ffff-ffff-ffff-ffffffffffff"},
		{4096, 4094, "ffe00000-0000-0000-0000-000000000000", "ffefffff-ffff-ffff-ffff-ffffffffffff"},
		{4096, 4095, "fff00000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-ffffffffffff"},
	}

	for _, tc := range cases {
		m := uuid_mask.New(tc.numDomains)

		lb := m.LowerBound(tc.domain)
		ub := m.UpperBound(tc.domain)
		require.Equalf(t, tc.expectedLB, lb.String(), "[lb%d/%d] mismatch", tc.domain, tc.numDomains)
		require.Equalf(t, tc.expectedUB, ub.String(), "[ub%d/%d] mismatch", tc.domain, tc.numDomains)
	}
}
