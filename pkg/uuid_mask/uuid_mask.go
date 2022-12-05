package uuid_mask

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type Domain uint32
type Domains []Domain
type UUID [16]byte

func (u UUID) String() string {
	if len(u) != 16 {
		return ""
	}
	var buf [36]byte
	encodeHex(buf[:], u)
	return string(buf[:])
}

func encodeHex(dst []byte, uuid UUID) {
	hex.Encode(dst[:], uuid[:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], uuid[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], uuid[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], uuid[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], uuid[10:])
}

// UUIDMask is useful for iterating over the entirety of UUID values.
type UUIDMask struct {
	numDomains int
}

func (m UUIDMask) NumDomains() int {
	return int(m.numDomains)
}

func New(numDomains int) UUIDMask {
	if numDomains < 2 || !powerOfTwo(numDomains) {
		panic("input must be power of two")
	}

	// FIXME(seanc@): fix half-baked UUID partitioning strategy
	if numDomains%16 != 0 {
		panic("input must be divisible by 16, sorry!")
	}

	return UUIDMask{
		numDomains: numDomains,
	}
}

// LowerBound returns a UUID representing the lowest value in domain N
func (m UUIDMask) LowerBound(domain Domain) UUID {
	if int(domain) > m.numDomains {
		panic(fmt.Sprintf("domain %d is larger than the number of domains %d", domain, m.numDomains))
	}

	domainLen := len(fmt.Sprintf("%x", m.numDomains-1))
	s := fmt.Sprintf("%0*x", domainLen, domain)
	lb := s + strings.Repeat("0", 32-len(s))

	u, err := uuid.Parse(lb)
	if err != nil {
		panic("invalid UUID")
	}

	return UUID(u)
}

func (m UUIDMask) UpperBound(domain Domain) UUID {
	if int(domain) > m.numDomains {
		panic(fmt.Sprintf("domain %d is larger than the number of domains %d", domain, m.numDomains))
	}

	domainLen := len(fmt.Sprintf("%x", m.numDomains-1))
	s := fmt.Sprintf("%0*x", domainLen, domain)
	ub := s + strings.Repeat("f", 32-len(s))

	u, err := uuid.Parse(ub)
	if err != nil {
		panic("invalid UUID")
	}

	return UUID(u)
}

func fromUInt32(i uint32) []byte {
	return []byte{
		byte(i >> 24),
		byte(i >> 16),
		byte(i >> 8),
		byte(i),
	}
}

func powerOfTwo(n int) bool {
	return (n > 0) && ((n & (n - 1)) == 0)
}
