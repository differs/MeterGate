package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"
)

var (
	seq      atomic.Uint64
	initTime = time.Now().UnixNano()
)

// newRequestID generates a globally-unique request ID: timestamp prefix +
// process entropy + monotonic sequence. It is the idempotency key for the
// entire billing chain (see docs: orders.request_id UNIQUE).
func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:]) + "-" + itoa(initTime) + "-" + itoa(int64(seq.Add(1)))
}

func itoa(v int64) string {
	return fmtInt(v)
}

// fmtInt is a tiny dependency-free int64 formatter.
func fmtInt(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
