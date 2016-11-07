package compress

import (
	"bytes"

	"github.com/golang/snappy"
	"github.com/mediocregopher/radix.v2/redis"
)

const (
	MinCompressSize = 256
)

var (
	magicSnappy = []byte("$sy$")
	buffer      = []byte{}
)

// Resp compresses an entire *Resp
func Resp(r *redis.Resp) *redis.Resp {
	if r.IsType(redis.Str) {
		return r
	}

	ms, err := r.Array()
	if err != nil || len(ms) < 1 {
		return r
	}

	changed := false
	items := make([]interface{}, len(ms))
	for i, arg := range ms {
		b, e := arg.Bytes()
		if e != nil {
			return r
		}
		if len(b) > MinCompressSize {
			items[i] = Bytes(b)
			changed = true
		} else {
			items[i] = arg
		}
	}
	if !changed {
		return r
	}
	return redis.NewResp(items)
}

// Items compresses *Resp that are items of a *Resp
func Items(rs []*redis.Resp) (args []*redis.Resp, changed bool) {
	args = make([]*redis.Resp, len(rs))
	for i := range rs {
		args[i] = rs[i]
		b, e := rs[i].Bytes()
		if e != nil {
			continue
		}
		if len(b) > MinCompressSize {
			changed = true
			args[i] = redis.NewResp(Bytes(b))
		}
	}
	if !changed {
		args = rs
	}
	return
}

// Bytes compress []bytes
func Bytes(b []byte) []byte {
	n := snappy.MaxEncodedLen(len(b)) + len(magicSnappy)
	// Create the required slice once
	buf := make([]byte, n)
	copy(buf, magicSnappy)
	c := snappy.Encode(buf[len(magicSnappy):], b)
	return buf[:len(c)+len(magicSnappy)]
}

// UResp uncompress a complex *Resp
func UResp(m *redis.Resp) *redis.Resp {
	if m.IsType(redis.Str) {
		b, ok := UItem(m)
		if ok {
			return redis.NewResp(b)
		}
		return m
	}

	ms, err := m.Array()
	if err != nil || len(ms) < 1 {
		return m
	}

	changed := false
	items := make([]*redis.Resp, len(ms))
	for i, item := range ms {
		b, ok := UItem(item)
		if ok {
			changed = true
			items[i] = redis.NewResp(b)
			continue
		}
		items[i] = item
	}

	if changed {
		return redis.NewResp(items)
	}
	return m

}

func UItem(item *redis.Resp) (*redis.Resp, bool) {
	if !item.IsType(redis.Str) {
		return item, false
	}

	b, e := item.Bytes()
	if e != nil {
		return item, false
	}

	if bytes.HasPrefix(b, magicSnappy) {
		uncompressed, e := snappy.Decode(nil, b[len(magicSnappy):])
		if e == nil {
			return redis.NewResp(uncompressed), true
		}
	}
	return item, false
}
