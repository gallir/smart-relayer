package lib

import (
	"bytes"

	"github.com/golang/snappy"
	"github.com/mediocregopher/radix.v2/redis"
)

var (
	magicSnappy     = []byte("$sy$")
	minCompressSize = 256
)

func Compress(r *redis.Resp) *redis.Resp {
	if r.IsType(redis.Str) {
		return r
	}

	ms, err := r.Array()
	if err != nil || len(ms) < 1 {
		return r
	}

	items := make([]interface{}, len(ms))
	for i, arg := range ms {
		b, e := arg.Bytes()
		if e != nil {
			return r
		}
		items[i] = CompressBytes(b)
	}
	return redis.NewResp(items)
}

func CompressBytes(b []byte) []byte {
	if len(b) < minCompressSize {
		return b
	}
	return append(magicSnappy, snappy.Encode(nil, b)...)
}

func Uncompress(m *redis.Resp) *redis.Resp {
	if m.IsType(redis.Str) {
		b := UncompressItem(m)
		if b == nil {
			return m
		}
		return redis.NewResp(b)
	}

	ms, err := m.Array()
	if err != nil || len(ms) < 1 {
		return m
	}

	changed := false
	items := make([]interface{}, 0, len(ms))
	for _, item := range ms {
		b := UncompressItem(item)
		if b != nil {
			changed = true
			items = append(items, b)
			continue
		}

		b, e := item.Bytes()
		if e != nil {
			// Fatal error, return the same resp
			return m
		}
		items = append(items, b)
	}

	if changed {
		return redis.NewResp(items)
	}
	return m

}

func UncompressItem(item *redis.Resp) []byte {
	if !item.IsType(redis.Str) {
		return nil
	}

	b, e := item.Bytes()
	if e != nil {
		return nil
	}

	if bytes.HasPrefix(b, magicSnappy) {
		uncompressed, e := snappy.Decode(nil, b[len(magicSnappy):])
		if e == nil {
			return uncompressed
		}
	}
	return nil
}
