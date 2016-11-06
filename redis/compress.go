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

func Compress(r *redis.Resp) *redis.Resp {
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
			items[i] = CompressBytes(b)
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

func CompressBytes(b []byte) []byte {
	n := snappy.MaxEncodedLen(len(b)) + len(magicSnappy)
	o := make([]byte, n)
	copy(o, magicSnappy)
	c := snappy.Encode(o[len(magicSnappy):], b)
	c = o[:len(c)+len(magicSnappy)]
	//fmt.Println("compressed", len(o), len(c), len(f), string(f[:14]), string(c[:10]))
	return c
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
