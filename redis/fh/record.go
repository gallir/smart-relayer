package fh

import (
	"log"
	"sync"
	"time"

	"github.com/pquerna/ffjson/ffjson"
)

type record struct {
	types     int
	Timestamp float64                `json:"_ts"`
	Data      map[string]interface{} `json:"data"`
	bytes     []byte                 // 0 json, 1 raw bytes
}

var reqPool = sync.Pool{
	New: func() interface{} {
		return record{}
	},
}

func newRecord() record {
	r := reqPool.Get().(record)
	r.Reset()
	r.Timestamp = float64(time.Now().UnixNano()) / float64(time.Nanosecond)
	return r
}

func putRecord(r record) {
	reqPool.Put(r)
}

func (r *record) Reset() {
	r.types = 0

	if r.bytes == nil {
		r.bytes = make([]byte, 0, 128)
	} else {
		r.bytes = r.bytes[:0]
	}

	if r.Data == nil {
		r.Data = make(map[string]interface{})
	} else {
		for i := range r.Data {
			delete(r.Data, i)
		}
	}
}

func (r *record) add(key, value string) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = value
	}
}

func (r *record) sadd(key, value string) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make([]interface{}, 0)
	}

	r.Data[key] = append(r.Data[key].([]interface{}), value)
}

func (r *record) mhset(key string, k string, v interface{}) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make(map[string]interface{})
	}

	r.Data[key].(map[string]interface{})[k] = v
}

func (r *record) Bytes() []byte {
	if r.types != 0 {
		return r.bytes
	}

	buf, err := ffjson.Marshal(r)
	if err != nil {
		log.Panic(err)
	}
	//ffjson.Pool(buf)

	return buf
}

func (r *record) Len() int {
	if r.types != 0 {
		return len(r.bytes)
	}

	return len(r.Data)
}
