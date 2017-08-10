package fh

import (
	"log"
	"sync"
	"time"

	"github.com/pquerna/ffjson/ffjson"
)

type record struct {
	types     int
	Timestamp time.Time              `json:"_ts"`
	Rows      map[string]interface{} `json:"rows"`
	bytes     []byte                 // 0 json, 1 raw bytes
}

var reqPool = sync.Pool{
	New: func() interface{} {
		return record{}
	},
}

func newRecord() record {
	r := reqPool.Get().(record)
	r.Timestamp = time.Now()
	r.Rows = make(map[string]interface{})
	r.bytes = nil
	r.types = 0
	return r
}

func putRecord(r record) {
	reqPool.Put(r)
}

func (r *record) add(key, value string) {
	if _, ok := r.Rows[key]; !ok {
		r.Rows[key] = value
	}
}

func (r *record) sadd(key, value string) {

	if _, ok := r.Rows[key]; !ok {
		r.Rows[key] = make([]interface{}, 0)
	}

	r.Rows[key] = append(r.Rows[key].([]interface{}), value)
}

func (r *record) mhset(key string, k string, v interface{}) {

	if _, ok := r.Rows[key]; !ok {
		r.Rows[key] = make(map[string]interface{})
	}

	r.Rows[key].(map[string]interface{})[k] = v
}

func (r *record) Bytes() []byte {
	if r.types == 0 {
		buf, err := ffjson.Marshal(r)
		if err != nil {
			log.Panic(err)
		}

		ffjson.Pool(buf)

		return buf
	}

	return r.bytes
}

func (r *record) Len() int {
	if r.types == 0 {
		return len(r.Rows)
	}

	return len(r.bytes)
}
