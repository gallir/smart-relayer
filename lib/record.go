package lib

import (
	"fmt"
	"log"
	"time"

	"github.com/pquerna/ffjson/ffjson"
	"github.com/spaolacci/murmur3"
)

type InterRecord struct {
	Types     int
	Timestamp float64                `json:"_ts"`
	Data      map[string]interface{} `json:"data"`
	Raw       []byte                 // 0 json, 1 Raw bytes
}

func (r *InterRecord) _make() {
	if r.Data != nil {
		return
	}

	r.Data = make(map[string]interface{}, 0)
}

func (r *InterRecord) Add(key, value string) {
	r._make()

	if _, ok := r.Data[key]; !ok {
		r.Data[key] = value
	}
}

func (r *InterRecord) Sadd(key, value string) {
	r._make()

	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make([]interface{}, 0)
	}

	r.Data[key] = append(r.Data[key].([]interface{}), value)
}

func (r *InterRecord) Mhset(key, k string, v interface{}) {
	r._make()

	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make(map[string]interface{})
	}

	r.Data[key].(map[string]interface{})[k] = v
}

func (r *InterRecord) Bytes() []byte {
	if r.Types != 0 {
		return r.Raw
	}

	buf, err := ffjson.Marshal(r)
	if err != nil {
		ffjson.Pool(buf)
		log.Printf("Error in ffjson: %s", err)
		return nil
	}

	return buf
}

func (r *InterRecord) BytesUniqID() ([]byte, string) {
	buf := r.Bytes()
	h := r.uniqID(buf)
	return buf, h
}

func (r *InterRecord) String() string {
	buf := r.Bytes()
	defer ffjson.Pool(buf)

	return string(buf)
}

func (r *InterRecord) StringUniqID() (string, string) {
	buf := r.Bytes()
	defer ffjson.Pool(buf)

	h := r.uniqID(buf)
	return string(buf), h
}

func (r *InterRecord) Len() int {
	if r.Types != 0 {
		return len(r.Raw)
	}

	return len(r.Data)
}

func (r *InterRecord) uniqID(b []byte) string {
	h := murmur3.New64()
	h.Write(b)
	return fmt.Sprintf("%d%0X", time.Now().UnixNano(), h.Sum64())
}
