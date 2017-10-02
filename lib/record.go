package lib

import (
	"fmt"
	"log"
	"time"

	"github.com/pquerna/ffjson/ffjson"
	"github.com/spaolacci/murmur3"
)

type InterRecord struct {
	Types     int                    `json:"type,omitempty"`
	Timestamp float64                `json:"_ts,number"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Raw       []byte                 `json:"raw,omitempty"` // 0 json, 1 Raw bytes
}

// NewInterRecord create a InterRecord struct to the connection
func NewInterRecord() *InterRecord {
	return &InterRecord{
		Timestamp: float64(time.Now().UnixNano()) / float64(time.Nanosecond),
		Data:      make(map[string]interface{}, 0),
	}
}

func (r *InterRecord) Add(key, value string) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = value
	}
}

func (r *InterRecord) Sadd(key, value string) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make([]interface{}, 0)
	}

	r.Data[key] = append(r.Data[key].([]interface{}), value)
}

func (r *InterRecord) Mhset(key, k string, v interface{}) {
	if _, ok := r.Data[key]; !ok {
		r.Data[key] = make(map[string]interface{})
	}

	r.Data[key].(map[string]interface{})[k] = v
}

// Bytes return the record in bytes
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

// BytesUniqID return the record content in bytes and the uniq ID
func (r *InterRecord) BytesUniqID() ([]byte, string) {
	buf := r.Bytes()
	h := r.uniqID(buf)
	return buf, h
}

// String return the record in string format
func (r *InterRecord) String() string {
	buf := r.Bytes()
	defer ffjson.Pool(buf)

	return string(buf)
}

// StringUniqID build a uniq ID based on the content in string format
func (r *InterRecord) StringUniqID() (string, string) {
	buf := r.Bytes()
	defer ffjson.Pool(buf)

	h := r.uniqID(buf)
	return string(buf), h
}

// Len return the bytes used in Raw or the len of the map data
func (r *InterRecord) Len() int {
	if r.Types != 0 {
		return len(r.Raw)
	}

	return len(r.Bytes())
}

func (r *InterRecord) uniqID(b []byte) string {
	h := murmur3.New64()
	h.Write(b)
	return fmt.Sprintf("%d%0X", time.Now().UnixNano(), h.Sum64())
}
