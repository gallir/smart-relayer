package redis2kvstore

import (
	"net/http"
	"sync"
	"testing"

	"github.com/gallir/smart-relayer/lib"
)

func TestHMSETPool(t *testing.T) {
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h := getPoolHMSet()
			h.Reset()
			putPoolHMSet(h)
		}()
	}
	wg.Wait()
}

func TestFieldPool(t *testing.T) {
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f := getPoolField()
			f.Reset()
			putPoolField(f)
		}()
	}
	wg.Wait()
}

func TestPendingPool(t *testing.T) {
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var p map[string]*Hmset
			p = getPending()
			putPending(p)
		}()
	}
	wg.Wait()
}

func TestEmptyMarshal(t *testing.T) {
	h := &Hmset{}
	b, err := h.Marshal()
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("%s", string(b))
}

func TestKeyMarshal(t *testing.T) {
	h := &Hmset{
		Key: "testing1",
	}
	b, err := h.Marshal()
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("%s", string(b))
}

func TestSentMarshal(t *testing.T) {
	h := &Hmset{
		Key:  "testing1",
		Sent: false,
	}
	b, err := h.Marshal()
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("%s", string(b))
}

func TestEmptyFieldsMarshal(t *testing.T) {
	h := &Hmset{
		Key:    "testing1",
		Sent:   false,
		Fields: make([]*Field, 0, 100),
	}
	b, err := h.Marshal()
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("%s", string(b))
}

func TestEmptyFieldsMarshal1(t *testing.T) {
	h := &Hmset{
		Key:    "testing1",
		Sent:   false,
		Fields: make([]*Field, 0, 100),
	}
	b, err := h.Marshal()
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("%s", string(b))
}

func TestServerSendWithZeroFields(t *testing.T) {
	srv := &Server{}

	h := getPoolHMSet()
	defer putPoolHMSet(h)

	srv.send("testing", 1200, h.clone())
}

func TestServerSendWithNilField(t *testing.T) {
	srv := &Server{
		client: &http.Client{},
		config: lib.RelayerConfig{
			URL: "http://localhost",
		},
	}

	h := getPoolHMSet()
	defer putPoolHMSet(h)

	f1 := getPoolField()
	h.Fields = append(h.Fields, f1)

	f2 := getPoolField()
	f2 = nil
	h.Fields = append(h.Fields, f2)

	srv.send("testing", 1200, h.clone())
}

func TestDoingMarshalWithSyncPoolAndManyGoRoutines(t *testing.T) {
	wg := sync.WaitGroup{}
	for c := 0; c < 2000000; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			h := getPoolHMSet()
			defer putPoolHMSet(h)

			h.Key = "test1"
			h.Sent = true

			f1 := getPoolField()
			h.Fields = append(h.Fields, f1)

			f2 := getPoolField()
			f2 = nil
			h.Fields = append(h.Fields, f2)

			go func(hn *Hmset) {
				defer putPoolHMSet(hn)
				_, err := hn.Marshal()
				if err != nil {
					t.Error(err)
					return
				}
			}(h.clone())

		}()
	}
	wg.Wait()
}
