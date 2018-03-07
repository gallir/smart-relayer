package fs

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

var (
	defaultTTL int64 = 60 * 60 // Seconds
)

type dirTTL struct {
	sync.Mutex
	m        sync.Map
	creating map[string]sync.Once
	tick     *time.Ticker
}

func NewDirCache() *dirTTL {
	d := &dirTTL{
		creating: make(map[string]sync.Once),
		tick:     time.NewTicker(1 * time.Second),
	}
	go d.clean()
	return d
}

func (d *dirTTL) clean() {
	for {
		<-d.tick.C
		d.m.Range(func(key, val interface{}) bool {
			if time.Now().Unix() > val.(int64)+defaultTTL {
				s := key.(string)
				d.m.Delete(s)
				log.Printf("FS dirTTL delete: %s", s)
			}
			return true
		})
	}
}

func (d *dirTTL) makeAll(key string) error {
	if _, ok := d.m.Load(key); ok {
		d.m.Store(key, time.Now().Unix())
		return nil
	}

	// Add or recover from the creation map
	d.Lock()
	_, ok := d.creating[key]
	if !ok {
		d.creating[key] = sync.Once{}
	}
	mkDirOnce := d.creating[key]
	d.Unlock()

	// https://golang.org/pkg/sync/#Once
	// Once is an object that will perform exactly one action.
	mkDirOnce.Do(func() {
		if err := os.MkdirAll(key, os.ModePerm); err != nil {
			log.Printf("File ERROR: %s", err)
			return
		}
		d.m.Store(key, time.Now().Unix())

		// Delete from the creation map
		d.Lock()
		lib.Debugf("FS dirTTL create: %s", key)
		delete(d.creating, key)
		d.Unlock()
	})

	return nil
}
