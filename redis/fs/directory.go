package fs

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

var (
	// defaultDirTTL is the time after that the directory will be removed from the cache
	defaultDirTTL = time.Minute * -1
	// defaultCleanInterval is the interval time to delete old records from the cache
	defaultCleanInterval = time.Second * 1
)

// MkDirCache store in memory the existing directories created
type MkDirCache struct {
	sync.Mutex
	m        sync.Map
	creating map[string]sync.Once
}

// NewDirCache is a process create a directory just for once
func NewDirCache() *MkDirCache {
	d := &MkDirCache{
		creating: make(map[string]sync.Once),
	}
	// Run a goroutine to remove the directory from the local cache
	go d.clean()
	return d
}

func (d *MkDirCache) clean() {
	for {
		// Wait for the next interval
		time.Sleep(defaultCleanInterval)

		// expires is the limit timestamp to keep a directory in the cache
		expires := time.Now().Add(defaultDirTTL).Unix()

		// Read all the values in the map and check if they are older than expires
		d.m.Range(func(key, val interface{}) bool {
			if val.(int64) < expires {
				// If the record is older than the expires delete it
				d.m.Delete(key)
				//lib.Debugf("FS MkDirCache delete: %s", key.(string))
			}
			return true
		})
	}
}

// makeAll is a function to create a directory just one time
// and store it in a sync.Map as local cache. The process to
// create a directory require some I/O to the filesystem that
// we meed to reduce. Also require to parse (Unmarshal) the path
// and check level by level in the directory tree. Storing in
// the cache and checking from there is a way to reduce all this
// steps at each call.
func (d *MkDirCache) makeAll(key string) error {
	// Check if the directory is in the cache
	if _, ok := d.m.Load(key); ok {
		return nil
	}

	// The process for create a directory could be called from
	// many goroutines so we need to know if another goroutine is
	// creating the directory and be sure that only one of them
	// create the directory. The other threads should wait until
	// the first thread create the directory. The following map
	// store the sync.Once struct using the directory name as key.
	d.Lock()
	mkDirOnce, ok := d.creating[key]
	if !ok {
		// If the directory is not in the map we add it
		mkDirOnce = sync.Once{}
		d.creating[key] = mkDirOnce
	}
	d.Unlock()

	// Will store the error returned by os.MkdirAll few lines below
	var err error

	// https://golang.org/pkg/sync/#Once
	// Once is an object that will perform exactly one action.
	mkDirOnce.Do(func() {
		if err = os.MkdirAll(key, os.ModePerm); err != nil {
			log.Printf("File ERROR: %s", err)
			return
		}
		d.m.Store(key, time.Now().Unix())

		// If the directory was created sucessfully we procede
		// to remove the directory from the map
		d.Lock()
		delete(d.creating, key)
		d.Unlock()
		lib.Debugf("FS MkDirCache create: %s", key)
	})

	return nil
}
