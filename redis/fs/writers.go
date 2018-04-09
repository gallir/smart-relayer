package fs

import (
	"io"
	"log"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

const (
	timeOutWrite       = 5 * time.Second
	retryWriter        = 2 * time.Second
	minSizeForCompress = 512
)

var (
	dirCache = NewDirCache()
)

type writer struct {
	srv  *Server
	C    chan *Msg
	done chan bool

	d        time.Time
	dirName  string
	fileName string
}

func newWriter(srv *Server, C chan *Msg) *writer {
	w := &writer{
		srv:  srv,
		C:    C,
		done: make(chan bool, 1),
	}

	go w.listen()

	return w
}

func (w *writer) listen() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	for {
		select {
		case m := <-w.C:
			if m == nil {
				continue
			}
			if err := w.writeTo(m); err == nil {
				putMsg(m)
			} else {
				log.Printf("FS ERROR Writer: %s", err)
				// send message back to the channel
				time.Sleep(retryWriter)
				w.C <- m
			}
		case <-w.done:
			return
		}
	}
}

func (w *writer) writeTo(m *Msg) error {
	dirName := m.fullpath()
	if err := dirCache.makeAll(dirName); err != nil {
		log.Printf("File ERROR: %s", err)
		return err
	}

	var fileName string
	if !w.srv.config.Compress || !m.gz {
		// Use the file name without .gz extension if the compression is
		// not active or if the size is smaller than 512 bytes (minSizeForCompress)
		fileName = dirName + "/" + m.filenamePlain()
	} else {
		// Use the extension .gz in the file name if is able to use the compression
		fileName = dirName + "/" + m.filenameGz()
	}

	tmpFile, err := os.Open(m.tmp)
	if err != nil {
		log.Printf("File ERROR: %s", err)
		return err
	}
	defer tmpFile.Close()

	newFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("File ERROR: %s", err)
		return err
	}
	defer newFile.Close()

	// After the time defined in timeOutWrite the write operation will return an error
	newFile.SetWriteDeadline(time.Now().Add(timeOutWrite))

	if _, err := io.Copy(newFile, tmpFile); err != nil {
		return err
	}

	time.Sleep(5 * time.Second)

	os.Remove(tmpFile.Name())

	return nil
}

func (w *writer) exit() {
	w.done <- true

	if atomic.LoadUint32(&w.srv.exiting) > 0 {
		for m := range w.C {
			if err := w.writeTo(m); err == nil {
				putMsg(m)
			}
		}
	}
}
