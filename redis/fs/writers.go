package fs

import (
	"compress/gzip"
	"log"
	"os"
	"time"

	"github.com/gallir/smart-relayer/lib"
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
	if !w.srv.config.Compress || m.b.Len() <= minSizeForCompress {
		// Use the file name without .gz extension if the compression is
		// not active or if the size is smaller than 512 bytes (minSizeForCompress)
		fileName = dirName + "/" + m.filenamePlain()
	} else {
		// Use the extension .gz in the file name if is able to use the compression
		fileName = dirName + "/" + m.filenameGz()
	}

	newFile, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("File ERROR: %s", err)
		return err
	}
	defer newFile.Close()

	// After the time defined in timeOutWrite the write operation will return an error
	newFile.SetDeadline(time.Now().Add(timeOutWrite))

	// Use the compression if is active in the configuration and the message
	// is bigger than 512 bytes (minSizeForCompress)
	if !w.srv.config.Compress || m.b.Len() <= minSizeForCompress {
		if _, err := newFile.Write(m.b.B); err != nil {
			log.Printf("File ERROR: writing log: %s", err)
			return err
		}
		return nil
	}

	// If the compression is ON
	zw, _ := gzip.NewWriterLevel(newFile, lib.GzCompressionLevel)
	defer zw.Close()

	if _, err := zw.Write(m.b.B); err != nil {
		log.Printf("File ERROR: gzip writing log: %s", err)
		return err
	}

	return nil
}

func (w *writer) exit() {
	w.done <- true

	if w.srv.exiting {
		for m := range w.C {
			if err := w.writeTo(m); err == nil {
				putMsg(m)
			}
		}
	}
}
