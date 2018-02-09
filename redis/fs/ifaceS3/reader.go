package ifaceS3

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func NewReaderUncompress(sess *session.Session, bucket string) *ReaderUncompress {
	r := &ReaderUncompress{
		sess:   sess,
		bucket: bucket,
		ch:     make(chan []byte),
	}
	return r
}

type ReaderUncompress struct {
	sess   *session.Session
	bucket string
	g      *gzip.Reader

	ch    chan []byte
	close bool
}

func (r *ReaderUncompress) Get(key string) ([]byte, error) {

	log.Printf("Key: %s", key)

	downloader := s3manager.NewDownloader(r.sess)

	options := func(d *s3manager.Downloader) {
		d.Concurrency = 1
	}

	buff := &bytes.Buffer{}
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer r.Close()

		_, err := downloader.Download(r,
			&s3.GetObjectInput{
				Bucket: &r.bucket,       // Required
				Key:    aws.String(key), // Required
			}, options)
		if err != nil {
			log.Printf("ERROR: %s", err)
			return
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		u, err := gzip.NewReader(r)
		if err != nil {
			log.Panicf("gz: NewReader failed: %s", err)
		}

		t := tar.NewReader(u)

		i := 0
		for {
			header, err := t.Next()

			if err == io.EOF {
				break
			}

			if err != nil {
				log.Panic(err)
				break
			}

			name := header.Name

			switch header.Typeflag {
			case tar.TypeDir:
				continue
			case tar.TypeReg:
				fmt.Println("(", i, ")", "Name: ", name)
				io.Copy(buff, t)
			default:
				log.Printf("unknown type")
			}

			i++
		}

	}()

	wg.Wait()

	return buff.Bytes(), nil
}

func (r *ReaderUncompress) Read(b []byte) (int, error) {
	t := <-r.ch
	copy(b, t)

	log.Printf("Read: %d -> %d | %d", len(b), len(t), len(r.ch))

	if r.close && len(r.ch) == 0 {
		return len(t), io.EOF
	}

	return len(t), nil

}

func (r *ReaderUncompress) WriteAt(b []byte, offset int64) (int, error) {

	log.Printf("* WriteAt: %d", len(b))

	bufSize := 4096
	l := len(b)

	if len(b) > bufSize {
		for {
			r.ch <- b[0:bufSize]
			log.Printf("  WriteAt: %d", len(b[0:bufSize]))
			b = b[bufSize+1:]
			if len(b) <= bufSize {
				log.Printf("  WriteAt Break: %d", len(b))
				break
			}
		}
	}

	log.Printf("  WriteAt END: %d", len(b))

	r.ch <- b
	return l, nil
}

func (r *ReaderUncompress) Close() {
	close(r.ch)
	//r.close = true
}
