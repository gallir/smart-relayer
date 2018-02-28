package ifaceS3

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (

	// main/2018/02/16/02/main_2018-02-16-02_17-25_3.tar
	fileRegexp, _ = regexp.Compile(`.*[\w\_]+_\d{4}-\d{2}-\d{2}-\d{2}_(\d{2})-(\d{2})_\d{1,2}\.tar`)
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

func (r *ReaderUncompress) Get(key, path string, t time.Time) ([]byte, error) {

	log.Printf("Path %s - Key %s", path, key)

	svc := s3.New(r.sess)

	var response []byte

	svc.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(r.bucket),
		Prefix: aws.String(path),
	}, func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
		for _, obj := range p.Contents {
			s := fileRegexp.FindStringSubmatch(*obj.Key)

			from, err := strconv.ParseInt(s[1], 10, 64)
			if err != nil {
				continue
			}
			to, err := strconv.ParseInt(s[2], 10, 64)
			if err != nil {
				continue
			}

			if t.UTC().Minute() >= int(from) && t.UTC().Minute() <= int(to) {
				log.Printf("s: %#v - original: %s", s, t.UTC())
				log.Printf("Object: %s", *obj.Key)

				if r, err := r.download(key, obj.Key); err == nil {
					response = r
					return true
				}
			}
		}
		return true
	})

	return response, nil
}

func (r *ReaderUncompress) download(key string, objKey *string) ([]byte, error) {

	buff := &aws.WriteAtBuffer{}

	downloader := s3manager.NewDownloader(r.sess)
	numBytes, err := downloader.Download(buff, &s3.GetObjectInput{
		Key:    objKey,
		Bucket: aws.String(r.bucket),
	})
	if err != nil {
		log.Printf("B: %d, E: %s", numBytes, err)
		return nil, err
	}

	t := tar.NewReader(bytes.NewReader(buff.Bytes()))
	for {
		h, err := t.Next()
		switch {
		case err == io.EOF:
			return nil, nil
		case err != nil:
			return nil, err
		case h == nil:
			// Not sure why.. wire
			continue
		}

		switch h.Typeflag {
		case tar.TypeReg:
			if strings.HasPrefix(h.Name, key) {
				log.Printf("H: %#v", h.Name)
				b := &bytes.Buffer{}

				if strings.HasSuffix(h.Name, ".gz") {
					rgz, err := gzip.NewReader(t)
					if err != nil {
						return nil, err
					}
					io.Copy(b, rgz)
				} else {
					io.Copy(b, t)
				}
				return b.Bytes(), nil
			}
		default:
			continue
		}
	}

	return nil, fmt.Errorf("Not found")
}
