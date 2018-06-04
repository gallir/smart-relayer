package ifaceS3

import (
	"archive/tar"
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/gzip"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/gallir/smart-relayer/lib"

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
	ch     chan []byte
	close  bool
}

func (r *ReaderUncompress) Get(key, path string, t time.Time) ([]byte, error) {
	lib.Debugf("S3 Get: %s: %s/%s", t.UTC(), path, key)

	svc := s3.New(r.sess)

	var response []byte

	err := svc.ListObjectsPages(&s3.ListObjectsInput{
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
				lib.Debugf("S3 Bucket: %s: %s", t.UTC(), *obj.Key)
				if r, err := r.download(key, obj.Key); err == nil {
					response = r
					return true
				}
			}
		}
		return true
	})

	if err != nil {
		log.Printf("FS S3 ERROR: %s", err)
	}

	return response, err
}

func (r *ReaderUncompress) download(key string, objKey *string) ([]byte, error) {

	buff, errTmp := ioutil.TempFile(os.TempDir(), "fslog-")
	if errTmp != nil {
		log.Printf("FS tempFile error: %s", errTmp)
		return nil, errTmp
	}
	defer os.Remove(buff.Name())

	downloader := s3manager.NewDownloader(r.sess)
	_, err := downloader.Download(buff, &s3.GetObjectInput{
		Key:    objKey,
		Bucket: aws.String(r.bucket),
	})
	if err != nil {
		return nil, err
	}

	t := tar.NewReader(buff)
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
				b := &bytes.Buffer{}

				lib.Debugf("S3 Object: %s", h.Name)

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
}
