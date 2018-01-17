package ifaceS3

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func Move(f io.ReadSeeker, sess *session.Session, fromTime, toTime, key, bucket string, fullpathFn, pathFn func(t time.Time) string) error {

	d, err := time.ParseDuration(fromTime)
	if err != nil {
		return err
	}

	until := time.Duration(15 * time.Minute)
	if toTime != "" {
		until, err = time.ParseDuration(toTime)
		if err != nil {
			return err
		}
	}

	if until >= d {
		return errors.New("From is after to")
	}

	from := time.Now().Add(-d).Add(-1 * time.Second)
	recDone := 0

	for {

		// Don't process files newer than X minutes
		if from.After(time.Now().Add(-until)) {
			break
		}

		from = from.Add(1 * time.Second)
		path := fullpathFn(from)

		files, err := ioutil.ReadDir(path)
		if err != nil {
			continue
		}

		s3Upload := func(name string, f *os.File) error {
			_, err := s3.New(sess).PutObject(&s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   f,
			})
			if err != nil {
				return err
			}
			return nil
		}

		// The idea of the next lines is read each log file and copy in one file,
		// trying to don't allocate the entire content in the memory.
		// In the same way the *WriteCompress will compress all the content in a
		// new file using an stream.
		wc, err := NewWriteCompress(pathFn(from), s3Upload)
		if err != nil {
			return err
		}

		for _, file := range files {

			if !file.Mode().IsRegular() {
				continue
			}

			// create a new dir/file header
			header, err := tar.FileInfoHeader(file, file.Name())
			if err != nil {
				log.Printf("ERROR tar FileInfoHeader: %s", err)
				continue
			}

			wc.t.WriteHeader(header)

			f, err := os.Open(fmt.Sprintf("%s/%s", path, file.Name()))
			if err != nil {
				log.Printf("STREAM: Can't read the file %s: %s", file.Name(), err)
				continue
			}

			if _, err := io.Copy(wc.t, f); err != nil {
				log.Printf("ERROR tar copy: %s", err)
				continue
			}

			// Close file (f)
			f.Close()

		}

		if err := wc.Close(); err != nil {
			return err
		}

		// Count total records
		recDone += wc.RecDone

		// for _, file := range files {
		// 	(os.Remove(fmt.Sprintf("%s/%s", path, file.Name()))
		// }
	}

	return nil

}
