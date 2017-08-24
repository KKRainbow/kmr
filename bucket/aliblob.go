package bucket

import (
"bufio"
"bytes"
"io"

"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

// AliBlobBucket use azure blob
type AliBlobBucket struct {
	bucket     *oss.Bucket
	bucketName string
}

type AliBlobObjectReader struct {
	ObjectReader
	reader io.ReadCloser
}

func (reader *AliBlobObjectReader) Close() error {
	return reader.reader.Close()
}

func (reader *AliBlobObjectReader) Read(p []byte) (int, error) {
	return reader.reader.Read(p)
}

type AliBlobObjectWriter struct {
	ObjectWriter
	bucket       *oss.Bucket
	name         string
	buffer       *bytes.Buffer
	bufferWriter *bufio.Writer
}

func (writer *AliBlobObjectWriter) Close() error {
	if err := writer.bufferWriter.Flush(); err != nil {
		return err
	}
	return writer.bucket.PutObject(writer.name, bytes.NewReader(writer.buffer.Bytes()))
}

func (writer *AliBlobObjectWriter) Write(data []byte) (int, error) {
	return writer.bufferWriter.Write(data)
}

// NewAliBlobBucket new ali blob bucket
func NewAliBlobBucket(endpoint, accessKeyId, accessKeySecret string, bucketName string) (Bucket, error) {
	client, err := oss.New(endpoint, accessKeyId, accessKeySecret)
	if err != nil {
		return nil, err
	}
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		return nil, err
	}
	return &AliBlobBucket{
		bucket:     bucket,
		bucketName: bucketName,
	}, nil
}

func (bk *AliBlobBucket) OpenRead(name string) (ObjectReader, error) {
	ioReader, err := bk.bucket.GetObject(name)
	if err != nil {
		return nil, err
	}
	return &AliBlobObjectReader{
		reader: ioReader,
	}, nil
}

func (bk *AliBlobBucket) OpenWrite(name string) (ObjectWriter, error) {
	var b bytes.Buffer
	return &AliBlobObjectWriter{
		bucket:       bk.bucket,
		name:         name,
		bufferWriter: bufio.NewWriter(&b),
		buffer:       &b,
	}, nil
}

func (bk *AliBlobBucket) Delete(key string) error {
	return bk.bucket.DeleteObject(key)
}

func (bk *AliBlobBucket) ListFiles() ([]string, error) {
	lsRes, err := bk.bucket.ListObjects()
	if err != nil {
		return nil, err
	}
	res := make([]string, 0)
	for _, o := range lsRes.Objects {
		res = append(res, o.Key)
	}
	return res, nil
}