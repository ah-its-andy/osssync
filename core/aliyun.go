package core

import (
	"io"
	"osssync/common/tracing"
	"strconv"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type AliOSSConfig struct {
	EndPoint        string
	AccessKeyId     string
	AccessKeySecret string

	UseCname      bool
	Timeout       int32
	SecurityToken string
	EnableMD5     bool
	EnableCRC     bool
	Proxy         string
	AuthProxy     string
}

type AliOSSFileInfo struct {
	bucketName    string
	objectName    string
	exists        bool
	contentLength int64

	metaData map[PropertyName]string

	client *oss.Client
	bucket *oss.Bucket
}

func OpenAliOSS(config AliOSSConfig, bucketName string, objectName string) (FileInfo, error) {
	client, err := oss.New(config.EndPoint, config.AccessKeyId, config.AccessKeySecret)
	if err != nil {
		return nil, tracing.Error(err)
	}
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		return nil, tracing.Error(err)
	}
	exists, err := bucket.IsObjectExist(objectName)
	if err != nil {
		return nil, tracing.Error(err)
	}
	if !exists {
		return &AliOSSFileInfo{
			bucketName: bucketName,
			objectName: objectName,
			exists:     false,
			client:     client,
			bucket:     bucket,
			metaData:   make(map[PropertyName]string),
		}, nil
	}
	metaHeader, err := bucket.GetObjectDetailedMeta(objectName)
	if err != nil {
		return nil, tracing.Error(err)
	}
	metaMap := make(map[PropertyName]string)
	for k, v := range metaHeader {
		metaMap[PropertyName(k)] = v[0]
	}
	fileInfo := &AliOSSFileInfo{
		bucketName: bucketName,
		objectName: objectName,
		exists:     true,
		client:     client,
		bucket:     bucket,
		metaData:   metaMap,
	}
	if contentLength, ok := metaMap[PropertyName_ContentLength]; ok {
		if contentLengthInt, err := strconv.ParseInt(contentLength, 10, 32); err == nil {
			fileInfo.contentLength = contentLengthInt
		}
	}
	return fileInfo, nil
}

func (fileInfo *AliOSSFileInfo) Name() string {
	lastIndexOf := strings.LastIndex(fileInfo.objectName, "/")
	if lastIndexOf == -1 {
		return fileInfo.objectName
	}
	return fileInfo.objectName[lastIndexOf+1:]
}
func (fileInfo *AliOSSFileInfo) Path() string {
	lastIndexOf := strings.LastIndex(fileInfo.objectName, "/")
	if lastIndexOf == -1 {
		return "/"
	} else {
		return fileInfo.objectName[:lastIndexOf]
	}
}

func (fileInfo *AliOSSFileInfo) Exists() (bool, error) {
	return fileInfo.exists, nil
}

func (fileInfo *AliOSSFileInfo) Size() int64 {
	return fileInfo.contentLength
}
func (fileInfo *AliOSSFileInfo) OpenRead() (io.Reader, error) {
	reader, err := fileInfo.bucket.GetObject(fileInfo.objectName)
	if err != nil {
		return nil, tracing.Error(err)
	}
	return reader, nil
}
func (fileInfo *AliOSSFileInfo) WriteAll(reader io.Reader) error {
	err := fileInfo.bucket.PutObject(fileInfo.objectName, reader)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}
func (fileInfo *AliOSSFileInfo) MD5() (string, error) {
	if md5, ok := fileInfo.metaData[PropertyName_ContentMD5]; ok {
		return md5, nil
	}
	return "", nil
}
func (fileInfo *AliOSSFileInfo) CRC32() (uint32, error) {
	if crc32, ok := fileInfo.metaData[PropertyName_ContentCRC32]; ok {
		if crc32Int, err := strconv.ParseUint(crc32, 10, 32); err == nil {
			return uint32(crc32Int), nil
		}
	}
	return 0, nil
}
func (fileInfo *AliOSSFileInfo) Properties() map[PropertyName]string {
	return fileInfo.metaData
}

func (fileInfo *AliOSSFileInfo) Remove() error {
	err := fileInfo.bucket.DeleteObject(fileInfo.objectName)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}
