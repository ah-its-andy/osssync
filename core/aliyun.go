package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"osssync/common/config"
	"osssync/common/tracing"
	"strconv"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type AliOSSCfgWrapper struct {
	Config AliOSSConfig `yaml:"alioss"`
}

type AliOSSConfig struct {
	EndPoint        string `yaml:"endpoint"`
	AccessKeyId     string `yaml:"access_key_id"`
	AccessKeySecret string `yaml:"access_key_secret"`

	UseCname      bool   `yaml:"use_cname"`
	Timeout       int32  `yaml:"timeout"`
	SecurityToken string `yaml:"security_token"`
	EnableMD5     bool   `yaml:"enable_md5"`
	EnableCRC     bool   `yaml:"enable_crc"`
	Proxy         string `yaml:"proxy"`
	AuthProxy     string `yaml:"auth_proxy"`
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
	if strings.HasPrefix(objectName, "/") {
		objectName = objectName[1:]
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

func (fileInfo *AliOSSFileInfo) Close() error {
	return nil
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
func (fileInfo *AliOSSFileInfo) Copy(src FileInfo) error {
	options := []oss.Option{}
	for k, v := range fileInfo.metaData {
		options = append(options, oss.Meta(string(k), v))
	}
	if src.Size() > 5*1024*1024 {
		return fileInfo.putChunkFile(src, 5, options...)
	} else {
		return fileInfo.putLittleFile(src, options...)
	}
}

func (fileInfo *AliOSSFileInfo) putLittleFile(src FileInfo, options ...oss.Option) error {
	reader, err := src.OpenRead()
	if err != nil {
		return tracing.Error(err)
	}
	defer src.Close()

	cryptoFile, err := NewCryptoChunkedFile(reader.(*os.File), int(src.Size()), []byte(config.RequireString(Arg_Salt)))
	if err != nil {
		return tracing.Error(err)
	}

	cryptoBuffer, err := cryptoFile.ComputeHash()
	if err != nil {
		return tracing.Error(err)
	}

	err = fileInfo.bucket.PutObject(fileInfo.objectName, bytes.NewReader(cryptoBuffer), options...)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func (fileInfo *AliOSSFileInfo) putChunkFile(src FileInfo, chunkSizeMb int64, options ...oss.Option) error {
	reader, err := src.OpenRead()
	if err != nil {
		return tracing.Error(err)
	}
	defer src.Close()

	cryptoFile, err := NewCryptoChunkedFile(reader.(*os.File), int(chunkSizeMb*1024*1024), []byte(config.RequireString(Arg_Salt)))
	if err != nil {
		return tracing.Error(err)
	}

	cryptoSize, err := cryptoFile.Size()
	if err != nil {
		return tracing.Error(err)
	}

	chunkNum := int(math.Ceil(float64(cryptoSize) / float64(chunkSizeMb*1024*1024)))
	chunks, err := splitFileByPartNum(cryptoSize, chunkNum)
	if err != nil {
		return tracing.Error(err)
	}
	// 步骤1：初始化一个分片上传事件，并指定存储类型为标准存储。
	imur, err := fileInfo.bucket.InitiateMultipartUpload(fileInfo.objectName, options...)
	if err != nil {
		return tracing.Error(err)
	}

	go cryptoFile.ComputeHash()

	// 步骤2：上传分片。
	var parts []oss.UploadPart
	for _, chunk := range chunks {
		cryptoBuffer := <-cryptoFile.ChunkBuffered()
		// 调用UploadPart方法上传每个分片。
		part, err := fileInfo.bucket.UploadPart(imur, bytes.NewBuffer(cryptoBuffer), chunk.Size, chunk.Number)
		if err != nil {
			fmt.Println("Error:", err)
			os.Exit(-1)
		}
		parts = append(parts, part)
	}

	// 步骤3：完成分片上传，指定文件读写权限为公共读。
	_, err = fileInfo.bucket.CompleteMultipartUpload(imur, parts)
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

func splitFileByPartNum(size int64, chunkNum int) ([]oss.FileChunk, error) {
	if chunkNum <= 0 || chunkNum > 10000 {
		return nil, errors.New("chunkNum invalid")
	}

	if int64(chunkNum) > size {
		return nil, errors.New("oss: chunkNum invalid")
	}

	var chunks []oss.FileChunk
	var chunk = oss.FileChunk{}
	var chunkN = (int64)(chunkNum)
	for i := int64(0); i < chunkN; i++ {
		chunk.Number = int(i + 1)
		chunk.Offset = i * (size / chunkN)
		if i == chunkN-1 {
			chunk.Size = size/chunkN + size%chunkN
		} else {
			chunk.Size = size / chunkN
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}
