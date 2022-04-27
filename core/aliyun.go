package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"osssync/common/config"
	"osssync/common/tracing"
	"strconv"
	"strings"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

func LsAliOss(config AliOSSConfig, basePath string, continueToken string) (*BucketInfo, error) {
	client, err := oss.New(config.EndPoint, config.AccessKeyId, config.AccessKeySecret)
	if err != nil {
		return nil, tracing.Error(err)
	}
	bucketName, err := ResolveBucketName(basePath)
	if err != nil {
		return nil, tracing.Error(err)
	}

	bucket, err := client.Bucket(bucketName)
	if err != nil {
		return nil, tracing.Error(err)
	}

	options := []oss.Option{
		oss.ContinuationToken(continueToken),
	}
	subPath, err := ResolveRelativePath(basePath)
	if err == nil {
		options = append(options, oss.Prefix(subPath))
	}

	lsRes, err := bucket.ListObjectsV2(options...)
	if err != nil {
		return nil, tracing.Error(err)
	}

	bucketInfo := &BucketInfo{
		BasePath:    basePath,
		SubPath:     subPath,
		Name:        bucketName,
		Objects:     make([]*ObjectInfo, 0),
		IsTruncated: lsRes.IsTruncated,
	}
	for _, object := range lsRes.Objects {
		absPath := fmt.Sprintf("oss://%s/%s", bucketName, object.Key)
		relativePath := strings.TrimPrefix(absPath, basePath)
		objInfo := &ObjectInfo{
			BasePath:     basePath,
			RelativePath: relativePath,
			Size:         object.Size,
			FileType:     FileType_AliOSS,
		}
		bucketInfo.Objects = append(bucketInfo.Objects, objInfo)
	}
	if lsRes.IsTruncated {
		bucketInfo.ContinueToken = lsRes.NextContinuationToken
	}
	return bucketInfo, nil
}

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
	objectDir     string
	relativePath  string
	exists        bool
	contentLength int64
	options       []oss.Option
	buffer        *BufferWriter

	metaData map[PropertyName]string

	client      *oss.Client
	bucket      *oss.Bucket
	imur        *oss.InitiateMultipartUploadResult
	uploadParts []oss.UploadPart
}

func normalizeAliOSSMetaKey(k string) string {
	return strings.Replace(strings.ToLower(k), "x-oss-meta-", "", 1)
}

func OpenAliOSS(config AliOSSConfig, bucketName string, objectDir string, relativePath string) (FileInfo, error) {
	client, err := oss.New(config.EndPoint, config.AccessKeyId, config.AccessKeySecret)
	if err != nil {
		return nil, tracing.Error(err)
	}
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		return nil, tracing.Error(err)
	}
	objectName := JoinUri(objectDir, relativePath)
	exists, err := bucket.IsObjectExist(objectName)
	if err != nil {
		return nil, tracing.Error(err)
	}
	if !exists {
		return &AliOSSFileInfo{
			bucketName:   bucketName,
			objectName:   objectName,
			objectDir:    objectDir,
			relativePath: relativePath,
			exists:       false,
			client:       client,
			bucket:       bucket,
			metaData:     make(map[PropertyName]string),
			buffer:       NewBufferWriter(0),
			uploadParts:  make([]oss.UploadPart, 0),
		}, nil
	}

	fileInfo := &AliOSSFileInfo{
		bucketName:   bucketName,
		objectName:   objectName,
		objectDir:    objectDir,
		relativePath: relativePath,
		exists:       true,
		client:       client,
		bucket:       bucket,
		metaData:     make(map[PropertyName]string),
		buffer:       NewBufferWriter(0),
		uploadParts:  make([]oss.UploadPart, 0),
	}
	err = fileInfo.refreshMetaData()
	if err != nil {
		return nil, tracing.Error(err)
	}
	if contentLength, ok := fileInfo.metaData["content-length"]; ok {
		if contentLengthInt, err := strconv.ParseInt(contentLength, 10, 32); err == nil {
			fileInfo.contentLength = contentLengthInt
		}
	}
	return fileInfo, nil
}

func (fileInfo *AliOSSFileInfo) FileType() string {
	return string(FileType_AliOSS)
}

func (fileInfo *AliOSSFileInfo) Reader() io.Reader {
	obj, err := fileInfo.bucket.GetObject(fileInfo.objectName)
	if err != nil {
		return nil
	}
	return obj
}

func (fileInfo *AliOSSFileInfo) refreshMetaData() error {
	metaHeader, err := fileInfo.bucket.GetObjectDetailedMeta(fileInfo.objectName)
	if err != nil {
		return tracing.Error(err)
	}
	metaMap := make(map[PropertyName]string)
	for k, v := range metaHeader {
		metaMap[PropertyName(normalizeAliOSSMetaKey(k))] = v[0]
	}
	fileInfo.metaData = metaMap
	return nil
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

func (fileInfo *AliOSSFileInfo) RelativePath() string {
	return fileInfo.objectName
}

func (fileInfo *AliOSSFileInfo) Exists() (bool, error) {
	return fileInfo.exists, nil
}

func (fileInfo *AliOSSFileInfo) Size() int64 {
	return fileInfo.contentLength
}

func (fileInfo *AliOSSFileInfo) MD5() (string, error) {
	if md5, ok := fileInfo.metaData[PropertyName_ContentMD5]; ok {
		return md5, nil
	}
	return "", nil
}
func (fileInfo *AliOSSFileInfo) CRC64() (uint64, error) {
	if CRC64, ok := fileInfo.metaData["x-oss-hash-crc64ecma"]; ok {
		if CRC64Int, err := strconv.ParseUint(CRC64, 10, 64); err == nil {
			return uint64(CRC64Int), nil
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

func (fileInfo *AliOSSFileInfo) Writer() io.Writer {
	return fileInfo.buffer
}
func (fileInfo *AliOSSFileInfo) Flush() error {
	if len(fileInfo.buffer.Bytes()) > 0 {
		dstObjName := fileInfo.objectName
		if config.GetValueOrDefault(Arg_Zip, false) {
			dstObjName = JoinUri(fileInfo.objectName, ".zip")
		}
		err := fileInfo.bucket.PutObject(dstObjName, bytes.NewReader(fileInfo.buffer.Bytes()), fileInfo.options...)
		if err != nil {
			return tracing.Error(err)
		}
	} else if fileInfo.imur != nil {
		_, err := fileInfo.bucket.CompleteMultipartUpload(*fileInfo.imur, fileInfo.uploadParts)
		if err != nil {
			return tracing.Error(err)
		}
	} else {
		return tracing.Error(errors.New("no data to upload"))
	}
	err := fileInfo.refreshMetaData()
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func (fileInfo *AliOSSFileInfo) WalkChunk(reader io.Reader, chunkSize int64, fileSize int64, writer FileChunkWriter) error {
	chunkNum := int(math.Ceil(float64(fileSize) / float64(chunkSize)))
	if chunkNum <= 0 || chunkNum > 10000 {
		return errors.New("chunkNum invalid")
	}

	if int64(chunkNum) > fileSize {
		return errors.New("oss: chunkNum invalid")
	}

	chunkReader := NewChunkReader(reader, chunkSize)
	defer chunkReader.Close()

	dstObjName := fileInfo.objectName
	if config.GetValueOrDefault(Arg_Zip, false) {
		dstObjName = JoinUri(fileInfo.objectName, ".zip")
	}
	// 步骤1：初始化一个分片上传事件，并指定存储类型为标准存储。
	imur, err := fileInfo.bucket.InitiateMultipartUpload(dstObjName, fileInfo.options...)
	if err != nil {
		return tracing.Error(err)
	}
	fileInfo.imur = &imur

	var chunkN = (int64)(chunkNum)
	for i := int64(0); i < chunkN; i++ {
		size := chunkSize
		if i == chunkN-1 {
			size = fileSize - i*chunkSize
		} else {
			size = chunkSize
		}
		chunk := &FileChunkInfo{
			Number:    i + 1,
			ChunkSize: size,
			Offset:    i * chunkSize,
		}
		_, buffer := chunkReader.ReadNext()
		_, err = writer(buffer, chunk)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fileInfo *AliOSSFileInfo) WriteChunk(content []byte, chunk *FileChunkInfo) (n int, err error) {
	part, err := fileInfo.bucket.UploadPart(*fileInfo.imur, bytes.NewBuffer(content),
		chunk.ChunkSize, int(chunk.Number))
	if err != nil {
		return 0, tracing.Error(err)
	}
	fileInfo.uploadParts = append(fileInfo.uploadParts, part)
	return len(content), nil
}
