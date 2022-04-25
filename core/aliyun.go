package core

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"osssync/common/logging"
	"osssync/common/tracing"
	"strconv"
	"strings"
	"sync"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	osscrypto "github.com/aliyun/aliyun-oss-go-sdk/oss/crypto"
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

	metaData map[PropertyName]string

	client *oss.Client
	bucket *oss.Bucket

	isCryptoFile bool
	cryptoSeed   string
	cryptoBucket *osscrypto.CryptoBucket
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

func (fileInfo *AliOSSFileInfo) UseEncryption(useMnemonic bool, content string) error {
	// 创建一个主密钥的描述信息，创建后不允许修改。主密钥描述信息和主密钥一一对应。
	// 如果所有的Object都使用相同的主密钥，主密钥描述信息可以为空，但后续不支持更换主密钥。
	// 如果主密钥描述信息为空，解密时无法判断使用的是哪个主密钥。
	// 强烈建议为每个主密钥都配置主密钥描述信息(json字符串), 由客户端保存主密钥和描述信息之间的对应关系（服务端不保存两者之间的对应关系）。

	// 由主密钥描述信息(json字符串)转换的map。
	materialDesc := make(map[string]string)

	// 创建一个主密钥。
	var seed int64
	if useMnemonic {
		seed = GetMnemonicSeed(content)
	} else {
		seed = GetPasswordSeed(content)
	}
	masterPrivateKey, err := GenerateRsaKey(seed)
	if err != nil {
		return tracing.Error(err)
	}

	pubK, err := GetPublicKeyPEM(masterPrivateKey, "PKIX")
	if err != nil {
		return tracing.Error(err)
	}

	privK, err := GetPrivateKeyPEM(masterPrivateKey, "PKCS8")

	// 根据主密钥描述信息创建一个主密钥对象。
	masterRsaCipher, err := osscrypto.CreateMasterRsa(materialDesc, string(pubK), string(privK))
	if err != nil {
		return tracing.Error(err)
	}

	// 根据主密钥对象创建一个用于加密的接口, 使用aes ctr模式加密。
	contentProvider := osscrypto.CreateAesCtrCipher(masterRsaCipher)

	// 获取一个用于客户端加密的已创建bucket。
	// 客户端加密bucket和普通bucket具有相似的用法。
	cryptoBucket, err := osscrypto.GetCryptoBucket(fileInfo.client, fileInfo.bucketName, contentProvider)
	if err != nil {
		return tracing.Error(err)
	}
	fileInfo.cryptoBucket = cryptoBucket
	fileInfo.isCryptoFile = true
	return nil
}

func (fileInfo *AliOSSFileInfo) FileType() string {
	return string(FileType_AliOSS)
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

func (fileInfo *AliOSSFileInfo) Stream() (FileStream, error) {
	pushTags := []PropertyName{PropertyName_ContentCRC64, PropertyName_ContentLength, PropertyName_ContentMD5, PropertyName_ContentModTime}
	opts := make([]oss.Option, 0)
	for k, v := range fileInfo.metaData {
		for _, pushTag := range pushTags {
			if pushTag == k {
				opts = append(opts, oss.Meta(string(pushTag), v))
			}
		}
	}
	return &AliOSSFileStream{
		ossFile:        fileInfo,
		client:         fileInfo.client,
		bucket:         fileInfo.bucket,
		cryptoBucket:   fileInfo.cryptoBucket,
		isCryptoStream: fileInfo.isCryptoFile,
		bucketName:     fileInfo.bucketName,
		objectName:     fileInfo.objectName,
		contentLength:  fileInfo.contentLength,
		options:        opts,
		uploadParts:    make([]oss.UploadPart, 0),
		buffer:         make([]byte, 0),
		flushLock:      &sync.Mutex{},
	}, nil
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

type AliOSSFileStream struct {
	localFile FileInfo

	bucketName    string
	objectName    string
	contentLength int64
	options       []oss.Option
	ossFile       *AliOSSFileInfo

	client         *oss.Client
	bucket         *oss.Bucket
	isCryptoStream bool
	cryptoBucket   *osscrypto.CryptoBucket
	imur           oss.InitiateMultipartUploadResult
	uploadParts    []oss.UploadPart

	buffer []byte
	offset int64

	flushLock *sync.Mutex
}

func (stream *AliOSSFileStream) Size() int64 {
	return stream.contentLength
}

func (stream *AliOSSFileStream) Seek(offset int64, whence int) (int64, error) {
	logging.Debug(fmt.Sprintf("File type %s not support seek", stream.ossFile.FileType()), nil)

	return 0, nil
}

func (stream *AliOSSFileStream) Read(p []byte) (n int, err error) {
	var reader io.ReadCloser
	if stream.isCryptoStream {
		reader, err = stream.cryptoBucket.GetObject(stream.objectName, stream.options...)
		if err != nil {
			return 0, tracing.Error(err)
		}
	} else {
		reader, err = stream.bucket.GetObject(stream.objectName, stream.options...)
		if err != nil {
			return 0, tracing.Error(err)
		}
	}
	defer reader.Close()

	return reader.Read(p)
}

func (stream *AliOSSFileStream) Write(p []byte) (n int, err error) {
	if stream.buffer == nil {
		stream.buffer = make([]byte, 0)
	}
	stream.buffer = append(stream.buffer, p...)
	return len(p), nil
}

func (stream *AliOSSFileStream) Flush() error {
	if stream.isCryptoStream {
		err := stream.flushToCryptoBucket()
		if err != nil {
			return tracing.Error(err)
		}
	} else {
		err := stream.flushToBucket()
		if err != nil {
			return tracing.Error(err)
		}
	}
	err := stream.ossFile.refreshMetaData()
	if err != nil {
		return tracing.Error(err)
	}

	return nil
}

func (stream *AliOSSFileStream) flushToBucket() error {
	if len(stream.uploadParts) == 0 {
		err := stream.bucket.PutObject(stream.objectName, bytes.NewReader(stream.buffer), stream.options...)
		if err != nil {
			return tracing.Error(err)
		}
		logging.Debug(fmt.Sprintf("flush object %s succeeded", stream.objectName), nil)
	} else {
		_, err := stream.bucket.CompleteMultipartUpload(stream.imur, stream.uploadParts)
		if err != nil {
			return tracing.Error(err)
		}
	}
	return nil
}

func (stream *AliOSSFileStream) flushToCryptoBucket() error {
	if len(stream.uploadParts) == 0 {
		err := stream.cryptoBucket.PutObject(stream.objectName, bytes.NewReader(stream.buffer), stream.options...)
		if err != nil {
			return tracing.Error(err)
		}
		logging.Debug(fmt.Sprintf("flush object %s succeeded", stream.objectName), nil)
	} else {
		_, err := stream.bucket.CompleteMultipartUpload(stream.imur, stream.uploadParts)
		if err != nil {
			return tracing.Error(err)
		}
	}
	return nil
}

func (stream *AliOSSFileStream) Close() error {
	return nil
}

func (stream *AliOSSFileStream) generateChunks(fileSize int64, chunkSize int64, chunkNum int64) ([]FileChunkWriter, error) {
	if int64(chunkNum) > fileSize {
		return nil, errors.New("oss: chunkNum invalid")
	}

	// 步骤1：初始化一个分片上传事件，并指定存储类型为标准存储。
	imur, err := stream.bucket.InitiateMultipartUpload(stream.objectName, stream.options...)
	if err != nil {
		return nil, tracing.Error(err)
	}
	stream.imur = imur

	var chunks []FileChunkWriter
	var chunkN = (int64)(chunkNum)
	for i := int64(0); i < chunkN; i++ {
		chunk := oss.FileChunk{}
		chunk.Number = int(i + 1)
		chunk.Offset = i * (fileSize / chunkN)
		if i == chunkN-1 {
			chunk.Size = fileSize - i*chunkSize
		} else {
			chunk.Size = chunkSize
		}

		chunkWriter := &AliOSSChunkFileWriter{
			fs:    stream,
			chunk: chunk,
		}
		chunks = append(chunks, chunkWriter)
	}
	return chunks, nil
}

func (stream *AliOSSFileStream) generateCryptoChunks(fileSize int64, chunkSize int64, chunkNum int64) ([]FileChunkWriter, error) {
	if int64(chunkNum) > fileSize {
		return nil, errors.New("oss: chunkNum invalid")
	}

	// 加密上下文信息。
	var cryptoContext osscrypto.PartCryptoContext
	cryptoContext.DataSize = fileSize

	// 期望的分片数，实际分片数以后续计算出来的为准。
	expectPartCount := int64(chunkNum)

	// 目前aes ctr加密分片大小需16个字节对齐。
	cryptoContext.PartSize = (fileSize / expectPartCount / 16) * 16
	actualChunkNum := int64(math.Ceil(float64(fileSize) / float64(cryptoContext.PartSize)))

	// 步骤1：初始化一个分片上传事件，并指定存储类型为标准存储。
	imur, err := stream.cryptoBucket.InitiateMultipartUpload(stream.objectName, &cryptoContext, stream.options...)
	if err != nil {
		return nil, tracing.Error(err)
	}
	stream.imur = imur

	var chunks []FileChunkWriter
	var chunkN = (int64)(actualChunkNum)
	for i := int64(0); i < chunkN; i++ {
		chunk := oss.FileChunk{}
		chunk.Number = int(i + 1)
		chunk.Offset = i * cryptoContext.PartSize
		if i == chunkN-1 {
			chunk.Size = fileSize - i*cryptoContext.PartSize
		} else {
			chunk.Size = cryptoContext.PartSize
		}

		chunkWriter := &AliOSSChunkFileWriter{
			fs:            stream,
			chunk:         chunk,
			cruptoContext: &cryptoContext,
		}
		chunks = append(chunks, chunkWriter)
	}

	return chunks, nil
}

func (stream *AliOSSFileStream) ChunkWrites(fileSize int64, chunkSize int64) ([]FileChunkWriter, error) {
	chunkNum := int(math.Ceil(float64(fileSize) / float64(chunkSize)))
	if chunkNum <= 0 || chunkNum > 10000 {
		return nil, errors.New("chunkNum invalid")
	}

	if stream.isCryptoStream {
		return stream.generateCryptoChunks(fileSize, chunkSize, int64(chunkNum))
	} else {
		return stream.generateChunks(fileSize, chunkSize, int64(chunkNum))
	}
}

type AliOSSChunkFileWriter struct {
	fs            *AliOSSFileStream
	chunk         oss.FileChunk
	cruptoContext *osscrypto.PartCryptoContext

	result *oss.UploadPart
}

func (writer *AliOSSChunkFileWriter) Write(p []byte) (n int, err error) {
	// 调用UploadPart方法上传每个分片。
	if writer.fs.isCryptoStream {
		part, err := writer.fs.cryptoBucket.UploadPart(writer.fs.imur, bytes.NewBuffer(p), writer.chunk.Size, writer.chunk.Number, *writer.cruptoContext)
		if err != nil {
			return 0, tracing.Error(err)
		}
		writer.result = &part
	} else {
		part, err := writer.fs.bucket.UploadPart(writer.fs.imur, bytes.NewBuffer(p), writer.chunk.Size, writer.chunk.Number)
		if err != nil {
			return 0, tracing.Error(err)
		}
		writer.result = &part
	}

	return len(p), nil
}

func (writer *AliOSSChunkFileWriter) Flush() error {
	if writer.result == nil {
		return nil
	}
	writer.fs.flushLock.Lock()
	defer writer.fs.flushLock.Unlock()
	writer.fs.uploadParts = append(writer.fs.uploadParts, *writer.result)
	writer.result = nil
	return nil
}

func (writer *AliOSSChunkFileWriter) Close() error {
	return nil
}

func (writer *AliOSSChunkFileWriter) Number() int64 {
	return int64(writer.chunk.Number)
}

func (writer *AliOSSChunkFileWriter) ChunkSize() int64 {
	return writer.chunk.Size
}

func (writer *AliOSSChunkFileWriter) Offset() int64 {
	return writer.chunk.Offset
}
