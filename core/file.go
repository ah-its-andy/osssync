package core

import (
	"crypto/md5"
	"hash/crc64"
	"io"
	"math"
	"os"
	"osssync/common/config"
	"osssync/common/tracing"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mr-tron/base58"
)

type FileChunkWriter func(content []byte, chunk *FileChunkInfo) (n int, err error)

type FileChunkInfo struct {
	Number    int64
	ChunkSize int64
	Offset    int64
}
type BucketInfo struct {
	BasePath      string
	SubPath       string
	Name          string
	ContinueToken string
	IsTruncated   bool
	Objects       []*ObjectInfo
}

type ObjectInfo struct {
	BasePath     string
	RelativePath string
	FileType     FileType
	Size         int64
}

type FileInfo interface {
	io.Closer

	FileType() string
	Name() string
	Path() string
	RelativePath() string
	Size() int64
	MD5() (string, error)
	CRC64() (uint64, error)
	Exists() (bool, error)
	Properties() map[PropertyName]string
	Remove() error

	Reader() io.Reader
	Writer() io.Writer
	Flush() error

	// Stream() (FileStream, error)
	WalkChunk(reader io.Reader, chunkSize int64, fileSize int64, writer FileChunkWriter) error
	WriteChunk(content []byte, chunk *FileChunkInfo) (n int, err error)
}

type CryptoFileInfo interface {
	FileInfo
	UseEncryption(useMnemonic bool, content string) error
}

type PhysicalFileInfo struct {
	path         string
	relativePath string
	statInfo     os.FileInfo
	exists       bool
	isIdle       bool

	md5       []byte
	md5Base58 string

	crc64 uint64

	f *os.File

	hashOnce sync.Once
}

func OpenPhysicalFile(dirPath string, relativePath string) (FileInfo, error) {
	filePath := JoinUri(dirPath, relativePath)
	fileInfo := &PhysicalFileInfo{isIdle: true}
	statInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			fileInfo.exists = true
			fileDirPath := filePath[:strings.LastIndex(filePath, "/")]
			_, err := os.Stat(fileDirPath)
			if err != nil {
				if os.IsNotExist(err) {
					err = os.MkdirAll(fileDirPath, 0755)
					if err != nil {
						return nil, tracing.Error(err)
					}
				} else {
					return nil, tracing.Error(err)
				}
			}
			fd, err := os.Create(filePath)
			if err != nil {
				return nil, tracing.Error(err)
			}
			fd.Close()
			statInfo, err = os.Stat(filePath)
			if err != nil {
				return nil, tracing.Error(err)
			}
		} else {
			return nil, tracing.Error(err)
		}
	}
	lastIndexOf := strings.LastIndex(filePath, "/")
	if lastIndexOf == -1 {
		fileInfo.path = "/"
	} else {
		fileInfo.path = filePath[:lastIndexOf]
	}
	fileInfo.statInfo = statInfo
	fileInfo.exists = true
	fileInfo.relativePath = relativePath

	f, err := os.OpenFile(JoinUri(fileInfo.Path(), fileInfo.Name()), os.O_RDWR, 0)
	if err != nil {
		return nil, tracing.Error(err)
	}
	fileInfo.f = f
	return fileInfo, nil
}

func (fileInfo *PhysicalFileInfo) Reader() io.Reader {
	return fileInfo.f
}

func (fileInfo *PhysicalFileInfo) FileType() string {
	return string(FileType_Physical)
}

func (fileInfo *PhysicalFileInfo) Close() error {
	return fileInfo.f.Close()
}

func (fileInfo *PhysicalFileInfo) Name() string {
	return fileInfo.statInfo.Name()
}
func (fileInfo *PhysicalFileInfo) Path() string {
	return fileInfo.path
}
func (fileInfo *PhysicalFileInfo) RelativePath() string {
	return fileInfo.relativePath
}
func (fileInfo *PhysicalFileInfo) Size() int64 {
	return fileInfo.statInfo.Size()
}

func (fileInfo *PhysicalFileInfo) Exists() (bool, error) {
	_, err := os.Stat(JoinUri(fileInfo.Path(), fileInfo.Name()))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, tracing.Error(err)
	}
	return true, nil
}

func (fileInfo *PhysicalFileInfo) WalkChunk(reader io.Reader, chunkSize int64, fileSize int64, writer FileChunkWriter) error {
	dstObjName := fileInfo.f.Name()
	if config.GetValueOrDefault(Arg_Zip, false) && !strings.HasSuffix(dstObjName, ".zip") {
		dstObjName = JoinUri(fileInfo.f.Name(), ".zip")
	}
	if fileInfo.f.Name() != dstObjName {
		fileInfo.f.Close()
		if _, err := os.Stat(dstObjName); err == nil {
			os.Remove(dstObjName)
		}
		fileInfo.f, _ = os.Create(dstObjName)
	}
	chunkNum := int64(math.Ceil(float64(fileSize) / float64(chunkSize)))
	chunkReader := NewChunkReader(reader, chunkSize)
	defer chunkReader.Close()
	var chunkN = (int64)(chunkNum)
	for i := int64(0); i < chunkN; i++ {
		size := chunkSize
		if i == chunkN-1 {
			size = fileSize - i*chunkSize
		} else {
			size = chunkSize
		}
		chunk := &FileChunkInfo{
			Number:    i,
			ChunkSize: size,
			Offset:    i * chunkSize,
		}

		_, buffer := chunkReader.ReadNext()

		_, err := writer(buffer, chunk)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (fileInfo *PhysicalFileInfo) Writer() io.Writer {
	dstObjName := fileInfo.f.Name()
	if config.GetValueOrDefault(Arg_Zip, false) && !strings.HasSuffix(dstObjName, ".zip") {
		dstObjName = JoinUri(fileInfo.f.Name(), ".zip")
	}
	if fileInfo.f.Name() != dstObjName {
		fileInfo.f.Close()
		if _, err := os.Stat(dstObjName); err == nil {
			os.Remove(dstObjName)
		}
		fileInfo.f, _ = os.Create(dstObjName)
	}
	return fileInfo.f
}
func (fileInfo *PhysicalFileInfo) Flush() error {
	return nil
}

func (fileInfo *PhysicalFileInfo) WriteChunk(content []byte, chunk *FileChunkInfo) (n int, err error) {
	size := int64(len(content))
	bufferSize := chunk.ChunkSize
	if size > bufferSize {
		return 0, ErrIndexOutOfRange
	}

	n, err = fileInfo.f.WriteAt(content, chunk.Offset)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (fileInfo *PhysicalFileInfo) ComputeHashOnce() error {
	file, err := os.Open(JoinUri(fileInfo.Path(), fileInfo.Name()))
	if err != nil {
		return tracing.Error(err)
	}
	defer file.Close()
	bufferSize := 1024 * 1024
	buffer := make([]byte, bufferSize)
	md5 := md5.New()
	CRC64 := crc64.New(crc64.MakeTable(crc64.ECMA))
	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return tracing.Error(err)
		}
		_, err = md5.Write(buffer[:n])
		if err != nil {
			return tracing.Error(err)
		}
		_, err = CRC64.Write(buffer[:n])
		if err != nil {
			return tracing.Error(err)
		}

	}
	fileInfo.md5 = md5.Sum(nil)
	fileInfo.md5Base58 = base58.Encode(fileInfo.md5)
	fileInfo.crc64 = CRC64.Sum64()
	return nil
}

func (fileInfo *PhysicalFileInfo) MD5() (string, error) {
	err := fileInfo.ComputeHashOnce()
	if err != nil {
		return "", tracing.Error(err)
	}

	return fileInfo.md5Base58, nil
}

func (fileInfo *PhysicalFileInfo) CRC64() (uint64, error) {
	err := fileInfo.ComputeHashOnce()
	if err != nil {
		return 0, tracing.Error(err)
	}
	return fileInfo.crc64, nil
}
func (fileInfo *PhysicalFileInfo) Properties() map[PropertyName]string {
	properties := map[PropertyName]string{
		PropertyName_ContentType:    "application/octet-stream",
		PropertyName_ContentMD5:     fileInfo.md5Base58,
		PropertyName_ContentCRC64:   strconv.FormatUint(uint64(fileInfo.crc64), 10),
		PropertyName_ContentName:    "",
		PropertyName_ContentModTime: "",
		PropertyName_ContentLength:  "0",
	}

	if fileInfo.statInfo != nil {
		properties[PropertyName_ContentLength] = strconv.FormatInt(fileInfo.statInfo.Size(), 10)
		properties[PropertyName_ContentName] = fileInfo.statInfo.Name()
		properties[PropertyName_ContentModTime] = fileInfo.statInfo.ModTime().Format(time.RFC3339)
	}

	return properties
}
func (fileInfo *PhysicalFileInfo) Remove() error {
	return os.Remove(JoinUri(fileInfo.Path(), fileInfo.Name()))
}
