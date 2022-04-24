package core

import (
	"crypto/md5"
	"hash/crc64"
	"io"
	"os"
	"osssync/common/tracing"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mr-tron/base58"
)

type FileInfo interface {
	io.Closer

	FileType() string
	Name() string
	Path() string
	Size() int64
	MD5() (string, error)
	CRC64() (uint64, error)
	Exists() (bool, error)
	Properties() map[PropertyName]string
	Remove() error

	Stream() (FileStream, error)
}

type PhysicalFileInfo struct {
	path     string
	statInfo os.FileInfo
	exists   bool
	isIdle   bool

	md5       []byte
	md5Base58 string

	crc64 uint64

	hashOnce sync.Once
}

func OpenPhysicalFile(filePath string) (FileInfo, error) {
	fileInfo := &PhysicalFileInfo{isIdle: true}
	statInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			fileInfo.exists = true
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

	return fileInfo, nil
}

func (fileInfo *PhysicalFileInfo) FileType() string {
	return string(FileType_Physical)
}

func (fileInfo *PhysicalFileInfo) openFile(flag int) (*os.File, error) {
	return os.OpenFile(JoinUri(fileInfo.Path(), fileInfo.Name()), flag, 0)
}

func (fileInfo *PhysicalFileInfo) Close() error {
	return nil
}

func (fileInfo *PhysicalFileInfo) Name() string {
	return fileInfo.statInfo.Name()
}
func (fileInfo *PhysicalFileInfo) Path() string {
	return fileInfo.path
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

func (fileInfo *PhysicalFileInfo) Stream() (FileStream, error) {
	file, err := fileInfo.openFile(os.O_RDWR)
	if err != nil {
		return nil, tracing.Error(err)
	}
	return &PhysicalFileStream{
		f:    file,
		stat: fileInfo.statInfo,
	}, nil
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
