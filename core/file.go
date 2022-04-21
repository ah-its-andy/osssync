package core

import (
	"crypto/md5"
	"hash/crc32"
	"io"
	"os"
	"osssync/common/tracing"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mr-tron/base58"
)

type FileInfo interface {
	Name() string
	Path() string
	Size() int64
	OpenRead() (io.Reader, error)
	Copy(src FileInfo) error
	MD5() (string, error)
	CRC32() (uint32, error)
	Exists() (bool, error)
	Properties() map[PropertyName]string
	Remove() error
	Close() error
}

type PhysicalFileInfo struct {
	path     string
	statInfo os.FileInfo
	exists   bool
	isIdle   bool
	file     *os.File

	md5       []byte
	md5Base58 string

	crc32 uint32

	hashOnce sync.Once
}

func OpenPhysicalFile(filePath string) (FileInfo, error) {
	fileInfo := &PhysicalFileInfo{}
	statInfo, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			fileInfo.exists = false
			return fileInfo, nil
		}
		return nil, tracing.Error(err)
	}
	lastIndexOf := strings.LastIndex(filePath, "/")
	if lastIndexOf == -1 {
		fileInfo.path = "/"
	} else {
		fileInfo.path = filePath[:lastIndexOf]
	}
	fileInfo.statInfo = statInfo
	fileInfo.exists = true

	err = fileInfo.ComputeHashOnce()
	if err != nil {
		return nil, tracing.Error(err)
	}

	return fileInfo, nil
}

func (fileInfo *PhysicalFileInfo) Close() error {
	if fileInfo.file != nil {
		return fileInfo.file.Close()
	}
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
	return fileInfo.exists, nil
}

func (fileInfo *PhysicalFileInfo) open() error {
	if fileInfo.isIdle {
		file, err := os.Open(filepath.Join(fileInfo.Path(), fileInfo.Name()))
		if err != nil {
			return tracing.Error(err)
		}
		fileInfo.file = file
		fileInfo.isIdle = false
	}
	return nil
}

func (fileInfo *PhysicalFileInfo) OpenRead() (io.Reader, error) {
	if err := fileInfo.open(); err != nil {
		return nil, tracing.Error(err)
	}
	return fileInfo.file, nil
}

func (fileInfo *PhysicalFileInfo) Copy(src FileInfo) error {
	if err := fileInfo.open(); err != nil {
		return tracing.Error(err)
	}
	reader, err := src.OpenRead()
	if err != nil {
		return tracing.Error(err)
	}
	_, err = io.Copy(fileInfo.file, reader)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func (fileInfo *PhysicalFileInfo) OpenWrite() (io.Writer, error) {
	if err := fileInfo.open(); err != nil {
		return nil, tracing.Error(err)
	}
	return fileInfo.file, nil
}

func (fileInfo *PhysicalFileInfo) ComputeHashOnce() error {
	var errOutter error

	fileInfo.hashOnce.Do(func() {
		file, err := os.Open(filepath.Join(fileInfo.Path(), fileInfo.Name()))
		if err != nil {
			errOutter = tracing.Error(err)
		}
		defer file.Close()
		bufferSize := 1024 * 1024
		buffer := make([]byte, bufferSize)
		md5 := md5.New()
		crc32 := crc32.New(crc32.IEEETable)
		for {
			n, err := file.Read(buffer)
			if err != nil {
				if err == io.EOF {
					break
				}
				errOutter = tracing.Error(err)
				break
			}
			_, err = md5.Write(buffer[:n])
			if err != nil {
				errOutter = tracing.Error(err)
				break
			}
			_, err = crc32.Write(buffer[:n])
			if err != nil {
				errOutter = tracing.Error(err)
				break
			}

		}
		fileInfo.md5 = md5.Sum(nil)
		fileInfo.md5Base58 = base58.Encode(fileInfo.md5)
		fileInfo.crc32 = crc32.Sum32()
	})
	if errOutter != nil {
		return errOutter
	}
	return nil
}

func (fileInfo *PhysicalFileInfo) MD5() (string, error) {
	return fileInfo.md5Base58, nil
}

func (fileInfo *PhysicalFileInfo) CRC32() (uint32, error) {
	return fileInfo.crc32, nil
}
func (fileInfo *PhysicalFileInfo) Properties() map[PropertyName]string {
	return map[PropertyName]string{
		PropertyName_ContentName:    fileInfo.statInfo.Name(),
		PropertyName_ContentLength:  strconv.FormatInt(fileInfo.statInfo.Size(), 10),
		PropertyName_ContentType:    "application/octet-stream",
		PropertyName_ContentMD5:     fileInfo.md5Base58,
		PropertyName_ContentCRC32:   strconv.FormatUint(uint64(fileInfo.crc32), 10),
		PropertyName_ContentModTime: fileInfo.statInfo.ModTime().Format(time.RFC3339),
	}
}
func (fileInfo *PhysicalFileInfo) Remove() error {
	return os.Remove(filepath.Join(fileInfo.Path(), fileInfo.Name()))
}
