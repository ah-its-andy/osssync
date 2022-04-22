package client

import (
	"crypto/md5"
	"fmt"
	"os"
	"osssync/common/dataAccess/nosqlite"
	"osssync/common/logging"
	"osssync/common/tracing"
	"osssync/core"
	"path/filepath"
	"strings"
	"sync"

	"github.com/mr-tron/base58"
)

func GetIndexName(f core.FileInfo) (string, error) {
	fileHashContents := make([]string, 0)
	fileProperties := f.Properties()
	if v, ok := fileProperties[core.PropertyName_ContentModTime]; ok {
		fileHashContents = append(fileHashContents, v)
	}
	if v, ok := fileProperties[core.PropertyName_ContentLength]; ok {
		fileHashContents = append(fileHashContents, v)
	}
	if v, ok := fileProperties[core.PropertyName_ContentName]; ok {
		fileHashContents = append(fileHashContents, v)
	}
	md5Cipher := md5.New()
	_, err := md5Cipher.Write([]byte(strings.Join(fileHashContents, ";")))
	if err != nil {
		return "", tracing.Error(err)
	}
	objectIndexName := base58.Encode(md5Cipher.Sum(nil))
	return objectIndexName, nil
}

func IndexFile(f core.FileInfo, fullIndex bool) error {
	objectIndexName, err := GetIndexName(f)
	if err != nil {
		return tracing.Error(err)
	}

	objectIndex, err := nosqlite.Get[ObjectIndexModel](objectIndexName)
	if err != nil && !tracing.IsError(err, nosqlite.ErrRecordNotFound) {
		return tracing.Error(err)
	}

	if err == nil && !fullIndex {
		return ErrIndexedAlready
	}

	nosqlite.Remove[ObjectIndexModel](objectIndex.Name)

	crc32, err := f.CRC32()
	if err != nil {
		return tracing.Error(err)
	}
	md5, err := f.MD5()
	if err != nil {
		return tracing.Error(err)
	}
	objectIndex = ObjectIndexModel{
		Name:     objectIndexName,
		FilePath: f.Path(),
		FileName: f.Name(),
		CRC32:    fmt.Sprintf("%d", crc32),
		MD5:      md5,
		Synced:   "",
		Size:     f.Size(),
	}
	err = nosqlite.Set(objectIndex.Name, objectIndex)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func IndexDir(path string, fullIndex bool) error {
	rds, err := os.ReadDir(path)
	if err != nil {
		return tracing.Error(err)
	}
	var wg sync.WaitGroup
	for _, rd := range rds {
		if rd.IsDir() {
			return IndexDir(filepath.Join(path, rd.Name()), fullIndex)
		}
		srcFileInfo, err := core.GetFile(filepath.Join(path, rd.Name()))
		if err != nil {
			return tracing.Error(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := IndexFile(srcFileInfo, fullIndex)
			if err != nil {
				if !tracing.IsError(err, ErrIndexedAlready) {
					logging.Error(err, nil)
				}
			} else {
				logging.Info("File has been indexed", map[string]interface{}{
					"path": srcFileInfo.Path(),
					"file": srcFileInfo.Name(),
				})
			}
		}()
	}
	wg.Wait()
	return nil
}
