package client

import (
	"fmt"
	"os"
	"osssync/common/dataAccess/nosqlite"
	"osssync/common/logging"
	"osssync/common/tracing"
	"osssync/core"
	"path/filepath"
	"strconv"
	"sync"
)

func IndexFile(f core.FileInfo, fullIndex bool) error {
	crc32, err := f.CRC32()
	if err != nil {
		return tracing.Error(err)
	}
	crc32Str := strconv.FormatInt(int64(crc32), 10)

	objectIndex, err := nosqlite.Get[ObjectIndexModel](crc32Str)
	if err != nil && !tracing.IsError(err, nosqlite.ErrRecordNotFound) {
		return tracing.Error(err)
	}

	if crc32Str == objectIndex.CRC32 {
		if fullIndex {
			nosqlite.Remove[ObjectIndexModel](objectIndex.Name)
		} else {
			return ErrIndexedAlready
		}
	}

	md5, err := f.MD5()
	if err != nil {
		return tracing.Error(err)
	}
	objectIndex = ObjectIndexModel{
		Name:     fmt.Sprintf("%d", crc32),
		FilePath: f.Path(),
		FileName: f.Name(),
		CRC32:    crc32Str,
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
