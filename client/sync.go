package client

import (
	"fmt"
	"io"
	"os"
	"osssync/common/config"
	"osssync/common/dataAccess/nosqlite"
	"osssync/common/logging"
	"osssync/common/tracing"
	"osssync/core"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var ErrIndexedAlready error = fmt.Errorf("indexed already")
var ErrObjectExists error = fmt.Errorf("object exists")
var ErrSyncedAlready error = fmt.Errorf("synced already")

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

func PushFile(src core.FileInfo, targetType core.FileType, fullIndex bool) error {
	err := IndexFile(src, fullIndex)
	if err != nil {
		if !tracing.IsError(err, ErrIndexedAlready) {
			return tracing.Error(err)
		}
	}

	crc32, err := src.CRC32()
	if err != nil {
		return tracing.Error(err)
	}
	crc32Str := strconv.FormatInt(int64(crc32), 10)

	indexedModel, err := nosqlite.Get[ObjectIndexModel](crc32Str)
	if err != nil {
		return tracing.Error(err)
	}

	synces := make([]string, 0)
	if indexedModel.Synced != "" {
		synces = strings.Split(indexedModel.Synced, ",")
	}

	for _, s := range synces {
		if s == string(targetType) {
			return ErrSyncedAlready
		}
	}

	targetFileInfo, err := core.GetFile(targetType, filepath.Join(src.Path(), src.Name()))
	if err != nil {
		return tracing.Error(err)
	}

	targetExists, err := targetFileInfo.Exists()
	if err != nil {
		return tracing.Error(err)
	}

	if targetExists {
		targetCrc32, err := targetFileInfo.CRC32()
		if err != nil {
			return tracing.Error(err)
		}
		if targetCrc32 == crc32 {
			return ErrObjectExists
		} else {
			err = targetFileInfo.Remove()
			if err != nil {
				return tracing.Error(err)
			}
		}
	}

	targetFileInfo.Properties()[core.PropertyName_ContentCRC32] = crc32Str

	fs, err := targetFileInfo.Stream()
	if err != nil {
		return tracing.Error(err)
	}
	defer fs.Close()

	srcFs, err := src.Stream()
	if err != nil {
		return tracing.Error(err)
	}

	fileSize := src.Size()
	chunkSizeMb := int64(config.GetValueOrDefault[float64](core.Arg_ChunkSizeMb, 5))
	chunkSize := chunkSizeMb * 1024 * 1024
	salt := []byte(config.RequireString(core.Arg_Salt))
	if chunkSize > fileSize {
		var bufWriter core.FileWriter
		if len(salt) > 0 {
			cryptoWriter, err := core.NewCryptoFileWriter(fs, core.CryptoOptions{
				Salt: []byte(config.RequireString(core.Arg_Salt)),
			})
			if err != nil {
				return tracing.Error(err)
			}
			bufWriter = cryptoWriter
		} else {
			bufWriter = fs
		}
		defer bufWriter.Close()
		_, err = io.Copy(bufWriter, srcFs)
		if err != nil {
			return tracing.Error(err)
		}
		err = bufWriter.Flush()
		if err != nil {
			return tracing.Error(err)
		}
	} else {
		// chunk writes
		var chunks []core.FileChunkWriter
		if len(salt) > 0 {
			cryptoChunks, err := core.GenerateCryptoChunkWrites(fs, chunkSize, core.CryptoOptions{
				Salt: []byte(config.RequireString(core.Arg_Salt)),
			})
			if err != nil {
				return tracing.Error(err)
			}
			chunks = cryptoChunks
		} else {
			streamChunks, err := fs.ChunkWrites(src.Size(), chunkSize)
			if err != nil {
				return tracing.Error(err)
			}
			chunks = streamChunks
		}

		_, err = srcFs.Seek(0, io.SeekStart)
		if err != nil {
			return tracing.Error(err)
		}
		for _, chunk := range chunks {
			tmp := make([]byte, chunkSize)
			_, err := srcFs.Read(tmp)

			if err != nil && err != io.EOF {
				return tracing.Error(err)
			}

			if err != io.EOF {
				_, err = srcFs.Seek(0, io.SeekCurrent)
				if err != nil {
					return tracing.Error(err)
				}
			}

			_, err = chunk.Write(tmp)
			if err != nil {
				return tracing.Error(err)
			}

			err = chunk.Flush()
			if err != nil {
				return tracing.Error(err)
			}
		}
		err = fs.Flush()
		if err != nil {
			return tracing.Error(err)
		}
	}

	synces = append(synces, string(targetType))
	indexedModel.Synced = strings.Join(synces, ",")
	err = nosqlite.Set(indexedModel.Name, indexedModel)
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
		srcFileInfo, err := core.GetFile(core.FileType_Physical, filepath.Join(path, rd.Name()))
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

func PushDir(path string, targetType core.FileType, fullIndex bool) error {
	rds, err := os.ReadDir(path)
	if err != nil {
		return tracing.Error(err)
	}
	var wg sync.WaitGroup
	for _, rd := range rds {
		if rd.IsDir() {
			return PushDir(filepath.Join(path, rd.Name()), targetType, fullIndex)
		}
		srcFileInfo, err := core.GetFile(core.FileType_Physical, filepath.Join(path, rd.Name()))
		if err != nil {
			return tracing.Error(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := PushFile(srcFileInfo, targetType, fullIndex)
			if err != nil {
				if !tracing.IsError(err, ErrObjectExists) {
					logging.Error(err, nil)
				}
			} else {
				logging.Info("File has been synced", map[string]interface{}{
					"path": srcFileInfo.Path(),
					"file": srcFileInfo.Name(),
				})
			}
		}()
	}
	wg.Wait()
	return nil
}
