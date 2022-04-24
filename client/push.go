package client

import (
	"fmt"
	"io"
	"os"
	"osssync/common/config"
	"osssync/common/logging"
	"osssync/common/tracing"
	"osssync/core"
	"path/filepath"
	"strconv"
	"sync"
)

var ErrIndexedAlready error = fmt.Errorf("indexed already")
var ErrObjectExists error = fmt.Errorf("object exists")
var ErrSyncedAlready error = fmt.Errorf("synced already")

func PushFile(src core.FileInfo, destPath string, fullIndex bool) error {
	dest, err := core.GetFile(filepath.Join(destPath, src.Name()))
	if err != nil {
		return tracing.Error(err)
	}

	CRC64, err := src.CRC64()
	if err != nil {
		return tracing.Error(err)
	}

	targetCRC64, err := dest.CRC64()
	if err != nil {
		return tracing.Error(err)
	}
	if targetCRC64 == CRC64 {
		return ErrObjectExists
	} else {
		err = dest.Remove()
		if err != nil {
			return tracing.Error(err)
		}
		targetFileInfo, err := core.GetFile(filepath.Join(destPath, src.Name()))
		if err != nil {
			return tracing.Error(err)
		}
		dest = targetFileInfo
	}

	dest.Properties()[core.PropertyName_ContentCRC64] = strconv.FormatUint(CRC64, 10)

	fs, err := dest.Stream()
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
	if chunkSize > fileSize {
		var bufWriter core.FileWriter
		bufWriter = fs
		defer bufWriter.Close()
		destBuffer := make([]byte, fileSize)
		n, err := srcFs.Read(destBuffer)
		// _, err = io.Copy(bufWriter, srcFs)
		if n == 0 || err != nil {
			return tracing.Error(err)
		}
		n, err = bufWriter.Write(destBuffer)
		if n == 0 || err != nil {
			return tracing.Error(err)
		}
		err = bufWriter.Flush()
		if err != nil {
			return tracing.Error(err)
		}
	} else {
		// chunk writes
		var chunks []core.FileChunkWriter
		streamChunks, err := fs.ChunkWrites(src.Size(), chunkSize)
		if err != nil {
			return tracing.Error(err)
		}
		chunks = streamChunks

		_, err = srcFs.Seek(0, io.SeekStart)
		if err != nil {
			return tracing.Error(err)
		}
		for _, chunk := range chunks {
			offset := chunkSize * (chunk.Number() - 1)
			bufferSize := chunkSize - chunk.Offset()
			if offset+chunkSize > fileSize {
				bufferSize = fileSize - offset
			}

			tmp := make([]byte, bufferSize)
			_, err := srcFs.Read(tmp)

			if err != nil && err != io.EOF {
				return tracing.Error(err)
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

	if err = CheckCRC64(src, dest); err != nil {
		logging.Warn(err.Error(), nil)
	}

	return nil
}

func PushDir(path string, destPath string, fullIndex bool) error {
	rds, err := os.ReadDir(path)
	if err != nil {
		return tracing.Error(err)
	}
	var wg sync.WaitGroup
	for _, rd := range rds {
		if rd.IsDir() {
			return PushDir(filepath.Join(path, rd.Name()), destPath, fullIndex)
		}
		srcFileInfo, err := core.GetFile(filepath.Join(path, rd.Name()))
		if err != nil {
			return tracing.Error(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := PushFile(srcFileInfo, destPath, fullIndex)
			if err != nil {
				if tracing.IsError(ErrSyncedAlready, err) {
					logging.Debug(fmt.Sprintf("File %s has been synced already", srcFileInfo.Name()), nil)
				} else if tracing.IsError(err, ErrObjectExists) {
					logging.Debug(fmt.Sprintf("File %s exists at remote storage provider", srcFileInfo.Name()), nil)
				} else {
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
