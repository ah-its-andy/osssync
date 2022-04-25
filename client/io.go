package client

import (
	"io"
	"io/ioutil"
	"osssync/common/config"
	"osssync/common/tracing"
	"osssync/core"
)

func TransferFile(srcPath string, dstPath string, relativePath string) error {
	srcFile, err := core.GetFile(srcPath, relativePath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	destFile, err := core.GetFile(dstPath, relativePath)
	if err != nil {
		return err
	}

	srcStream, err := srcFile.Stream()
	if err != nil {
		return err
	}
	defer srcStream.Close()

	destStream, err := destFile.Stream()
	if err != nil {
		return err
	}
	defer destStream.Close()

	fileSize := srcFile.Size()
	chunkSizeMb := int64(config.GetValueOrDefault[float64](core.Arg_ChunkSizeMb, 5))
	if chunkSizeMb <= 0 {
		chunkSizeMb = 5
	}
	chunkSize := chunkSizeMb * 1024 * 1024
	if chunkSize > fileSize {
		buffer, err := ioutil.ReadAll(srcStream)
		if err != nil {
			return err
		}
		_, err = destStream.Write(buffer)
		if err != nil {
			return err
		}
	} else {
		chunks, err := destStream.ChunkWrites(srcFile.Size(), chunkSize)
		if err != nil {
			return err
		}
		_, err = srcStream.Seek(0, io.SeekStart)
		if err != nil {
			return tracing.Error(err)
		}

		for _, chunk := range chunks {
			offset := chunk.ChunkSize() * (chunk.Number() - 1)
			bufferSize := chunk.ChunkSize()
			if offset+chunk.ChunkSize() > fileSize {
				bufferSize = fileSize - offset
			}

			tmp := make([]byte, bufferSize)
			_, err := srcStream.Read(tmp)

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
		err = destStream.Flush()
		if err != nil {
			return tracing.Error(err)
		}
	}
	return nil
}
