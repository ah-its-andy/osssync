package client

import (
	"io"
	"osssync/common/config"
	"osssync/common/logging"
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
	defer destFile.Close()

	fileSize := srcFile.Size()
	chunkSizeMb := int64(config.GetValueOrDefault[float64](core.Arg_ChunkSizeMb, 5))
	if chunkSizeMb <= 0 {
		chunkSizeMb = 5
	}
	chunkSize := chunkSizeMb * 1024 * 1024
	if chunkSize > fileSize {
		srcReader := srcFile.Reader()
		// buffer := make([]byte, fileSize)
		// _, err := srcReader.ReadAt(buffer, 0)
		// if err != nil {
		// 	return err
		// }
		destWriter := destFile.Writer()
		// _, err = destWriter.Write(buffer)
		// if err != nil {
		// 	return err
		// }
		_, err = io.Copy(destWriter, srcReader)
		if err != nil {
			return tracing.Error(err)
		}
	} else {
		err = destFile.WalkChunk(srcFile.Reader(), chunkSize, fileSize, destFile.WriteChunk)
		if err != nil {
			return tracing.Error(err)
		}
	}
	err = destFile.Flush()
	if err != nil {
		return err
	}
	if err = CheckCRC64(srcFile, destFile); err != nil {
		logging.Warn(err.Error(), nil)
	}
	return nil
}
