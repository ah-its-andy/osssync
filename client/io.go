package client

import (
	"fmt"
	"io"
	"os"
	"osssync/common/config"
	"osssync/common/logging"
	"osssync/common/tracing"
	"osssync/core"
	"strings"
)

func TransferFile(srcPath string, dstPath string, relativePath string) error {
	srcStat, err := os.Stat(core.JoinUri(srcPath, relativePath))
	if err != nil {
		return tracing.Error(err)
	}

	fileSize := srcStat.Size()
	chunkSizeMb := int64(config.GetValueOrDefault[float64](core.Arg_ChunkSizeMb, 5))
	if chunkSizeMb <= 0 {
		chunkSizeMb = 5
	}
	chunkSize := chunkSizeMb * 1024 * 1024
	var srcReader io.Reader
	destRelativePath := relativePath
	var destCrc64 uint64
	if config.GetValueOrDefault(core.Arg_Zip, false) {
		if !strings.HasSuffix(relativePath, ".crypto") {
			destRelativePath = core.JoinUri(relativePath, ".crypto")
		}
		destCrc64 = core.GetCrytoFileCrc64(core.JoinUri(dstPath, destRelativePath))
	}

	srcCrc64, err := core.ComputeCrc64(core.JoinUri(srcPath, relativePath))
	if err != nil {
		return tracing.Error(err)
	}

	if srcCrc64 == destCrc64 {
		logging.Info(fmt.Sprintf("%s:%s is up to date", srcPath, relativePath), nil)
		return nil
	}

	destFile, err := core.GetFile(dstPath, destRelativePath)
	if err != nil {
		return tracing.Error(err)
	}
	defer destFile.Close()
	destExists, err := destFile.Exists()
	if err != nil {
		return tracing.Error(err)
	}
	if destExists {
		destCrc64, err = destFile.CRC64()
		if err != nil {
			return tracing.Error(err)
		}
	}
	if srcCrc64 == destCrc64 {
		logging.Info(fmt.Sprintf("%s:%s is up to date", srcPath, relativePath), nil)
		return nil
	} else if destExists {
		err = destFile.Remove()
		if err != nil {
			return tracing.Error(err)
		}
		destFile.Close()
		destFile, err = core.GetFile(dstPath, destRelativePath)
		if err != nil {
			return tracing.Error(err)
		}
	}

	if config.GetValueOrDefault(core.Arg_Zip, false) {
		pwd := config.RequireString(core.Arg_Password)
		seed := core.GetPasswordSeed(pwd)
		pk, err := core.GenerateRsaKey(seed)
		if err != nil {
			return tracing.Error(err)
		}

		cryptoFilePath, err := core.EncryptFile(srcPath, config.RequireString(core.Arg_TmpDir), relativePath, &pk.PublicKey, srcCrc64)
		if err != nil {
			return tracing.Error(err)
		}
		defer os.Remove(cryptoFilePath)
		cryptoFile, err := os.OpenFile(cryptoFilePath, os.O_RDONLY, 0644)
		if err != nil {
			return tracing.Error(err)
		}
		defer cryptoFile.Close()
		srcReader = cryptoFile
	} else {
		srcFile, err := core.GetFile(srcPath, relativePath)
		if err != nil {
			return tracing.Error(err)
		}
		defer srcFile.Close()
		srcReader = srcFile.Reader()
	}

	destWriter := destFile.Writer()
	if chunkSize > fileSize {
		_, err = CopyFile(destWriter, srcReader)
		if err != nil {
			return tracing.Error(err)
		}
	} else {
		err = destFile.WalkChunk(srcReader, chunkSize, fileSize, destFile.WriteChunk)
		if err != nil {
			return tracing.Error(err)
		}
	}
	err = destFile.Flush()
	if err != nil {
		return tracing.Error(err)
	}

	return nil
}

func WriteZip(bufferSize int64, reader io.Reader, zip *core.ZipFileInfo) (n int, err error) {
	var eof bool
	var nv int
	for {
		buffer := make([]byte, bufferSize)
		_, err = reader.Read(buffer)
		if err != nil {
			if err == io.EOF {
				endBuf := make([]byte, n)
				copy(endBuf, buffer[:n])
				buffer = endBuf
				eof = true
			} else {
				return 0, err
			}
		}
		mn, err := zip.Write(buffer)
		if err != nil {
			return 0, err
		}
		nv += mn
		if eof {
			zip.WriteEof()
			break
		}
	}
	return nv, nil
}

func CopyFile(writer io.Writer, reader io.Reader) (n int64, err error) {
	if zipFile, ok := reader.(*core.ZipFileInfo); ok {
		bufferSize := zipFile.ChunkSize
		buffer := make([]byte, bufferSize)
		for {
			n, err := zipFile.Read(buffer)
			if err != nil {
				if err == io.EOF {
					endBuf := make([]byte, n)
					copy(endBuf, buffer[:n])
					buffer = make([]byte, n)
					copy(buffer, endBuf)
					n, err = writer.Write(buffer)
					if err != nil {
						return int64(n), err
					}
					break
				}
				return 0, err
			}
			n, err = writer.Write(buffer)
			if err != nil {
				return int64(n), err
			}
		}
		return int64(len(buffer)), nil
	} else {
		return io.Copy(writer, reader)
	}
}
