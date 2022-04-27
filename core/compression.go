package core

import (
	"encoding/json"
	"hash/crc64"
	"io"
	"os"
	"strconv"
	"strings"

	ezip "github.com/alexmullins/zip"
)

func GetZipCrc64(destPath string, relativePath string) uint64 {
	if fileType := ResolveUriType(destPath); fileType != FileType_Physical {
		return 0
	}
	zipFilePath := JoinUri(destPath, relativePath)
	zipFile, err := os.Open(zipFilePath)
	if err != nil {
		return 0
	}
	defer zipFile.Close()

	zipFileStat, err := zipFile.Stat()
	if err != nil {
		return 0
	}

	archive, err := ezip.NewReader(zipFile, zipFileStat.Size())
	if err != nil {
		return 0
	}
	if len(archive.File) == 0 {
		return 0
	}
	file := archive.File[0]
	extra := make(map[string]interface{})
	if err := json.Unmarshal(file.Extra, &extra); err != nil {
		return 0
	}
	crc64, ok := extra["x-osssync-crc64"].(string)
	if !ok {
		return 0
	}
	crc64Int, err := strconv.ParseUint(crc64, 10, 64)
	if err != nil {
		return 0
	}
	return crc64Int
}

func ZipTo(dirPath string, destPath string, relativePath string, password string, crc64V uint64) (string, error) {
	fileInfo, err := os.Stat(JoinUri(dirPath, relativePath))
	if err != nil {
		return "", err
	}

	srcFile, err := os.Open(JoinUri(dirPath, relativePath))
	if err != nil {
		return "", err
	}
	defer srcFile.Close()

	zipFileName := strconv.FormatUint(crc64.Checksum([]byte(relativePath), crc64.MakeTable(crc64.ECMA)), 10) + "zip"
	zipFilePath := JoinUri(destPath, zipFileName)
	if _, err := os.Stat(zipFilePath); err == nil {
		os.Remove(zipFilePath)
	}
	zipFile, err := os.Create(zipFilePath)
	if err != nil {
		return "", err
	}
	defer zipFile.Close()
	archive := ezip.NewWriter(zipFile)
	defer archive.Close()
	header, err := ezip.FileInfoHeader(fileInfo)
	if err != nil {
		return "", err
	}
	if password != "" {
		header.SetPassword(password)
	}
	extraMap := make(map[string]interface{})
	extraMap["x-osssync-token"] = "1"
	extraMap["x-osssync-crc64"] = strconv.FormatUint(crc64V, 10)
	extraMap["x-osssync-entrypted-password"] = password
	extraJSON, err := json.Marshal(extraMap)
	if err != nil {
		return "", err
	}
	header.Extra = make([]byte, len(extraJSON))
	copy(header.Extra, extraJSON)

	archiveWriter, err := archive.CreateHeader(header)
	if err != nil {
		return "", err
	}

	io.Copy(archiveWriter, srcFile)

	archive.Flush()
	return zipFilePath, nil
}

type ZipChunkWriter struct {
	relativePath string
	buffeSize    int64
	buffer       []byte
	full         chan []byte
	eof          bool
}

func NewZipChunkWriter(relativePath string, buffeSize int64, chunkNum int64) *ZipChunkWriter {
	return &ZipChunkWriter{
		buffeSize: buffeSize,
		buffer:    make([]byte, 0),
		full:      make(chan []byte),
	}
}

func (writer *ZipChunkWriter) sendBuffer(buffer []byte) {
	writer.full <- buffer
}

func (writer *ZipChunkWriter) Close() error {
	close(writer.full)
	return nil
}

func (writer *ZipChunkWriter) nextBlock() {
	writer.buffer = make([]byte, 0)
}

func (writer *ZipChunkWriter) Write(p []byte) (n int, err error) {
	if writer.eof {
		return 0, io.EOF
	}
	remainCapcity := writer.buffeSize - int64(len(writer.buffer))
	remainContent := make([]byte, 0)
	if len(p) > int(remainCapcity) {
		writer.buffer = append(writer.buffer, p[:remainCapcity]...)
		remainContent = make([]byte, len(p)-int(remainCapcity))
		copy(remainContent, p[remainCapcity:])
	} else if len(p) < int(remainCapcity) {
		bufferSize := len(writer.buffer) + len(p)
		buffer := make([]byte, bufferSize)
		copy(buffer, writer.buffer[:len(writer.buffer)])
		copy(buffer[len(writer.buffer)+1:], p)
		writer.buffer = buffer
	} else {
		writer.buffer = append(writer.buffer, p...)
	}
	if len(writer.buffer) == int(writer.buffeSize) {
		buffer := make([]byte, len(writer.buffer))
		copy(buffer, writer.buffer)
		writer.nextBlock()
		writer.sendBuffer(buffer)
	}
	if len(remainContent) > 0 {
		return writer.Write(remainContent)
	}
	return len(p), nil
}

type ZipFileInfo struct {
	header *ezip.FileHeader

	chunkWriter   *ZipChunkWriter
	archive       *ezip.Writer
	archiveWriter io.Writer

	ChunkSize int64
	ChunkNum  int64
}

func NewZipFileInfo(dirPath string, relativePath string, chunkSize int64, chunkNum int64, password string) (*ZipFileInfo, error) {
	fileInfo, err := os.Stat(JoinUri(dirPath, relativePath))
	if err != nil {
		return nil, err
	}

	chunkWriter := NewZipChunkWriter(relativePath, chunkSize, chunkNum)

	archive := ezip.NewWriter(chunkWriter)

	header, err := ezip.FileInfoHeader(fileInfo)
	if err != nil {
		return nil, err
	}
	header.Name = relativePath[strings.LastIndex(relativePath, "/")+1:]
	header.Method = ezip.Deflate
	if password != "" {
		header.SetPassword(password)
	}

	archiveWriter, err := archive.CreateHeader(header)
	if err != nil {
		return nil, err
	}

	return &ZipFileInfo{
		header:        header,
		chunkWriter:   chunkWriter,
		archive:       archive,
		archiveWriter: archiveWriter,
		ChunkSize:     chunkSize,
		ChunkNum:      chunkNum,
	}, nil
}

func (zip *ZipFileInfo) Read(p []byte) (n int, err error) {
	if buffer, ok := <-zip.chunkWriter.full; ok {
		copy(p, buffer)
		return len(buffer), nil
	} else {
		return 0, io.EOF
	}
}

func (zip *ZipFileInfo) Write(p []byte) (n int, err error) {
	n, err = zip.archiveWriter.Write(p)
	if err != nil {
		return n, err
	}
	zip.archive.Flush()
	return n, nil
}

func (zip *ZipFileInfo) WriteEof() error {
	return zip.chunkWriter.Close()
}

func (zip *ZipFileInfo) Close() error {
	return zip.archive.Close()
}
