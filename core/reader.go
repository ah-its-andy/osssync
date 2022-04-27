package core

import (
	"hash/crc64"
	"io"
	"os"
)

func ComputeCrc64(filePath string) (uint64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	crc64 := crc64.New(crc64.MakeTable(crc64.ECMA))
	//buffer size is 1M
	bufferSize := 1024 * 1024
	buffer := make([]byte, bufferSize)
	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		_, err = crc64.Write(buffer[:n])
		if err != nil {
			return 0, err
		}
	}
	return crc64.Sum64(), nil
}

func NewChunkReader(reader io.Reader, chunkSize int64) *ChunkReader {
	return &ChunkReader{
		reader:    reader,
		chunkSize: chunkSize,
	}
}

type ChunkReader struct {
	reader    io.Reader
	chunkSize int64
}

func (r *ChunkReader) ReadNext() (n int, content []byte) {
	content = make([]byte, r.chunkSize)
	n, err := r.reader.Read(content)
	if err != nil {
		if err == io.EOF {
			retBuf := make([]byte, n)
			copy(retBuf, content)
			return n, retBuf
		}
		panic(err)
	}
	return n, content
}

func (r *ChunkReader) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *ChunkReader) Close() error {
	if closer, ok := r.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
