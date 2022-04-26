package core

import "io"

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
