package core

import "io"

type BufferWriter struct {
	io.Writer

	capacity int64
	offset   int64
	buffer   []byte
}

func NewBufferWriter(capacity int64) *BufferWriter {
	return &BufferWriter{
		capacity: capacity,
	}
}

func (writer *BufferWriter) Write(p []byte) (n int, err error) {
	if writer.capacity == 0 {
		writer.buffer = append(writer.buffer, p...)
		writer.offset += int64(len(p))
		return len(p), nil
	}
	if writer.capacity-writer.offset < int64(len(p)) {
		return 0, ErrIndexOutOfRange
	}

	writer.buffer = append(writer.buffer, p...)
	writer.offset += int64(len(p))
	return len(p), nil
}

func (writer *BufferWriter) Bytes() []byte {
	return writer.buffer
}
