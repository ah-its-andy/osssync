package core

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"os"
)

type FileWriter interface {
	io.Writer
	io.Closer

	Flush() error
}

type FileChunkWriter interface {
	io.Writer
	io.Closer
	Number() int64
	ChunkSize() int64
	Offset() int64
	Flush() error
}

type FileReader interface {
	io.Reader
	io.Closer

	Seek(offset int64, whence int) (int64, error)
}

type FileStream interface {
	FileWriter
	FileReader

	Size() int64
	ChunkWrites(fileSize int64, chunkSize int64) ([]FileChunkWriter, error)
}

type PhysicalFileStream struct {
	f    *os.File
	stat os.FileInfo
}

func (stream *PhysicalFileStream) Size() int64 {
	return stream.stat.Size()
}

func (stream *PhysicalFileStream) Write(p []byte) (n int, err error) {
	return stream.f.Write(p)
}

func (stream *PhysicalFileStream) Flush() error {
	return nil
}

func (stream *PhysicalFileStream) Close() error {
	return stream.f.Close()
}

func (stream *PhysicalFileStream) Seek(offset int64, whence int) (int64, error) {
	return stream.f.Seek(offset, whence)
}

func (stream *PhysicalFileStream) Read(p []byte) (n int, err error) {
	return stream.f.Read(p)
}

func (stream *PhysicalFileStream) ChunkWrites(fileSize int64, chunkSize int64) ([]FileChunkWriter, error) {
	chunkNum := int64(math.Ceil(float64(fileSize) / float64(chunkSize)))

	var chunks []FileChunkWriter
	var chunkN = (int64)(chunkNum)
	for i := int64(0); i < chunkN; i++ {
		chunk := &PhysicalChunkedFileWriter{
			number: i + 1,
			offset: i * (fileSize / chunkN),
		}
		if i == chunkN-1 {
			chunk.chunkSize = fileSize/chunkN + fileSize%chunkN
		} else {
			chunk.chunkSize = fileSize / chunkN
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

var ErrIndexOutOfRange error = fmt.Errorf("index out of range")

type PhysicalChunkedFileWriter struct {
	chunkSize int64
	offset    int64
	number    int64
	fw        FileWriter
	closed    bool

	buffer []byte
}

func (writer *PhysicalChunkedFileWriter) throwIfClosed() error {
	if writer.closed {
		return fmt.Errorf("file writer is closed")
	}
	return nil
}

func (writer *PhysicalChunkedFileWriter) Write(p []byte) (int, error) {
	err := writer.throwIfClosed()
	if err != nil {
		return 0, err
	}

	if len(p) > int(writer.chunkSize)-int(writer.offset) {
		return 0, ErrIndexOutOfRange
	}

	copy(writer.buffer[int(writer.offset):], p)
	writer.offset += int64(len(p))
	return len(p), nil
}

func (writer *PhysicalChunkedFileWriter) Close() error {
	writer.closed = true
	return nil
}

func (writer *PhysicalChunkedFileWriter) Number() int64 {
	return writer.number
}

func (writer *PhysicalChunkedFileWriter) ChunkSize() int64 {
	return writer.chunkSize
}

func (writer *PhysicalChunkedFileWriter) Offset() int64 {
	return writer.offset
}

func (writer *PhysicalChunkedFileWriter) Flush() error {
	_, err := writer.fw.Write(writer.buffer)
	if err != nil {
		return err
	}
	writer.offset = 0
	writer.buffer = make([]byte, writer.chunkSize)
	return nil
}

type CryptoOptions struct {
	KeySize int
	Salt    []byte
}

func GenerateCryptoChunkWrites(stream FileStream, chunkSize int64, cryptoOptions CryptoOptions) ([]FileChunkWriter, error) {
	block, err := NewCipherBlock(cryptoOptions)
	if err != nil {
		return nil, err
	}

	// header size
	headerSize := 16 + aes.BlockSize
	// 0: chunk size, 1-4: crc32, 5-16: 0x00, 17-ends: iv

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	fileSize := stream.Size() + int64(headerSize)
	streamChunks, err := stream.ChunkWrites(fileSize, chunkSize)
	chunks := make([]FileChunkWriter, len(streamChunks))
	for i, chunk := range streamChunks {
		chunks[i] = &CryptoChunkedFileWriter{
			chunkFile: chunk,
			chunkSize: chunkSize,
			number:    int64(i + 1),
			offset:    0,
			buffer:    make([]byte, chunkSize),
			block:     block,
			iv:        iv,
		}
	}
	return chunks, nil
}

func NewCipherBlock(cryptoOptions CryptoOptions) (cipher.Block, error) {
	block, err := aes.NewCipher(cryptoOptions.Salt)
	if err != nil {
		return nil, err
	}
	return block, nil
}

type CryptoFileWriter struct {
	fw    FileWriter
	block cipher.Block
	iv    []byte

	buffer []byte
}

func NewCryptoFileWriter(fw FileWriter, cryptoOpts CryptoOptions) (*CryptoFileWriter, error) {
	block, err := NewCipherBlock(cryptoOpts)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}
	return &CryptoFileWriter{
		fw:     fw,
		block:  block,
		iv:     iv,
		buffer: make([]byte, cryptoOpts.KeySize),
	}, nil
}

func (writer *CryptoFileWriter) Write(p []byte) (n int, err error) {
	writer.buffer = append(writer.buffer, p...)
	return len(p), nil
}

func (writer *CryptoFileWriter) Flush() error {
	if len(writer.buffer) == 0 {
		return nil
	}

	mode := cipher.NewCBCEncrypter(writer.block, writer.iv)
	blockBuffer := make([]byte, len(writer.buffer))
	mode.CryptBlocks(blockBuffer, writer.buffer)

	_, err := writer.fw.Write(blockBuffer)
	if err != nil {
		return err
	}

	return writer.Flush()
}

func (writer *CryptoFileWriter) Close() error {
	return nil
}

type CryptoChunkedFileWriter struct {
	chunkFile FileChunkWriter
	block     cipher.Block
	iv        []byte

	chunkSize int64
	number    int64
	offset    int64

	buffer []byte
}

func (writer *CryptoChunkedFileWriter) Write(p []byte) (int, error) {
	if len(p) > int(writer.chunkFile.ChunkSize())-int(writer.chunkFile.Offset()) {
		return 0, ErrIndexOutOfRange
	}

	copy(writer.buffer[int(writer.offset):], p)
	writer.offset += int64(len(p))
	return len(p), nil
}

func (writer *CryptoChunkedFileWriter) Flush() error {
	if len(writer.buffer) == 0 {
		return writer.chunkFile.Flush()
	}

	mode := cipher.NewCBCEncrypter(writer.block, writer.iv)
	blockBuffer := make([]byte, len(writer.buffer))
	mode.CryptBlocks(blockBuffer, writer.buffer)

	_, err := writer.chunkFile.Write(blockBuffer)
	if err != nil {
		return err
	}
	return writer.chunkFile.Flush()
}

func (writer *CryptoChunkedFileWriter) Close() error {
	return writer.chunkFile.Close()
}

func (writer *CryptoChunkedFileWriter) Number() int64 {
	return writer.number
}
func (writer *CryptoChunkedFileWriter) ChunkSize() int64 {
	return writer.chunkSize
}
func (writer *CryptoChunkedFileWriter) Offset() int64 {
	return writer.offset
}
