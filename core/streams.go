package core

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/chentaihan/aesCbc"
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
	f          *os.File
	stat       os.FileInfo
	offset     int64
	readOffset int64
}

func (stream *PhysicalFileStream) Size() int64 {
	return stream.stat.Size()
}

func (stream *PhysicalFileStream) Write(p []byte) (n int, err error) {
	_, err = stream.f.WriteAt(p, stream.offset)
	if err != nil {
		return 0, err
	}
	stream.offset += int64(len(p))
	return len(p), nil
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
	_, err = stream.f.ReadAt(p, stream.readOffset)
	if err != nil {
		return 0, err
	}
	stream.readOffset += int64(len(p))
	return len(p), nil
}

func (stream *PhysicalFileStream) ChunkWrites(fileSize int64, chunkSize int64) ([]FileChunkWriter, error) {
	chunkNum := int64(math.Ceil(float64(fileSize) / float64(chunkSize)))

	var chunks []FileChunkWriter
	var chunkN = (int64)(chunkNum)
	for i := int64(0); i < chunkN; i++ {
		chunk := &PhysicalChunkedFileWriter{
			number: i + 1,
			offset: 0,
			fw:     stream,
		}
		if i == chunkN-1 {
			chunk.chunkSize = fileSize - i*chunkSize
		} else {
			chunk.chunkSize = chunkSize
		}
		chunk.buffer = make([]byte, chunk.chunkSize)
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

	size := len(p)
	bufferSize := int(writer.chunkSize) - int(writer.offset)
	if size > bufferSize {
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
	Salt []byte
}

func GenerateCryptoHeader(chunkSize int64, algorithm int, blockSize int, iv []byte, crc32 int64) []byte {
	chunkSizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(chunkSizeBytes, uint64(chunkSize))
	algorithmBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(algorithmBytes, uint32(algorithm))
	blockSizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockSizeBytes, uint32(blockSize))
	crc32Bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(crc32Bytes, uint64(crc32))
	ivBytes := make([]byte, len(iv))
	copy(ivBytes, iv)

	headerSize := 4 + len(chunkSizeBytes) + len(algorithmBytes) + len(blockSizeBytes) + len(ivBytes) + len(crc32Bytes)
	headerSizeBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(headerSizeBytes, uint32(headerSize))

	header := make([]byte, headerSize)
	// 0 - 3: header size
	copy(header[0:], headerSizeBytes)
	// 4-11: chunkSize
	copy(header[4:], chunkSizeBytes)
	// 12-15: algorithm
	copy(header[12:], algorithmBytes)
	// 16-19: blockSize
	copy(header[16:], blockSizeBytes)
	// 20-: iv
	copy(header[20:], ivBytes)
	return header
}

func GenerateCryptoChunkWrites(stream FileStream, size int64, chunkSize int64, crc32 int64, cryptoOptions CryptoOptions) ([]FileChunkWriter, error) {
	block, err := NewCipherBlock(cryptoOptions)
	if err != nil {
		return nil, err
	}

	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	headerBuffer := GenerateCryptoHeader(chunkSize, 0, block.BlockSize(), iv, crc32)
	headerSize := len(headerBuffer)

	fileSize := size + int64(headerSize)
	blockNum := int64(math.Ceil(float64(fileSize) / float64(block.BlockSize())))
	fullBlockSize := blockNum * int64(block.BlockSize())

	streamChunks, err := stream.ChunkWrites(fullBlockSize, chunkSize)
	chunks := make([]FileChunkWriter, len(streamChunks))
	for i, chunk := range streamChunks {
		chunks[i] = &CryptoChunkedFileWriter{
			chunkFile:  chunk,
			chunkSize:  chunkSize,
			number:     int64(i + 1),
			offset:     0,
			buffer:     make([]byte, chunkSize),
			cryptoOpts: cryptoOptions,
			block:      block,
			iv:         iv,
		}
	}
	_, err = chunks[0].Write(headerBuffer)
	if err != nil {
		return nil, err
	}
	return chunks, nil
}

func NewCipherBlock(cryptoOptions CryptoOptions) (cipher.Block, error) {
	salt := make([]byte, len(cryptoOptions.Salt))
	copy(salt, cryptoOptions.Salt)
	if len(salt) < 32 {
		salt = pkcs5Padding(salt, 32)
	} else if len(salt) > 32 {
		salt = salt[:32]
	}

	block, err := aes.NewCipher(salt)
	if err != nil {
		return nil, err
	}
	return block, nil
}

type CryptoFileWriter struct {
	fw    FileWriter
	block cipher.Block
	iv    []byte

	offset int64
	buffer []byte

	cryptoOpts CryptoOptions
}

func NewCryptoFileWriter(fw FileWriter, size int64, crc32 int64, cryptoOpts CryptoOptions) (*CryptoFileWriter, error) {
	block, err := NewCipherBlock(cryptoOpts)
	if err != nil {
		return nil, err
	}
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	headerBuffer := GenerateCryptoHeader(0, 0, block.BlockSize(), iv, crc32)
	headerSize := len(headerBuffer)
	buffer := make([]byte, headerSize+int(size))
	copy(buffer[0:], headerBuffer)
	return &CryptoFileWriter{
		block:      block,
		fw:         fw,
		iv:         iv,
		buffer:     buffer,
		offset:     int64(headerSize),
		cryptoOpts: cryptoOpts,
	}, nil
}

func (writer *CryptoFileWriter) Write(p []byte) (n int, err error) {
	copy(writer.buffer[writer.offset:], p)
	writer.offset += int64(len(p))
	return len(p), nil
}

func (writer *CryptoFileWriter) Flush() error {
	if len(writer.buffer) == 0 {
		return nil
	}

	content := make([]byte, len(writer.buffer))
	copy(content, writer.buffer)

	blockBuffer := aesCbc.AesEncrypt(writer.cryptoOpts.Salt, writer.iv, content)

	_, err := writer.fw.Write(blockBuffer)
	if err != nil {
		return err
	}

	return writer.fw.Flush()
}

func (writer *CryptoFileWriter) Close() error {
	return nil
}

type CryptoChunkedFileWriter struct {
	block     cipher.Block
	chunkFile FileChunkWriter
	iv        []byte

	cryptoOpts CryptoOptions

	chunkSize int64
	number    int64
	offset    int64

	buffer []byte
}

func (writer *CryptoChunkedFileWriter) Write(p []byte) (int, error) {
	payloadSize := len(p)
	chunkSize := int(writer.chunkFile.ChunkSize()) - int(writer.Offset())
	if payloadSize > chunkSize {
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
	content := make([]byte, len(writer.buffer))
	copy(content, writer.buffer)
	blockBuffer := aesCbc.AesEncrypt(writer.cryptoOpts.Salt, writer.iv, content)

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

func pkcs5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func pkcs5Trimming(encrypt []byte) []byte {
	padding := encrypt[len(encrypt)-1]
	return encrypt[:len(encrypt)-int(padding)]
}
