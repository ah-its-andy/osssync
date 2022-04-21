package core

import (
	"crypto/aes"
	"crypto/cipher"
	"io"
	"os"
	"osssync/common/tracing"
	"sync"
)

type CryptoChunkedFile struct {
	f         *os.File
	chunkSize int
	offset    int
	key       []byte
	iv        []byte

	cipher cipher.Block

	cryptoBuffer []byte
	eof          bool

	c chan []byte

	mu *sync.Mutex
}

func NewCryptoChunkedFile(f *os.File, chunkSize int, key []byte) (*CryptoChunkedFile, error) {
	c := &CryptoChunkedFile{
		f:         f,
		chunkSize: chunkSize,
		key:       key,
		c:         make(chan []byte),
		mu:        &sync.Mutex{},
	}
	c.cipher, _ = aes.NewCipher(key)
	iv := make([]byte, aes.BlockSize)
	_, err := io.ReadFull(f, iv)
	if err != nil {
		return nil, tracing.Error(err)
	}
	c.iv = iv
	return c, nil
}

func (chunk *CryptoChunkedFile) Size() (int64, error) {
	stat, err := chunk.f.Stat()
	if err != nil {
		return 0, tracing.Error(err)
	}
	return int64(2) + int64(len(chunk.iv)) + stat.Size(), nil
}

func (chunk *CryptoChunkedFile) ComputeHash() ([]byte, error) {
	chunk.f.Seek(0, os.SEEK_SET)

	err := chunk.fillHeader()
	if err != nil {
		return chunk.cryptoBuffer, tracing.Error(err)
	}
	for {
		buffer := make([]byte, chunk.chunkSize)

		_, err := chunk.f.Read(buffer)
		chunk.offset = chunk.offset + chunk.chunkSize
		if err != nil {
			if err == io.EOF {
				chunk.eof = true
				err = chunk.computeCryptoBuffer(buffer)
				if err != nil {
					return chunk.cryptoBuffer, tracing.Error(err)
				}
				break
			}
			return chunk.cryptoBuffer, tracing.Error(err)
		}
		err = chunk.computeCryptoBuffer(buffer)
		if err != nil {
			return chunk.cryptoBuffer, tracing.Error(err)
		}
	}
	return chunk.cryptoBuffer, nil
}

func (chunk *CryptoChunkedFile) computeCryptoBuffer(buffer []byte) error {
	if len(buffer) == 0 {
		return nil
	}

	mode := cipher.NewCBCEncrypter(chunk.cipher, chunk.iv)
	blockBuffer := make([]byte, len(buffer))
	mode.CryptBlocks(blockBuffer, buffer)
	chunk.cryptoBuffer = append(chunk.cryptoBuffer, blockBuffer...)
	err := chunk.SendBuffer()
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func (chunk *CryptoChunkedFile) fillHeader() error {
	size := 2 + len(chunk.iv)

	header := make([]byte, size)
	// 0 : chunk size
	header[0] = byte(chunk.chunkSize)
	// 1: iv size
	header[1] = byte(len(chunk.iv))

	copy(header[2:], chunk.iv)
	chunk.cryptoBuffer = append(chunk.cryptoBuffer, header...)
	return nil
}

func (chunk *CryptoChunkedFile) ChunkBuffered() chan []byte {
	return chunk.c
}

func (chunk *CryptoChunkedFile) SendBuffer() error {
	if chunk.eof {
		if len(chunk.cryptoBuffer) > 0 {
			chunk.c <- chunk.cryptoBuffer
			chunk.cryptoBuffer = make([]byte, 0)
		}
		return io.EOF
	}

	if len(chunk.cryptoBuffer) >= chunk.chunkSize {
		tmp := make([]byte, chunk.chunkSize)
		copy(tmp, chunk.cryptoBuffer[:chunk.chunkSize])
		remainBuffer := chunk.cryptoBuffer[chunk.chunkSize:]
		chunk.cryptoBuffer = make([]byte, len(remainBuffer))
		copy(chunk.cryptoBuffer, remainBuffer)
		chunk.c <- tmp
	}

	return nil
}
