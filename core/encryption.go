package core

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc64"
	"io"
	"os"
	"strconv"

	"github.com/chentaihan/aesCbc"
)

var ErrBlockCRC64NotMatch error = errors.New("block crc64 not match")
var ErrHeaderTypeNotMatch error = errors.New("header type not match")
var ErrVersionNotMatch error = errors.New("version not match")

type CryptoFileHeader struct {
	// HeaderSize: 4
	HeaderSize int32
	// HeaderType: 4
	HeaderType int32
	// Version: 4
	Version int32
	// CRC64: 8
	CRC64 uint64
	// Algorithm: 4
	Algorithm int32
	// ModifyTime: 8
	ModifyTime int64
	// ChunkSize: 4
	ChunkSize int32
	// NameSize: 4
	NameSize int32
	// IVSize: 4
	IVSize int32
	// EncryptedPasswordSize: 4
	EncryptedPasswordSize int32
	// ExtraSize: 4
	ExtraSize int32
	/* FIXED SIZE: 52 */
	// Name: NameSize
	Name []byte
	// IV: IVSize
	IV []byte
	// EncryptedPassword: EncryptedPasswordSize
	EncryptedPassword []byte
	// Extra: ExtraSize
	Extra []byte
}

func (header *CryptoFileHeader) Bytes() []byte {
	headerSize := 52 + header.NameSize + header.IVSize + header.EncryptedPasswordSize + header.ExtraSize
	header.HeaderSize = int32(headerSize)
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, header.HeaderSize)
	binary.Write(buf, binary.LittleEndian, header.HeaderType)
	binary.Write(buf, binary.LittleEndian, header.Version)
	binary.Write(buf, binary.LittleEndian, header.CRC64)
	binary.Write(buf, binary.LittleEndian, header.Algorithm)
	binary.Write(buf, binary.LittleEndian, header.ModifyTime)
	binary.Write(buf, binary.LittleEndian, header.ChunkSize)
	binary.Write(buf, binary.LittleEndian, header.NameSize)
	binary.Write(buf, binary.LittleEndian, header.IVSize)
	binary.Write(buf, binary.LittleEndian, header.EncryptedPasswordSize)
	binary.Write(buf, binary.LittleEndian, header.ExtraSize)
	binary.Write(buf, binary.LittleEndian, header.Name)
	binary.Write(buf, binary.LittleEndian, header.IV)
	binary.Write(buf, binary.LittleEndian, header.EncryptedPassword)
	binary.Write(buf, binary.LittleEndian, header.Extra)
	return buf.Bytes()
}

func GetCrytoFileCrc64(filePath string) uint64 {
	file, err := os.Open(filePath)
	if err != nil {
		return 0
	}
	defer file.Close()

	_, err = file.Seek(4, io.SeekStart)
	if err != nil {
		return 0
	}
	headerTypeBuf := make([]byte, 4)
	_, err = file.Read(headerTypeBuf)
	if err != nil {
		return 0
	}
	headerType := binary.LittleEndian.Uint32(headerTypeBuf)
	if headerType != 1 {
		return 0
	}

	_, err = file.Seek(12, io.SeekStart)
	if err != nil {
		return 0
	}
	crc64Buf := make([]byte, 8)
	_, err = file.Read(crc64Buf)
	if err != nil {
		return 0
	}
	return binary.LittleEndian.Uint64(crc64Buf)
}

func GetCryptoFileHeader(filePath string) (*CryptoFileHeader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	header, err := ReadCryptoFileHeader(file)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func ReadCryptoFileHeader(reader io.Reader) (*CryptoFileHeader, error) {
	headerSizeBuf := make([]byte, 4)
	_, err := reader.Read(headerSizeBuf)
	if err != nil {
		return nil, err
	}
	headerSize := binary.LittleEndian.Uint32(headerSizeBuf)
	headerBuf := make([]byte, headerSize)
	copy(headerBuf, headerSizeBuf)
	_, err = reader.Read(headerBuf[4:])
	if err != nil {
		return nil, err
	}
	header := ParseCryptoFileHeader(headerBuf)
	return header, nil
}

func ParseCryptoFileHeader(content []byte) *CryptoFileHeader {
	header := &CryptoFileHeader{}
	buf := bytes.NewBuffer(content)
	binary.Read(buf, binary.LittleEndian, &header.HeaderType)
	binary.Read(buf, binary.LittleEndian, &header.Version)
	binary.Read(buf, binary.LittleEndian, &header.CRC64)
	binary.Read(buf, binary.LittleEndian, &header.Algorithm)
	binary.Read(buf, binary.LittleEndian, &header.ModifyTime)
	binary.Read(buf, binary.LittleEndian, &header.ChunkSize)
	binary.Read(buf, binary.LittleEndian, &header.NameSize)
	binary.Read(buf, binary.LittleEndian, &header.IVSize)
	binary.Read(buf, binary.LittleEndian, &header.EncryptedPasswordSize)
	binary.Read(buf, binary.LittleEndian, &header.ExtraSize)
	header.Name = buf.Next(int(header.NameSize))
	header.IV = buf.Next(int(header.IVSize))
	header.EncryptedPassword = buf.Next(int(header.EncryptedPasswordSize))
	header.Extra = buf.Next(int(header.ExtraSize))
	return header
}

type EncryptBlock struct {
	// 0:4
	BlockSize int32
	// 4:12
	CRC64 uint64
	// 12:
	Content []byte
}

func (block *EncryptBlock) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, block.BlockSize)
	binary.Write(buf, binary.LittleEndian, block.CRC64)
	binary.Write(buf, binary.LittleEndian, block.Content)
	return buf.Bytes()
}

func (block *EncryptBlock) Decode(iv, password []byte) ([]byte, error) {
	buf := make([]byte, len(block.Content))
	copy(buf, block.Content)
	blockSizeBuf := buf[:4]
	blockSize := binary.LittleEndian.Uint32(blockSizeBuf)
	block.BlockSize = int32(blockSize)

	crcvBuf := buf[4:12]
	crcv := binary.LittleEndian.Uint64(crcvBuf)
	block.CRC64 = crcv

	contentBuf := buf[12:]
	content := aesCbc.AesDecrypt(contentBuf, iv, password)
	decodedCrc := crc64.Checksum(content, crc64.MakeTable(crc64.ECMA))
	if decodedCrc != crcv {
		return nil, ErrBlockCRC64NotMatch
	}

	return content, nil
}

func GenerateEncyptedBlock(content []byte, iv, password []byte) *EncryptBlock {
	crcv := crc64.Checksum(content, crc64.MakeTable(crc64.ECMA))
	encrytedBuf := aesCbc.AesEncrypt(content, iv, password)
	block := &EncryptBlock{
		BlockSize: int32(len(content)),
		CRC64:     crcv,
		Content:   encrytedBuf,
	}
	return block
}

func EncryptFile(dirPath string, destPath string, relativePath string, pk *rsa.PublicKey, crc64V uint64) (string, error) {

	fileInfo, err := os.Stat(JoinUri(dirPath, relativePath))
	if err != nil {
		return "", err
	}

	srcFile, err := os.Open(JoinUri(dirPath, relativePath))
	if err != nil {
		return "", err
	}
	defer srcFile.Close()

	destFileName := strconv.FormatUint(crc64.Checksum([]byte(relativePath), crc64.MakeTable(crc64.ECMA)), 10) + ".crypto"
	destFilePath := JoinUri(destPath, destFileName)
	if _, err := os.Stat(destFilePath); err == nil {
		os.Remove(destFilePath)
	}
	destFile, err := os.Create(destFilePath)
	if err != nil {
		return "", err
	}
	defer destFile.Close()

	fileName := []byte(fileInfo.Name())
	fileNameSize := len(fileName)
	extra := make(map[string]interface{})
	extraJSON, err := json.Marshal(extra)
	if err != nil {
		return "", err
	}
	extraSize := len(extraJSON)

	passwordBuf := make([]byte, 32)
	_, err = io.ReadFull(rand.Reader, passwordBuf)
	if err != nil {
		return "", err
	}

	encryptedPassword, err := rsa.EncryptOAEP(
		sha256.New(),
		rand.Reader,
		pk,
		passwordBuf,
		nil)
	if err != nil {
		return "", err
	}

	ivBuf := make([]byte, 32)
	_, err = io.ReadFull(rand.Reader, ivBuf)
	if err != nil {
		return "", err
	}

	header := &CryptoFileHeader{
		HeaderType:            0,
		Version:               1,
		CRC64:                 crc64V,
		Algorithm:             1,
		ModifyTime:            fileInfo.ModTime().Unix(),
		ChunkSize:             1024 * 1024,
		NameSize:              int32(fileNameSize),
		IVSize:                32,
		EncryptedPasswordSize: int32(len(encryptedPassword)),
		ExtraSize:             int32(extraSize),
		Name:                  fileName,
		IV:                    ivBuf,
		EncryptedPassword:     encryptedPassword,
		Extra:                 extraJSON,
	}
	_, err = destFile.Write(header.Bytes())
	if err != nil {
		return "", err
	}

	var eof bool
	for {
		chunkBuf := make([]byte, header.ChunkSize)
		n, err := srcFile.Read(chunkBuf)
		if err != nil {
			if err == io.EOF {
				eof = true
				endBuf := make([]byte, n)
				copy(endBuf, chunkBuf[:n])
				chunkBuf = endBuf
			}
			return "", err
		}
		block := GenerateEncyptedBlock(chunkBuf, ivBuf, passwordBuf)
		_, err = destFile.Write(block.Bytes())
		if err != nil {
			return "", err
		}
		if eof {
			break
		}
	}
	return destFileName, nil
}

func DecryptFile(sourcePath string, destPath string, relativePath string, pk *rsa.PrivateKey) error {
	file, err := os.Open(JoinUri(sourcePath, relativePath))
	if err != nil {
		return err
	}
	defer file.Close()

	destFilePath := JoinUri(destPath, relativePath)
	if _, err := os.Stat(destFilePath); err == nil {
		os.Remove(destFilePath)
	}
	destFile, err := os.Create(destFilePath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	headerSizeBuf := make([]byte, 4)
	_, err = file.Read(headerSizeBuf)
	if err != nil {
		return err
	}
	headerSize := binary.LittleEndian.Uint32(headerSizeBuf)
	headerBuf := make([]byte, headerSize)
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	header := ParseCryptoFileHeader(headerBuf)

	if header.HeaderType != 0 {
		return ErrHeaderTypeNotMatch
	}

	if header.Version != 1 {
		return ErrVersionNotMatch
	}

	password, err := rsa.DecryptOAEP(
		sha256.New(),
		rand.Reader,
		pk,
		header.EncryptedPassword,
		nil)
	if err != nil {
		return err
	}

	// read first block
	_, err = file.Seek(int64(headerSize), io.SeekStart)
	if err != nil {
		return err
	}
	var eof bool
	crcCipher := crc64.New(crc64.MakeTable(crc64.ECMA))
	for {
		blockSizeBuf := make([]byte, 4)
		_, err = file.Read(blockSizeBuf)
		if err != nil {
			if err == io.EOF {
				eof = true
			}
			return err
		}
		blockSize := binary.LittleEndian.Uint32(blockSizeBuf)
		blockBuf := make([]byte, blockSize+8)
		_, err = file.Read(blockBuf)
		if err != nil {
			return err
		}
		blockCrc64Buf := blockBuf[:8]
		blockCrc64 := binary.LittleEndian.Uint64(blockCrc64Buf)
		blockContentBuf := blockBuf[8:]
		block := &EncryptBlock{
			BlockSize: int32(blockSize),
			CRC64:     blockCrc64,
			Content:   blockContentBuf,
		}
		blockContent, err := block.Decode(header.IV, password)
		if err != nil {
			return err
		}
		decodedBlockCrc64 := crc64.Checksum(blockContent, crc64.MakeTable(crc64.ECMA))
		if decodedBlockCrc64 != block.CRC64 {
			return ErrCRC64NotMatch
		}
		_, err = destFile.Write(blockContent)
		if err != nil {
			return err
		}
		_, err = crcCipher.Write(blockContent)
		if err != nil {
			return err
		}
		if eof {
			break
		}
	}

	fileCrc64 := crcCipher.Sum64()
	if fileCrc64 != header.CRC64 {
		return ErrCRC64NotMatch
	}
	return nil
}
