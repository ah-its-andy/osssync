package core

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/asn1"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"hash/crc64"
	"math/rand"

	"github.com/tyler-smith/go-bip39"
)

func UseCryptoIfPossible(fileInfo FileInfo, mnemonic string, password string) error {
	if mnemonic == "" && password == "" {
		return nil
	}
	if cf, ok := fileInfo.(CryptoFileInfo); !ok {
		return fmt.Errorf("File type %s does not support encryption", fileInfo.FileType())
	} else if mnemonic != "" {
		return cf.UseEncryption(true, mnemonic)
	} else if password != "" {
		return cf.UseEncryption(false, password)
	}

	return nil
}

type MnemonicKey struct {
	MasterKey string
	PublicKey string
}

func PrintMnemonic() (string, error) {
	mnemonic, err := GenerateMnemonic()
	if err != nil {
		return "", err
	}
	fmt.Println()
	fmt.Println("========= [IMPORTANT] Please write down the following mnemonic phrase =========")
	fmt.Println(mnemonic)
	fmt.Println("========= [IMPORTANT] Please write down the following mnemonic phrase =========")
	fmt.Println()
	return mnemonic, nil
}

func GenerateMnemonic() (string, error) {
	// Generate a mnemonic for memorization or user-friendly seeds
	entropy, err := bip39.NewEntropy(256)
	if err != nil {
		return "", err
	}
	mnemonic, err := bip39.NewMnemonic(entropy)
	if err != nil {
		return "", err
	}
	return mnemonic, nil

}

func GetMnemonicSeed(mnemonic string) int64 {
	seed := int64(binary.LittleEndian.Uint64(bip39.NewSeed(mnemonic, "")))
	return seed
}

func GetPasswordSeed(password string) int64 {
	crc64Cipher := crc64.New(crc64.MakeTable(crc64.ECMA))
	crc64Cipher.Write([]byte(password))
	seed := int64(binary.LittleEndian.Uint64(crc64Cipher.Sum(nil)))
	return seed
}

func GenerateRsaKey(seed int64) (*rsa.PrivateKey, error) {
	r := rand.New(rand.NewSource(seed))
	return rsa.GenerateKey(r, 4096)
}

func GetPrivateKeyPEM(pk *rsa.PrivateKey) []byte {
	var pb = &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(pk),
	}

	return pem.EncodeToMemory(pb)
}

func GetPublicKeyPEM(pk *rsa.PrivateKey) ([]byte, error) {
	asn1Bytes, err := asn1.Marshal(pk.PublicKey)
	if err != nil {
		return nil, err
	}

	pb := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: asn1Bytes,
	}

	return pem.EncodeToMemory(pb), nil

}
