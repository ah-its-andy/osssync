package core

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"fmt"
	"hash/crc64"
	"math/rand"

	"github.com/tyler-smith/go-bip39"
)

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

func GetPrivateKeyPEM(pk *rsa.PrivateKey, keyFormat string) ([]byte, error) {
	var buffer []byte
	var blockType string
	if keyFormat == "PKCS1" {
		blockType = "RSA PRIVATE KEY"
		buffer = x509.MarshalPKCS1PrivateKey(pk)
	} else if keyFormat == "PKCS8" {
		blockType = "PRIVATE KEY"
		b, err := x509.MarshalPKCS8PrivateKey(pk)
		if err != nil {
			return nil, err
		}
		buffer = make([]byte, len(b))
		copy(buffer, b)
	}
	var pb = &pem.Block{
		Type:  blockType,
		Bytes: buffer,
	}

	return pem.EncodeToMemory(pb), nil
}

func GetPublicKeyPEM(pk *rsa.PrivateKey, keyFormat string) ([]byte, error) {
	var buffer []byte
	var blockType string
	if keyFormat == "PKCS1" {
		blockType = "RSA PUBLIC KEY"
		buffer = x509.MarshalPKCS1PublicKey(&pk.PublicKey)
	} else if keyFormat == "PKIX" {
		blockType = "PUBLIC KEY"
		b, err := x509.MarshalPKIXPublicKey(&pk.PublicKey)
		if err != nil {
			return nil, err
		}
		buffer = make([]byte, len(b))
		copy(buffer, b)
	}

	pb := &pem.Block{
		Type:  blockType,
		Bytes: buffer,
	}

	return pem.EncodeToMemory(pb), nil

}
