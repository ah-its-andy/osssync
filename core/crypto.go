package core

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/asn1"
	"encoding/binary"
	"encoding/pem"
	"fmt"
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

func GenerateRsaKey(mnemonic string) (*rsa.PrivateKey, error) {
	seed := int64(binary.LittleEndian.Uint64(bip39.NewSeed(mnemonic, "")))
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
