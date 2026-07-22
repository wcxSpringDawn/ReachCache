package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"math/big"
)

const base62Chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

var secretKey = []byte("reachcache-shortlink-secret-2026")

func generateShortCode(url string) string {
	h := hmac.New(sha256.New, secretKey)
	h.Write([]byte(url))
	hash := h.Sum(nil)

	code := base62Encode(hash[:8])
	return code
}

func base62Encode(data []byte) string {
	n := new(big.Int).SetBytes(data)
	base := big.NewInt(62)
	mod := new(big.Int)
	result := make([]byte, 0, 11)

	for n.Sign() > 0 {
		n.DivMod(n, base, mod)
		result = append(result, base62Chars[mod.Int64()])
	}

	for len(result) < 8 {
		result = append(result, '0')
	}

	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return string(result)
}

func createCollisionSafeCode(url string, retry int) string {
	seed := make([]byte, 8)
	binary.LittleEndian.PutUint64(seed, uint64(retry))

	h := hmac.New(sha256.New, secretKey)
	h.Write(seed)
	h.Write([]byte(url))
	hash := h.Sum(nil)

	return base62Encode(hash[:8])
}
