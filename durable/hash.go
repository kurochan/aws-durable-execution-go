package durable

import (
	"crypto/md5"
	"encoding/hex"
)

func HashID(input string) string {
	sum := md5.Sum([]byte(input))
	return hex.EncodeToString(sum[:])[:16]
}
