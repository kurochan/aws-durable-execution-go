package durable

import (
	"crypto/md5"
	"encoding/hex"
)

// HashID returns the stable short operation ID used for backend checkpoint
// records.
func HashID(input string) string {
	sum := md5.Sum([]byte(input))
	return hex.EncodeToString(sum[:])[:16]
}
