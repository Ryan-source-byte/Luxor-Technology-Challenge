package proof

import (
	"crypto/sha256"
	"encoding/hex"
)

func Result(serverNonce, clientNonce string) string {
	sum := sha256.Sum256([]byte(serverNonce + clientNonce))
	return hex.EncodeToString(sum[:])
}
