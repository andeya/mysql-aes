package modifier

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var aesKey = generate16BytesKey("12345678")

func TestaesEncrypt(t *testing.T) {
	s := base64.StdEncoding.EncodeToString(aesEncrypt("test组", aesKey))
	assert.Equal(t, "6o235VH2yTQlxR9JxWxzIw==", s)
	s2 := base64.StdEncoding.EncodeToString(aesEncrypt("aes-test-save", aesKey))
	assert.Equal(t, "2+tl5OyCHeOPRIWZ5kLf3Q==", s2)
}

func TestAESDecrypt(t *testing.T) {
	s, err := base64.StdEncoding.DecodeString("6o235VH2yTQlxR9JxWxzIw==")
	assert.NoError(t, err)
	assert.Equal(t, []byte("test组"), aesDecrypt(s, aesKey))
}

func TestMd5Key(t *testing.T) {
	checksum := md5.Sum([]byte("12345678901234567890123456789012"))
	aesKey := hex.EncodeToString(checksum[:])
	t.Log(aesKey, " len =", len(aesKey))
	s := base64.StdEncoding.EncodeToString(aesEncrypt("test组", generate16BytesKey(aesKey)))
	assert.Equal(t, "7myaNoTj312Pr4RG6j3GUA==", s)
}

func TestAesKey(t *testing.T) {
	rootKey := "12345678901234567890123456789012"
	checksum := md5.Sum([]byte(rootKey))
	subKey := hex.EncodeToString(checksum[:])
	aesKey := generate16BytesKey(subKey)
	t.Log("subKey =", subKey)
	t.Log("HEX(aesKey) =", hex.EncodeToString(aesKey))
	t.Logf("SET @KEY := UNHEX('%s');", hex.EncodeToString(aesKey))
	assert.NotEqual(t, []byte(subKey), aesKey)
}

func getHexAesKey(subKey string) string {
	return hex.EncodeToString(generate16BytesKey(subKey))
}

func getAesEncryptedLength(txtLen int) int {
	s := base64.StdEncoding.EncodeToString(aesEncrypt(
		strings.Repeat("0", txtLen),
		generate16BytesKey(strings.Repeat("0", 32)),
	))
	return len(s)
}

func TestAesEncryptedLength(t *testing.T) {
	t.Log(getAesEncryptedLength(510))
	assert.Equal(t, 24, getAesEncryptedLength(10))
}
