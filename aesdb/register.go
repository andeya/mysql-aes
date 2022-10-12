package aesdb

import (
	"database/sql"

	"github.com/andeya/gust"
	"github.com/andeya/mysql-aes/modifier"
)

// RegisterAesTable Register the AES information of the specified DB Table
// NOTE:
// Exec method does not support transparent AES
// Raw().Rows() and Raw().Row() methods do not support transparent AES decryption
// When calling Raw().Scan(&v), if v is not the corresponding Model, Model(m).Raw().Scan(&v) should be called instead
// The length of the ciphertext is: if the length of the plaintext is 16n+m (m<16), then the length of the ciphertext is (16n+16)x4/3, and the ciphertext has increased by at least 1/3
func RegisterAesTable(db *sql.DB, tableName, aesKey string, aesColumnNames []string) gust.Errable[error] {
	return gust.ToErrable(modifier.RegisterAesTable(db, tableName, aesKey, aesColumnNames))
}
