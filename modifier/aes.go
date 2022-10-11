package modifier

import (
	"database/sql"
	"errors"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/andeya/gust"
	_ "github.com/pingcap/parser/test_driver"
)

var globalAesDbInfos = gust.NewRWMutex(make([]*DbInfo, 0, 128))

// RegisterAesTable Register mysql table field information that requires automatic encryption and decryption
func RegisterAesTable(db *sql.DB, tableName, aesKey string, aesColumnNames []string) error {
	if db == nil || tableName == "" {
		return errors.New("RegisterAesTable: invalid dbName or tableName")
	}
	global := globalAesDbInfos.Lock()
	defer func() {
		globalAesDbInfos.Unlock(global)
	}()
	newDbInfo, err := newDbInfo(db)
	if err != nil {
		return err
	}
	for _, dbInfo := range global {
		if dbInfo.DbName == newDbInfo.DbName {
			return dbInfo.recordTable(tableName, aesKey, aesColumnNames).Err()
		}
	}
	global = append(global, newDbInfo)
	return newDbInfo.recordTable(tableName, aesKey, aesColumnNames).Err()
}

var allowMysqlAES atomic.Value

func AllowMysqlAES() bool {
	allow, ok := allowMysqlAES.Load().(bool)
	if ok {
		return allow
	}
	allow = gust.BoolOpt(os.LookupEnv("ALLOW_MYSQL_AES")).XMapOr(false, func(s string) any {
		return gust.Ret(strconv.ParseBool(s)).UnwrapOr(false)
	}).(bool)
	allowMysqlAES.Store(allow)
	return allow
}

func QueryDbName(db *sql.DB) (string, error) {
	var dbName string
	err := db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return "", err
	}
	return dbName, nil
}

func getDbInfo(dbName string) gust.Option[*DbInfo] {
	defer globalAesDbInfos.RUnlock()
	for _, dbInfo := range globalAesDbInfos.RLock() {
		if dbInfo.DbName == dbName {
			return gust.Some(dbInfo)
		}
	}
	return gust.None[*DbInfo]()
}
