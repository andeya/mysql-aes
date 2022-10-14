package gormplugin

import (
	"unsafe"

	"github.com/andeya/gust"
	"github.com/andeya/mysql-aes/modifier"
	"github.com/jinzhu/gorm"
	_ "github.com/pingcap/parser/test_driver"
)

var dbMap = gust.NewMutex(make(map[uintptr]gust.Errable[error]))

func RegisterAesCallback(db *gorm.DB) gust.Errable[error] {
	if !modifier.AllowMysqlAES() {
		return gust.NonErrable[error]()
	}
	m := dbMap.Lock()
	defer dbMap.Unlock(m)
	key := uintptr(unsafe.Pointer(db))
	if err, ok := m[key]; ok {
		return err
	}
	ctl, regErr := newDbController(db)
	if regErr != nil {
		m[key] = gust.ToErrable[error](regErr)
		return m[key]
	}
	{
		db.Callback().Query().Before("gorm:query").Register("aes:query", ctl.queryCallback)
		db.Callback().RowQuery().Before("gorm:row_query").Register("aes:row_query", ctl.queryCallback)
	}
	{
		db.Callback().Delete().Before("gorm:delete").Register("aes:delete", ctl.deleteCallback)
	}
	{
		db.Callback().Create().Before("gorm:create").Register("aes:before_create", ctl.createCallback)
	}
	{
		db.Callback().Update().Before("gorm:update").Register("aes:before_update", ctl.updateCallback)
	}
	m[key] = gust.NonErrable[error]()
	return m[key]
}

// RegisterAesTable Register the AES information of the specified DB Table
// NOTE:
// Exec method does not support transparent AES
// Raw().Rows() and Raw().Row() methods do not support transparent AES decryption
// When calling Raw().Scan(&v), if v is not the corresponding Model, Model(m).Raw().Scan(&v) should be called instead
// The length of the ciphertext is: if the length of the plaintext is 16n+m (m<16), then the length of the ciphertext is (16n+16)x4/3, and the ciphertext has increased by at least 1/3
func RegisterAesTable(db *gorm.DB, tableName, aesKey string, aesColumnNames []string) gust.Errable[error] {
	return gust.ToErrable(modifier.RegisterAesTable(db.DB(), tableName, aesKey, aesColumnNames))
}

type dbController struct {
	dbName string
}

func newDbController(db *gorm.DB) (*dbController, error) {
	dbName, err := modifier.QueryDbName(db.DB())
	if err != nil {
		return nil, err
	}
	return &dbController{dbName: dbName}, nil
}

type aesCallback struct {
	dbName    string
	tableName string
	*modifier.AesConverter
	*gorm.Scope
}

func (c *dbController) newAesCallback(scope *gorm.Scope) gust.Option[aesCallback] {
	ctl := modifier.NewAesConverter(c.dbName)
	if ctl.IsSome() {
		return gust.Some(aesCallback{dbName: c.dbName, tableName: scope.TableName(), Scope: scope, AesConverter: ctl.UnwrapUnchecked()})
	}
	return gust.None[aesCallback]()
}

func (c *dbController) queryCallback(scope *gorm.Scope) {
	if scope.HasError() {
		return
	}
	c.newAesCallback(scope).Inspect(func(a aesCallback) {
		queryAesScope{
			aesCallback: a,
			isSelect:    true,
		}.callback()
	})
}

func (c *dbController) createCallback(scope *gorm.Scope) {
	if scope.HasError() {
		return
	}
	c.newAesCallback(scope).Inspect(func(a aesCallback) {
		createAesScope{
			aesCallback: a,
		}.callback()
	})
}

func (c *dbController) deleteCallback(scope *gorm.Scope) {
	if scope.HasError() {
		return
	}
	c.newAesCallback(scope).Inspect(func(a aesCallback) {
		queryAesScope{
			aesCallback: a,
			isSelect:    false,
		}.callback()
	})
}

func (c *dbController) updateCallback(scope *gorm.Scope) {
	if scope.HasError() {
		return
	}
	c.newAesCallback(scope).Inspect(func(a aesCallback) {
		updateAesScope{
			queryAesScope: queryAesScope{
				aesCallback: a,
				isSelect:    false,
			},
			createAesScope: createAesScope{
				aesCallback: a,
			},
		}.callback()
	})
}

type (
	search struct {
		db               *gorm.DB
		whereConditions  []map[string]interface{}
		orConditions     []map[string]interface{}
		notConditions    []map[string]interface{}
		havingConditions []map[string]interface{}
		joinConditions   []map[string]interface{}
		initAttrs        []interface{}
		assignAttrs      []interface{}
		selects          map[string]interface{}
		omits            []string
		orders           []interface{}
		preload          []searchPreload
		offset           interface{}
		limit            interface{}
		group            string
		tableName        string
		raw              bool
		Unscoped         bool
		ignoreOrderQuery bool
	}
	searchPreload struct {
		schema     string
		conditions []interface{}
	}
)

func (a aesCallback) scopeSearch() *search {
	return (*search)(unsafe.Pointer(a.Scope.Search))
}
