package gormplugin2

import (
	"context"
	"strings"
	"sync"

	"github.com/andeya/gust"
	"github.com/andeya/mysql-aes/modifier"
	_ "github.com/pingcap/parser/test_driver"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var onceRegister sync.Once
var regErr error

func RegisterAesCallback(db *gorm.DB) gust.Errable[error] {
	onceRegister.Do(func() {
		if !modifier.AllowMysqlAES() {
			return
		}
		var ctl *dbController
		ctl, regErr = newDbController(db)
		if regErr != nil {
			return
		}
		gust.TryPanic(db.Callback().Query().Before("gorm:query").Register("aes:query", ctl.queryCallback))
		gust.TryPanic(db.Callback().Delete().Before("gorm:delete").Register("aes:delete", ctl.deleteCallback))
		gust.TryPanic(db.Callback().Create().Before("gorm:create").Register("aes:before_create", ctl.createCallback))
		gust.TryPanic(db.Callback().Update().Before("gorm:update").Register("aes:before_update", ctl.updateCallback))
		gust.TryPanic(db.Callback().Raw().Before("gorm:raw").Register("aes:raw", ctl.rawCallback))
		gust.TryPanic(db.Callback().Row().Before("gorm:row").Register("aes:row", ctl.rawCallback))
	})
	return gust.ToErrable(regErr)
}

// RegisterAesTable Register the AES information of the specified DB Table
// NOTE:
// Raw().Rows() and Raw().Row() methods do not support transparent AES decryption
// When calling Raw().Scan(&v), if v is not the corresponding Model, Model(m).Raw().Scan(&v) should be called instead
// The length of the ciphertext is: if the length of the plaintext is 16n+m (m<16), then the length of the ciphertext is (16n+16)x4/3, and the ciphertext has increased by at least 1/3
func RegisterAesTable(db *gorm.DB, tableName, aesKey string, aesColumnNames []string) gust.Errable[error] {
	rawDB, err := db.DB()
	if err != nil {
		return gust.ToErrable(err)
	}
	return gust.ToErrable(modifier.RegisterAesTable(rawDB, tableName, aesKey, aesColumnNames))
}

type dbController struct {
	dbName string
}

func newDbController(db *gorm.DB) (*dbController, error) {
	rawDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	dbName, err := modifier.QueryDbName(rawDB)
	if err != nil {
		return nil, err
	}
	return &dbController{dbName: dbName}, nil
}

type aesCallback struct {
	dbName string
	*modifier.AesConverter
	*gorm.DB
}

func (c *dbController) newAesCallback(scope *gorm.DB) gust.Option[aesCallback] {
	ctl := modifier.NewAesConverter(c.dbName)
	if ctl.IsSome() {
		return gust.Some(aesCallback{dbName: c.dbName, DB: scope, AesConverter: ctl.UnwrapUnchecked()})
	}
	return gust.None[aesCallback]()
}

func (c *dbController) queryCallback(scope *gorm.DB) {
	if scope.Error != nil {
		return
	}
	c.newAesCallback(scope).Inspect(func(a aesCallback) {
		queryAesScope{
			aesCallback: a,
		}.callback()
	})
}

func (c *dbController) createCallback(scope *gorm.DB) {
	if scope.Error != nil {
		return
	}
	c.newAesCallback(scope).Inspect(func(a aesCallback) {
		createAesScope{
			aesCallback: a,
		}.callback()
	})
}

func (c *dbController) deleteCallback(scope *gorm.DB) {
	if scope.Error != nil {
		return
	}
	c.newAesCallback(scope).Inspect(func(a aesCallback) {
		queryAesScope{
			aesCallback: a,
		}.callback()
	})
}

func (c *dbController) updateCallback(scope *gorm.DB) {
	if scope.Error != nil {
		return
	}
	c.newAesCallback(scope).Inspect(func(a aesCallback) {
		updateAesScope{
			aesCallback: a,
		}.callback()
	})
}

func (c *dbController) rawCallback(scope *gorm.DB) {
	if scope.Error != nil {
		return
	}
	ctl := modifier.NewAesConverter(c.dbName)
	if ctl.IsNone() {
		return
	}
	rawAesScope{
		aesCallback: aesCallback{dbName: c.dbName, DB: scope, AesConverter: ctl.UnwrapUnchecked()},
	}.callback()
}

func (a aesCallback) getExprSql(c clause.Expression) string {
	originSQL := a.DB.Statement.SQL
	a.DB.Statement.SQL = strings.Builder{}
	c.Build(a.DB.Statement)
	s := a.DB.Statement.SQL.String()
	a.DB.Statement.SQL = originSQL
	return s
}

func (a aesCallback) decryptClauses() {
	for name, c := range a.DB.Statement.Clauses {
		switch c.Name {
		case "WHERE":
			oldClause := c
			c.Builder = func(c clause.Clause, builder clause.Builder) {
				newSql, _ := a.ToDecryptedWhereConditions(a.DB.Statement.Table, a.getExprSql(oldClause), true)
				_, _ = builder.WriteString(newSql)
			}
			a.DB.Statement.Clauses[name] = c
		case "SELECT":
			if a.hasJoins() {
				// count(*)
				oldClause := c
				c.Builder = func(c clause.Clause, builder clause.Builder) {
					newSql, _ := a.ToDecryptedSelectFields(a.DB.Statement.Table, true, a.getExprSql(oldClause))
					_, _ = builder.WriteString("SELECT ")
					_, _ = builder.WriteString(strings.Join(newSql, ","))
				}
				a.DB.Statement.Clauses[name] = c
			}
		default:
			continue
		}
	}
}

type joinCtxKey int8

func (a aesCallback) initJoinsCtx() bool {
	if a.DB.Statement.Context != nil {
		has, ok := a.DB.Statement.Context.Value(joinCtxKey(0)).(bool)
		if has {
			return true
		}
		if ok {
			return false
		}
	}
	if len(a.DB.Statement.Joins) > 0 {
		if a.DB.Statement.Context != nil {
			a.DB.Statement.Context = context.WithValue(a.DB.Statement.Context, joinCtxKey(0), true)
		} else {
			a.DB.Statement.Context = context.WithValue(context.Background(), joinCtxKey(0), true)
		}
		return true
	}
	if a.DB.Statement.Context != nil {
		a.DB.Statement.Context = context.WithValue(a.DB.Statement.Context, joinCtxKey(0), false)
	} else {
		a.DB.Statement.Context = context.WithValue(context.Background(), joinCtxKey(0), false)
	}
	return false
}

func (a aesCallback) hasJoins() bool {
	has, _ := a.DB.Statement.Context.Value(joinCtxKey(0)).(bool)
	return has
}
