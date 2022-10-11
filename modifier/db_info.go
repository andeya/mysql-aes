package modifier

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/andeya/gust"
)

type DbInfo struct {
	DbName     string
	TableInfos sync.Map // *TableInfo
	db         *sql.DB
}

func newDbInfo(db *sql.DB) (*DbInfo, error) {
	dbName, err := QueryDbName(db)
	if err != nil {
		return nil, err
	}
	return &DbInfo{
		DbName:     dbName,
		TableInfos: sync.Map{},
		db:         db,
	}, nil
}

func (a *DbInfo) recordTable(tableName string, aesKey string, aesFields []string) gust.Result[*TableInfo] {
	tab, ok := a.TableInfos.Load(tableName)
	if ok {
		return gust.Ok(tab.(*TableInfo))
	}
	for i, name := range aesFields {
		x := strings.Split(strings.Trim(name, "`"), ".")
		switch len(x) {
		case 1:
			name = x[0]
		case 2:
			if tableName != x[0] {
				return gust.Err[*TableInfo](fmt.Errorf("recordTable: does not match: tableName=%s, aesColumnName=%s", tableName, aesFields[i]))
			}
			name = x[1]
		case 3:
			if a.DbName != x[0] {
				return gust.Err[*TableInfo](fmt.Errorf("recordTable: does not match: dbName=%s, aesColumnName=%s", a.DbName, aesFields[i]))
			}
			if tableName != x[1] {
				return gust.Err[*TableInfo](fmt.Errorf("recordTable: does not match: tableName=%s, aesColumnName=%s", tableName, aesFields[i]))
			}
			name = x[2]
		default:
			return gust.Err[*TableInfo](fmt.Errorf("recordTable: invalid aesColumnName=%s", aesFields[i]))
		}
		if name == "" {
			return gust.Err[*TableInfo](fmt.Errorf("recordTable: invalid aesColumnName=%s", aesFields[i]))
		}
		aesFields[i] = name
	}
	allFields, err := a.queryFields(tableName)
	if err != nil {
		return gust.Err[*TableInfo](err)
	}
	newTab := newTableInfo(a.DbName, tableName, aesKey, aesFields, allFields)
	a.TableInfos.Store(tableName, newTab)
	return gust.Ok(newTab)
}

func (a *DbInfo) getTable(tableName string) gust.Option[*TableInfo] {
	v, _ := a.TableInfos.Load(tableName)
	t, ok := v.(*TableInfo)
	return gust.BoolOpt(t, ok)
}

func (a *DbInfo) getOrRecordTable(tableName string) gust.Result[*TableInfo] {
	t := a.getTable(tableName)
	if t.IsSome() {
		return t.OkOr(nil)
	}
	return a.recordTable(tableName, "", nil)
}

func (a *DbInfo) GetAesTable(tableName string) gust.Option[*TableInfo] {
	v, _ := a.TableInfos.Load(tableName)
	t, ok := v.(*TableInfo)
	if ok && t.IsAes() {
		return gust.Some(t)
	}
	return gust.None[*TableInfo]()
}

func (a *DbInfo) queryFields(tableName string) ([]string, error) {
	sqlStr := `SELECT COLUMN_NAME fName FROM information_schema.columns 
			WHERE table_schema = ? AND table_name = ?`

	var result []string

	rows, err := a.db.Query(sqlStr, a.DbName, tableName)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var f string
		err = rows.Scan(&f)
		if err != nil {
			return nil, err
		}
		result = append(result, f)
	}
	return result, nil
}
