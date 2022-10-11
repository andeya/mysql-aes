package modifier

import (
	"github.com/andeya/gust"
)

type TabCtx struct {
	*DbInfo
	cacheTables []*TableInfo
	tabAlias    map[string]string // key: table alias, value: table name
}

func newTabCtx(dbName string) gust.Option[*TabCtx] {
	d := getDbInfo(dbName)
	if d.IsNone() {
		return gust.None[*TabCtx]()
	}
	return gust.Some(&TabCtx{
		DbInfo:      d.UnwrapUnchecked(),
		cacheTables: nil,
		tabAlias:    nil,
	})
}

func (a *TabCtx) recordTable(tableName, tableAlias string) error {
	if a.tabAlias == nil {
		a.tabAlias = make(map[string]string)
	}
	if a.cacheTables != nil {
		for _, t := range a.cacheTables {
			if t.tableName == tableName {
				a.tabAlias[tableAlias] = tableName
				return nil
			}
		}
	} else {
		a.cacheTables = make([]*TableInfo, 0, 1)
	}
	tab := a.DbInfo.getOrRecordTable(tableName)
	if tab.IsErr() {
		return tab.UnwrapErr()
	}
	a.cacheTables = append(a.cacheTables, tab.Unwrap())
	a.tabAlias[tableAlias] = tableName
	return nil
}

func (a *TabCtx) getTableByName(tableName string) gust.Option[*TableInfo] {
	for _, t := range a.cacheTables {
		if t.tableName == tableName {
			return gust.Some(t)
		}
	}
	tab := a.DbInfo.getOrRecordTable(tableName)
	if tab.IsOk() {
		a.cacheTables = append(a.cacheTables, tab.Unwrap())
		a.tabAlias[tableName] = tableName // table alias is the same as table name
	}
	return tab.Ok()
}

func (a *TabCtx) getTableByAlias(tableAlias string) gust.Option[*TableInfo] {
	tableName := a.tabAlias[tableAlias]
	for _, t := range a.cacheTables {
		if tableName == "" || t.tableName == tableName {
			return gust.Some(t)
		}
	}
	return gust.None[*TableInfo]()
}

func (a *TabCtx) isAesField(tableAlias, fieldName string) bool {
	tab := a.getTableByAlias(tableAlias)
	if tab.IsNone() {
		return false
	}
	return tab.UnwrapUnchecked().IsAesField(fieldName)
}

func (a *TabCtx) getAliasByName(tableName string) gust.Option[string] {
	for alias, name := range a.tabAlias {
		if name == tableName {
			return gust.Some(alias)
		}
	}
	return gust.None[string]()
}
