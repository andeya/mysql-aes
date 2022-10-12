package gormplugin

import (
	"strings"
)

const queryKey = "query"
const argsKey = "args"

type queryAesScope struct {
	aesCallback
	isSelect bool
}

func (scope queryAesScope) callback() {
	scopeSearch := scope.scopeSearch()
	if scopeSearch.raw && scope.isSelect {
		scope.rawCallback(scopeSearch)
		return
	}
	for _, conditions := range [][]map[string]interface{}{scopeSearch.whereConditions, scopeSearch.orConditions, scopeSearch.notConditions} {
		for _, condition := range conditions {
			v, err := scope.ToDecryptedWhereConditions(scope.tableName, condition[queryKey].(string), false)
			if err != nil {
				scope.Err(err)
				return
			}
			condition[queryKey] = v
		}
	}
	for _, condition := range scopeSearch.joinConditions {
		v, err := scope.ToDecryptedJoinConditions(scope.tableName, condition[queryKey].(string))
		if err != nil {
			scope.Err(err)
			return
		}
		condition[queryKey] = v
	}

	if !scope.isSelect {
		return
	}
	var selectFields []string
	if len(scopeSearch.selects) == 0 {
		scopeSearch.selects = map[string]interface{}{
			argsKey: []interface{}{},
		}
		fields := scope.Fields()
		hasJoin := len(scopeSearch.joinConditions) > 0
		tableName := scope.QuotedTableName()
		for _, field := range fields {
			var name string
			if hasJoin {
				name = tableName + "." + field.DBName
			} else {
				name = field.DBName
			}
			newFields, err := scope.ToDecryptedSelectFields(scope.tableName, false, name)
			if err != nil {
				scope.Err(err)
				return
			}
			selectFields = append(selectFields, newFields...)
		}
	} else {
		newFields, err := scope.ToDecryptedSelectFields(scope.tableName, false, scopeSearch.selects[queryKey].(string))
		if err != nil {
			scope.Err(err)
			return
		}
		selectFields = append(selectFields, newFields...)
	}
	scopeSearch.selects[queryKey] = strings.Join(selectFields, ",")
}

func (scope queryAesScope) rawCallback(scopeSearch *search) {
	sql, err := scope.ConvertDml(scopeSearch.whereConditions[0][queryKey].(string))
	if err != nil {
		scope.Err(err)
		return
	}
	scopeSearch.whereConditions[0][queryKey] = sql
}
