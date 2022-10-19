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
	hasJoin := len(scopeSearch.joinConditions) > 0
	if !hasJoin {
		if scope.GetAesTable(scope.tableName).IsNone() {
			return
		}
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
	if len(scopeSearch.selects) == 0 {
		selectFields := scope.ExpandWildcard(scope.tableName, hasJoin)
		if len(selectFields) > 0 {
			scopeSearch.selects = map[string]interface{}{
				argsKey:  []interface{}{},
				queryKey: strings.Join(selectFields, ","),
			}
		}
	} else {
		var selectString string
		switch value := scopeSearch.selects[queryKey].(type) {
		case string:
			selectString = value
		case []string:
			selectString = strings.Join(value, ", ")
		}
		selectFields, err := scope.ToDecryptedSelectFields(scope.tableName, false, selectString)
		if err != nil {
			scope.Err(err)
			return
		}
		scopeSearch.selects[queryKey] = strings.Join(selectFields, ",")
	}
}

func (scope queryAesScope) rawCallback(scopeSearch *search) {
	sql, err := scope.ConvertDml(scopeSearch.whereConditions[0][queryKey].(string))
	if err != nil {
		scope.Err(err)
		return
	}
	scopeSearch.whereConditions[0][queryKey] = sql
}
