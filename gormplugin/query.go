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
		hasJoin := len(scopeSearch.joinConditions) > 0
		selectFields = scope.ExpandWildcard(scope.tableName, hasJoin)
	} else {
		var selectString string
		switch value := scopeSearch.selects[queryKey].(type) {
		case string:
			selectString = value
		case []string:
			selectString = strings.Join(value, ", ")
		}
		newFields, err := scope.ToDecryptedSelectFields(scope.tableName, false, selectString)
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
