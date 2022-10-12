package gormplugin

import (
	"fmt"
)

type updateAesScope struct {
	queryAesScope
	createAesScope
}

func (scope updateAesScope) callback() {
	scope.queryAesScope.callback()
	tab := scope.queryAesScope.GetAesTable(scope.queryAesScope.tableName)
	if tab.IsNone() {
		return
	}
	table := tab.UnwrapUnchecked()
	if updateAttrs, ok := scope.createAesScope.InstanceGet("gorm:update_attrs"); ok {
		kvs := updateAttrs.(map[string]interface{})
		for column, value := range kvs {
			if table.IsAesField(column) {
				r := table.GetEncryptedColumnValue(value)
				if r.IsErr() {
					scope.createAesScope.Err(r.UnwrapErr())
					return
				}
				optVal := r.Unwrap()
				if optVal.IsSome() {
					kvs[column] = optVal.Unwrap()
				} else {
					scope.createAesScope.Scope.Err(fmt.Errorf("createAesScope: encryption is not supported, column=%s", column))
					return
				}
			}
		}
	} else {
		scope.createAesScope.callback()
	}
}
