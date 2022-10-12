package gormplugin

import (
	"fmt"
	"reflect"

	"github.com/jinzhu/gorm"
)

type createAesScope struct {
	aesCallback
}

func (scope createAesScope) callback() {
	tab := scope.GetAesTable(scope.tableName)
	if tab.IsNone() {
		return
	}
	table := tab.UnwrapUnchecked()
	for _, field := range scope.Scope.Fields() {
		if !scope.changeableField(field) {
			continue
		}
		if field.IsBlank && field.HasDefaultValue {
		} else if !field.IsPrimaryKey || !field.IsBlank {
			if table.IsAesField(field.DBName) {
				r := table.GetEncryptedColumnValue(field.Field.Interface())
				if r.IsErr() {
					scope.Scope.Err(r.UnwrapErr())
					return
				}
				optVal := r.Unwrap()
				if optVal.IsSome() {
					field.Field = reflect.ValueOf(optVal.Unwrap())
				} else {
					scope.Scope.Err(fmt.Errorf("createAesScope: encryption is not supported, column=%s", field.DBName))
					return
				}
			}
		}
	}
}

func (scope createAesScope) changeableField(field *gorm.Field) bool {
	if selectAttrs := scope.SelectAttrs(); len(selectAttrs) > 0 {
		for _, attr := range selectAttrs {
			if field.Name == attr || field.DBName == attr {
				return true
			}
		}
		return false
	}
	for _, attr := range scope.OmitAttrs() {
		if field.Name == attr || field.DBName == attr {
			return false
		}
	}
	return true
}
