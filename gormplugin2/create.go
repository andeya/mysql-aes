package gormplugin2

import (
	"fmt"

	"github.com/andeya/gust/vec"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
)

type createAesScope struct {
	aesCallback
}

func (scope createAesScope) callback() {
	if scope.Statement.SQL.Len() > 0 {
		panic("createAesScope: Statement.SQL is not empty")
	}
	scope.Statement.SQL.Grow(180)
	scope.Statement.AddClauseIfNotExists(clause.Insert{})
	createCalues := callbacks.ConvertToCreateValues(scope.Statement)
	r := scope.GetAesTable(scope.Statement.Table)
	if r.IsSome() {
		tab := r.UnwrapUnchecked()
		for i, column := range createCalues.Columns {
			if vec.Includes(scope.DB.Statement.Omits, column.Name) {
				continue
			}
			if !tab.IsAesField(column.Name) {
				continue
			}
			for _, values := range createCalues.Values {
				ret := tab.GetEncryptedColumnValue(values[i])
				if ret.IsErr() {
					scope.AddError(ret.UnwrapErr())
					return
				}
				optVal := ret.Unwrap()
				if optVal.IsSome() {
					values[i] = optVal.Unwrap()
				} else {
					scope.AddError(fmt.Errorf("createAesScope: encryption is not supported, column=%s", column.Name))
					return
				}
			}
		}
	}
	scope.Statement.AddClause(createCalues)
	scope.Statement.Build(scope.Statement.BuildClauses...)
}
