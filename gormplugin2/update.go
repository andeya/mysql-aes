package gormplugin2

import (
	"fmt"

	"github.com/andeya/gust/vec"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
)

type updateAesScope struct {
	aesCallback
}

func (scope updateAesScope) callback() {
	if scope.Statement.Schema != nil {
		for _, c := range scope.Statement.Schema.UpdateClauses {
			scope.Statement.AddClause(c)
		}
	}
	scope.aesCallback.decryptClauses()
	scope.Statement.SQL.Grow(180)
	scope.Statement.AddClauseIfNotExists(clause.Update{})
	if set := callbacks.ConvertToAssignments(scope.Statement); len(set) != 0 {
		r := scope.GetAesTable(scope.DB.Statement.Table)
		if r.IsSome() {
			tab := r.UnwrapUnchecked()
			for i, assignment := range set {
				if vec.Includes(scope.DB.Statement.Omits, assignment.Column.Name) {
					continue
				}
				if !tab.IsAesField(assignment.Column.Name) {
					continue
				}
				ret := tab.GetEncryptedColumnValue(assignment.Value)
				if ret.IsErr() {
					scope.AddError(ret.UnwrapErr())
					return
				}
				optVal := ret.Unwrap()
				if optVal.IsSome() {
					assignment.Value = optVal.Unwrap()
				} else {
					scope.AddError(fmt.Errorf("updateAesScope: encryption is not supported, column=%s", assignment.Column.Name))
					return
				}
				set[i] = assignment
			}
		}
		scope.Statement.AddClause(set)
	} else if _, ok := scope.Statement.Clauses["SET"]; !ok {
		return
	}
	scope.Statement.Build(scope.Statement.BuildClauses...)
}
