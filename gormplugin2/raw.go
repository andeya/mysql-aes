package gormplugin2

import (
	"strings"
)

type rawAesScope struct {
	aesCallback
}

func (scope rawAesScope) callback() {
	sql, err := scope.ConvertDml(scope.DB.Statement.SQL.String())
	if err != nil {
		scope.AddError(err)
		return
	}
	scope.DB.Statement.SQL = strings.Builder{}
	_, _ = scope.DB.Statement.SQL.WriteString(sql)
}
