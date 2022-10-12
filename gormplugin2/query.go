package gormplugin2

type queryAesScope struct {
	aesCallback
}

func (scope queryAesScope) callback() {
	scope.initJoinsCtx()
	if len(scope.DB.Statement.Joins) > 0 {
		for i, join := range scope.DB.Statement.Joins {
			if join.Name != "" {
				var err error
				scope.DB.Statement.Joins[i].Name, err = scope.ToDecryptedJoinConditions(scope.DB.Statement.Table, join.Name)
				if err != nil {
					scope.AddError(err)
					return
				}
			}
		}
	}
	scope.decryptClauses()
	if scope.DB.Statement.SQL.String() != "" {
		rawAesScope{aesCallback: scope.aesCallback}.callback()
	}
	if len(scope.DB.Statement.Selects) > 0 {
		var err error
		scope.DB.Statement.Selects, err = scope.ToDecryptedSelectFields(scope.Statement.Table, false, scope.DB.Statement.Selects...)
		if err != nil {
			scope.AddError(err)
			return
		}
	} else {
		scope.DB.Statement.Selects = scope.ExpandWildcard(scope.DB.Statement.Table, scope.hasJoins())
	}
}
