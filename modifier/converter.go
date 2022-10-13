package modifier

import (
	"fmt"
	"strings"

	"github.com/andeya/gust"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
)

type AesConverter struct {
	*TabCtx
}

func NewAesConverter(dbName string) gust.Option[*AesConverter] {
	tab := newTabCtx(dbName)
	if tab.IsNone() {
		return gust.None[*AesConverter]()
	}
	return gust.Some(&AesConverter{TabCtx: tab.UnwrapUnchecked()})
}

func (a *AesConverter) ConvertDml(sql string) (string, error) {
	stmt, err := a.parseStmt(sql)
	if err != nil {
		return "", err
	}
	err = a.ConvertDmlAst(stmt)
	if err != nil {
		return "", err
	}
	return formatNode(stmt)
}

func (a *AesConverter) ConvertDmlAst(stmt ast.StmtNode) error {
	var err error
	switch t := stmt.(type) {
	case *ast.SelectStmt:
		if t.From != nil {
			err = a.convertJoin(t.From.TableRefs)
			if err != nil {
				return err
			}
		}
		err = a.convertSelect(t)
	case *ast.UpdateStmt:
		if t.TableRefs != nil {
			err = a.convertJoin(t.TableRefs.TableRefs)
			if err != nil {
				return err
			}
		}
		err = a.convertUpdate(t)
	case *ast.DeleteStmt:
		if t.TableRefs != nil {
			err = a.convertJoin(t.TableRefs.TableRefs)
			if err != nil {
				return err
			}
		}
		err = a.convertDelete(t)
	case *ast.InsertStmt:
		if t.Table != nil {
			err = a.convertJoin(t.Table.TableRefs)
			if err != nil {
				return err
			}
		}
		err = a.convertInsert(t)
	}
	return err
}

func (a *AesConverter) ToDecryptedWhereConditions(tableName, cond string, hadWherePrefix bool) (string, error) {
	cond = strings.TrimSpace(cond)
	if len(cond) == 0 || len(tableName) == 0 {
		return cond, nil
	}
	var err error
	var prefix string
	if hadWherePrefix {
		prefix = "SELECT * FROM `" + tableName + "` "
	} else {
		prefix = "SELECT * FROM `" + tableName + "` WHERE "
	}
	stmt, err := a.parseStmt(prefix + cond)
	if err != nil {
		return "", err
	}
	t := stmt.(*ast.SelectStmt)
	if t.From != nil {
		err = a.convertJoin(t.From.TableRefs)
		if err != nil {
			return "", err
		}
	}
	if t.Where != nil {
		t.Where, err = a.convertExpr(t.Where)
		if err != nil {
			return "", err
		}
	}
	sql, err := formatNode(t)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(sql, prefix), nil
}

func (a *AesConverter) ToDecryptedJoinConditions(tableName, join string) (string, error) {
	join = strings.TrimSpace(join)
	if len(join) == 0 {
		return join, nil
	}
	var err error
	var prefix string
	if strings.HasPrefix(tableName, "`") {
		prefix = "SELECT * FROM " + tableName + " "
	} else {
		prefix = "SELECT * FROM `" + tableName + "` "
	}
	stmt, err := a.parseStmt(prefix + join)
	if err != nil {
		return "", err
	}
	t := stmt.(*ast.SelectStmt)
	if t.From != nil {
		err = a.convertJoin(t.From.TableRefs)
		if err != nil {
			return "", err
		}
	}
	sql, err := formatNode(t)
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(sql, prefix), nil
}

func (a *AesConverter) ToDecryptedSelectFields(tableName string, hadSelectPrefix bool, selectFields ...string) ([]string, error) {
	selectFields = cleanEmpty(selectFields)
	var prefix string
	if !hadSelectPrefix {
		prefix = "SELECT "
	}
	var suffix = " FROM `" + tableName + "`"
	stmt, err := a.parseStmt(prefix + strings.Join(selectFields, ",") + suffix)
	if err != nil {
		return nil, err
	}
	selectStmt := stmt.(*ast.SelectStmt)
	if selectStmt.From != nil {
		err = a.convertJoin(selectStmt.From.TableRefs)
		if err != nil {
			return nil, err
		}
	}
	err = a.decryptSelectFieldExpr(selectStmt)
	if err != nil {
		return nil, err
	}
	if selectStmt.Fields == nil {
		return nil, err
	}
	var fields = make([]string, len(selectStmt.Fields.Fields))
	for i, field := range selectStmt.Fields.Fields {
		fields[i], err = formatNode(field)
		if err != nil {
			return nil, err
		}
	}
	return fields, nil
}

func (a *AesConverter) ExpandWildcard(tableName string, addAlias bool) []string {
	tab := a.GetAesTable(tableName)
	if tab.IsNone() {
		// No encrypted fields, no need to expand
		return nil
	}
	if addAlias {
		return tab.UnwrapUnchecked().getSelectFields(a.getAliasByName(tableName))
	}
	return tab.UnwrapUnchecked().getSelectFields(gust.None[string]())
}

func (a *AesConverter) parseStmt(sql string) (ast.StmtNode, error) {
	return parser.New().ParseOneStmt(sql, "", "")
}

func (a *AesConverter) convertResultSetNode(rsNode ast.ResultSetNode) (err error) {
	switch t := rsNode.(type) {
	case *ast.SelectStmt:
		return NewAesConverter(a.DbInfo.DbName).UnwrapUnchecked().ConvertDmlAst(t)
	case *ast.SubqueryExpr:
		aa := newConverter(a)
		t.Accept(aa)
		if aa.err != nil {
			return aa.err
		}
	case *ast.TableSource:
		if t.AsName.O != "" {
			tableName, ok := t.Source.(*ast.TableName)
			if ok {
				return a.recordTable(tableName.Name.O, t.AsName.O)
			} else {
				// Aliases are only useful when extending SELECT *, which is unlikely to be the case for temporary tables, and unlikely for SELECT tmp.*. It's hard to solve
			}
		}
		return a.convertResultSetNode(t.Source)
	case *ast.TableName:
		return a.recordTable(t.Name.O, t.Name.O)
	case *ast.Join:
		return a.convertJoin(t)
	case *ast.SetOprStmt:
		aa := newConverter(a)
		t.Accept(aa)
		if aa.err != nil {
			return aa.err
		}
	}
	return nil
}

func (a *AesConverter) convertJoin(join *ast.Join) (err error) {
	if join == nil {
		return nil
	}
	// 1. convert ResultSetNode
	for _, node := range []ast.ResultSetNode{join.Left, join.Right} {
		err = a.convertResultSetNode(node)
		if err != nil {
			return err
		}
	}
	// 2. convert join on
	if join.On != nil && join.On.Expr != nil {
		join.On.Expr, err = a.convertExpr(join.On.Expr)
		if err != nil {
			return err
		}
	}
	return
}

func (a *AesConverter) convertSelect(t *ast.SelectStmt) (err error) {
	// 1. convert where
	if t.Where != nil {
		t.Where, err = a.convertExpr(t.Where)
		if err != nil {
			return err
		}
	}
	// 2. convert select fields
	return a.decryptSelectFieldExpr(t)
}

func (a *AesConverter) convertDelete(t *ast.DeleteStmt) (err error) {
	if t.Where != nil {
		t.Where, err = a.convertExpr(t.Where)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *AesConverter) convertUpdate(t *ast.UpdateStmt) (err error) {
	// 1. convert where
	if t.Where != nil {
		t.Where, err = a.convertExpr(t.Where)
		if err != nil {
			return err
		}
	}
	// 2. convert update list
	if len(a.cacheTables) == 0 {
		return nil
	}
	tab := a.cacheTables[0]
	for _, assign := range t.List {
		a.convertAssignment(tab, assign)
	}
	return nil
}

func (a *AesConverter) convertInsert(t *ast.InsertStmt) error {
	if len(a.cacheTables) == 0 {
		return nil
	}
	// 1. convert set list
	tab := a.cacheTables[0]
	for _, assign := range t.Setlist {
		a.convertAssignment(tab, assign)
	}
	// 2. convert values list
	for i, column := range t.Columns {
		if !tab.IsAesField(column.Name.O) {
			continue
		}
		for _, exprList := range t.Lists {
			if len(exprList) <= i {
				return fmt.Errorf("insert column count(%d) not match value count(%d), table: %s, column: %s",
					len(t.Columns), len(exprList), tab.tableName, column.Name.String())
			}
			exprList[i] = tab.EncryptValueNode(exprList[i])
		}
	}
	return nil
}

func (a *AesConverter) convertAssignment(tab *TableInfo, assign *ast.Assignment) {
	if !tab.IsAesField(assign.Column.Name.O) {
		return
	}
	assign.Expr = tab.EncryptValueNode(assign.Expr)
}

func (a *AesConverter) decryptColumnNameExpr(columnNameExpr *ast.ColumnNameExpr) (ast.ExprNode, gust.Option[*ast.ColumnName]) {
	columnName := getColumnName(columnNameExpr)
	if columnName.IsSome() {
		col := columnName.UnwrapUnchecked()
		tab := a.TabCtx.getTableByAlias(col.Table.O)
		if tab.IsSome() {
			t := tab.UnwrapUnchecked()
			if t.IsAesField(col.Name.O) {
				return t.decryptValueNode(columnNameExpr), columnName
			}
		}
	} else {
		// no need to deal with
	}
	return columnNameExpr, gust.None[*ast.ColumnName]()
}

func (a *AesConverter) convertExpr(expr ast.ExprNode) (ast.ExprNode, error) {
	var newExpr ast.ExprNode
	var err error
	switch t := expr.(type) {
	case *ast.ColumnNameExpr:
		newExpr, _ = a.decryptColumnNameExpr(t)
		return newExpr, nil
	case *ast.BetweenExpr:
		t.Expr, err = a.convertExpr(t.Expr)
		if err != nil {
			return nil, err
		}
		t.Left, err = a.convertExpr(t.Left)
		if err != nil {
			return nil, err
		}
		t.Right, err = a.convertExpr(t.Right)
	case *ast.BinaryOperationExpr:
		t.L, err = a.convertExpr(t.L)
		if err != nil {
			return nil, err
		}
		t.R, err = a.convertExpr(t.R)
	case *ast.IsNullExpr:
		t.Expr, err = a.convertExpr(t.Expr)
	case *ast.IsTruthExpr:
		t.Expr, err = a.convertExpr(t.Expr)
	case *ast.ParenthesesExpr:
		t.Expr, err = a.convertExpr(t.Expr)
	case *ast.PatternInExpr:
		t.Expr, err = a.convertExpr(t.Expr)
		if err != nil {
			return nil, err
		}
		t.Sel, err = a.convertExpr(t.Sel)
		if err != nil {
			return nil, err
		}
		for i, node := range t.List {
			t.List[i], err = a.convertExpr(node)
			if err != nil {
				return nil, err
			}
		}
	case *ast.PatternLikeExpr:
		t.Expr, err = a.convertExpr(t.Expr)
		if err != nil {
			return nil, err
		}
		t.Pattern, err = a.convertExpr(t.Pattern)
	case *ast.CompareSubqueryExpr:
		t.L, err = a.convertExpr(t.L)
		if err != nil {
			return nil, err
		}
		t.R, err = a.convertExpr(t.R)
	case *ast.ExistsSubqueryExpr:
		t.Sel, err = a.convertExpr(t.Sel)
	case *ast.PositionExpr:
		t.P, err = a.convertExpr(t.P)
	case *ast.UnaryOperationExpr:
		t.V, err = a.convertExpr(t.V)
	case *ast.SubqueryExpr:
		aa := newConverter(a)
		t.Accept(aa)
		if aa.err != nil {
			return t, aa.err
		}
	}
	return expr, err
}

func (a *AesConverter) decryptSelectFieldExpr(stmt *ast.SelectStmt) error {
	fields := stmt.Fields
	defer func() {
		stmt.Fields = fields
	}()
	var wildCardField *ast.SelectField
	for _, field := range fields.Fields {
		if field.WildCard != nil {
			wildCardField = field
			break
		}
		expr, _ := field.Expr.(*ast.ColumnNameExpr)
		if expr == nil {
			continue
		}
		r, columnName := a.decryptColumnNameExpr(expr)
		if columnName.IsNone() {
			continue
		}
		field.Expr = r
		if field.AsName.O == "" {
			field.AsName = columnName.UnwrapUnchecked().Name
		}
	}
	if wildCardField == nil {
		return nil
	}
	var tableAlias = wildCardField.WildCard.Table.O
	tab := a.getTableByAlias(tableAlias)
	if tab.IsNone() {
		return nil
	}
	table := tab.UnwrapUnchecked()
	// join
	if len(a.tabAlias) > 1 && tableAlias == "" {
		tableAlias = a.getAliasByName(table.tableName).UnwrapOr(table.tableName)
	}
	fields.Fields = table.newSelectFields(tableAlias)
	return nil
}

func formatNode(node ast.Node) (string, error) {
	if node == nil {
		return "", nil
	}
	buf := &strings.Builder{}
	flags := format.RestoreKeyWordUppercase | format.RestoreStringSingleQuotes | format.RestoreNameBackQuotes
	err := node.Restore(&format.RestoreCtx{
		Flags:     flags,
		In:        buf,
		DefaultDB: "",
	})
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func getColumnName(expr ast.ExprNode) gust.Option[*ast.ColumnName] {
	// Only traverse the outermost
	x, _ := expr.(*ast.ColumnNameExpr)
	if x != nil && x.Name != nil {
		return gust.Some(x.Name)
	}
	return gust.None[*ast.ColumnName]()
}

func cleanEmpty(a []string) []string {
	var c = 0
	for _, s := range a {
		s = strings.TrimSpace(s)
		if len(s) > 0 {
			a[c] = s
			c++
		}
	}
	return a[:c]
}
