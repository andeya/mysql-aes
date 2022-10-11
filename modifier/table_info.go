package modifier

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/andeya/gust"
	"github.com/andeya/gust/vec"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
)

type TableInfo struct {
	dbName          string
	tableName       string
	aesKeyBytes     []byte
	aesKeyHex       string
	fields          []*FieldInfo
	sqlSelectFields []string
	isAes           bool
}

type FieldInfo struct {
	fieldName string
	isAes     bool
}

func newTableInfo(dbName, tableName, aesKey string, aesFields, allFields []string) *TableInfo {
	tab := &TableInfo{
		dbName:    dbName,
		tableName: tableName,
	}
	if len(aesFields) > 0 {
		tab.aesKeyBytes = generate16BytesKey(aesKey)
		tab.aesKeyHex = hex.EncodeToString(tab.aesKeyBytes)
	}

	for _, field := range allFields {
		isAes := vec.Includes(aesFields, field)
		if isAes {
			tab.isAes = true
			tab.sqlSelectFields = append(tab.sqlSelectFields, tab.decryptedSelectField(field, ""))
		} else {
			tab.sqlSelectFields = append(tab.sqlSelectFields, fmt.Sprintf("`%s`", field))
		}
		tab.fields = append(tab.fields, &FieldInfo{
			fieldName: field,
			isAes:     isAes,
		})
	}
	return tab
}

func (a *TableInfo) getSelectFields(alias gust.Option[string]) []string {
	if alias.IsNone() {
		return a.sqlSelectFields
	}
	var fields = make([]string, 0, len(a.fields))
	for _, field := range a.fields {
		if field.isAes {
			fields = append(fields, a.decryptedSelectField(field.fieldName, alias.Unwrap())+" AS `"+field.fieldName+"`")
		} else {
			fields = append(fields, fmt.Sprintf("`%s`.`%s`", alias.Unwrap(), field.fieldName))
		}
	}
	return fields
}

func (a *TableInfo) IsAes() bool {
	return a.isAes
}

func (a *TableInfo) IsAesField(fieldName string) bool {
	if a.isAes {
		for _, field := range a.fields {
			if field.fieldName == fieldName {
				return field.isAes
			}
		}
	}
	return false
}

func (a *TableInfo) decryptedSelectField(columnName string, tableAlias string) string {
	if columnName == "" {
		return columnName
	}
	if strings.Contains(columnName, "CAST(AES_DECRYPT(FROM_BASE64(") {
		return columnName
	}
	s, _ := formatNode(&ast.SelectField{
		Offset:   0,
		WildCard: nil,
		Expr: a.decryptValueNode(&ast.ColumnNameExpr{
			Name: &ast.ColumnName{
				Schema: model.CIStr{},
				Table:  model.NewCIStr(tableAlias),
				Name:   model.NewCIStr(columnName),
			},
			Refer: nil,
		}),
		AsName:    model.NewCIStr(columnName),
		Auxiliary: false,
	})
	return s
}

func (a *TableInfo) EncryptValueNode(value ast.ExprNode) ast.ExprNode {
	return &ast.FuncCallExpr{
		FnName: model.NewCIStr("TO_BASE64"),
		Args: []ast.ExprNode{
			&ast.FuncCallExpr{
				FnName: model.NewCIStr("AES_ENCRYPT"),
				Args: []ast.ExprNode{
					value,
					&ast.FuncCallExpr{
						FnName: model.NewCIStr("UNHEX"),
						Args: []ast.ExprNode{
							ast.NewValueExpr(a.aesKeyHex, "", ""),
						},
					},
				},
			},
		},
	}
}

func (a *TableInfo) decryptValueNode(value ast.ExprNode) ast.ExprNode {
	return &ast.FuncCallExpr{
		FnName: model.NewCIStr("AES_DECRYPT"),
		Args: []ast.ExprNode{
			&ast.FuncCallExpr{
				FnName: model.NewCIStr("FROM_BASE64"),
				Args:   []ast.ExprNode{value},
			},
			&ast.FuncCallExpr{
				FnName: model.NewCIStr("UNHEX"),
				Args:   []ast.ExprNode{ast.NewValueExpr(a.aesKeyHex, "", "")},
			},
		},
	}
}

func (a *TableInfo) newSelectFields(tableAlias string) []*ast.SelectField {
	var fields = make([]*ast.SelectField, 0, len(a.fields))
	for _, field := range a.fields {
		nameNode := &ast.ColumnName{
			Schema: model.CIStr{},
			Table:  model.NewCIStr(tableAlias),
			Name:   model.NewCIStr(field.fieldName),
		}
		var expr ast.ExprNode = &ast.ColumnNameExpr{
			Name:  nameNode,
			Refer: nil,
		}
		if field.isAes {
			fields = append(fields, &ast.SelectField{
				Offset:    0,
				WildCard:  nil,
				Expr:      a.decryptValueNode(expr),
				AsName:    nameNode.Name,
				Auxiliary: false,
			})
		} else {
			fields = append(fields, &ast.SelectField{
				Offset:    0,
				WildCard:  nil,
				Expr:      expr,
				Auxiliary: false,
			})
		}
	}
	return fields
}

func (a *TableInfo) GetEncryptedColumnValue(v any) gust.Result[gust.Option[string]] {
	s, ok, err := convertValue(v)
	if err != nil {
		return gust.Err[gust.Option[string]](err)
	}
	if !ok {
		return gust.Ok[gust.Option[string]](gust.None[string]())
	}
	return gust.Ok(gust.Some(base64.StdEncoding.EncodeToString(aesEncrypt(s, a.aesKeyBytes))))
}
