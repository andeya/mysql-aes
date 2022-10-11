package modifier

import "github.com/pingcap/parser/ast"

func newConverter(a *AesConverter) *SubConverter {
	return &SubConverter{
		AesConverter: a,
	}
}

var _ ast.Visitor = SubConverter{}

type SubConverter struct {
	*AesConverter
	err error
}

func (s SubConverter) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch t := n.(type) {
	case *ast.WithClause:
		for _, e := range t.CTEs {
			s.err = s.recordTable(e.Name.O, "")
			if s.err != nil {
				return n, false
			}
			s.err = s.convertResultSetNode(e.Query)
			if s.err != nil {
				return n, false
			}
			// There is no need to traverse 'e.ColNameList' since there is no encryption mark for the temporary table
		}
	case *ast.SubqueryExpr:
		s.err = s.convertResultSetNode(t.Query)
	}
	return n, s.err != nil
}

func (s SubConverter) Leave(n ast.Node) (node ast.Node, ok bool) {
	return n, s.err == nil
}
