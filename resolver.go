package sharding

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
	"gorm.io/gorm"
	"strconv"
)

// DBResolver
type DBResolver struct {
	Sources map[string]gorm.Dialector

	ShardingColumn string

	ShardingAlgorithm func(columnValue interface{}) (key string, err error)

	sharding       *Sharding
	connPools      map[string]gorm.ConnPool
	globalConnPool gorm.ConnPool

	datas []interface{}
}

type TableResolver struct {
}

func (r *DBResolver) resolve(stmt *gorm.Statement, op Operation) (connPool gorm.ConnPool) {
	if u, ok := stmt.Clauses[usingSourceName].Expression.(usingSource); ok && u.Use != "" {
		if p, ok := r.connPools[u.Use]; ok {
			connPool = p
		}
	} else {
		sql := stmt.SQL.String()

		shardingValue := GetShardingColumnValue(sql, r.ShardingColumn, stmt.Vars...)
		if shardingValue == nil {
			return nil
		}

		source, err := r.ShardingAlgorithm(shardingValue)
		if err != nil {
			return nil
		}

		connPool = r.connPools[source]
	}

	if stmt.DB.PrepareStmt {
		if preparedStmt, ok := r.sharding.prepareStmtStore[connPool]; ok {
			return &gorm.PreparedStmtDB{
				ConnPool: connPool,
				Mux:      preparedStmt.Mux,
				Stmts:    preparedStmt.Stmts,
			}
		}
	}

	return
}

func GetShardingColumnValue(sql string, col string, args ...any) any {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// Do something with the err
	}

	valMap := argsToValMap(args...)

	var whereExpr *sqlparser.Where

	switch st := stmt.(type) {
	case *sqlparser.Select:
		whereExpr = st.Where
	case *sqlparser.Update:
		whereExpr = st.Where
	case *sqlparser.Delete:
		whereExpr = st.Where
	case *sqlparser.Insert:
		_ = st
		column := st.Columns.FindColumn(sqlparser.NewColIdent(col))
		switch rows := st.Rows.(type) {
		case sqlparser.Values:
			return getVarVal(rows[0][column], valMap)
		}
	}

	if whereExpr != nil {
		var val any
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			if val != nil {
				return false, err
			}
			if compare, ok := node.(*sqlparser.ComparisonExpr); ok {
				if compare.Operator == sqlparser.EqualStr {
					if n, ok := compare.Left.(*sqlparser.ColName); ok {
						if n.Name.Equal(sqlparser.NewColIdent(col)) {
							val = getVarVal(compare.Right, valMap)
							return false, nil
						}
					}
				} else if compare.Operator == sqlparser.InStr {
					if n, ok := compare.Left.(*sqlparser.ColName); ok {
						if n.Name.Equal(sqlparser.NewColIdent(col)) {
							if vv, ok := compare.Right.(sqlparser.ValTuple); ok {
								vvv := sqlparser.Exprs(vv)
								val = getVarVal(vvv[0], valMap)
							}

							return false, nil
						}
					} else if tuple, ok := compare.Left.(sqlparser.ValTuple); ok {
						index := -1
						exprs := sqlparser.Exprs(tuple)
						for i, expr := range exprs {
							if n, ok := expr.(*sqlparser.ColName); ok {
								if n.Name.Equal(sqlparser.NewColIdent(col)) {
									index = i
								}
							}
						}

						if index >= 0 {
							if vv, ok := compare.Right.(sqlparser.ValTuple); ok {
								vvv := sqlparser.Exprs(vv)
								if vvvv, ok := vvv[0].(sqlparser.ValTuple); ok {
									ves := sqlparser.Exprs(vvvv)
									val = getVarVal(ves[index], valMap)
								}
							}
							return false, err
						}

					}
				}
			}
			return true, nil
		}, whereExpr)
		return val
	}

	return nil
}

func getVarVal(expr sqlparser.Expr, valMap map[string]any) any {
	if v, ok := expr.(*sqlparser.SQLVal); ok {
		if v.Type == sqlparser.ValArg {
			return valMap[string(v.Val)]
		} else {
			switch v.Type {
			case sqlparser.IntVal:
				num, err := strconv.Atoi(string(v.Val))
				if err != nil {
					return nil
				}
				return num
			case sqlparser.StrVal, sqlparser.ValArg:
				return string(v.Val)
			default:
				return v.Val
			}
		}
	}

	return nil
}

func argsToValMap(args ...any) map[string]any {
	m := make(map[string]any, len(args))

	for i, arg := range args {
		m[fmt.Sprintf(":v%d", i+1)] = arg
	}

	return m
}
