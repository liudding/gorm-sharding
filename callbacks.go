package sharding

import (
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
	"reflect"
	"strings"
)

func (s *Sharding) registerCallbacks(db *gorm.DB) {
	s.Callback().Create().Before("*").Register("gorm:sharding", s.switchSourceForCreate)
	s.Callback().Query().Before("*").Register("gorm:sharding", s.switchSourceForQuery)
	s.Callback().Update().Before("*").Register("gorm:sharding", s.switchSourceForUpdate)
	s.Callback().Delete().Before("*").Register("gorm:sharding", s.switchSourceForDelete)
	s.Callback().Row().Before("*").Register("gorm:sharding", s.switchSource)
	s.Callback().Raw().Before("*").Register("gorm:sharding", s.switchSource)
}

func (s *Sharding) switchSource(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		if db.Statement.SQL.Len() == 0 {
			callbacks.BuildQuerySQL(db)
		}

		if rawSQL := strings.TrimSpace(db.Statement.SQL.String()); len(rawSQL) > 10 && strings.EqualFold(rawSQL[:6], "select") && !strings.EqualFold(rawSQL[len(rawSQL)-10:], "for update") {
			db.Statement.ConnPool = s.resolve(db.Statement, Read)
		} else {
			db.Statement.ConnPool = s.resolve(db.Statement, Write)
		}
	}
}

func (s *Sharding) switchSourceForCreate(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		if db.Statement.SQL.Len() == 0 {
			db.Statement.SQL.Grow(180)
			db.Statement.AddClauseIfNotExists(clause.Insert{})
			db.Statement.AddClause(callbacks.ConvertToCreateValues(db.Statement))

			db.Statement.Build(db.Statement.BuildClauses...)
		}

		db.Statement.ConnPool = s.resolve(db.Statement, Write)
	}
}

func (s *Sharding) switchSourceForQuery(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		if db.Statement.SQL.Len() == 0 {
			callbacks.BuildQuerySQL(db)
		}

		db.Statement.ConnPool = s.resolve(db.Statement, Read)
	}
}

func (s *Sharding) switchSourceForUpdate(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		if db.Statement.SQL.Len() == 0 {
			db.Statement.SQL.Grow(180)
			db.Statement.AddClauseIfNotExists(clause.Update{})
			if _, ok := db.Statement.Clauses["SET"]; !ok {
				if set := callbacks.ConvertToAssignments(db.Statement); len(set) != 0 {
					defer delete(db.Statement.Clauses, "SET")
					db.Statement.AddClause(set)
				} else {

				}
			}

			db.Statement.Build(db.Statement.BuildClauses...)
		}

		db.Statement.ConnPool = s.resolve(db.Statement, Write)
	}
}

func (s *Sharding) switchSourceForDelete(db *gorm.DB) {
	if !isTransaction(db.Statement.ConnPool) {
		if db.Statement.SQL.Len() == 0 {
			db.Statement.SQL.Grow(100)
			db.Statement.AddClauseIfNotExists(clause.Delete{})

			if db.Statement.Schema != nil {
				_, queryValues := schema.GetIdentityFieldValuesMap(db.Statement.Context, db.Statement.ReflectValue, db.Statement.Schema.PrimaryFields)
				column, values := schema.ToQueryValues(db.Statement.Table, db.Statement.Schema.PrimaryFieldDBNames, queryValues)

				if len(values) > 0 {
					db.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
				}

				if db.Statement.ReflectValue.CanAddr() && db.Statement.Dest != db.Statement.Model && db.Statement.Model != nil {
					_, queryValues = schema.GetIdentityFieldValuesMap(db.Statement.Context, reflect.ValueOf(db.Statement.Model), db.Statement.Schema.PrimaryFields)
					column, values = schema.ToQueryValues(db.Statement.Table, db.Statement.Schema.PrimaryFieldDBNames, queryValues)

					if len(values) > 0 {
						db.Statement.AddClause(clause.Where{Exprs: []clause.Expression{clause.IN{Column: column, Values: values}}})
					}
				}
			}

			db.Statement.AddClauseIfNotExists(clause.From{})

			db.Statement.Build(db.Statement.BuildClauses...)
		}

		db.Statement.ConnPool = s.resolve(db.Statement, Write)
	}
}

func isTransaction(connPool gorm.ConnPool) bool {
	_, ok := connPool.(gorm.TxCommitter)
	return ok
}
