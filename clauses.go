package sharding

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Operation specifies dbresolver mode
type Operation string

const (
	writeName = "gorm:sharding:write"
	readName  = "gorm:sharding:read"
)

// ModifyStatement modify operation mode
func (op Operation) ModifyStatement(stmt *gorm.Statement) {
	var optName string
	if op == Write {
		optName = writeName
		stmt.Settings.Delete(readName)
	} else if op == Read {
		optName = readName
		stmt.Settings.Delete(writeName)
	}

	if optName != "" {
		stmt.Settings.Store(optName, struct{}{})
		if fc := stmt.DB.Callback().Query().Get("gorm:sharding"); fc != nil {
			fc(stmt.DB)
		}
	}
}

// Build implements clause.Expression interface
func (op Operation) Build(clause.Builder) {
}

// Use specifies configuration
func Use(str string) clause.Expression {
	return using{Use: str}
}

type using struct {
	Use string
}

const usingName = "gorm:sharding:using"

// ModifyStatement modify operation mode
func (u using) ModifyStatement(stmt *gorm.Statement) {
	stmt.Clauses[usingName] = clause.Clause{Expression: u}
	if fc := stmt.DB.Callback().Query().Get("gorm:sharding"); fc != nil {
		fc(stmt.DB)
	}
}

// Build implements clause.Expression interface
func (u using) Build(clause.Builder) {
}

// UseSource specifies source configuration
func UseSource(str string) clause.Expression {
	return usingSource{Use: str}
}

type usingSource struct {
	Use string
}

const usingSourceName = "gorm:sharding:using_source"

// ModifyStatement modify operation mode
func (u usingSource) ModifyStatement(stmt *gorm.Statement) {
	stmt.Clauses[usingSourceName] = clause.Clause{Expression: u}
}

// Build implements clause.Expression interface
func (u usingSource) Build(clause.Builder) {
}
