package sharding

import (
	"gorm.io/gorm"
	"sync"
)

const (
	Write Operation = "write"
	Read  Operation = "read"
)

type Sharding struct {
	*gorm.DB
	resolvers map[string]*DBResolver
	global    *DBResolver

	prepareStmtStore map[gorm.ConnPool]*gorm.PreparedStmtDB
}

// Register
func Register(resolver *DBResolver, data ...any) *Sharding {
	return (&Sharding{}).Register(resolver, data...)
}

func (s *Sharding) Register(resolver *DBResolver, data ...any) *Sharding {
	if s.resolvers == nil {
		s.resolvers = map[string]*DBResolver{}
	}

	if s.prepareStmtStore == nil {
		s.prepareStmtStore = map[gorm.ConnPool]*gorm.PreparedStmtDB{}
	}

	if s.DB != nil {
		err := s.compileResolveConnPools(resolver)
		if err != nil {
			return nil
		}
	}

	resolver.sharding = s

	if len(data) > 0 {
		for _, d := range data {
			if t, ok := d.(string); ok {
				s.resolvers[t] = resolver
			} else {
				stmt := &gorm.Statement{DB: s.DB}
				if err := stmt.Parse(data); err == nil {
					s.resolvers[stmt.Table] = resolver
				} else {
					// 出错了
				}
			}
		}
	} else if s.global == nil {
		s.global = resolver
	} else {
		// errors
	}

	return s
}

func (s *Sharding) compileResolveConnPools(resolver *DBResolver) error {
	var err error
	if len(resolver.Sources) == 0 {
		resolver.globalConnPool = s.DB.Config.ConnPool
	} else if resolver.connPools, err = s.convertToConnPool(resolver.Sources); err != nil {
		return err
	}

	return nil
}

func (s *Sharding) resolve(stmt *gorm.Statement, op Operation) gorm.ConnPool {
	if r := s.getResolver(stmt); r != nil {
		return r.resolve(stmt, op)
	}
	return stmt.ConnPool
}

func (s *Sharding) getResolver(stmt *gorm.Statement) *DBResolver {
	if len(s.resolvers) > 0 {
		if u, ok := stmt.Clauses[usingName].Expression.(using); ok && u.Use != "" {
			if r, ok := s.resolvers[u.Use]; ok {
				return r
			}
		}

		if stmt.Table != "" {
			if r, ok := s.resolvers[stmt.Table]; ok {
				return r
			}
		}

		if stmt.Model != nil {
			if err := stmt.Parse(stmt.Model); err == nil {
				if r, ok := s.resolvers[stmt.Table]; ok {
					return r
				}
			}
		}

		if stmt.Schema != nil {
			if r, ok := s.resolvers[stmt.Schema.Table]; ok {
				return r
			}
		}

		if rawSQL := stmt.SQL.String(); rawSQL != "" {
			if r, ok := s.resolvers[getTableFromRawSQL(rawSQL)]; ok {
				return r
			}
		}
	}

	return s.global
}

// Name plugin name for Gorm plugin interface
func (s *Sharding) Name() string {
	return "gorm:sharding"
}

// Initialize implement for Gorm plugin interface
func (s *Sharding) Initialize(db *gorm.DB) error {
	s.DB = db
	s.registerCallbacks(db)

	for _, resolver := range s.resolvers {
		err := s.compileResolveConnPools(resolver)
		if err != nil {
			return err
		}
	}

	if s.global != nil {
		err := s.compileResolveConnPools(s.global)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Sharding) convertToConnPool(dialectors map[string]gorm.Dialector) (map[string]gorm.ConnPool, error) {
	connPools := make(map[string]gorm.ConnPool)

	config := *s.DB.Config
	for k, dialect := range dialectors {
		if db, err := gorm.Open(dialect, &config); err == nil {
			connPool := db.Config.ConnPool
			if preparedStmtDB, ok := connPool.(*gorm.PreparedStmtDB); ok {
				connPool = preparedStmtDB.ConnPool
			}

			s.prepareStmtStore[connPool] = &gorm.PreparedStmtDB{
				ConnPool:    db.Config.ConnPool,
				Stmts:       map[string]*gorm.Stmt{},
				Mux:         &sync.RWMutex{},
				PreparedSQL: make([]string, 0, 100),
			}

			connPools[k] = connPool
		} else {
			return nil, err
		}
	}

	return connPools, nil
}
