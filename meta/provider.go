package meta

import (
	"sort"
	"sync"

	stdsql "database/sql"

	"github.com/dolthub/go-mysql-server/sql"
)

type DbProvider struct {
	mu      *sync.RWMutex
	engine  *stdsql.DB
	dsnName string
}

var _ sql.DatabaseProvider = (*DbProvider)(nil)
var _ sql.MutableDatabaseProvider = (*DbProvider)(nil)

func NewDBProvider(dsnName string) *DbProvider {
	engine, err := stdsql.Open("duckdb", dsnName+".db")
	if err != nil {
		panic(err)
	}
	return &DbProvider{
		mu:      &sync.RWMutex{},
		engine:  engine,
		dsnName: dsnName,
	}
}

// AllDatabases implements sql.DatabaseProvider.
func (prov *DbProvider) AllDatabases(ctx *sql.Context) []sql.Database {
	prov.mu.RLock()
	defer prov.mu.RUnlock()

	rows, err := prov.engine.Query("SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = ?", prov.dsnName)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	all := []sql.Database{}
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			panic(err)
		}

		switch schemaName {
		case "information_schema", "main", "pg_catalog":
			continue
		}

		all = append(all, NewDatabase(schemaName, prov.engine))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Name() < all[j].Name()
	})

	return all
}

// Database implements sql.DatabaseProvider.
func (prov *DbProvider) Database(ctx *sql.Context, name string) (sql.Database, error) {
	prov.mu.RLock()
	defer prov.mu.RUnlock()

	ok, err := hasDatabase(prov.engine, prov.dsnName, name)
	if err != nil {
		panic(err)
	}

	if ok {
		return NewDatabase(name, prov.engine), nil
	}
	return nil, sql.ErrDatabaseNotFound.New(name)
}

// HasDatabase implements sql.DatabaseProvider.
func (prov *DbProvider) HasDatabase(ctx *sql.Context, name string) bool {
	prov.mu.RLock()
	defer prov.mu.RUnlock()

	ok, err := hasDatabase(prov.engine, prov.dsnName, name)
	if err != nil {
		panic(err)
	}

	return ok
}

func hasDatabase(engine *stdsql.DB, dstName string, name string) (bool, error) {
	rows, err := engine.Query("SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = ? AND schema_name = ?", dstName, name)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), nil
}

// CreateDatabase implements sql.MutableDatabaseProvider.
func (prov *DbProvider) CreateDatabase(ctx *sql.Context, name string) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()

	_, err := prov.engine.Exec(`CREATE SCHEMA "` + name + `"`)
	if err != nil {
		return err
	}

	return nil
}

// DropDatabase implements sql.MutableDatabaseProvider.
func (prov *DbProvider) DropDatabase(ctx *sql.Context, name string) error {
	prov.mu.Lock()
	defer prov.mu.Unlock()

	_, err := prov.engine.Exec(`DROP SCHEMA "` + name + `" CASCADE`)
	if err != nil {
		return err
	}

	return nil
}
