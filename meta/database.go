package meta

import (
	stdsql "database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

type Database struct {
	mu     *sync.RWMutex
	name   string
	engine *stdsql.DB
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)

func NewDatabase(name string, engine *stdsql.DB) *Database {
	return &Database{
		mu:     &sync.RWMutex{},
		name:   name,
		engine: engine}
}

// GetTableNames implements sql.Database.
func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tbls, err := d.tablesInsensitive()
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(tbls))
	for _, tbl := range tbls {
		names = append(names, tbl.Name())
	}
	return names, nil
}

// GetTableInsensitive implements sql.Database.
func (d *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tbls, err := d.tablesInsensitive()
	if err != nil {
		return nil, false, err
	}

	tbl, ok := tbls[strings.ToLower(tblName)]
	return tbl, ok, nil
}

func (d *Database) tablesInsensitive() (map[string]sql.Table, error) {
	rows, err := d.engine.Query("SELECT DISTINCT table_name FROM duckdb_tables() where schema_name = ?", d.name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tbls := make(map[string]sql.Table)
	for rows.Next() {
		var tblName string
		if err := rows.Scan(&tblName); err != nil {
			return nil, err
		}
		tbls[strings.ToLower(tblName)] = NewTable(tblName, d)
	}
	return tbls, nil
}

// Name implements sql.Database.
func (d *Database) Name() string {
	return d.name
}

// CreateTable implements sql.TableCreator.
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// TODO: support primary keys, collation, and comment
	return nil
}

// DropTable implements sql.TableDropper.
func (d *Database) DropTable(ctx *sql.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.engine.Exec(fmt.Sprintf(`USE %s; DROP TABLE "%s"`, d.name, name))
	return err
}

// RenameTable implements sql.TableRenamer.
func (d *Database) RenameTable(ctx *sql.Context, oldName string, newName string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.engine.Exec(fmt.Sprintf(`USE %s; ALTER TABLE "%s" RENAME TO "%s"`, d.name, oldName, newName))
	return err
}
