package meta

import (
	"fmt"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

type Table struct {
	mu   *sync.RWMutex
	name string
	db   *Database
}

var _ sql.Table = (*Table)(nil)
var _ sql.AlterableTable = (*Table)(nil)

func NewTable(name string, db *Database) *Table {
	return &Table{
		mu:   &sync.RWMutex{},
		name: name,
		db:   db}
}

// Collation implements sql.Table.
func (t *Table) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Name implements sql.Table.
func (t *Table) Name() string {
	return t.name
}

// PartitionRows implements sql.Table.
func (t *Table) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return nil, fmt.Errorf("unimplemented (table: %s, query: %s)", t.name, ctx.Query())
}

// Partitions implements sql.Table.
func (t *Table) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return nil, fmt.Errorf("unimplemented (table: %s, query: %s)", t.name, ctx.Query())
}

// Schema implements sql.Table.
func (t *Table) Schema() sql.Schema {
	t.mu.RLock()
	defer t.mu.RUnlock()

	rows, err := t.db.engine.Query(`
		SELECT column_name, data_type, is_nullable FROM duckdb_columns() WHERE schema_name = ? AND table_name = ?
	`, t.db.name, t.name)
	if err != nil {
		panic(ErrDuckDB.New(err))
	}
	defer rows.Close()

	var schema sql.Schema
	for rows.Next() {
		var columnName, dataType string
		var isNullable bool
		if err := rows.Scan(&columnName, &dataType, &isNullable); err != nil {
			panic(ErrDuckDB.New(err))
		}

		column := &sql.Column{
			Name:     columnName,
			Type:     mysqlDateType(dataType),
			Nullable: isNullable,
		}

		schema = append(schema, column)
	}

	if err := rows.Err(); err != nil {
		panic(ErrDuckDB.New(err))
	}

	return schema
}

// String implements sql.Table.
func (t *Table) String() string {
	return t.name
}

// AddColumn implements sql.AlterableTable.
func (t *Table) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
		t.name,
		column.Name,
		duckdbDataType(column.Type))

	if !column.Nullable {
		sql += " NOT NULL"
	}

	if column.Default != nil {
		sql += fmt.Sprintf(" DEFAULT %s", column.Default.String())
	}

	_, err := t.db.engine.Exec(sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// DropColumn implements sql.AlterableTable.
func (t *Table) DropColumn(ctx *sql.Context, columnName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	sql := fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", t.name, columnName)

	_, err := t.db.engine.Exec(sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	return nil
}

// ModifyColumn implements sql.AlterableTable.
func (t *Table) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	sql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s", t.name, columnName)

	sql += fmt.Sprintf(" SET DATA TYPE %s", duckdbDataType(column.Type))

	if column.Nullable {
		sql += " DROP NOT NULL"
	} else {
		sql += " SET NOT NULL"
	}

	if column.Default != nil {
		sql += fmt.Sprintf(" SET DEFAULT %s", column.Default.String())
	} else {
		sql += " DROP DEFAULT"
	}

	_, err := t.db.engine.Exec(sql)
	if err != nil {
		return ErrDuckDB.New(err)
	}

	if columnName != column.Name {
		renameSQL := fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", t.name, columnName, column.Name)
		_, err = t.db.engine.Exec(renameSQL)
		if err != nil {
			return ErrDuckDB.New(err)
		}
	}

	return nil
}

// Updater implements sql.AlterableTable.
func (t *Table) Updater(ctx *sql.Context) sql.RowUpdater {
	panic(fmt.Sprintf("unimplemented (table: %s, query: %s)", t.name, ctx.Query()))
}
