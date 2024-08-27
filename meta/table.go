package meta

import "github.com/dolthub/go-mysql-server/sql"

type Table struct {
	name string
	db   *Database
}

var _ sql.Table = (*Table)(nil)
var _ sql.AlterableTable = (*Table)(nil)

func NewTable(name string, db *Database) *Table {
	return &Table{name: name, db: db}
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
func (t *Table) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	panic("unimplemented")
}

// Partitions implements sql.Table.
func (t *Table) Partitions(*sql.Context) (sql.PartitionIter, error) {
	panic("unimplemented")
}

// Schema implements sql.Table.
func (t *Table) Schema() sql.Schema {
	panic("unimplemented")
}

// String implements sql.Table.
func (t *Table) String() string {
	return t.name
}

// AddColumn implements sql.AlterableTable.
func (t *Table) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	panic("unimplemented")
}

// DropColumn implements sql.AlterableTable.
func (t *Table) DropColumn(ctx *sql.Context, columnName string) error {
	panic("unimplemented")
}

// ModifyColumn implements sql.AlterableTable.
func (t *Table) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	panic("unimplemented")
}

// Updater implements sql.AlterableTable.
func (t *Table) Updater(ctx *sql.Context) sql.RowUpdater {
	panic("unimplemented")
}
