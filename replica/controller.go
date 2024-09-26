package replica

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"fmt"
	"sync"
	"unsafe"

	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apecloud/myduckserver/backend"
	"github.com/apecloud/myduckserver/binlogreplication"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
)

type DeltaController struct {
	mutex  sync.Mutex
	tables map[tableIdentifier]*deltaAppender
	pool   *backend.ConnectionPool
}

func (c *DeltaController) GetDeltaAppender(
	databaseName, tableName string,
	schema sql.Schema,
) (binlogreplication.DeltaAppender, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.tables == nil {
		c.tables = make(map[tableIdentifier]*deltaAppender)
	}

	id := tableIdentifier{databaseName, tableName}
	appender, ok := c.tables[id]
	if ok {
		return appender, nil
	}
	appender, err := newDeltaAppender(schema)
	if err != nil {
		return nil, err
	}
	c.tables[id] = appender
	return appender, nil
}

// Flush writes the accumulated changes to the database.
// TODO(fan): We have to block all other operations to ensure the ACID of the flush.
func (c *DeltaController) Flush(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Due to DuckDB's limitations of indexes, specifically over-eagerly unique constraint checking,
	// we have to split the flush into two transactions:
	// 1. Delete rows that are being updated.
	// 2. Insert new rows.
	//
	// Otherwise, we would get
	//   a unique constraint violation error for INSERT,
	//   or data corruption for INSERT OR REPLACE|IGNORE INTO.
	//
	// This is a BIG pitfall and seems unlikely to be fixed in DuckDB in the near future,
	// but we have to live with it.
	//
	// The consequence is, if the server crashes after the first transcation,
	// the database may be left in an inconsistent state.
	//
	// The ultimate solution is to wait for DuckDB to improve its index handling.
	// In the meantime, we could contribute a patch to DuckDB to support atomic MERGE INTO,
	// which is another way to avoid the issue elegantly.
	//
	// See:
	//  https://duckdb.org/docs/sql/indexes.html#limitations-of-art-indexes
	//  https://github.com/duckdb/duckdb/issues/14133

	records := make(map[tableIdentifier]arrow.Record, len(c.tables))
	for table, appender := range c.tables {
		records[table] = appender.Build()
	}
	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()

	buf := &bytes.Buffer{}

	return nil
}

func (c *DeltaController) delete(ctx context.Context, tables map[tableIdentifier]*deltaAppender, records map[tableIdentifier]arrow.Record, buf *bytes.Buffer) error {
	tx, err := c.pool.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for table, record := range records {
		appender := tables[table]
		if err := c.deleteFromTable(ctx, tx, table, appender.BaseSchema(), record, buf); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (c *DeltaController) insert(ctx context.Context, tables map[tableIdentifier]*deltaAppender, records map[tableIdentifier]arrow.Record, buf *bytes.Buffer) error {
	tx, err := c.pool.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for table, record := range records {
		appender := tables[table]
		if err := c.insertIntoTable(ctx, tx, table, appender.BaseSchema(), record, buf); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (c *DeltaController) deleteFromTable(
	ctx context.Context,
	tx *stdsql.Tx,
	table tableIdentifier,
	schema sql.Schema,
	record arrow.Record,
	buf *bytes.Buffer,
) error {
	buf.Reset()
	w := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()))
	if err := w.Write(record); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	bytes := buf.Bytes()
	size := len(bytes)
	ptr := unsafe.Pointer(&bytes[0])

	qualifiedTableName := catalog.ConnectIdentifiersANSI(table.dbName, table.tableName)

	pkColumns := make([]int, 0, 1) // Most tables have a single-column primary key
	for i, col := range schema {
		if col.PrimaryKey {
			pkColumns = append(pkColumns, i)
		}
	}
	pkList := catalog.QuoteIdentifierANSI(schema[pkColumns[0]].Name)
	for _, i := range pkColumns[1:] {
		pkList += ", " + catalog.QuoteIdentifierANSI(schema[i].Name)
	}

	ipcSQL := fmt.Sprintf(
		" FROM scan_arrow_ipc([{ptr: %d::ubigint, size: %d::ubigint}])",
		uintptr(ptr), size,
	)

	pkSQL := "SELECT " + pkList + ipcSQL

	// Delete rows that are being updated.
	//
	// For single-column primary key, the plan for `IN` is optimized to a SEMI JOIN,
	// which is more efficient than ordinary INNER JOIN.
	// DuckDB does not support multiple columns in `IN` clauses, so we need to handle this case separately.
	var deleteSQL string
	if len(pkColumns) == 1 {
		deleteSQL = "DELETE FROM " + qualifiedTableName + " WHERE " + pkList + " IN (" + pkSQL + ")"
	} else {
		deleteSQL = "DELETE FROM " + qualifiedTableName + " AS base USING (" + pkSQL + ") AS del WHERE "
		for i, pk := range pkColumns {
			if i > 0 {
				deleteSQL += " AND "
			}
			columnName := catalog.QuoteIdentifierANSI(baseSchema[pk].Name)
			deleteSQL += "base." + columnName + " = del." + columnName
		}
	}
	_, err := tx.ExecContext(ctx, deleteSQL)
	return err
}

func (c *DeltaController) insertIntoTable(
	ctx context.Context,
	tx *stdsql.Tx,
	table tableIdentifier,
	schema sql.Schema,
	record arrow.Record,
	buf *bytes.Buffer,
) error {
	buf.Reset()
	w := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()))
	if err := w.Write(record); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}

	bytes := buf.Bytes()
	size := len(bytes)
	ptr := unsafe.Pointer(&bytes[0])

	qualifiedTableName := catalog.ConnectIdentifiersANSI(table.dbName, table.tableName)

	pkColumns := make([]int, 0, 1) // Most tables have a single-column primary key
	for i, col := range schema {
		if col.PrimaryKey {
			pkColumns = append(pkColumns, i)
		}
	}
	pkList := catalog.QuoteIdentifierANSI(schema[pkColumns[0]].Name)
	for _, i := range pkColumns[1:] {
		pkList += ", " + catalog.QuoteIdentifierANSI(schema[i].Name)
	}

	ipcSQL := fmt.Sprintf(
		" FROM scan_arrow_ipc([{ptr: %d::ubigint, size: %d::ubigint}])",
		uintptr(ptr), size,
	)
	deltaSQL := "SELECT * EXCLUDE (txn_tag, txn_server)" + ipcSQL
}
