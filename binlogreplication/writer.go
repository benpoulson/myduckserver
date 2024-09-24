package binlogreplication

import (
	"github.com/apache/arrow/go/v17/arrow/array"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

type EventType int8

const (
	// IMPORTANT: The order of these values is important.
	// We translate UPDATE to DELETE + INSERT, so DELETE should come first.
	DeleteEvent EventType = iota
	UpdateEvent
	InsertEvent
)

func (e EventType) String() string {
	switch e {
	case DeleteEvent:
		return "DELETE"
	case UpdateEvent:
		return "UPDATE"
	case InsertEvent:
		return "INSERT"
	default:
		return "UNKNOWN"
	}
}

type TableWriter interface {
	Insert(ctx *sql.Context, keyRows []sql.Row) error
	Delete(ctx *sql.Context, keyRows []sql.Row) error
	Update(ctx *sql.Context, keyRows []sql.Row, valueRows []sql.Row) error
	Close() error
}

type DeltaAppender interface {
	Field(i int) array.Builder
	Fields() []array.Builder
	Action() *array.Int8Builder
	TxnTag() *array.BinaryDictionaryBuilder
	TxnServer() *array.BinaryDictionaryBuilder
	TxnGroup() *array.BinaryDictionaryBuilder
	TxnSeqNumber() *array.Uint64Builder
}

type TableWriterProvider interface {
	// GetTableWriter returns a TableWriter for writing to the specified |table| in the specified |database|.
	GetTableWriter(
		ctx *sql.Context, engine *sqle.Engine,
		databaseName, tableName string,
		schema sql.Schema,
		columnCount, rowCount int,
		identifyColumns, dataColumns mysql.Bitmap,
		eventType EventType,
		foreignKeyChecksDisabled bool,
	) (TableWriter, error)

	// GetDeltaAppender returns an ArrowAppender for appending updates to the specified |table| in the specified |database|.
	GetDeltaAppender(
		ctx *sql.Context, engine *sqle.Engine,
		databaseName, tableName string,
		schema sql.Schema,
	) (DeltaAppender, error)
}
