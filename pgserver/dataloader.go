package pgserver

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// DataLoader allows callers to insert rows from multiple chunks into a table. Rows encoded in each chunk will not
// necessarily end cleanly on a chunk boundary, so DataLoader implementations must handle recognizing partial, or
// incomplete records, and saving that partial record until the next call to LoadChunk, so that it may be prefixed
// with the incomplete record.
type DataLoader interface {
	// LoadChunk reads the records from |data| and inserts them into the previously configured table. Data records
	// are not guaranteed to stard and end cleanly on chunk boundaries, so implementations must recognize incomplete
	// records and save them to prepend on the next processed chunk.
	LoadChunk(ctx *sql.Context, data *bufio.Reader) error

	// Abort aborts the current load operation and releases all used resources.
	Abort(ctx *sql.Context) error

	// Finish finalizes the current load operation and commits the inserted rows so that the data becomes visibile
	// to clients. Implementations should check that the last call to LoadChunk did not end with an incomplete
	// record and return an error to the caller if so. The returned LoadDataResults describe the load operation,
	// including how many rows were inserted.
	Finish(ctx *sql.Context) (*LoadDataResults, error)
}

// LoadDataResults contains the results of a load data operation, including the number of rows loaded.
type LoadDataResults struct {
	// RowsLoaded contains the total number of rows inserted during a load data operation.
	RowsLoaded int32
}

func NewCsvDataLoader(ctx *sql.Context, table sql.InsertableTable, options *tree.CopyOptions) (DataLoader, error) {
	return nil, nil
}

// buildLoadData translates a MySQL LOAD DATA statement
// into a DuckDB INSERT INTO statement and executes it.
func (h *ConnectionHandler) buildLoadData(ctx *sql.Context, root sql.Node, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData) (sql.RowIter, error) {
	if load.Local {
		return db.buildClientSideLoadData(ctx, insert, dst, load)
	}
	return db.buildServerSideLoadData(ctx, insert, dst, load)
}

// Since the data is sent to the server in the form of a byte stream,
// we use a Unix pipe to stream the data to DuckDB.
func (h *ConnectionHandler) buildClientSideLoadData(ctx *sql.Context, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData) (sql.RowIter, error) {
	_, localInfile, ok := sql.SystemVariables.GetGlobal("local_infile")
	if !ok {
		return nil, fmt.Errorf("error: local_infile variable was not found")
	}

	if localInfile.(int8) == 0 {
		return nil, fmt.Errorf("local_infile needs to be set to 1 to use LOCAL")
	}

	reader, err := ctx.LoadInfile(load.File)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Create the FIFO pipe
	pipeDir := filepath.Join(db.provider.DataDir(), "pipes", "load-data")
	if err := os.MkdirAll(pipeDir, 0755); err != nil {
		return nil, err
	}
	pipeName := strconv.Itoa(int(ctx.ID())) + ".pipe"
	pipePath := filepath.Join(pipeDir, pipeName)
	if err := syscall.Mkfifo(pipePath, 0600); err != nil {
		return nil, err
	}
	defer os.Remove(pipePath)

	// Write the data to the FIFO pipe.
	go func() {
		pipe, err := os.OpenFile(pipePath, os.O_WRONLY, 0600)
		if err != nil {
			return
		}
		defer pipe.Close()
		io.Copy(pipe, reader)
	}()

	return db.executeLoadData(ctx, insert, dst, load, pipePath)
}

// In the non-local case, we can directly use the file path to read the data.
func (h *ConnectionHandler) buildServerSideLoadData(ctx *sql.Context, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData) (sql.RowIter, error) {
	return db.executeLoadData(ctx, insert, dst, load, load.File)
}

func (h *ConnectionHandler) executeLoadData(ctx *sql.Context, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData, filePath string) (sql.RowIter, error) {
	// Build the DuckDB INSERT INTO statement.
	var b strings.Builder
	b.Grow(256)

	keyless := sql.IsKeyless(dst.Schema())
	b.WriteString("INSERT")
	if load.IsIgnore && !keyless {
		b.WriteString(" OR IGNORE")
	} else if load.IsReplace && !keyless {
		b.WriteString(" OR REPLACE")
	}
	b.WriteString(" INTO ")

	qualifiedTableName := catalog.ConnectIdentifiersANSI(insert.Database().Name(), dst.Name())
	b.WriteString(qualifiedTableName)

	if len(load.ColNames) > 0 {
		b.WriteString(" (")
		b.WriteString(catalog.QuoteIdentifierANSI(load.ColNames[0]))
		for _, col := range load.ColNames[1:] {
			b.WriteString(", ")
			b.WriteString(catalog.QuoteIdentifierANSI(col))
		}
		b.WriteString(")")
	}

	b.WriteString(" FROM ")
	b.WriteString("read_csv('")
	b.WriteString(filePath)
	b.WriteString("'")

	b.WriteString(", auto_detect = false")
	b.WriteString(", header = false")
	b.WriteString(", null_padding = true")

	b.WriteString(", new_line = ")
	if len(load.LinesTerminatedBy) == 1 {
		b.WriteString(singleQuotedDuckChar(load.LinesTerminatedBy))
	} else {
		b.WriteString(`'\r\n'`)
	}

	b.WriteString(", sep = ")
	b.WriteString(singleQuotedDuckChar(load.FieldsTerminatedBy))

	b.WriteString(", quote = ")
	b.WriteString(singleQuotedDuckChar(load.FieldsEnclosedBy))

	// TODO(fan): DuckDB does not support the `\` escape mode of MySQL yet.
	if load.FieldsEscapedBy == `\` {
		b.WriteString(`, escape = ''`)
	} else {
		b.WriteString(", escape = ")
		b.WriteString(singleQuotedDuckChar(load.FieldsEscapedBy))
	}

	// > If FIELDS ENCLOSED BY is not empty, a field containing
	// > the literal word NULL as its value is read as a NULL value.
	// > If FIELDS ESCAPED BY is empty, NULL is written as the word NULL.
	b.WriteString(", allow_quoted_nulls = false, nullstr = ")
	if len(load.FieldsEnclosedBy) > 0 || len(load.FieldsEscapedBy) == 0 {
		b.WriteString(`'NULL'`)
	} else {
		b.WriteString(`'\N'`)
	}

	if load.IgnoreNum > 0 {
		b.WriteString(", skip = ")
		b.WriteString(strconv.FormatInt(load.IgnoreNum, 10))
	}

	b.WriteString(", columns = ")
	if err := columnTypeHints(&b, dst, dst.Schema(), load.ColNames); err != nil {
		return nil, err
	}

	b.WriteString(")")

	// Execute the DuckDB INSERT INTO statement.
	duckSQL := b.String()
	ctx.GetLogger().Trace(duckSQL)

	result, err := adapter.Exec(ctx, duckSQL)
	if err != nil {
		return nil, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	insertId, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	return sql.RowsToRowIter(sql.NewRow(types.OkResult{
		RowsAffected: uint64(affected),
		InsertID:     uint64(insertId),
	})), nil
}

func singleQuotedDuckChar(s string) string {
	if len(s) == 0 {
		return `''`
	}
	r := []rune(s)[0]
	if r == '\\' {
		return `'\'` // Slash does not need to be escaped in DuckDB
	}
	return strconv.QuoteRune(r) // e.g., tab -> '\t'
}
