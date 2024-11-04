package pgserver

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/cockroachdb/cockroachdb-parser/pkg/sql/sem/tree"
	"github.com/dolthub/go-mysql-server/sql"
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
	// Create the FIFO pipe
	pipeDir := filepath.Join("/tmp", "pipes", "load-data")
	if err := os.MkdirAll(pipeDir, 0755); err != nil {
		return nil, err
	}
	pipeName := strconv.Itoa(int(ctx.ID())) + ".pipe"
	pipePath := filepath.Join(pipeDir, pipeName)
	if err := syscall.Mkfifo(pipePath, 0600); err != nil {
		return nil, err
	}

	pipe, err := os.OpenFile(pipePath, os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	loader := &CsvDataLoader{
		ctx:      ctx,
		table:    table,
		options:  options,
		pipePath: pipePath,
		pipe:     pipe,
	}

	// Start the INSERT execution in a separate goroutine.
	go func() {
		// Build the DuckDB INSERT INTO statement.
		var b strings.Builder
		b.Grow(256)

		b.WriteString("INSERT INTO ")
		b.WriteString(loader.table.Name())
		b.WriteString(" FROM read_csv('")
		b.WriteString(loader.pipePath)
		b.WriteString("', auto_detect = false, header = false, null_padding = true")

		if loader.options.Delimiter != nil {
			b.WriteString(", sep = ")
			b.WriteString(singleQuotedDuckChar(loader.options.Delimiter.String()))
		}

		if loader.options.Quote != nil {
			b.WriteString(", quote = ")
			b.WriteString(singleQuotedDuckChar(loader.options.Quote.RawString()))
		}

		if loader.options.Escape != nil {
			b.WriteString(", escape = ")
			b.WriteString(singleQuotedDuckChar(loader.options.Escape.RawString()))
		}

		b.WriteString(")")

		// Execute the DuckDB INSERT INTO statement.
		duckSQL := b.String()
		ctx.GetLogger().Trace(duckSQL)

		result, err := adapter.Exec(ctx, duckSQL)
		if err != nil {
			ctx.GetLogger().Error(err)
			return
		}

		_, err = result.RowsAffected()
		if err != nil {
			ctx.GetLogger().Error(err)
			return
		}
	}()

	return loader, nil
}

type CsvDataLoader struct {
	ctx      *sql.Context
	table    sql.InsertableTable
	options  *tree.CopyOptions
	pipePath string
	pipe     *os.File
}

func (loader *CsvDataLoader) LoadChunk(ctx *sql.Context, data *bufio.Reader) error {
	// Write the data to the FIFO pipe.
	_, err := io.Copy(loader.pipe, data)
	return err
}

func (loader *CsvDataLoader) Abort(ctx *sql.Context) error {
	loader.pipe.Close()
	return os.Remove(loader.pipePath)
}

func (loader *CsvDataLoader) Finish(ctx *sql.Context) (*LoadDataResults, error) {
	loader.pipe.Close()
	// Since the INSERT is done in NewCsvDataLoader, we just need to return the results here.
	return &LoadDataResults{
		RowsLoaded: 0, // This should be updated to reflect the actual rows loaded.
	}, nil
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
