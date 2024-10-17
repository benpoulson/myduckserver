package backend

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/apecloud/myduckserver/adapter"
	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func isRewritableLoadData(node *plan.LoadData) bool {
	fmt.Printf("%#v\n", node)
	return len(node.FieldsTerminatedBy) == 1 &&
		len(node.FieldsEnclosedBy) <= 1 &&
		len(node.FieldsEscapedBy) <= 1 &&
		len(node.LinesStartingBy) == 0 &&
		len(node.LinesTerminatedBy) <= 2 &&
		strings.Trim(node.LinesTerminatedBy, "\r\n") == "" &&
		areAllExpressionsNil(node.SetExprs) &&
		areAllExpressionsNil(node.UserVars) &&
		isSupportedFileCharacterSet(node.Charset)
}

func areAllExpressionsNil(exprs []sql.Expression) bool {
	for _, expr := range exprs {
		if expr != nil {
			return false
		}
	}
	return true
}

func isSupportedFileCharacterSet(charset string) bool {
	return len(charset) == 0 ||
		strings.HasPrefix(strings.ToLower(charset), "utf8") ||
		strings.EqualFold(charset, "ascii") ||
		strings.EqualFold(charset, "binary")
}

// buildLoadData translates a MySQL LOAD DATA statement
// into a DuckDB INSERT INTO statement and executes it.
func (db *DuckBuilder) buildLoadData(ctx *sql.Context, root sql.Node, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData) (sql.RowIter, error) {
	if load.Local {
		return db.buildLoadDataLocal(ctx, insert, dst, load)
	}
	return db.buildLoadDataNonLocal(ctx, insert, dst, load)
}

// Since the data is sent to the server in the form of a byte stream,
// we use a Unix pipe to stream the data to DuckDB.
func (db *DuckBuilder) buildLoadDataLocal(ctx *sql.Context, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData) (sql.RowIter, error) {
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
	pipePath := filepath.Join(pipeDir, strconv.Itoa(int(ctx.ID())))
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
func (db *DuckBuilder) buildLoadDataNonLocal(ctx *sql.Context, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData) (sql.RowIter, error) {
	_, secureFileDir, ok := sql.SystemVariables.GetGlobal("secure_file_priv")
	if !ok {
		return nil, fmt.Errorf("error: secure_file_priv variable was not found")
	}

	if err := isUnderSecureFileDir(secureFileDir, load.File); err != nil {
		return nil, sql.ErrLoadDataCannotOpen.New(err.Error())
	}
	return db.executeLoadData(ctx, insert, dst, load, load.File)
}

func (db *DuckBuilder) executeLoadData(ctx *sql.Context, insert *plan.InsertInto, dst sql.InsertableTable, load *plan.LoadData, filePath string) (sql.RowIter, error) {
	// Build the DuckDB INSERT INTO statement.
	var b strings.Builder
	b.Grow(128)

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

	b.WriteString(", sep = ")
	b.WriteString(fmt.Sprintf("%q", load.FieldsTerminatedBy[0]))

	if len(load.FieldsEnclosedBy) > 0 {
		b.WriteString(", quote = ")
		b.WriteString(fmt.Sprintf("%q", load.FieldsEnclosedBy))
	}
	if len(load.FieldsEscapedBy) > 0 {
		b.WriteString(", escape = ")
		b.WriteString(fmt.Sprintf("%q", load.FieldsEscapedBy))
	}
	b.WriteString(`, nullstr = ['\N', 'NULL']`)

	if load.IgnoreNum > 0 {
		b.WriteString(", skip = ")
		b.WriteString(strconv.FormatInt(load.IgnoreNum, 10))
	}

	b.WriteString(")")

	// Execute the DuckDB INSERT INTO statement.
	duckSQL := b.String()
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

// isUnderSecureFileDir ensures that fileStr is under secureFileDir or a subdirectory of secureFileDir, errors otherwise
// Copied from https://github.com/dolthub/go-mysql-server/blob/main/sql/rowexec/rel.go
func isUnderSecureFileDir(secureFileDir interface{}, fileStr string) error {
	if secureFileDir == nil || secureFileDir == "" {
		return nil
	}
	sStat, err := os.Stat(secureFileDir.(string))
	if err != nil {
		return err
	}
	fStat, err := os.Stat(filepath.Dir(fileStr))
	if err != nil {
		return err
	}
	if os.SameFile(sStat, fStat) {
		return nil
	}

	fileAbsPath, filePathErr := filepath.Abs(fileStr)
	if filePathErr != nil {
		return filePathErr
	}
	secureFileDirAbsPath, _ := filepath.Abs(secureFileDir.(string))
	if strings.HasPrefix(fileAbsPath, secureFileDirAbsPath) {
		return nil
	}
	return sql.ErrSecureFilePriv.New()
}
