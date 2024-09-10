package catalog

import (
	stdsql "database/sql"

	"github.com/dolthub/go-mysql-server/sql"
)

type rowInserter struct {
	tempTableName string
	conn          *stdsql.Conn
}

var _ sql.RowInserter = &rowInserter{}

func (ri *rowInserter) StatementBegin(ctx *sql.Context) {

}

func (ri *rowInserter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {

}

func (ri *rowInserter) StatementComplete(ctx *sql.Context) error {
	return nil
}

func (ri *rowInserter) Close(ctx *sql.Context) error {

}

func (ri *rowInserter) Insert(ctx *sql.Context, row sql.Row) error {
}
