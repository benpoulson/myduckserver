package adapter

import (
	stdsql "database/sql"

	"github.com/dolthub/go-mysql-server/sql"
)

type ConnHolder interface {
	GetConn(ctx *sql.Context) (*stdsql.Conn, error)
}
