package adapter

import (
	"context"
	stdsql "database/sql"

	"github.com/dolthub/go-mysql-server/sql"
)

type ConnectionHolder interface {
	GetConn(ctx context.Context) (*stdsql.Conn, error)
}

func GetConn(ctx *sql.Context) (*stdsql.Conn, error) {
	return ctx.Session.(ConnectionHolder).GetConn(ctx)
}

func QueryContext(ctx *sql.Context, query string, args ...interface{}) (*stdsql.Rows, error) {
	conn, err := GetConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.QueryContext(ctx, query, args...)
}

func ExecContext(ctx *sql.Context, query string, args ...interface{}) (stdsql.Result, error) {
	conn, err := GetConn(ctx)
	if err != nil {
		return nil, err
	}
	return conn.ExecContext(ctx, query, args...)
}
