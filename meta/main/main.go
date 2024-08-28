package main

import (
	"context"
	"fmt"

	"github.com/apecloud/myduckserver/meta"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
)

const (
	address = "localhost"
	port    = 3306
)

// NewSessionBuilder returns a session for the given in-memory database provider suitable to use in a test server
// This can't be defined as server.SessionBuilder because importing it would create a circular dependency,
// but it's the same signature.
func NewSessionBuilder(pro *meta.DbProvider) func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		host := ""
		user := ""
		mysqlConnectionUser, ok := conn.UserData.(sql.MysqlConnectionUser)
		if ok {
			host = mysqlConnectionUser.Host
			user = mysqlConnectionUser.User
		}

		client := sql.Client{Address: host, User: user, Capabilities: conn.Capabilities}
		baseSession := sql.NewBaseSessionWithClientServer(addr, client, conn.ConnectionID)
		return baseSession, nil
	}
}

func main() {
	provider := meta.NewDBProvider("mysql")
	engine := sqle.NewDefault(provider)

	session := sql.NewBaseSession()
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase("mysql")

	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("%s:%d", address, port),
	}
	s, err := server.NewServer(config, engine, NewSessionBuilder(provider), nil)
	if err != nil {
		panic(err)
	}
	if err = s.Start(); err != nil {
		panic(err)
	}

}
