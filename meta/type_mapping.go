package meta

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// TODO: implement it
func duckdbDataType(t sql.Type) string {
	return t.String()
}

// TODO: implement it
func mysqlDateType(string) sql.Type {
	return types.StringType{}
}
