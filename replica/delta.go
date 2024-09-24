package replica

import (
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apecloud/myduckserver/binlogreplication"
	"github.com/apecloud/myduckserver/myarrow"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type tableIdentifier struct {
	databaseName, tableName string
}

type deltaAppender struct {
	schema   sql.Schema
	appender myarrow.ArrowAppender
}

var _ binlogreplication.DeltaAppender = &deltaAppender{}

// Create a new appender.
// Add action and GTID columns to the schema:
//
//	 https://mariadb.com/kb/en/gtid/
//		https://dev.mysql.com/doc/refman/9.0/en/replication-gtids-concepts.html
func newDeltaAppender(schema sql.Schema) (*deltaAppender, error) {
	augmented := make(sql.Schema, 0, len(schema)+5)
	augmented = append(augmented, &sql.Column{
		Name: "action",
		Type: types.Int8,
	}, &sql.Column{
		Name: "txn_domain",
		Type: types.Text,
	}, &sql.Column{
		Name: "txn_server",
		Type: types.Text,
	}, &sql.Column{
		Name: "txn_group",
		Type: types.Text,
	}, &sql.Column{
		Name: "txn_seq",
		Type: types.Uint64,
	})
	augmented = append(augmented, schema...)

	appender, err := myarrow.NewArrowAppender(augmented, 1, 2, 3)
	if err != nil {
		return nil, err
	}

	return &deltaAppender{
		schema:   augmented,
		appender: appender,
	}, nil
}

func (a *deltaAppender) Field(i int) array.Builder {
	return a.appender.Field(i + 5)
}

func (a *deltaAppender) Fields() []array.Builder {
	return a.appender.Fields()[5:]
}

func (a *deltaAppender) Schema() sql.Schema {
	return a.schema
}

func (a *deltaAppender) Action() *array.Int8Builder {
	return a.appender.Field(0).(*array.Int8Builder)
}

func (a *deltaAppender) TxnTag() *array.BinaryDictionaryBuilder {
	return a.appender.Field(1).(*array.BinaryDictionaryBuilder)
}

func (a *deltaAppender) TxnServer() *array.BinaryDictionaryBuilder {
	return a.appender.Field(2).(*array.BinaryDictionaryBuilder)
}

func (a *deltaAppender) TxnGroup() *array.BinaryDictionaryBuilder {
	return a.appender.Field(3).(*array.BinaryDictionaryBuilder)
}

func (a *deltaAppender) TxnSeqNumber() *array.Uint64Builder {
	return a.appender.Field(4).(*array.Uint64Builder)
}