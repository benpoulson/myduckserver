package replica

import (
	"sync"

	"github.com/apecloud/myduckserver/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql"
)

type DeltaController struct {
	mutex  sync.Mutex
	tables map[tableIdentifier]*deltaAppender
}

func (c *DeltaController) GetDeltaAppender(
	databaseName, tableName string,
	schema sql.Schema,
) (binlogreplication.DeltaAppender, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.tables == nil {
		c.tables = make(map[tableIdentifier]*deltaAppender)
	}

	id := tableIdentifier{databaseName, tableName}
	appender, ok := c.tables[id]
	if ok {
		return appender, nil
	}
	appender, err := newDeltaAppender(schema)
	if err != nil {
		return nil, err
	}
	c.tables[id] = appender
	return appender, nil
}
