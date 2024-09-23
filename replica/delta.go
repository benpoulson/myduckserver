package replica

import (
	"sync"

	"github.com/apecloud/myduckserver/myarrow"
	"github.com/dolthub/go-mysql-server/sql"
)

type tableIdentifier struct {
	databaseName, tableName string
}

type DeltaController struct {
	mutex  sync.Mutex
	tables map[tableIdentifier]*myarrow.ArrowAppender
}

func (c *DeltaController) GetDeltaAppender(
	databaseName, tableName string,
	schema sql.Schema,
) (*myarrow.ArrowAppender, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.tables == nil {
		c.tables = make(map[tableIdentifier]*myarrow.ArrowAppender)
	}

	id := tableIdentifier{databaseName, tableName}
	appender, ok := c.tables[id]
	if ok {
		return appender, nil
	}

	// Create a new appender
	appender, err := myarrow.NewArrowAppender(schema)
	if err != nil {
		return nil, err
	}
	c.tables[id] = appender
	return appender, nil
}
