// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package backend

import (
	"context"
	stdsql "database/sql"
	"sync"

	"github.com/apecloud/myduckserver/catalog"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/sirupsen/logrus"
)

type ConnectionPool struct {
	*stdsql.DB
	catalog string
	conns   sync.Map // map[uint32]*stdsql.Conn, but sync.Map is concurrent-safe
}

func NewConnectionPool(catalog string, db *stdsql.DB) *ConnectionPool {
	return &ConnectionPool{
		DB:      db,
		catalog: catalog,
	}
}

func (p *ConnectionPool) GetConn(ctx context.Context, id uint32) (*stdsql.Conn, error) {
	var conn *stdsql.Conn
	entry, ok := p.conns.Load(id)
	if !ok {
		c, err := p.DB.Conn(ctx)
		if err != nil {
			return nil, err
		}
		p.conns.Store(id, c)
		conn = c
	} else {
		conn = entry.(*stdsql.Conn)
	}
	return conn, nil
}

func (p *ConnectionPool) GetConnForSchema(ctx context.Context, id uint32, schemaName string) (*stdsql.Conn, error) {
	conn, err := p.GetConn(ctx, id)
	if err != nil {
		return nil, err
	}

	if schemaName != "" {
		var currentSchema string
		if err := conn.QueryRowContext(context.Background(), "SELECT CURRENT_SCHEMA()").Scan(&currentSchema); err != nil {
			logrus.WithError(err).Error("Failed to get current schema")
			return nil, err
		} else if currentSchema != schemaName {
			if _, err := conn.ExecContext(context.Background(), "USE "+catalog.FullSchemaName(p.catalog, schemaName)); err != nil {
				if catalog.IsDuckDBSetSchemaNotFoundError(err) {
					return nil, sql.ErrDatabaseNotFound.New(schemaName)
				}
				logrus.WithField("schema", schemaName).WithError(err).Error("Failed to switch schema")
				return nil, err
			}
		}
	}

	return conn, nil
}

func (p *ConnectionPool) CloseConn(id uint32) error {
	entry, ok := p.conns.Load(id)
	if ok {
		conn := entry.(*stdsql.Conn)
		if err := conn.Close(); err != nil {
			logrus.WithError(err).Warn("Failed to close connection")
			return err
		}
		p.conns.Delete(id)
	}
	return nil
}
