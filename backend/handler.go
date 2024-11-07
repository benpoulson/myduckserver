// Copyright 2024-2025 ApeCloud, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"context"
	"fmt"
	"regexp"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
)

type MyHandler struct {
	*server.Handler
	pool *ConnectionPool
}

// Precompile regex for performance
var autoIncrementRegex = regexp.MustCompile(`(?i)AUTO_INCREMENT=\d+`)
var showSlaveStatusRegex = regexp.MustCompile(`(?i)^show\s+slave\s+status\s*;?$`)

// ResultModifier is a function type that transforms a Result
type ResultModifier func(*sqltypes.Result) *sqltypes.Result

func (h *MyHandler) ConnectionClosed(c *mysql.Conn) {
	h.pool.CloseConn(c.ConnectionID)
	h.Handler.ConnectionClosed(c)
}

func (h *MyHandler) ComInitDB(c *mysql.Conn, schemaName string) error {
	_, err := h.pool.GetConnForSchema(context.Background(), c.ConnectionID, schemaName)
	if err != nil {
		return err
	}
	return h.Handler.ComInitDB(c, schemaName)
}

func wrapResultCallback(callback mysql.ResultSpoolFn, modifiers ...ResultModifier) mysql.ResultSpoolFn {
	return func(res *sqltypes.Result, more bool) error {
		// Apply all modifiers in sequence
		result := res
		for _, modifier := range modifiers {
			result = modifier(result)
		}
		return callback(result, more)
	}
}

// replaceFieldNames modifies field names to maintain compatibility with older MySQL clients
// by replacing "Replica_" with "Slave_" and "Source" with "Master"
func replaceShowSlaveStatusFieldNames(result *sqltypes.Result) *sqltypes.Result {
	if result == nil || result.Fields == nil {
		return result
	}

	for i, field := range result.Fields {
		name := field.Name
		// Replace any "Replica_" with "Slave_"
		if regexp.MustCompile(`^Replica_`).MatchString(name) {
			result.Fields[i].Name = regexp.MustCompile(`^Replica_`).ReplaceAllString(name, "Slave_")
		}
		// Replace any "Source" with "Master"
		if regexp.MustCompile(`Source`).MatchString(name) {
			result.Fields[i].Name = regexp.MustCompile(`Source`).ReplaceAllString(name, "Master")
		}
	}
	return result
}

func (h *MyHandler) ComMultiQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	callback mysql.ResultSpoolFn,
) (string, error) {
	modifiers := []ResultModifier{}
	query = autoIncrementRegex.ReplaceAllString(query, "")
	if showSlaveStatusRegex.MatchString(query) {
		modifiers = append(modifiers, replaceShowSlaveStatusFieldNames)
		query = showSlaveStatusRegex.ReplaceAllString(query, "SHOW REPLICA STATUS;")
	}

	return h.Handler.ComMultiQuery(ctx, c, query, wrapResultCallback(callback, modifiers...))
}

// Naive query rewriting. This is just a temporary solution
// and should be replaced with a more robust implementation.
func (h *MyHandler) ComQuery(
	ctx context.Context,
	c *mysql.Conn,
	query string,
	callback mysql.ResultSpoolFn,
) error {
	modifiers := []ResultModifier{}
	query = autoIncrementRegex.ReplaceAllString(query, "")
	if showSlaveStatusRegex.MatchString(query) {
		modifiers = append(modifiers, replaceShowSlaveStatusFieldNames)
		query = showSlaveStatusRegex.ReplaceAllString(query, "SHOW REPLICA STATUS;")
	}

	return h.Handler.ComQuery(ctx, c, query, wrapResultCallback(callback, modifiers...))
}

func WrapHandler(pool *ConnectionPool) server.HandlerWrapper {
	return func(h mysql.Handler) (mysql.Handler, error) {
		handler, ok := h.(*server.Handler)
		if !ok {
			return nil, fmt.Errorf("expected *server.Handler, got %T", h)
		}

		return &MyHandler{
			Handler: handler,
			pool:    pool,
		}, nil
	}
}
