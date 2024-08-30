// Copyright 2024 ApeCloud, Inc.

// Copyright 2020-2021 Dolthub, Inc.
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

package meta

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/dolthub/vitess/go/mysql"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
)

const testNumPartitions = 5

type IndexDriverInitializer func([]sql.Database) sql.IndexDriver

type MetaHarness struct {
	name                      string
	parallelism               int
	numTablePartitions        int
	readonly                  bool
	provider                  sql.DatabaseProvider
	indexDriverInitializer    IndexDriverInitializer
	driver                    sql.IndexDriver
	nativeIndexSupport        bool
	skippedQueries            map[string]struct{}
	session                   sql.Session
	retainSession             bool
	setupData                 []setup.SetupScript
	externalProcedureRegistry sql.ExternalStoredProcedureRegistry
	server                    bool
	mu                        *sync.Mutex
}

var _ enginetest.Harness = (*MetaHarness)(nil)
var _ enginetest.IndexDriverHarness = (*MetaHarness)(nil)
var _ enginetest.IndexHarness = (*MetaHarness)(nil)
var _ enginetest.ForeignKeyHarness = (*MetaHarness)(nil)
var _ enginetest.KeylessTableHarness = (*MetaHarness)(nil)
var _ enginetest.ClientHarness = (*MetaHarness)(nil)
var _ enginetest.ServerHarness = (*MetaHarness)(nil)
var _ sql.ExternalStoredProcedureProvider = (*MetaHarness)(nil)

func NewMetaHarness(name string, parallelism int, numTablePartitions int, useNativeIndexes bool, driverInitializer IndexDriverInitializer) *MetaHarness {
	externalProcedureRegistry := sql.NewExternalStoredProcedureRegistry()
	for _, esp := range memory.ExternalStoredProcedures {
		externalProcedureRegistry.Register(esp)
	}

	var useServer bool
	if _, ok := os.LookupEnv("SERVER_ENGINE_TEST"); ok {
		useServer = true
	}

	return &MetaHarness{
		name:                      name,
		numTablePartitions:        numTablePartitions,
		indexDriverInitializer:    driverInitializer,
		parallelism:               parallelism,
		nativeIndexSupport:        useNativeIndexes,
		skippedQueries:            make(map[string]struct{}),
		externalProcedureRegistry: externalProcedureRegistry,
		mu:                        &sync.Mutex{},
		server:                    useServer,
	}
}

func NewDefaultMetaHarness() *MetaHarness {
	return NewMetaHarness("default", 1, testNumPartitions, true, nil)
}

func NewReadOnlyMetaHarness() *MetaHarness {
	h := NewDefaultMetaHarness()
	h.readonly = true
	return h
}

func (m *MetaHarness) SessionBuilder() server.SessionBuilder {
	return func(ctx context.Context, c *mysql.Conn, addr string) (sql.Session, error) {
		host := ""
		user := ""
		mysqlConnectionUser, ok := c.UserData.(sql.MysqlConnectionUser)
		if ok {
			host = mysqlConnectionUser.Host
			user = mysqlConnectionUser.User
		}
		client := sql.Client{Address: host, User: user, Capabilities: c.Capabilities}
		baseSession := sql.NewBaseSessionWithClientServer(addr, client, c.ConnectionID)
		return memory.NewSession(baseSession, m.getProvider()), nil
	}
}

// ExternalStoredProcedure implements the sql.ExternalStoredProcedureProvider interface
func (m *MetaHarness) ExternalStoredProcedure(_ *sql.Context, name string, numOfParams int) (*sql.ExternalStoredProcedureDetails, error) {
	return m.externalProcedureRegistry.LookupByNameAndParamCount(name, numOfParams)
}

// ExternalStoredProcedures implements the sql.ExternalStoredProcedureProvider interface
func (m *MetaHarness) ExternalStoredProcedures(_ *sql.Context, name string) ([]sql.ExternalStoredProcedureDetails, error) {
	return m.externalProcedureRegistry.LookupByName(name)
}

func (m *MetaHarness) InitializeIndexDriver(dbs []sql.Database) {
	if m.indexDriverInitializer != nil {
		m.driver = m.indexDriverInitializer(dbs)
	}
}

func (m *MetaHarness) NewSession() *sql.Context {
	m.session = m.newSession()
	return m.NewContext()
}

func (m *MetaHarness) SkipQueryTest(query string) bool {
	_, ok := m.skippedQueries[strings.ToLower(query)]
	return ok
}

func (m *MetaHarness) QueriesToSkip(queries ...string) {
	for _, query := range queries {
		m.skippedQueries[strings.ToLower(query)] = struct{}{}
	}
}

func (m *MetaHarness) UseServer() {
	m.server = true
}

func (m *MetaHarness) IsUsingServer() bool {
	return m.server
}

type SkippingMetaHarness struct {
	MetaHarness
}

var _ enginetest.SkippingHarness = (*SkippingMetaHarness)(nil)

func NewSkippingMetaHarness() *SkippingMetaHarness {
	return &SkippingMetaHarness{
		MetaHarness: *NewDefaultMetaHarness(),
	}
}

func (s SkippingMetaHarness) SkipQueryTest(query string) bool {
	return true
}

func (m *MetaHarness) Setup(setupData ...[]setup.SetupScript) {
	m.setupData = nil
	for i := range setupData {
		m.setupData = append(m.setupData, setupData[i]...)
	}
}

func (m *MetaHarness) NewEngine(t *testing.T) (enginetest.QueryEngine, error) {
	if !m.retainSession {
		m.session = nil
		m.provider = nil
	}
	engine, err := enginetest.NewEngine(t, m, m.getProvider(), m.setupData, memory.NewStatsProv())
	if err != nil {
		return nil, err
	}

	if m.server {
		return enginetest.NewServerQueryEngine(t, engine, m.SessionBuilder())
	}

	return engine, nil
}

func (m *MetaHarness) SupportsNativeIndexCreation() bool {
	return m.nativeIndexSupport
}

func (m *MetaHarness) SupportsForeignKeys() bool {
	return true
}

func (m *MetaHarness) SupportsKeylessTables() bool {
	return true
}

func (m *MetaHarness) Parallelism() int {
	return m.parallelism
}

func (m *MetaHarness) NewContext() *sql.Context {
	if m.session == nil {
		m.session = m.newSession()
	}

	return sql.NewContext(
		context.Background(),
		sql.WithSession(m.session),
	)
}

func (m *MetaHarness) newSession() *memory.Session {
	baseSession := enginetest.NewBaseSession()
	session := memory.NewSession(baseSession, m.getProvider())
	if m.driver != nil {
		session.GetIndexRegistry().RegisterIndexDriver(m.driver)
	}
	return session
}

func (m *MetaHarness) NewContextWithClient(client sql.Client) *sql.Context {
	baseSession := sql.NewBaseSessionWithClientServer("address", client, 1)

	return sql.NewContext(
		context.Background(),
		sql.WithSession(memory.NewSession(baseSession, m.getProvider())),
	)
}

func (m *MetaHarness) IndexDriver(dbs []sql.Database) sql.IndexDriver {
	if m.indexDriverInitializer != nil {
		return m.indexDriverInitializer(dbs)
	}
	return nil
}

func (m *MetaHarness) WithProvider(provider sql.DatabaseProvider) *MetaHarness {
	ret := *m
	ret.provider = provider
	return &ret
}

func (m *MetaHarness) getProvider() sql.DatabaseProvider {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.provider == nil {
		m.provider = m.NewDatabaseProvider().(*DbProvider)
	}

	return m.provider
}

func (m *MetaHarness) NewDatabaseProvider() sql.MutableDatabaseProvider {
	return NewInMemoryDBProvider()
}

func (m *MetaHarness) Provider() *DbProvider {
	return m.getProvider().(*DbProvider)
}

func (m *MetaHarness) ValidateEngine(ctx *sql.Context, e *sqle.Engine) error {
	return sanityCheckEngine(ctx, e)
}

func sanityCheckEngine(ctx *sql.Context, e *sqle.Engine) (err error) {
	for _, db := range e.Analyzer.Catalog.AllDatabases(ctx) {
		if err = sanityCheckDatabase(ctx, db); err != nil {
			return err
		}
	}
	return
}

func sanityCheckDatabase(ctx *sql.Context, db sql.Database) error {
	names, err := db.GetTableNames(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		t, ok, err := db.GetTableInsensitive(ctx, name)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("expected to find table %s", name)
		}
		if t.Name() != name {
			return fmt.Errorf("unexpected table name (%s !=  %s)", name, t.Name())
		}
	}
	return nil
}
