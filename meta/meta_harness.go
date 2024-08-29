package meta

import (
	"context"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
)

type MetaHarness struct {
	name        string
	parallelism int
	session     sql.Session
	setupData   []setup.SetupScript
}

var _ enginetest.Harness = (*MetaHarness)(nil)

func NewMetaHarness(name string, parallelism int) *MetaHarness {
	return &MetaHarness{name: name, parallelism: parallelism}
}

func NewDefaultMetaHarness() *MetaHarness {
	return NewMetaHarness("default", 1)
}

// NewContext implements enginetest.Harness.
func (m *MetaHarness) NewContext() *sql.Context {
	if m.session == nil {
		m.session = m.newSession()
	}
	return sql.NewContext(context.Background(), sql.WithSession(m.session))
}

func (m *MetaHarness) newSession() *sql.BaseSession {
	return sql.NewBaseSession()
}

// NewEngine implements enginetest.Harness.
func (m *MetaHarness) NewEngine(t *testing.T) (enginetest.QueryEngine, error) {
	m.session = nil

	return enginetest.NewEngineWithProvider(t, m, NewInMemoryDBProvider()), nil
}

// Parallelism implements enginetest.Harness.
func (m *MetaHarness) Parallelism() int {
	return m.parallelism
}

// Setup implements enginetest.Harness.
func (m *MetaHarness) Setup(setupData ...[]setup.SetupScript) {
	m.setupData = nil
	for _, data := range setupData {
		m.setupData = append(m.setupData, data...)
	}
}
