package meta

import (
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
)

func TestCreateDatabase(t *testing.T) {
	enginetest.TestCreateDatabase(t, NewDefaultMetaHarness())
}
