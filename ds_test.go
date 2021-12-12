package encrepo

import (
	"path/filepath"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/require"
)

func TestCase(t *testing.T) {
	key := testingKey(t)
	ds, err := NewSQLCipherDatastore("sqlite3", filepath.Join(t.TempDir(), "test.sqlite"), "blocks", key)
	require.NoError(t, err)
	require.NoError(t, ds.Put(datastore.KeyWithNamespaces([]string{"A", "B"}), ([]byte)("42")))
	qr, err := ds.Query(query.Query{Prefix: "a"})
	require.NoError(t, err)
	vals, err := qr.Rest()
	require.NoError(t, err)
	require.Len(t, vals, 0)
	qr, err = ds.Query(query.Query{Prefix: "A"})
	require.NoError(t, err)
	vals, err = qr.Rest()
	require.NoError(t, err)
	require.Len(t, vals, 1)
	require.Equal(t, "/A/B", vals[0].Key)
	require.Equal(t, ([]byte)("42"), vals[0].Value)
	require.NoError(t, ds.Close())
}
