package encrepo

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/stretchr/testify/require"
)

func TestCase(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	key := testingKey(t)
	salt := testingSalt(t)
	opts := SQLCipherDatastoreOptions{PlaintextHeader: true, Salt: salt, JournalMode: "WAL"}
	ds, err := NewSQLCipherDatastore("sqlite3", filepath.Join(t.TempDir(), "test.sqlite"), "blocks", key, opts)
	require.NoError(t, err)
	require.NoError(t, ds.Put(ctx, datastore.KeyWithNamespaces([]string{"A", "B"}), ([]byte)("42")))
	qr, err := ds.Query(ctx, query.Query{Prefix: "a"})
	require.NoError(t, err)
	vals, err := qr.Rest()
	require.NoError(t, err)
	require.Len(t, vals, 0)
	qr, err = ds.Query(ctx, query.Query{Prefix: "A"})
	require.NoError(t, err)
	vals, err = qr.Rest()
	require.NoError(t, err)
	require.Len(t, vals, 1)
	require.Equal(t, "/A/B", vals[0].Key)
	require.Equal(t, ([]byte)("42"), vals[0].Value)
	require.NoError(t, ds.Close())
}

const sqliteHeaderMagic = "SQLite format 3\000"
const walModeMagic = uint8(2)
const walModeWriteMagicOffset = 18
const walModeReadMagicOffset = 19

func TestDBHeader(t *testing.T) {
	key := testingKey(t)
	salt := testingSalt(t)
	opts := SQLCipherDatastoreOptions{PlaintextHeader: true, Salt: salt, JournalMode: "WAL"}
	dbPath := filepath.Join(t.TempDir(), "test.sqlite")

	// create datastore
	ds, err := NewSQLCipherDatastore("sqlite3", dbPath, "blocks", key, opts)
	require.NoError(t, err)
	require.NoError(t, ds.Close())

	// open db file
	reader, err := os.Open(dbPath)
	require.NoError(t, err)

	// read db header
	headerBytes := make([]byte, walModeReadMagicOffset+1)
	n, err := reader.Read(headerBytes)
	require.NoError(t, err)
	require.Equal(t, walModeReadMagicOffset+1, n)

	// check for db plaintext header mode
	require.Equal(t, sqliteHeaderMagic, string(headerBytes[:len(sqliteHeaderMagic)]))

	// check for db WAL mode
	require.Equal(t, walModeMagic, headerBytes[walModeWriteMagicOffset])
	require.Equal(t, walModeMagic, headerBytes[walModeReadMagicOffset])

	// close db file
	require.NoError(t, reader.Close())
}
