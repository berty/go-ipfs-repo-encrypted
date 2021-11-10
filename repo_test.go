package encrepo

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-datastore"
	config "github.com/ipfs/go-ipfs-config"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

func TestRepo(t *testing.T) {
	key := testingKey(t)
	dbPath := filepath.Join(t.TempDir(), "db.sqlite")

	isInit, err := IsInitialized(dbPath, key)
	require.NoError(t, err)
	require.False(t, isInit)

	err = Init(dbPath, key, &config.Config{})
	require.NoError(t, err)

	isInit, err = IsInitialized(dbPath, key)
	require.NoError(t, err)
	require.True(t, isInit)

	err = Init(dbPath, key, &config.Config{})
	require.NoError(t, err)

	r, err := Open(dbPath, key)
	require.NoError(t, err)
	defer requireClose(t, r)

	ds := r.Datastore()
	require.NotNil(t, ds)

	ks := r.Keystore()
	require.NotNil(t, ks)
}

func TestSetAPIAddrTwice(t *testing.T) {
	key := testingKey(t)
	dbPath := filepath.Join(t.TempDir(), "db.sqlite")

	err := Init(dbPath, key, &config.Config{})
	require.NoError(t, err)

	r, err := Open(dbPath, key)
	require.NoError(t, err)
	defer requireClose(t, r)

	require.NoError(t, r.SetAPIAddr(ma.StringCast("/ip4/127.0.0.1")))
	require.NoError(t, r.SetAPIAddr(ma.StringCast("/ip4/127.0.0.42")))
}

func TestNewSQLiteDatastoreTwice(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db.sqlite")
	ds, err := NewSQLiteDatastore("sqlite3", dbPath, "data")
	require.NoError(t, err)
	require.NoError(t, ds.Close())
	ds, err = NewSQLiteDatastore("sqlite3", dbPath, "data")
	require.NoError(t, err)
	require.NoError(t, ds.Close())
}

func TestGetSize(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "db.sqlite")
	ds, err := NewSQLiteDatastore("sqlite3", dbPath, "data")
	require.NoError(t, err)
	defer requireClose(t, ds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	buf := []byte("42\x0042")
	key := datastore.NewKey("key")
	err = ds.Put(ctx, key, buf)
	require.NoError(t, err)

	sz, err := ds.GetSize(ctx, key)
	require.NoError(t, err)

	require.Equal(t, len(buf), sz)

	require.NoError(t, ds.Close())
}
