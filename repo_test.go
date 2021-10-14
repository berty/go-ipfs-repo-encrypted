package encrepo

import (
	"path/filepath"
	"testing"

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

	require.NoError(t, r.SetAPIAddr(ma.StringCast("/ip4/127.0.0.1")))
	require.NoError(t, r.SetAPIAddr(ma.StringCast("/ip4/127.0.0.42")))
}
