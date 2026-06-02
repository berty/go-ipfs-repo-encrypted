package encrepo

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	datastore "github.com/ipfs/go-datastore"
	config "github.com/ipfs/kubo/config"
)

// tests ported from fsrepo

// swap arg order
func testRepoPath(p string, t *testing.T) string {
	dir, err := os.MkdirTemp(t.TempDir(), p)
	require.NoError(t, err)
	return filepath.Join(dir, "db.sqlite")
}

func testingDatastoreConfig() config.Datastore {
	base := config.DefaultDatastoreConfig()
	base.Spec = nil
	return base
}

func TestInitIdempotence(t *testing.T) {
	t.Parallel()
	path := testRepoPath("", t)
	key := testingKey(t)
	salt := testingSalt(t)
	opts := SQLCipherDatastoreOptions{PlaintextHeader: true, Salt: salt, JournalMode: "WAL"}
	for i := 0; i < 10; i++ {
		require.NoError(t, Init(path, key, opts, &config.Config{Datastore: testingDatastoreConfig()}), i)
	}
}

func Remove(repoPath string) error {
	repoPath = filepath.Clean(repoPath)
	return os.RemoveAll(repoPath)
}

func testingKey(t *testing.T) []byte {
	t.Helper()
	buf := make([]byte, 32)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	return buf
}

func testingSalt(t *testing.T) []byte {
	t.Helper()
	buf := make([]byte, saltLength)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	return buf
}

func TestCanManageReposIndependently(t *testing.T) {
	t.Parallel()
	pathA := testRepoPath("a", t)
	pathB := testRepoPath("b", t)

	aKey := testingKey(t)
	bKey := testingKey(t)

	aSalt := testingSalt(t)
	bSalt := testingSalt(t)

	aOpts := SQLCipherDatastoreOptions{PlaintextHeader: true, Salt: aSalt}
	bOpts := SQLCipherDatastoreOptions{PlaintextHeader: true, Salt: bSalt}

	t.Log("initialize two repos")
	require.NoError(t, Init(pathA, aKey, aOpts, &config.Config{Datastore: testingDatastoreConfig()}), "a", "should initialize successfully")
	require.NoError(t, Init(pathB, bKey, bOpts, &config.Config{Datastore: testingDatastoreConfig()}), "b", "should initialize successfully")

	t.Log("ensure repos initialized")
	isInit, err := IsInitialized(pathA, aKey, aOpts)
	require.NoError(t, err)
	require.True(t, isInit, "a should be initialized")
	isInit, err = IsInitialized(pathB, bKey, bOpts)
	require.NoError(t, err)
	require.True(t, isInit, "b should be initialized")

	t.Log("open the two repos")
	repoA, err := Open(pathA, aKey, aOpts)
	require.NoError(t, err, "a")
	repoB, err := Open(pathB, bKey, bOpts)
	require.NoError(t, err, "b")

	t.Log("close and remove b while a is open")
	require.NoError(t, repoB.Close(), "close b")
	require.NoError(t, Remove(pathB), "remove b")

	t.Log("close and remove a")
	require.NoError(t, repoA.Close())
	require.NoError(t, Remove(pathA))
}

func TestDatastoreGetNotAllowedAfterClose(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := testRepoPath("test", t)

	key := testingKey(t)
	salt := testingSalt(t)
	opts := SQLCipherDatastoreOptions{PlaintextHeader: true, Salt: salt, JournalMode: "WAL"}
	isInit, err := IsInitialized(path, key, opts)
	require.NoError(t, err)
	require.False(t, isInit)
	require.NoError(t, Init(path, key, opts, &config.Config{Datastore: testingDatastoreConfig()}))
	r, err := Open(path, key, opts)
	require.NoError(t, err)

	k := "key"
	data := []byte(k)
	require.NoError(t, r.Datastore().Put(ctx, datastore.NewKey(k), data), "Put should be successful")

	require.NoError(t, r.Close())
	_, err = r.Datastore().Get(ctx, datastore.NewKey(k))
	require.Error(t, err, "after closer, Get should be fail")
}

func TestDatastorePersistsFromRepoToRepo(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	path := testRepoPath("test", t)
	key := testingKey(t)
	salt := testingSalt(t)
	opts := SQLCipherDatastoreOptions{PlaintextHeader: true, Salt: salt, JournalMode: "WAL"}

	require.NoError(t, Init(path, key, opts, &config.Config{Datastore: testingDatastoreConfig()}))
	r1, err := Open(path, key, opts)
	require.NoError(t, err)

	k := "key"
	expected := []byte(k)
	require.NoError(t, r1.Datastore().Put(ctx, datastore.NewKey(k), expected), "using first repo, Put should be successful")
	require.NoError(t, r1.Close())

	r2, err := Open(path, key, opts)
	require.NoError(t, err)
	actual, err := r2.Datastore().Get(ctx, datastore.NewKey(k))
	require.NoError(t, err, "using second repo, Get should be successful")
	require.NoError(t, r2.Close())
	require.True(t, bytes.Equal(expected, actual), "data should match")
}

func TestOpenMoreThanOnceInSameProcess(t *testing.T) {
	t.Parallel()
	path := testRepoPath("", t)

	key := testingKey(t)
	salt := testingSalt(t)
	opts := SQLCipherDatastoreOptions{PlaintextHeader: true, Salt: salt, JournalMode: "WAL"}

	require.NoError(t, Init(path, key, opts, &config.Config{Datastore: testingDatastoreConfig()}))

	r1, err := Open(path, key, opts)
	require.NoError(t, err, "first repo should open successfully")
	r2, err := Open(path, key, opts)
	require.NoError(t, err, "second repo should open successfully")
	require.True(t, r1 == r2, "second open returns same value")

	require.NoError(t, r1.Close())
	require.NoError(t, r2.Close())
}
