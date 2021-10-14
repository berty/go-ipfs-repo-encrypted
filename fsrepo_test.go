package encrepo

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-ipfs/thirdparty/assert"
	"github.com/stretchr/testify/require"

	datastore "github.com/ipfs/go-datastore"
	config "github.com/ipfs/go-ipfs-config"
)

// tests ported from fsrepo

// swap arg order
func testRepoPath(p string, t *testing.T) string {
	dir, err := ioutil.TempDir(t.TempDir(), p)
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
	for i := 0; i < 10; i++ {
		require.NoError(t, Init(path, key, &config.Config{Datastore: testingDatastoreConfig()}), i)
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

func TestCanManageReposIndependently(t *testing.T) {
	t.Parallel()
	pathA := testRepoPath("a", t)
	pathB := testRepoPath("b", t)

	aKey := testingKey(t)
	bKey := testingKey(t)

	t.Log("initialize two repos")
	assert.Nil(Init(pathA, aKey, &config.Config{Datastore: testingDatastoreConfig()}), t, "a", "should initialize successfully")
	assert.Nil(Init(pathB, bKey, &config.Config{Datastore: testingDatastoreConfig()}), t, "b", "should initialize successfully")

	t.Log("ensure repos initialized")
	isInit, err := IsInitialized(pathA, aKey)
	require.NoError(t, err)
	require.True(t, isInit, "a should be initialized")
	isInit, err = IsInitialized(pathB, bKey)
	require.NoError(t, err)
	require.True(t, isInit, "b should be initialized")

	t.Log("open the two repos")
	repoA, err := Open(pathA, aKey)
	assert.Nil(err, t, "a")
	repoB, err := Open(pathB, bKey)
	assert.Nil(err, t, "b")

	t.Log("close and remove b while a is open")
	assert.Nil(repoB.Close(), t, "close b")
	assert.Nil(Remove(pathB), t, "remove b")

	t.Log("close and remove a")
	assert.Nil(repoA.Close(), t)
	assert.Nil(Remove(pathA), t)
}

func TestDatastoreGetNotAllowedAfterClose(t *testing.T) {
	t.Parallel()
	path := testRepoPath("test", t)

	key := testingKey(t)
	isInit, err := IsInitialized(path, key)
	require.NoError(t, err)
	require.False(t, isInit)
	require.NoError(t, Init(path, key, &config.Config{Datastore: testingDatastoreConfig()}))
	r, err := Open(path, key)
	require.NoError(t, err)

	k := "key"
	data := []byte(k)
	assert.Nil(r.Datastore().Put(datastore.NewKey(k), data), t, "Put should be successful")

	assert.Nil(r.Close(), t)
	_, err = r.Datastore().Get(datastore.NewKey(k))
	assert.Err(err, t, "after closer, Get should be fail")
}

func TestDatastorePersistsFromRepoToRepo(t *testing.T) {
	t.Parallel()
	path := testRepoPath("test", t)
	key := testingKey(t)

	assert.Nil(Init(path, key, &config.Config{Datastore: testingDatastoreConfig()}), t)
	r1, err := Open(path, key)
	assert.Nil(err, t)

	k := "key"
	expected := []byte(k)
	assert.Nil(r1.Datastore().Put(datastore.NewKey(k), expected), t, "using first repo, Put should be successful")
	assert.Nil(r1.Close(), t)

	r2, err := Open(path, key)
	assert.Nil(err, t)
	actual, err := r2.Datastore().Get(datastore.NewKey(k))
	assert.Nil(err, t, "using second repo, Get should be successful")
	assert.Nil(r2.Close(), t)
	assert.True(bytes.Equal(expected, actual), t, "data should match")
}

func TestOpenMoreThanOnceInSameProcess(t *testing.T) {
	t.Parallel()
	path := testRepoPath("", t)

	key := testingKey(t)

	assert.Nil(Init(path, key, &config.Config{Datastore: testingDatastoreConfig()}), t)

	r1, err := Open(path, key)
	assert.Nil(err, t, "first repo should open successfully")
	r2, err := Open(path, key)
	assert.Nil(err, t, "second repo should open successfully")
	assert.True(r1 == r2, t, "second open returns same value")

	assert.Nil(r1.Close(), t)
	assert.Nil(r2.Close(), t)
}
