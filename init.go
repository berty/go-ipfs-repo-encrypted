package encrepo

import (
	"os"
	"path/filepath"

	config "github.com/ipfs/go-ipfs-config"
	"github.com/pkg/errors"
)

const tableName = "ipfs"

func IsInitialized(dbPath string) (bool, error) {
	// packageLock is held to ensure that another caller doesn't attempt to
	// Init or Remove the repo while this call is in progress.
	packageLock.Lock()
	defer packageLock.Unlock()

	return isInitialized(dbPath)
}

// isInitialized reports whether the repo is initialized. Caller must
// hold the packageLock.
func isInitialized(path string) (bool, error) {
	dbPath := filepath.Join(path, "leveldb")

	_, err := os.Stat(dbPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, errors.Wrap(err, "stat leveldb directory")
	}

	return true, nil
}

func Init(path string, key []byte, conf *config.Config) error {
	if len(conf.Datastore.Spec) != 0 {
		return errors.New("Config.Datastore.Spec not supported")
	}

	storeKey := false
	if len(key) == 0 {
		storeKey = true
		var err error
		if key, err = secureRandomBytes(32); err != nil {
			return err
		}
	}

	// packageLock must be held to ensure that the repo is not initialized more
	// than once.
	packageLock.Lock()
	defer packageLock.Unlock()

	isInit, err := isInitialized(path)
	if err != nil {
		return err
	}
	if isInit {
		return nil
	}

	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return err
	}

	ds, err := newRootDatastore(path, key)
	if err != nil {
		return err
	}

	if err := initConfig(ds, conf); err != nil {
		return err
	}

	/*if err := migrations.WriteRepoVersion(repoPath, RepoVersion); err != nil {
		return err
	}*/

	if storeKey {
		keyPath := filepath.Join(path, "storage.key")
		if err := os.WriteFile(keyPath, key, os.ModePerm); err != nil {
			return err
		}
	}

	return ds.Close()
}
