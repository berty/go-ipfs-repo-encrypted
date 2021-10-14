package encrepo

import (
	config "github.com/ipfs/go-ipfs-config"
	"github.com/pkg/errors"
)

const tableName = "ipfs"

func IsInitialized(dbPath string, key []byte) (bool, error) {
	// packageLock is held to ensure that another caller doesn't attempt to
	// Init or Remove the repo while this call is in progress.
	packageLock.Lock()
	defer packageLock.Unlock()

	return isInitialized(dbPath, key)
}

// isInitialized reports whether the repo is initialized. Caller must
// hold the packageLock.
func isInitialized(dbPath string, key []byte) (bool, error) {
	ds, err := OpenSQLCipherDatastore("sqlite3", dbPath, tableName, key)
	if err == ErrDatabaseNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return isConfigInitialized(ds), nil
}

func Init(dbPath string, key []byte, conf *config.Config) error {
	// packageLock must be held to ensure that the repo is not initialized more
	// than once.
	packageLock.Lock()
	defer packageLock.Unlock()

	isInit, err := isInitialized(dbPath, key)
	if err != nil {
		return err
	}
	if isInit {
		return nil
	}

	ds, err := NewSQLCipherDatastore("sqlite3", dbPath, tableName, key)
	if err != nil {
		return err
	}

	if err := initConfig(ds, conf); err != nil {
		return err
	}

	if len(conf.Datastore.Spec) != 0 {
		return errors.New("Config.Datastore.Spec not supported")
	}

	/*if err := migrations.WriteRepoVersion(repoPath, RepoVersion); err != nil {
		return err
	}*/

	return nil
}
