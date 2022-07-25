package encrepo

import (
	"context"

	sync_ds "github.com/ipfs/go-datastore/sync"
	config "github.com/ipfs/go-ipfs-config"
	"github.com/pkg/errors"
)

const tableName = "ipfs"

func IsInitialized(dbPath string, key []byte, opts SQLCipherDatastoreOptions) (bool, error) {
	// packageLock is held to ensure that another caller doesn't attempt to
	// Init or Remove the repo while this call is in progress.
	packageLock.Lock()
	defer packageLock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	return isInitialized(ctx, dbPath, key, opts)
}

// isInitialized reports whether the repo is initialized. Caller must
// hold the packageLock.
func isInitialized(ctx context.Context, dbPath string, key []byte, opts SQLCipherDatastoreOptions) (bool, error) {
	uds, err := OpenSQLCipherDatastore("sqlite3", dbPath, tableName, key, opts)
	if err == ErrDatabaseNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	ds := sync_ds.MutexWrap(uds)

	initialized := isConfigInitialized(ctx, ds)

	if err := uds.Close(); err != nil {
		return false, err
	}

	return initialized, nil
}

func Init(dbPath string, key []byte, opts SQLCipherDatastoreOptions, conf *config.Config) error {
	// packageLock must be held to ensure that the repo is not initialized more
	// than once.
	packageLock.Lock()
	defer packageLock.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	isInit, err := isInitialized(ctx, dbPath, key, opts)
	if err != nil {
		return err
	}
	if isInit {
		return nil
	}

	uds, err := NewSQLCipherDatastore("sqlite3", dbPath, tableName, key, opts)
	if err != nil {
		return err
	}

	ds := sync_ds.MutexWrap(uds)

	if err := initConfig(ctx, ds, conf); err != nil {
		return err
	}

	if len(conf.Datastore.Spec) != 0 {
		return errors.New("Config.Datastore.Spec not supported")
	}

	/*if err := migrations.WriteRepoVersion(repoPath, RepoVersion); err != nil {
		return err
	}*/

	return uds.Close()
}
