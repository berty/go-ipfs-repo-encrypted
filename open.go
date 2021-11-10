package encrepo

import (
	"context"

	"github.com/ipfs/go-datastore"
	sync_ds "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/pkg/errors"
)

var (
	onlyOne repo.OnlyOne
)

func Open(dbPath string, key []byte) (repo.Repo, error) {
	fn := func() (repo.Repo, error) {
		return open(dbPath, key)
	}
	return onlyOne.Open(dbPath, fn)
}

func open(dbPath string, key []byte) (repo.Repo, error) {
	packageLock.Lock()
	defer packageLock.Unlock()

	uroot, err := OpenSQLCipherDatastore("sqlite3", dbPath, tableName, key)
	if err != nil {
		return nil, errors.Wrap(err, "instantiate datastore")
	}

	root := sync_ds.MutexWrap(uroot)

	ctx, cancel := context.WithCancel(context.Background())

	conf, err := getConfigFromDatastore(ctx, root)
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "get config")
	}

	return &encRepo{
		root:   root,
		ds:     NewNamespacedDatastore(root, datastore.NewKey("data")),
		ks:     KeystoreFromDatastore(ctx, NewNamespacedDatastore(root, datastore.NewKey("keys"))),
		config: conf,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}
