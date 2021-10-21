package encrepo

import (
	"path/filepath"

	"github.com/ipfs/go-datastore"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/mount"
	flatds "github.com/ipfs/go-ds-flatfs"
	levelds "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/pkg/errors"
)

var (
	onlyOne repo.OnlyOne
)

func Open(path string, key []byte) (repo.Repo, error) {
	fn := func() (repo.Repo, error) {
		return open(path, key)
	}
	return onlyOne.Open(path, fn)
}

func open(path string, key []byte) (repo.Repo, error) {
	packageLock.Lock()
	defer packageLock.Unlock()

	isInit, err := isInitialized(path, key)
	if err != nil {
		return nil, err
	}
	if !isInit {
		return nil, errors.New("repo is not initialized")
	}

	root, err := newRootDatastore(path, key)
	if err != nil {
		return nil, err
	}

	conf, err := getConfigFromDatastore(root)
	if err != nil {
		return nil, errors.Wrap(err, "get config")
	}

	return &encRepo{
		root:   root,
		ds:     NewNamespacedDatastore(root, datastore.NewKey("data")),
		ks:     KeystoreFromDatastore(root, "keys"),
		config: conf,
	}, nil
}

func newRootDatastore(path string, key []byte) (*mount.Datastore, error) {
	lds, err := levelds.NewDatastore(filepath.Join(path, "leveldb"), nil)
	if err != nil {
		return nil, err
	}
	elds, err := Wrap(lds, key)
	if err != nil {
		return nil, err
	}

	shardFunc, err := flatds.ParseShardFunc("/repo/flatfs/shard/v1/next-to-last/2")
	if err != nil {
		return nil, err
	}
	fds, err := flatds.CreateOrOpen(filepath.Join(path, "flatfs"), shardFunc, true)
	if err != nil {
		return nil, err
	}
	efds, err := Wrap(fds, key)
	if err != nil {
		return nil, err
	}
	return mount.New([]mount.Mount{
		{Prefix: ds.NewKey(""), Datastore: elds},
		{Prefix: ds.NewKey("data/blocks"), Datastore: efds},
	}), nil
}
