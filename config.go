package encrepo

import (
	"context"
	"encoding/json"

	"github.com/ipfs/go-datastore"
	config "github.com/ipfs/go-ipfs-config"
	"github.com/pkg/errors"
)

const configKey = "config"

func isConfigInitialized(ctx context.Context, ds datastore.Datastore) bool {
	has, err := ds.Has(ctx, datastore.NewKey(configKey))
	if err != nil {
		return false
	}
	return has
}

func initConfig(ctx context.Context, ds datastore.Datastore, conf *config.Config) error {
	if isConfigInitialized(ctx, ds) {
		return nil
	}

	// initialization is the one time when it's okay to write to the config
	// without reading the config from disk and merging any user-provided keys
	// that may exist.
	if err := writeConfigToDatastore(ctx, ds, conf); err != nil {
		return err
	}

	return nil
}

func readConfigFromDatastore(ctx context.Context, ds datastore.Datastore, dest interface{}) error {
	confBytes, err := ds.Get(ctx, datastore.NewKey(configKey))
	switch err {
	case nil:
		if err := json.Unmarshal(confBytes, dest); err != nil {
			return errors.Wrap(err, "unmarshal config")
		}
		return nil
	case datastore.ErrNotFound:
		return datastore.ErrNotFound
	default:
		return errors.Wrap(err, "get config")
	}
}

func writeConfigToDatastore(ctx context.Context, ds datastore.Datastore, src interface{}) error {
	confBytes, err := config.Marshal(src)
	if err != nil {
		return errors.Wrap(err, "marshal config")
	}
	if err := ds.Put(ctx, datastore.NewKey(configKey), confBytes); err != nil {
		return errors.Wrap(err, "put config in ds")
	}
	return nil
}

func getConfigFromDatastore(ctx context.Context, ds datastore.Datastore) (*config.Config, error) {
	var conf config.Config
	err := readConfigFromDatastore(ctx, ds, &conf)
	switch err {
	case nil:
		return &conf, nil
	case datastore.ErrNotFound:
		return nil, nil
	default:
		return nil, err
	}
}
