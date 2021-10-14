package encrepo

import (
	"encoding/json"

	"github.com/ipfs/go-datastore"
	config "github.com/ipfs/go-ipfs-config"
	"github.com/pkg/errors"
)

const configKey = "config"

func isConfigInitialized(ds datastore.Datastore) bool {
	has, err := ds.Has(datastore.NewKey(configKey))
	if err != nil {
		return false
	}
	return has
}

func initConfig(ds datastore.Datastore, conf *config.Config) error {
	if isConfigInitialized(ds) {
		return nil
	}

	// initialization is the one time when it's okay to write to the config
	// without reading the config from disk and merging any user-provided keys
	// that may exist.
	if err := writeConfigToDatastore(ds, conf); err != nil {
		return err
	}

	return nil
}

func readConfigFromDatastore(ds datastore.Datastore, dest interface{}) error {
	confBytes, err := ds.Get(datastore.NewKey(configKey))
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

func writeConfigToDatastore(ds datastore.Datastore, src interface{}) error {
	confBytes, err := config.Marshal(src)
	if err != nil {
		return errors.Wrap(err, "marshal config")
	}
	if err := ds.Put(datastore.NewKey(configKey), confBytes); err != nil {
		return errors.Wrap(err, "put config in ds")
	}
	return nil
}

func getConfigFromDatastore(ds datastore.Datastore) (*config.Config, error) {
	var conf config.Config
	err := readConfigFromDatastore(ds, &conf)
	switch err {
	case nil:
		return &conf, nil
	case datastore.ErrNotFound:
		return nil, nil
	default:
		return nil, err
	}
}
