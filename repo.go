package encrepo

import (
	"context"
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-filestore"
	config "github.com/ipfs/go-ipfs-config"
	keystore "github.com/ipfs/go-ipfs-keystore"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/common"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/pkg/errors"
)

type encRepo struct {
	root   datastore.Datastore
	ds     repo.Datastore
	ks     keystore.Keystore
	config *config.Config
	closed bool
}

var _ repo.Repo = (*encRepo)(nil)

// Config returns the ipfs configuration file from the repo. Changes made
// to the returned config are not automatically persisted.
func (r *encRepo) Config() (*config.Config, error) {
	packageLock.Lock()
	defer packageLock.Unlock()

	if r.closed {
		return nil, errors.New("cannot access config, repo not open")
	}

	return r.config, nil
}

// BackupConfig creates a backup of the current configuration file using
// the given prefix for naming.
func (r *encRepo) BackupConfig(prefix string) (string, error) {
	// Not implemented since the implementation of this in fsrepo makes no sense
	// The backup file name is randomly generated within the function but not returned, so it's not possible to find the backup file afterwards
	return "", errors.New("not implemented")
}

// SetConfig persists the given configuration struct to storage.
func (r *encRepo) SetConfig(updated *config.Config) error {
	packageLock.Lock()
	defer packageLock.Unlock()

	return r.setConfig(updated)
}

// SetConfig persists the given configuration struct to storage.
func (r *encRepo) setConfig(updated *config.Config) error {
	// to avoid clobbering user-provided keys, must read the config from disk
	// as a map, write the updated struct values to the map and write the map
	// to disk.
	var mapconf map[string]interface{}
	if err := readConfigFromDatastore(r.root, &mapconf); err != nil {
		return err
	}
	m, err := config.ToMap(updated)
	if err != nil {
		return err
	}
	for k, v := range m {
		mapconf[k] = v
	}

	// Do not use `*r.config = ...`. This will modify the *shared* config
	// returned by `r.Config`.
	conf, err := config.FromMap(mapconf)
	if err != nil {
		return err
	}

	if err := writeConfigToDatastore(r.root, conf); err != nil {
		return err
	}

	r.config = conf

	return nil
}

// SetConfigKey sets the given key-value pair within the config and persists it to storage.
func (r *encRepo) SetConfigKey(key string, value interface{}) error {
	packageLock.Lock()
	defer packageLock.Unlock()

	if r.closed {
		return errors.New("repo is closed")
	}

	// Load into a map so we don't end up writing any additional defaults to the config file.
	var mapconf map[string]interface{}
	if err := readConfigFromDatastore(r.root, &mapconf); err != nil {
		return err
	}
	// Load private key to guard against it being overwritten.
	// NOTE: this is a temporary measure to secure this field until we move
	// keys out of the config file.
	pkval, err := common.MapGetKV(mapconf, config.PrivKeySelector)
	if err != nil {
		return err
	}

	// Set the key in the map.
	if err := common.MapSetKV(mapconf, key, value); err != nil {
		return err
	}

	// replace private key, in case it was overwritten.
	if err := common.MapSetKV(mapconf, config.PrivKeySelector, pkval); err != nil {
		return err
	}

	// This step doubles as to validate the map against the struct
	// before serialization
	conf, err := config.FromMap(mapconf)
	if err != nil {
		return err
	}

	// Write config
	return r.setConfig(conf)
}

// GetConfigKey reads the value for the given key from the configuration in storage.
func (r *encRepo) GetConfigKey(key string) (interface{}, error) {
	packageLock.Lock()
	defer packageLock.Unlock()

	if r.closed {
		return nil, errors.New("repo is closed")
	}

	var cfg map[string]interface{}
	if err := readConfigFromDatastore(r.root, &cfg); err != nil {
		return nil, err
	}
	return common.MapGetKV(cfg, key)
}

// Datastore returns a reference to the configured data storage backend.
func (r *encRepo) Datastore() repo.Datastore {
	return r.ds
}

// GetStorageUsage returns the number of bytes stored.
func (r *encRepo) GetStorageUsage(ctx context.Context) (uint64, error) {
	return datastore.DiskUsage(ctx, r.Datastore())
}

// Keystore returns a reference to the key management interface.
func (r *encRepo) Keystore() keystore.Keystore {
	return r.ks
}

// FileManager returns a reference to the filestore file manager.
func (r *encRepo) FileManager() *filestore.FileManager {
	return nil
}

// SetAPIAddr sets the API address in the repo.
func (r *encRepo) SetAPIAddr(addr ma.Multiaddr) error {
	packageLock.Lock()
	defer packageLock.Unlock()

	bytes, err := addr.MarshalBinary()
	if err != nil {
		return errors.Wrap(err, "marshal ma")
	}
	key := datastore.NewKey("api")
	if err := r.root.Put(context.Background(), key, bytes); err != nil {
		return errors.Wrap(err, fmt.Sprintf("put '%s' in ds", key))
	}
	return nil
}

// SwarmKey returns the configured shared symmetric key for the private networks feature.
func (r *encRepo) SwarmKey() ([]byte, error) {
	swarmKey, err := r.root.Get(context.Background(), datastore.NewKey("swarm.key"))
	switch err {
	case nil:
		return swarmKey, nil
	case datastore.ErrNotFound:
		return nil, nil
	default:
		return nil, err
	}
}

func (r *encRepo) Close() error {
	packageLock.Lock()
	defer packageLock.Unlock()

	if r.closed {
		return errors.New("repo is already closed")
	}

	r.closed = true

	return r.root.Close()
}
