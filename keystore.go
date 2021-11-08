package encrepo

import (
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	keystore "github.com/ipfs/go-ipfs-keystore"
	ci "github.com/libp2p/go-libp2p-core/crypto"
)

type dsks struct {
	ds datastore.Datastore
}

var _ keystore.Keystore = (*dsks)(nil)

func KeystoreFromDatastore(ds datastore.Datastore) keystore.Keystore {
	return &dsks{ds}
}

// Has returns whether or not a key exists in the Keystore
func (ks *dsks) Has(id string) (bool, error) {
	return ks.ds.Has(datastore.NewKey(id))
}

// Put stores a key in the Keystore, if a key with the same name already exists, returns ErrKeyExists
func (ks *dsks) Put(id string, val ci.PrivKey) error {
	valBytes, err := ci.MarshalPrivateKey(val)
	if err != nil {
		return err
	}

	key := datastore.NewKey(id)

	has, err := ks.ds.Has(key)
	if err != nil {
		return err
	}

	if has {
		return keystore.ErrKeyExists
	}

	return ks.ds.Put(key, valBytes)
}

// Get retrieves a key from the Keystore if it exists, and returns ErrNoSuchKey
// otherwise.
func (ks *dsks) Get(id string) (ci.PrivKey, error) {
	valBytes, err := ks.ds.Get(datastore.NewKey(id))
	if err != nil {
		if err == datastore.ErrNotFound {
			return nil, keystore.ErrNoSuchKey
		}
		return nil, err
	}
	return ci.UnmarshalPrivateKey(valBytes)
}

// Delete removes a key from the Keystore
func (ks *dsks) Delete(id string) error {
	return ks.ds.Delete(datastore.NewKey(id))
}

// List returns a list of key identifier
func (ks *dsks) List() ([]string, error) {
	res, err := ks.ds.Query(query.Query{KeysOnly: true})
	if err != nil {
		return nil, err
	}
	entries, err := res.Rest()
	if err != nil {
		return nil, err
	}
	l := make([]string, len(entries))
	for i, e := range entries {
		l[i] = e.Key
	}
	return l, nil
}