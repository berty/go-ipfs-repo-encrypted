package encrepo

import (
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	keystore "github.com/ipfs/go-ipfs-keystore"
	ci "github.com/libp2p/go-libp2p-core/crypto"
)

type dsks struct {
	ds        datastore.Datastore
	namespace string
}

var _ keystore.Keystore = (*dsks)(nil)

func KeystoreFromDatastore(ds datastore.Datastore, namespace string) keystore.Keystore {
	return &dsks{ds, namespace}
}

// Has returns whether or not a key exists in the Keystore
func (ks *dsks) Has(id string) (bool, error) {
	return ks.ds.Has(ks.keyFromString(id))
}

// Put stores a key in the Keystore, if a key with the same name already exists, returns ErrKeyExists
func (ks *dsks) Put(id string, val ci.PrivKey) error {
	valBytes, err := ci.MarshalPrivateKey(val)
	if err != nil {
		return err
	}

	key := ks.keyFromString(id)

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
	valBytes, err := ks.ds.Get(ks.keyFromString(id))
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
	return ks.ds.Delete(ks.keyFromString(id))
}

// List returns a list of key identifier
func (ks *dsks) List() ([]string, error) {
	res, err := ks.ds.Query(query.Query{Prefix: ks.namespace, KeysOnly: true})
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

func (ks *dsks) keyFromString(id string) datastore.Key {
	return datastore.KeyWithNamespaces([]string{ks.namespace, id})
}
