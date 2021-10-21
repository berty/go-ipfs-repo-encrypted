package encrepo

import (
	"crypto/cipher"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/pkg/errors"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/sha3"
)

type dscrypt struct {
	key  []byte
	aead cipher.AEAD
	uds  datastore.Datastore
}

var _ datastore.Datastore = (*dscrypt)(nil)

func Wrap(uds datastore.Datastore, key []byte) (datastore.Datastore, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, fmt.Errorf("invalid key length, expected %d bytes, got %d", chacha20poly1305.KeySize, len(key))
	}
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, errors.Wrap(err, "initialize datastore cipher")
	}
	kt := &keyTransform{encryptionKey: key, aead: aead}
	// kt := &keySliceTransform{encryptionKey: key, aead: aead}
	return KeyTransformWrap(&dscrypt{
		key:  key,
		aead: aead,
		uds:  uds,
	}, kt), nil
}

func secureRandomBytes(length int) ([]byte, error) {
	nonce := make([]byte, length)
	_, err := crand.Read(nonce)
	return nonce, err
}

func (ds *dscrypt) encrypt(pt []byte) ([]byte, error) {
	nonce, err := secureRandomBytes(chacha20poly1305.NonceSizeX)
	if err != nil {
		return nil, errors.Wrap(err, "generate nonce")
	}
	ct := ds.aead.Seal(nil, nonce, pt, nil)
	return append(nonce, ct...), nil
}

func (ds *dscrypt) decrypt(ct []byte) ([]byte, error) {
	if len(ct) < chacha20poly1305.NonceSizeX {
		return nil, fmt.Errorf("ciphertext too short, expected at least %d nonce bytes, got %d", chacha20poly1305.NonceSizeX, len(ct))
	}
	return ds.aead.Open(nil, ct[:chacha20poly1305.NonceSizeX], ct[chacha20poly1305.NonceSizeX:], nil)
}

type keyTransform struct {
	encryptionKey []byte
	aead          cipher.AEAD
}

func (kt *keyTransform) ConvertKey(key datastore.Key) (datastore.Key, error) {
	return encryptKey(kt.aead, kt.encryptionKey, key)
}

func (kt *keyTransform) InvertKey(key datastore.Key) (datastore.Key, error) {
	return decryptKey(kt.aead, key)
}

type keySliceTransform struct {
	encryptionKey []byte
	aead          cipher.AEAD
}

func (kt *keySliceTransform) ConvertKey(key datastore.Key) (datastore.Key, error) {
	slice := key.List()
	conv := make([]string, len(slice))
	for i, atom := range slice {
		a, err := encryptKey(kt.aead, kt.encryptionKey, datastore.NewKey(atom))
		if err != nil {
			return datastore.Key{}, err
		}
		conv[i] = a.String()
	}
	return datastore.KeyWithNamespaces(conv), nil
}

func (kt *keySliceTransform) InvertKey(key datastore.Key) (datastore.Key, error) {
	slice := key.List()
	conv := make([]string, len(slice))
	for i, atom := range slice {
		a, err := decryptKey(kt.aead, datastore.NewKey(atom))
		if err != nil {
			return datastore.Key{}, err
		}
		conv[i] = a.String()
	}
	return datastore.KeyWithNamespaces(conv), nil
}

func encryptKey(aead cipher.AEAD, encryptionKey []byte, key datastore.Key) (datastore.Key, error) {
	kb := key.Bytes()

	// The Keccak hash function, that was selected by NIST as the SHA-3 competition winner, doesn't need this nested approach and can be used to generate a MAC by simply prepending the key to the message, as it is not susceptible to length-extension attacks.[6]
	h := sha3.New256()
	_, err := h.Write(append(encryptionKey, kb...))
	if err != nil {
		return datastore.Key{}, err
	}
	nonce := h.Sum(nil)[:chacha20poly1305.NonceSizeX]

	ekb := append(nonce, aead.Seal(nil, nonce, kb, nil)...)
	return datastore.NewKey(base64.RawURLEncoding.EncodeToString(ekb)), nil
}

func decryptKey(aead cipher.AEAD, key datastore.Key) (datastore.Key, error) {
	kb, err := base64.RawURLEncoding.DecodeString(key.String())
	if err != nil {
		return datastore.Key{}, err
	}

	if len(kb) <= chacha20poly1305.NonceSizeX {
		return datastore.Key{}, fmt.Errorf("ct too short, expected more than %d bytes, got %d", 32, len(kb))
	}

	pt, err := aead.Open(nil, kb[:chacha20poly1305.NonceSizeX], kb[chacha20poly1305.NonceSizeX:], nil)
	if err != nil {
		return datastore.Key{}, err
	}

	return datastore.NewKey(string(pt)), nil
}

// Get retrieves the object `value` named by `key`.
// Get will return ErrNotFound if the key is not mapped to a value.
func (ds *dscrypt) Get(key datastore.Key) ([]byte, error) {
	uval, err := ds.uds.Get(key)
	if err != nil {
		return nil, err
	}
	return ds.decrypt(uval)
}

// Has returns whether the `key` is mapped to a `value`.
// In some contexts, it may be much cheaper only to check for existence of
// a value, rather than retrieving the value itself. (e.g. HTTP HEAD).
// The default implementation is found in `GetBackedHas`.
func (ds *dscrypt) Has(key datastore.Key) (exists bool, err error) {
	return ds.uds.Has(key)
}

// GetSize returns the size of the `value` named by `key`.
// In some contexts, it may be much cheaper to only get the size of the
// value rather than retrieving the value itself.
func (ds *dscrypt) GetSize(key datastore.Key) (int, error) {
	size, err := ds.uds.GetSize(key)
	if err != nil {
		return 0, err
	}
	// ANSWERME: should we return the actual data size or the size on disk with nonce and overhead
	return size - (chacha20poly1305.NonceSizeX + ds.aead.Overhead()), nil
}

// Query searches the datastore and returns a query result. This function
// may return before the query actually runs. To wait for the query:
//
//   result, _ := ds.Query(q)
//
//   // use the channel interface; result may come in at different times
//   for entry := range result.Next() { ... }
//
//   // or wait for the query to be completely done
//   entries, _ := result.Rest()
//   for entry := range entries { ... }
//
func (ds *dscrypt) Query(q query.Query) (query.Results, error) {
	cqr, err := ds.uds.Query(q)
	if err != nil {
		return nil, err
	}
	qr := query.ResultsFromIterator(q, query.Iterator{
		Next: func() (query.Result, bool) {
			r, ok := cqr.NextSync()
			if !ok {
				return r, false
			}
			if r.Error == nil {
				val, err := ds.decrypt(r.Value)
				if err != nil {
					r.Error = err
					return r, true
				}
				r.Value = val
			}
			return r, true
		},
		Close: func() error {
			return cqr.Close()
		},
	})
	return query.NaiveQueryApply(q, qr), nil
}

// Put stores the object `value` named by `key`.
//
// The generalized Datastore interface does not impose a value type,
// allowing various datastore middleware implementations (which do not
// handle the values directly) to be composed together.
//
// Ultimately, the lowest-level datastore will need to do some value checking
// or risk getting incorrect values. It may also be useful to expose a more
// type-safe interface to your application, and do the checking up-front.
func (ds *dscrypt) Put(key datastore.Key, value []byte) error {
	ct, err := ds.encrypt(value)
	if err != nil {
		return err
	}
	return ds.uds.Put(key, ct)
}

// Delete removes the value for given `key`. If the key is not in the
// datastore, this method returns no error.
func (ds *dscrypt) Delete(key datastore.Key) error {
	return ds.uds.Delete(key)
}

// Sync guarantees that any Put or Delete calls under prefix that returned
// before Sync(prefix) was called will be observed after Sync(prefix)
// returns, even if the program crashes. If Put/Delete operations already
// satisfy these requirements then Sync may be a no-op.
//
// If the prefix fails to Sync this method returns an error.
func (ds *dscrypt) Sync(prefix datastore.Key) error {
	return ds.uds.Sync(prefix)
}

func (ds *dscrypt) Close() error {
	return ds.uds.Close()
}
