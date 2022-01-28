package encrepo

import (
	"fmt"
	"os"

	sqlds "github.com/ipfs/go-ds-sql"
	sqliteds "github.com/ipfs/go-ds-sql/sqlite"
	sqlite3 "github.com/mutecomm/go-sqlcipher/v4"
	"github.com/pkg/errors"
)

func NewSQLiteDatastore(driver, dbPath, table string) (*sqlds.Datastore, error) {
	return (&sqliteds.Options{Driver: driver, DSN: dbPath, Table: table}).Create()
}

func NewSQLCipherDatastore(driver, dbPath, table string, key []byte) (*sqlds.Datastore, error) {
	if err := checkDBCrypto(dbPath, len(key) != 0); err != nil {
		return nil, err
	}
	return (&sqliteds.Options{Driver: driver, DSN: dbPath, Table: table, Key: key}).Create()
}

func OpenSQLCipherDatastore(driver, dbPath, table string, key []byte) (*sqlds.Datastore, error) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, ErrDatabaseNotFound
	}
	return NewSQLCipherDatastore(driver, dbPath, table, key)
}

var (
	ErrDatabaseNotFound = errors.New("database not found")
)

func checkDBCrypto(dbPath string, shouldBeEncrypted bool) error {
	fi, err := os.Stat(dbPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "failed to stat db file at "+dbPath)
	}
	if fi.IsDir() {
		return fmt.Errorf("%s is a directory, not a db file", dbPath)
	}

	hasEncryptedDB, err := sqlite3.IsEncrypted(dbPath)
	if err != nil {
		return errors.Wrap(err, "failed to check if db is encrypted")
	}

	if shouldBeEncrypted {
		if !hasEncryptedDB {
			return errors.New("key provided while datastore db is NOT encrypted")
		}
	} else if hasEncryptedDB {
		return errors.New("missing key, db is encrypted")
	}

	return nil
}
