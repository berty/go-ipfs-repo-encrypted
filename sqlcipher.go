package encrepo

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	sqlds "github.com/ipfs/go-ds-sql"
	sqliteds "github.com/ipfs/go-ds-sql/sqlite"
	sqlite3 "github.com/mutecomm/go-sqlcipher/v4"
	"github.com/pkg/errors"
)

type SQLCipherDatastoreOptions struct {
	PlaintextHeader bool
	Salt            []byte
	JournalMode     string
}

func NewSQLiteDatastore(driver, dbPath, table string) (*sqlds.Datastore, error) {
	return (&sqliteds.Options{Driver: driver, DSN: dbPath, Table: table}).Create()
}

const saltLength = 16

func NewSQLCipherDatastore(driver, dbPath, table string, key []byte, opts SQLCipherDatastoreOptions) (*sqlds.Datastore, error) {
	if !opts.PlaintextHeader { // enabling plaintext header breaks encryption detection
		if err := checkDBCrypto(dbPath, len(key) != 0); err != nil {
			return nil, err
		}
	}

	args := []string{}
	if opts.JournalMode != "" {
		args = append(args, "_journal_mode="+opts.JournalMode)
	}

	if opts.PlaintextHeader {
		if len(opts.Salt) != saltLength {
			return nil, fmt.Errorf("bad salt, expected %d bytes, got %d", saltLength, len(opts.Salt))
		}
		args = append(args, "_pragma_cipher_plaintext_header_size=32")
		args = append(args, fmt.Sprintf("_pragma_cipher_salt=x'%s'", hex.EncodeToString(opts.Salt)))
	}

	dsn := dbPath
	if len(args) != 0 {
		dsn += "?" + strings.Join(args, "&")
	}

	return (&sqliteds.Options{Driver: driver, DSN: dsn, Table: table, Key: key}).Create()
}

func OpenSQLCipherDatastore(driver, dbPath, table string, key []byte, opts SQLCipherDatastoreOptions) (*sqlds.Datastore, error) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, ErrDatabaseNotFound
	}

	return NewSQLCipherDatastore(driver, dbPath, table, key, opts)
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
