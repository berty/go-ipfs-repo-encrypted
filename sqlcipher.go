package encrepo

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	sqlds "github.com/ipfs/go-ds-sql"
	"github.com/jimsmart/schema"
	sqlite3 "github.com/mutecomm/go-sqlcipher/v4"
	"github.com/pkg/errors"
)

// Queries are the sqlite queries for a given table.
type Queries struct {
	deleteQuery  string
	existsQuery  string
	getQuery     string
	putQuery     string
	queryQuery   string
	prefixQuery  string
	limitQuery   string
	offsetQuery  string
	getSizeQuery string
}

// NewQueries creates a new sqlite set of queries for the passed table
func NewQueries(tbl string) Queries {
	return Queries{
		deleteQuery:  fmt.Sprintf("DELETE FROM %s WHERE key = $1", tbl),
		existsQuery:  fmt.Sprintf("SELECT exists(SELECT 1 FROM %s WHERE key=$1)", tbl),
		getQuery:     fmt.Sprintf("SELECT data FROM %s WHERE key = $1", tbl),
		putQuery:     fmt.Sprintf("INSERT OR REPLACE INTO %s(key, data) VALUES($1, $2)", tbl),
		queryQuery:   fmt.Sprintf("SELECT key, data FROM %s", tbl),
		prefixQuery:  ` WHERE key LIKE '%s%%' ORDER BY key`,
		limitQuery:   ` LIMIT %d`,
		offsetQuery:  ` OFFSET %d`,
		getSizeQuery: fmt.Sprintf("SELECT length(data) FROM %s WHERE key = $1", tbl),
	}
}

// Delete returns the sqlite query for deleting a row.
func (q Queries) Delete() string {
	return q.deleteQuery
}

// Exists returns the sqlite query for determining if a row exists.
func (q Queries) Exists() string {
	return q.existsQuery
}

// Get returns the sqlite query for getting a row.
func (q Queries) Get() string {
	return q.getQuery
}

// Put returns the sqlite query for putting a row.
func (q Queries) Put() string {
	return q.putQuery
}

// Query returns the sqlite query for getting multiple rows.
func (q Queries) Query() string {
	return q.queryQuery
}

// Prefix returns the sqlite query fragment for getting a rows with a key prefix.
func (q Queries) Prefix() string {
	return q.prefixQuery
}

// Limit returns the sqlite query fragment for limiting results.
func (q Queries) Limit() string {
	return q.limitQuery
}

// Offset returns the sqlite query fragment for returning rows from a given offset.
func (q Queries) Offset() string {
	return q.offsetQuery
}

// GetSize returns the sqlite query for determining the size of a value.
func (q Queries) GetSize() string {
	return q.getSizeQuery
}

func NewSQLiteDatastore(driver, url, table string) (*sqlds.Datastore, error) {
	// Open test db
	db, err := sql.Open(driver, url)
	if err != nil {
		return nil, errors.Wrap(err, "open db")
	}

	// Create table if not exists
	_, err = db.Exec(formatSchema(table))
	if err != nil {
		return nil, errors.Wrap(err, "create table")
	}

	// Use sqlite queries
	queries := NewQueries(table)

	// Instantiate ds
	return sqlds.NewDatastore(db, queries), nil
}

func NewSQLCipherDatastore(driver, dbPath, table string, key []byte) (*sqlds.Datastore, error) {
	ds, err := NewSQLiteDatastore(driver, dbURLFromPath(dbPath, key), table)
	if err != nil {
		return nil, err
	}
	if err := ds.Close(); err != nil {
		return nil, err
	}
	return OpenSQLCipherDatastore(driver, dbPath, table, key)
}

var (
	ErrDatabaseNotFound = errors.New("database not found")
	ErrTableNotFound    = errors.New("table not found")
)

var columnsDef = []colDef{
	{"key", "TEXT PRIMARY KEY"},
	{"data", "BLOB"},
}

func formatSchema(table string) string {
	columns := make([]string, len(columnsDef))
	for i, c := range columnsDef {
		columns[i] = c.name + " " + c.kind
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	%s
) WITHOUT ROWID;
`, table, strings.Join(columns, ",\n\t"))
}

func OpenSQLCipherDatastore(driver, dbPath, table string, key []byte) (*sqlds.Datastore, error) {
	// Check db file
	_, err := os.Stat(dbPath)
	if os.IsNotExist(err) {
		return nil, ErrDatabaseNotFound
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to stat db file")
	}

	// Check db crypto
	if err := checkDBCrypto(dbPath, len(key) != 0); err != nil {
		return nil, err
	}

	// Open db conn
	db, err := sql.Open(driver, dbURLFromPath(dbPath, key))
	if err != nil {
		return nil, errors.Wrap(err, "open db")
	}

	// Check that the table exists
	has, err := hasTable(db, table)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read tables names")
	}
	if !has {
		return nil, ErrTableNotFound
	}

	// Check the table schema
	columns, err := schema.ColumnTypes(db, "", table)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read table columns")
	}
	for _, expectedCol := range columnsDef {
		found := false
		for _, col := range columns {
			if col.Name() == expectedCol.name {
				/*if col.DatabaseTypeName() != expectedCol.kind {
					return nil, fmt.Errorf("bad column '%s': expected '%s', got '%s'", expectedCol.name, expectedCol.kind, col.DatabaseTypeName())
				}*/
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("column '%s' not found", expectedCol.name)
		}
	}

	// Use sqlite queries
	queries := NewQueries(table)

	// Instantiate datastore
	return sqlds.NewDatastore(db, queries), nil
}

type colDef struct {
	name string
	kind string
}

func hasTable(db *sql.DB, name string) (bool, error) {
	tables, err := schema.TableNames(db)
	if err != nil {
		return false, err
	}
	for _, t := range tables {
		if t[1] == name {
			return true, nil
		}
	}
	return false, nil
}

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

func dbURLFromPath(dbPath string, key []byte) string {
	if len(key) != 0 {
		hexKey := hex.EncodeToString(key)
		return fmt.Sprintf("%s?_pragma_key=x'%s'&_pragma_cipher_page_size=4096", dbPath, hexKey)
	}
	return dbPath
}
