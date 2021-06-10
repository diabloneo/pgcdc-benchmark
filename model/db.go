package model

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// basicDB contains connection information to a database
var (
	registerDBs = make(map[string]*gorm.DB, 1)
	basicDB     *gorm.DB
	basicDBLock *sync.RWMutex
)

func init() {
	basicDBLock = new(sync.RWMutex)
}

// ConnectionArg contains information of a database connection
type ConnectionArg struct {
	Host            string
	Port            int
	User            string
	Password        string
	DBName          string
	SSLMode         string
	ConnMaxLifetime time.Duration
	MaxIdleConns    int
	MaxOpenConns    int

	// Replication determines whether the connection should use the replication protocol
	// instead of the normal protocol.
	//
	// Ref: https://www.postgresql.org/docs/13/libpq-connect.html
	Replication string
}

func (arg *ConnectionArg) String() string {
	return fmt.Sprintf("%s:%d", arg.Host, arg.Port)
}

// Copy returns a copy.
func (arg *ConnectionArg) Copy() *ConnectionArg {
	if arg == nil {
		return nil
	}
	newArg := *arg
	return &newArg
}

// DNS returns DNS string used by database driver.
func (arg *ConnectionArg) DSN() string {
	if arg.SSLMode == "" {
		arg.SSLMode = "disable"
	}

	newPart := func(k string, v interface{}) string {
		return fmt.Sprintf("%s=%v", k, v)
	}
	parts := []string{
		newPart("host", arg.Host),
		newPart("port", arg.Port),
		newPart("user", arg.User),
		newPart("password", arg.Password),
		newPart("dbname", arg.DBName),
		newPart("sslmode", arg.SSLMode),
	}
	if arg.Replication != "" {
		parts = append(parts, newPart("replication", arg.Replication))
	}

	return strings.Join(parts, " ")
}

// RegisterDB registers a database client.
func RegisterDB(name string, arg *ConnectionArg) (err error) {
	basicDBLock.Lock()
	defer basicDBLock.Unlock()

	basicDB := registerDBs[name]
	if basicDB != nil {
		defer func(oldDB *gorm.DB) {
			if e := closeDB(basicDB); e != nil {
				log.Printf("Failed to close old db connection: %s", e)
			}
		}(basicDB)
	}

	dsn := arg.DSN()
	basicDB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return errors.Wrap(err, "open database")
	}

	registerDBs[name] = basicDB

	return nil
}

func closeDB(db *gorm.DB) error {
	sqldb, err := db.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	if err = sqldb.Close(); err != nil {
		return errors.Wrap(err, "close old db connections")
	}
	return nil
}

// CloseDB close a registered client.
func CloseDB(name string) (err error) {
	basicDBLock.Lock()
	defer basicDBLock.Unlock()

	basicDB, ok := registerDBs[name]
	if !ok {
		return nil
	}
	delete(registerDBs, name)
	if basicDB == nil {
		return nil
	}

	if err = closeDB(basicDB); err != nil {
		return errors.Wrap(err, "close db")
	}

	return nil
}

// DB wraps gorm.DB object to provides more methods.
type DB struct {
	*gorm.DB
}

// Debug start debug mode.
func (db *DB) Debug() *DB {
	db.DB = db.DB.Debug()
	return db
}

// NewDB creates a DB object which copied from basicDB
func NewDB(name string) *DB {
	basicDBLock.RLock()
	defer basicDBLock.RUnlock()

	basicDB := registerDBs[name]
	if basicDB == nil {
		return nil
	}

	db := &DB{}
	db.DB = basicDB.Session(&gorm.Session{})

	return db
}

// WithDB creates a DB object with given gorm.DB object
func WithDB(tx *gorm.DB) *DB {
	db := &DB{}
	db.DB = tx
	return db
}
