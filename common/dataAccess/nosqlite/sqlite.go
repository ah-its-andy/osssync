package nosqlite

import (
	"context"
	"database/sql"
	"osssync/common/tracing"

	_ "github.com/logoove/sqlite"
)

var Factory *SQLiteConnFactory

func Init(dsn string) error {
	conn, err := sql.Open("sqlite3", dsn)
	defer conn.Close()
	if err != nil {
		return tracing.Error(err)
	}
	Factory = &SQLiteConnFactory{dsn: dsn}
	return nil
}

type SQLiteConnFactory struct {
	// sample : "file:locked.sqlite?cache=shared"
	dsn string
}

func (factory *SQLiteConnFactory) CreateConnection(ctx context.Context) (*sql.DB, error) {
	conn, err := sql.Open("sqlite3", factory.dsn)
	if err != nil {
		return nil, tracing.Error(err)
	}
	return conn, err
}
