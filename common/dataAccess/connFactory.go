package dataaccess

import (
	"context"
	"database/sql"
)


type ConnectionFactory interface {
	CreateConnection(ctx context.Context) (*sql.DB, error)
}
