package nosqlite

type NoSqliteEntity interface {
	ID() string
	TableName() string
}

type KV struct {
	K string
	V string
}
