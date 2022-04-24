package nosqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"osssync/common/tracing"
	"strings"
	"sync"
)

var ErrRecordNotFound error = errors.New("record not found")
var createTableLock = &sync.Mutex{}

func IfNoRows(err error) bool {
	return "sql: no rows in result set" == err.Error()
}

func CreateTableIfNotExists[T NoSqliteEntity]() error {
	createTableLock.Lock()
	defer createTableLock.Unlock()
	fake := *new(T)
	db, err := Factory.CreateConnection(context.Background())
	if err != nil {
		return tracing.Error(err)
	}
	defer db.Close()

	var tableNameValue string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE name=?", fake.TableName()).Scan(&tableNameValue)
	if err != nil && !IfNoRows(err) {
		return tracing.Error(err)
	}
	if tableNameValue == "" || tableNameValue != fake.TableName() {
		fields := []string{
			`"id" text(64) PRIMARY KEY`,
			`"name" text(255) NOT NULL`,
			`"data" TEXT`,
		}
		_, err = db.Exec(fmt.Sprintf(`CREATE TABLE "%s" (%s);`, fake.TableName(), strings.Join(fields, ",")))
		if err != nil {
			return tracing.Error(err)
		}
		_, err = db.Exec(fmt.Sprintf(`CREATE UNIQUE INDEX "%s_name_idx" ON "%s" ( "name" COLLATE BINARY ASC );`, fake.TableName(), fake.TableName()))
		if err != nil {
			return tracing.Error(err)
		}
		err = InitIndexTable(db, fake.TableName())
		if err != nil {
			return tracing.Error(err)
		}
	}

	return nil
}

func Set[T NoSqliteEntity](name string, data T, indexes ...KV) error {
	err := CreateTableIfNotExists[T]()
	if err != nil {
		return tracing.Error(err)
	}

	var payload string
	jsonData, err := json.Marshal(data)
	if err == nil {
		payload = string(jsonData)
	}

	db, err := Factory.CreateConnection(context.Background())
	if err != nil {
		return tracing.Error(err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return tracing.Error(err)
	}
	defer tx.Rollback()

	obj, err := Get[T](name)
	if err != nil && err != ErrRecordNotFound {
		return tracing.Error(err)
	}

	objID := GenerateUUID()
	sql := ""
	args := make([]interface{}, 0)
	if err == ErrRecordNotFound {
		sql = fmt.Sprintf(`INSERT INTO "%s" ("id", "name", "data") VALUES (?, ?, ?)`, (*new(T)).TableName())
		args = append(args, objID, name, payload)
	} else {
		sql = fmt.Sprintf(`UPDATE "%s" SET "data" = ? WHERE "id" = ?`, (*new(T)).TableName())
		args = append(args, payload, obj.ID())
		objID = obj.ID()
	}

	_, err = tx.Exec(sql, args...)
	if err != nil {
		return tracing.Error(err)
	}

	for _, index := range indexes {
		err = SetIndex[T](tx, objID, index.K, index.V)
		if err != nil {
			return tracing.Error(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return tracing.Error(err)
	}

	return nil
}

func Get[T NoSqliteEntity](name string) (T, error) {
	err := CreateTableIfNotExists[T]()
	if err != nil {
		return *new(T), tracing.Error(err)
	}

	db, err := Factory.CreateConnection(context.Background())
	if err != nil {
		return *new(T), tracing.Error(err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return *new(T), tracing.Error(err)
	}
	defer tx.Rollback()

	fake := *new(T)

	var payload string
	err = tx.QueryRow(fmt.Sprintf(`SELECT "data" FROM "%s" WHERE "name" = ?`, fake.TableName()), name).Scan(&payload)
	if err != nil {
		if IfNoRows(err) {
			return *new(T), ErrRecordNotFound
		}
		return *new(T), tracing.Error(err)
	}
	var item T
	err = json.Unmarshal([]byte(payload), &item)
	if err != nil {
		return *new(T), tracing.Error(err)
	}
	return item, nil
}

func GetByIndex[T NoSqliteEntity](indexes ...KV) ([]T, error) {
	err := CreateTableIfNotExists[T]()
	if err != nil {
		return nil, tracing.Error(err)
	}

	db, err := Factory.CreateConnection(context.Background())
	if err != nil {
		return nil, tracing.Error(err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		return nil, tracing.Error(err)
	}
	defer tx.Rollback()

	idxes, err := GetIndexes[T](tx, indexes...)
	if err != nil {
		if err == ErrRecordNotFound {
			return nil, ErrRecordNotFound
		}
		return nil, tracing.Error(err)
	}
	ids := make([]string, 0)
	for _, idx := range idxes {
		ids = append(ids, idx.ID)
	}

	sql := fmt.Sprintf(`SELECT "data" FROM "%s" WHERE "id" IN (?)`, (*new(T)).TableName())
	rows, err := tx.Query(sql, ids)
	if err != nil {
		if IfNoRows(err) {
			return nil, ErrRecordNotFound
		}
		return nil, tracing.Error(err)
	}

	items := make([]T, 0)
	for rows.Next() {
		var payload string
		err = rows.Scan(&payload)
		if err != nil {
			return nil, tracing.Error(err)
		}
		var item T
		err = json.Unmarshal([]byte(payload), &item)
		if err != nil {
			return nil, tracing.Error(err)
		}
		items = append(items, item)
	}

	return items, nil
}

func GetAll[T NoSqliteEntity]() (map[string]T, error) {
	err := CreateTableIfNotExists[T]()
	if err != nil {
		return make(map[string]T), tracing.Error(err)
	}

	db, err := Factory.CreateConnection(context.Background())
	if err != nil {
		return nil, tracing.Error(err)
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf(`SELECT "name","data" FROM "%s"`, (*new(T)).TableName()))
	if err != nil {
		if IfNoRows(err) {
			return make(map[string]T), ErrRecordNotFound
		}
		return nil, tracing.Error(err)
	}
	defer rows.Close()

	result := make(map[string]T)
	for rows.Next() {
		var name string
		var data string
		err = rows.Scan(&name, &data)
		if err != nil {
			return nil, tracing.Error(err)
		}
		var item T
		err = json.Unmarshal([]byte(data), &item)
		if err != nil {
			return nil, tracing.Error(err)
		}
		result[name] = item
	}

	return result, nil
}

func Remove[T NoSqliteEntity](name string) error {
	err := CreateTableIfNotExists[T]()
	if err != nil {
		return tracing.Error(err)
	}

	db, err := Factory.CreateConnection(context.Background())
	if err != nil {
		return tracing.Error(err)
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf(`DELETE FROM "%s" WHERE "name" = ?`, (*new(T)).TableName()), name)
	if err != nil {
		return tracing.Error(err)
	}

	return nil
}
