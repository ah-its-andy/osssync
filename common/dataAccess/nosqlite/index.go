package nosqlite

import (
	"database/sql"
	"fmt"
	"osssync/common/tracing"
	"strings"
)

type DynamicIndexModel struct {
	ID         string
	ObjectID   string
	FieldName  string
	FieldValue string
}

func InitIndexTable(db *sql.DB, tableName string) error {
	createSql := fmt.Sprintf(`CREATE TABLE "%s_dyanmicidx" ("id" text(64),
		"object_id" text(64) NOT NULL,
		"field_name" TEXT(255) NOT NULL,
		"field_value" TEXT(1024) NOT NULL,
		PRIMARY KEY ("id"),
		CONSTRAINT "dynamic_index_unikey" UNIQUE ("object_id" COLLATE BINARY ASC, "field_name" COLLATE BINARY ASC)
	  );`, tableName)
	indexSql := fmt.Sprintf(`CREATE INDEX "%s_dyanmicidx_idx"
	ON "%s_dyanmicidx" (
	  "field_name" COLLATE BINARY ASC,
	  "field_value" COLLATE BINARY ASC
	);`, tableName, tableName)
	_, err := db.Exec(createSql)
	if err != nil {
		return tracing.Error(err)
	}
	_, err = db.Exec(indexSql)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func GetIndexes[T NoSqliteEntity](db *sql.Tx, filters ...KV) ([]DynamicIndexModel, error) {
	tableName := (*new(T)).TableName()

	sql := fmt.Sprintf(`SELECT "id", "object_id", "field_name", "field_value" FROM "%s_dyanmicidx" AS t0 `, tableName)
	filterSql := []string{
		`t0."field_name" = ?`,
		`t0."field_value" = ?`,
	}

	if len(filters) > 1 {
		joins := make([]string, 0)
		for i := 1; i < len(filters); i++ {
			joinSql := fmt.Sprintf(`INNER JOIN "%s_dyanmicidx" AS t%d ON t0."object_id" = t%d."object_id"`, tableName, i, i)
			filterSql = append(filterSql, fmt.Sprintf(`t%d."field_name" = ?`, i))
			filterSql = append(filterSql, fmt.Sprintf(`t%d."field_value" = ?`, i))
			joins = append(joins, joinSql)
		}
		joinSql := strings.Join(joins, " ")
		sql = fmt.Sprintf(`%s %s`, sql, joinSql)
	}

	whereSql := strings.Join(filterSql, " AND ")
	sql = fmt.Sprintf(`%s WHERE %s`, sql, whereSql)
	args := make([]interface{}, 0)
	for _, filter := range filters {
		args = append(args, filter.K, filter.V)
	}

	rows, err := db.Query(sql, args...)
	if err != nil {
		if IfNoRows(err) {
			return nil, ErrRecordNotFound
		}
		return nil, tracing.Error(err)
	}
	defer rows.Close()
	var result []DynamicIndexModel
	for rows.Next() {
		var model DynamicIndexModel
		err = rows.Scan(&model.ID, &model.ObjectID, &model.FieldName, &model.FieldValue)
		if err != nil {
			return nil, tracing.Error(err)
		}
		result = append(result, model)
	}
	return result, nil
}

func SetIndex[T NoSqliteEntity](db *sql.Tx, objId string, name string, value string) error {
	tableName := (*new(T)).TableName()

	sql := ""
	args := make([]interface{}, 0)
	indexes, err := GetIndexes[T](db, KV{K: name, V: value})
	if err != nil {
		if tracing.IsError(err, ErrRecordNotFound) {
			sql = fmt.Sprintf(`INSERT INTO "%s_dyanmicidx" ("id", "object_id", "field_name", "field_value") VALUES (?, ?, ?, ?)`, tableName)
			args = []interface{}{GenerateUUID(), objId, name, value}
		}
		return tracing.Error(err)
	}
	var index *DynamicIndexModel
	for _, idx := range indexes {
		if idx.ObjectID == objId {
			index = &idx
		}
	}

	if index == nil {
		sql = fmt.Sprintf(`INSERT INTO "%s_dyanmicidx" ("id", "object_id", "field_name", "field_value") VALUES (?, ?, ?, ?)`, tableName)
		args = []interface{}{GenerateUUID(), objId, name, value}
	} else {
		sql = fmt.Sprintf(`UPDATE "%s_dyanmicidx" SET "field_value" = ? WHERE "id" = ?`, tableName)
		args = []interface{}{value, index.ID}
	}
	_, err = db.Exec(sql, args...)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func RemoveIndex[T NoSqliteEntity](db *sql.Tx, objId string) error {
	tableName := (*new(T)).TableName()
	_, err := db.Exec(fmt.Sprintf(`DELETE FROM "%s_dyanmicidx" WHERE "object_id" = ?`, tableName), objId)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}
