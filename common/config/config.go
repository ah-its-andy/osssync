package config

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"osssync/common/tracing"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.zfy.link/zfy-backend/ecosystem/nullable"
	"gopkg.in/yaml.v2"
	"gorm.io/gorm"
)

var rootPath string
var data map[string]interface{}
var initDataOnce sync.Once

func SetRootPath(path string) {
	rootPath = path
}

func GetRootPath() string {
	return rootPath
}

func IsAvailable() bool {
	return data != nil
}

func PanicIfNotAvailable() {
	if !IsAvailable() {
		panic("config is not available")
	}
}

func Init() {
	initDataOnce.Do(func() {
		if data == nil {
			data = make(map[string]interface{})
		}
		err := readFromRoot()
		if err != nil {
			panic(err)
		}
	})
}

func AttachFile(filePath string) error {
	err := readFromFile(filePath)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func AttachValue(k string, v interface{}) {
	if data == nil {
		data = make(map[string]interface{})
	}
	data[k] = v
}

func BindYaml(filePath string, receiver any) error {
	f, err := os.Open(filePath)
	if err != nil {
		return tracing.Error(err)
	}
	defer f.Close()

	buffer, err := ioutil.ReadAll(f)
	if err != nil {
		return tracing.Error(err)
	}

	err = yaml.Unmarshal(buffer, receiver)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func findPath(path string) (interface{}, bool) {
	PanicIfNotAvailable()
	ks := strings.Split(path, ".")
	if len(ks) == 0 {
		return nil, false
	}
	return findV(ks, 0, data)
}

func findV(ks []string, index int, data interface{}) (interface{}, bool) {
	if index >= len(ks) {
		return data, true
	}
	k := ks[index]
	if reflect.TypeOf(data).Kind() == reflect.Map {
		m := data.(map[string]interface{})
		v, ok := m[k]
		if !ok {
			return nil, false
		}
		return findV(ks, index+1, v)
	} else if reflect.TypeOf(data).Kind() == reflect.Slice {
		s := data.([]interface{})
		if len(s) <= 0 {
			return nil, false
		}
		return findV(ks, index, s[0])
	} else {
		return data, true
	}
}

func GetValue[T any](k string) (T, bool) {
	v, ok := findPath(k)
	if !ok {
		return *(new(T)), false
	}
	if tv, ok := v.(T); ok {
		return tv, true
	} else {
		return *(new(T)), false
	}
}

func GetString(k string) (string, bool) {
	v, ok := findPath(k)
	if !ok {
		return "", false
	}
	return ConvertToStr(v), true
}

func RequireValue[T any](k string) T {
	v, ok := GetValue[T](k)
	if !ok {
		panic(fmt.Sprintf("config value %s is required", k))
	}
	return v
}

func RequireString(k string) string {
	v, ok := GetString(k)
	if !ok {
		panic(fmt.Sprintf("config value %s is required", k))
	}
	return v
}

func GetValueOrDefault[T any](k string, defaultValue T) T {
	v, ok := GetValue[T](k)
	if !ok {
		return defaultValue
	}
	return v
}

func GetStringOrDefault(k string, defaultValue string) string {
	v, ok := GetString(k)
	if !ok {
		return defaultValue
	}
	return v
}

func readFromRoot() error {
	rds, err := os.ReadDir(rootPath)
	if err != nil {
		return tracing.Error(err)
	}
	for _, rd := range rds {
		if rd.IsDir() || (!strings.HasSuffix(rd.Name(), ".yaml")) {
			continue
		}
		err = readFromFile(filepath.Join(rootPath, rd.Name()))
		if err != nil {
			return tracing.Error(err)
		}
	}
	return nil
}

func readFromFile(filePath string) error {
	if data == nil {
		data = make(map[string]interface{})
	}
	f, err := os.Open(filePath)
	if err != nil {
		return tracing.Error(err)
	}
	defer f.Close()

	buffer, err := ioutil.ReadAll(f)
	if err != nil {
		return tracing.Error(err)
	}

	values := make(map[string]interface{})
	err = yaml.Unmarshal(buffer, &values)
	if err != nil {
		return tracing.Error(err)
	}

	for k, v := range values {
		data[k] = v
	}

	return nil
}

func ConvertToStr(v interface{}) string {
	if v == nil {
		return ""
	}
	switch v.(type) {
	case string:
		return v.(string)
	case int:
		return strconv.Itoa(v.(int))
	case int64:
		return strconv.FormatInt(v.(int64), 10)
	case int32:
		return strconv.Itoa(int(v.(int32)))
	case int16:
		return strconv.Itoa(int(v.(int16)))
	case int8:
		return strconv.Itoa(int(v.(int8)))
	case uint:
		return strconv.FormatUint(uint64(v.(uint)), 10)
	case uint64:
		return strconv.FormatUint(v.(uint64), 10)
	case uint32:
		return strconv.FormatUint(uint64(v.(uint32)), 10)
	case uint16:
		return strconv.FormatUint(uint64(v.(uint16)), 10)
	case uint8:
		return strconv.FormatUint(uint64(v.(uint8)), 10)
	case float32:
		return strconv.FormatFloat(float64(v.(float32)), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v.(float64), 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v.(bool))
	case time.Time:
		return v.(time.Time).Format("2006-01-02 15:04:05")
	case sql.NullString:
		nv := v.(sql.NullString)
		return nullable.GetString(nv, "")
	case sql.NullBool:
		nv := v.(sql.NullBool)
		if nv.Valid {
			return strconv.FormatBool(nv.Bool)
		} else {
			return ""
		}

	case sql.NullInt64:
		nv := v.(sql.NullInt64)
		if nv.Valid {
			return strconv.FormatInt(nv.Int64, 10)
		} else {
			return ""
		}

	case sql.NullInt32:
		nv := v.(sql.NullInt32)
		if nv.Valid {
			return strconv.Itoa(int(nv.Int32))
		} else {
			return ""
		}

	case sql.NullInt16:
		nv := v.(sql.NullInt16)
		if nv.Valid {
			return strconv.Itoa(int(nv.Int16))
		} else {
			return ""
		}

	case sql.NullTime:
		nv := v.(sql.NullTime)
		if nv.Valid {
			return nv.Time.Format("2006-01-02 15:04:05")
		} else {
			return ""
		}

	case sql.NullFloat64:
		nv := v.(sql.NullFloat64)
		if nv.Valid {
			return strconv.FormatFloat(nv.Float64, 'f', -1, 64)
		} else {
			return ""
		}

	case gorm.DeletedAt:
		nv := v.(gorm.DeletedAt)
		if nv.Valid {
			return nv.Time.Format("2006-01-02 15:04:05")
		} else {
			return ""
		}

	default:
		panic(fmt.Sprintf("unsupport convert %v type %s", v, reflect.TypeOf(v).Name()))
	}
}
