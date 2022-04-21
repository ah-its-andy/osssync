package logging

import (
	"database/sql"
	"fmt"
	"os"
	"osssync/common/config"
	"osssync/common/tracing"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.zfy.link/zfy-backend/ecosystem/nullable"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
)

var initOnce sync.Once
var path string
var fileName string
var maxAgeMinutes int64
var rotationTimeMinutes int64
var enableStdOut bool
var level string
var rotateWriter *rotatelogs.RotateLogs

func Init() {
	initOnce.Do(func() {
		err := initLogger()
		if err != nil {
			panic(err)
		}
	})
}

func PanicIfNotAvailable() {
	if rotateWriter == nil {
		panic("logging not available")
	}
}

func initLogger() error {
	config.PanicIfNotAvailable()

	path = config.GetStringOrDefault("logging.path", "/var/log")
	fileName = config.GetStringOrDefault("logging.fileName", "osssync.log")
	maxAgeMinutes = int64(config.GetValueOrDefault[float64]("logging.maxAge", 60*24*30))
	rotationTimeMinutes = int64(config.GetValueOrDefault[float64]("logging.rotationTime", 60))
	enableStdOut = config.GetValueOrDefault[bool]("logging.enableStdOut", true)
	level = config.GetStringOrDefault("logging.level", "error")

	logFileFullName := filepath.Join(path, fileName)
	writer, err := rotatelogs.New(
		logFileFullName+".%Y%m%d%H%M",
		rotatelogs.WithLinkName(path),
		//保留最近30天的日志
		//rotatelogs.WithMaxAge(time.Duration(720)*time.Hour),
		rotatelogs.WithMaxAge(time.Minute*time.Duration(maxAgeMinutes)),

		//rotatelogs.WithRotationTime(time.Duration(30)*time.Minute),
		rotatelogs.WithRotationTime(time.Minute*time.Duration(rotationTimeMinutes)),
	)
	if err != nil {
		return tracing.Error(err)
	}

	logrus.SetLevel(convertLevel(level))
	logrus.SetOutput(os.Stdout)
	rotateWriter = writer
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

	default:
		panic(fmt.Sprintf("unsupport convert %v type %s", v, reflect.TypeOf(v).Name()))
	}
}

func Print(level string, message string, err error, fields map[string]interface{}) {
	flats := make([]string, 0)
	if fields != nil {
		for k, v := range fields {
			flats = append(flats, k+"="+ConvertToStr(v))
		}
	}
	msg := message
	if err != nil {
		msg = message + " " + err.Error()
	}
	fmt.Printf("[%s] %s %s\n", level, msg, strings.Join(flats, " "))
}

func Write(level string, message string, err error, fields map[string]interface{}) {
	PanicIfNotAvailable()

	logFields := logrus.Fields{
		"level":   level,
		"error":   err,
		"message": message,
	}

	if fields != nil {
		for k, v := range fields {
			logFields[k] = v
		}
	}

	Print(level, message, err, fields)

	entry := logrus.WithFields(logFields)
	switch level {
	case "debug":
		entry.Debug()
	case "info":
		entry.Info()
	case "warning":
		entry.Warn()
	case "error":
		entry.Error()
	case "fatal":
		entry.Fatal()
	case "panic":
		entry.Panic()
		if err != nil {
			panic(err)
		} else {
			panic(message)
		}
	}
}

func Error(err error, data map[string]interface{}) {
	Write("error", "", err, data)
}

func Debug(message string, data map[string]interface{}) {
	Write("debug", message, nil, data)
}

func Info(message string, data map[string]interface{}) {
	Write("info", message, nil, data)
}

func convertLevel(level string) logrus.Level {
	switch level {
	case "trace":
		return logrus.TraceLevel
	case "debug":
		return logrus.DebugLevel
	case "info":
		return logrus.InfoLevel
	case "warning":
		return logrus.WarnLevel
	case "error":
		return logrus.ErrorLevel
	case "fatal":
		return logrus.FatalLevel
	case "panic":
		return logrus.PanicLevel
	}

	return logrus.ErrorLevel
}
