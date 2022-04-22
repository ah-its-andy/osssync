package app

import (
	"fmt"
	"os"
	"osssync/client"
	"osssync/common/config"
	"osssync/common/dataAccess/nosqlite"
	"osssync/common/logging"
	"osssync/common/tracing"
	"osssync/core"
	"path/filepath"
)

func Startup() error {
	configFilePath, _ := config.GetString(core.Arg_Config)
	if configFilePath == "" {
		configFilePath, _ = filepath.Abs("./")
	}
	statInfo, err := os.Stat(configFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("config file not found: %s", configFilePath)
		}
		return tracing.Error(err)
	}
	if statInfo.IsDir() {
		config.SetRootPath(configFilePath)
		config.Init()
	} else {
		err = config.AttachFile(configFilePath)
		if err != nil {
			return tracing.Error(err)
		}
	}

	logging.Init()
	dbPath, _ := config.GetString("db.path")
	if dbPath == "" {
		dbPath, _ = filepath.Abs("./")
		dbPath = filepath.Join(dbPath, "osssync.db")
		logging.Info(fmt.Sprintf("db path not found, use default: %s", dbPath), nil)
	} else {
		if !filepath.IsAbs(dbPath) {
			dbPath, _ = filepath.Abs(dbPath)
		}
	}
	err = nosqlite.Init(fmt.Sprintf("file:%s?cache=shared", dbPath))
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func Run() error {
	sourcePath := config.RequireString(core.Arg_SourcePath)
	statInfo, err := os.Stat(sourcePath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source path not found: %s", sourcePath)
		}
		return tracing.Error(err)
	}
	if statInfo.IsDir() {
		operation := config.RequireString(core.Arg_Operation)
		switch operation {
		case "index":
			return client.IndexDir(sourcePath, config.RequireValue[bool](core.Arg_FullIndex))

		case "push":
			return client.PushDir(sourcePath,
				core.FileType(config.RequireString(core.Arg_Provider)),
				config.RequireValue[bool](core.Arg_FullIndex))

		case "pull":
			return fmt.Errorf("pull operation is not supported yet")

		case "sync":
			return fmt.Errorf("sync operation is not supported yet")

		default:
			return fmt.Errorf("unknown operation: %s", operation)
		}

	} else {
		return fmt.Errorf("source path is not a directory: %s", sourcePath)
	}
	return nil
}
