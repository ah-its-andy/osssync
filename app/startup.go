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
		if config.RequireValue[bool](core.Arg_IndexOnly) {
			err = client.IndexDir(sourcePath, config.RequireValue[bool](core.Arg_FullIndex))
		} else {
			err = client.SyncDir(sourcePath,
				core.FileType(config.RequireString(core.Arg_Provider)),
				config.RequireValue[bool](core.Arg_FullIndex))
		}
		if err != nil {
			return tracing.Error(err)
		}
	} else {
		return fmt.Errorf("source path is not a directory: %s", sourcePath)
	}
	return nil
}
