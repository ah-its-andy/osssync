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

	if statInfo, err := os.Stat(config.RequireString(core.Arg_TmpDir)); err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(config.RequireString(core.Arg_TmpDir), 0755)
			if err != nil {
				return tracing.Error(err)
			}
		} else {
			return tracing.Error(err)
		}
	} else {
		if !statInfo.IsDir() {
			return fmt.Errorf("%s is not a directory", config.RequireString(core.Arg_TmpDir))
		}
	}

	logging.Init()
	dbPath, _ := config.GetString(core.Arg_DbPath)
	if dbPath == "" {
		dbPath, _ = filepath.Abs("./")
		dbPath = core.JoinUri(dbPath, "osssync.db")
		logging.Info(fmt.Sprintf("db path not found, use default: %s", dbPath), nil)
	} else {
		if !filepath.IsAbs(dbPath) {
			dbPath, _ = filepath.Abs(dbPath)
		}
		dbPath = core.JoinUri(dbPath, "osssync.db")
		logging.Info(fmt.Sprintf("db path: %s", dbPath), nil)
	}
	err = nosqlite.Init(fmt.Sprintf("file:%s?cache=shared", dbPath))
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func Run() error {
	operation := config.RequireString(core.Arg_Operation)
	if operation == "generateKey" {
		_, err := core.PrintMnemonic()
		if err != nil {
			return tracing.Error(err)
		}
		return nil
	}

	sourcePath := config.RequireString(core.Arg_SourcePath)
	destPath := config.GetStringOrDefault(core.Arg_DestPath, "")

	switch operation {

	case "push":
		return client.PushDir(sourcePath,
			destPath,
			config.RequireValue[bool](core.Arg_FullIndex))

	case "pull":
		return client.Pull(sourcePath, destPath)

	case "sync":
		return fmt.Errorf("sync operation is not supported yet")

	default:
		return fmt.Errorf("unknown operation: %s", operation)
	}
	return nil
}
