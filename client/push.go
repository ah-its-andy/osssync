package client

import (
	"fmt"
	"os"
	"osssync/common/config"
	"osssync/common/logging"
	"osssync/common/tracing"
	"osssync/core"
	"strings"
	"sync"
)

var ErrIndexedAlready error = fmt.Errorf("indexed already")
var ErrObjectExists error = fmt.Errorf("object exists")
var ErrSyncedAlready error = fmt.Errorf("synced already")

func PushFile(srcPath string, dstPath string, relativePath string, fullIndex bool) error {
	err := TransferFile(srcPath, dstPath, relativePath)
	if err != nil {
		return tracing.Error(err)
	}
	return nil
}

func PushDir(path string, destPath string, fullIndex bool) error {
	rds, err := os.ReadDir(path)
	if err != nil {
		return tracing.Error(err)
	}

	if len(rds) == 0 {
		logging.Info(fmt.Sprintf("Directory %s is empty", path), nil)
		return nil
	}

	sourcePath := config.RequireString(core.Arg_SourcePath)
	var wg sync.WaitGroup
	for _, rd := range rds {
		rdName := rd.Name()
		if strings.HasPrefix(rdName, ".") {
			logging.Info(fmt.Sprintf("Ignore file %s", rdName), nil)
			continue
		}
		if rd.IsDir() {
			subPath := core.JoinUri(path, rd.Name())
			logging.Info(fmt.Sprintf("Enter directory %s", subPath), nil)
			err = PushDir(subPath, destPath, fullIndex)
			if err != nil {
				return tracing.Error(err)
			}
			continue
		}
		filePath := core.JoinUri(path, rd.Name())
		relativePath := strings.TrimPrefix(filePath, sourcePath)

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := PushFile(sourcePath, destPath, relativePath, fullIndex)
			if err != nil {
				if tracing.IsError(ErrSyncedAlready, err) {
					logging.Debug(fmt.Sprintf("File [%s] has been synced already", relativePath), nil)
				} else if tracing.IsError(err, ErrObjectExists) {
					logging.Debug(fmt.Sprintf("File [%s] exists at remote storage provider", relativePath), nil)
				} else {
					logging.Error(err, nil)
				}
			} else {
				logging.Info(fmt.Sprintf("File [%s] successfully synced", relativePath), nil)
			}
		}()
	}
	wg.Wait()
	return nil
}
