package client

import (
	"fmt"
	"osssync/common/config"
	"osssync/common/logging"
	"osssync/common/tracing"
	"osssync/core"
	"sync"
)

func Pull(srcPath string, destPath string) error {
	fileType := core.ResolveUriType(srcPath)

	if fileType == core.FileType_AliOSS {
		credentialFilePath := config.RequireString(core.Arg_CredentialsFile)
		aliCfg := core.AliOSSCfgWrapper{}
		err := config.BindYaml(credentialFilePath, &aliCfg)
		if err != nil {
			return tracing.Error(err)
		}
		bk, err := core.LsAliOss(aliCfg.Config, srcPath, "")
		if err != nil {
			return tracing.Error(err)
		}
		PullAliBucket(aliCfg.Config, bk, destPath)
	} else {
		logging.Info(fmt.Sprintf("File type %s is not supported for pull", fileType), nil)
	}

	return nil
}

func PullAliBucket(config core.AliOSSConfig, bucketInfo *core.BucketInfo, destPath string) {
	bkInfo := bucketInfo
	var wg sync.WaitGroup
	for {
		for _, objectInfo := range bucketInfo.Objects {
			basePath := objectInfo.BasePath
			relativePath := objectInfo.RelativePath
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := TransferFile(basePath, destPath, relativePath)
				if err != nil {
					logging.Error(err, nil)
				}
			}()
		}
		if !bkInfo.IsTruncated {
			break
		}
		bk, err := core.LsAliOss(config, bucketInfo.BasePath, bucketInfo.ContinueToken)
		if err != nil {
			logging.Error(err, nil)
		}
		bkInfo = bk
	}
	wg.Wait()
}
