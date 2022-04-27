package core

import (
	"fmt"
	"os/user"
	"osssync/common/config"
	"osssync/common/tracing"
	"path/filepath"
	"strings"
)

func GetFile(dirPath string, relativePath string) (fileInfo FileInfo, err error) {
	fileType := ResolveUriType(dirPath)
	switch fileType {
	case FileType_Physical:
		fileInfo, err = OpenPhysicalFile(absFilePath(dirPath), relativePath)

	case FileType_AliOSS:
		credentialFilePath := config.RequireString(Arg_CredentialsFile)
		aliCfg := AliOSSCfgWrapper{}
		err := config.BindYaml(credentialFilePath, &aliCfg)
		if err != nil {
			return nil, tracing.Error(err)
		}
		bucketName, err := ResolveBucketName(dirPath)
		if err != nil {
			return nil, tracing.Error(err)
		}
		objectName, err := ResolveRelativePath(dirPath)
		if err != nil {
			return nil, tracing.Error(err)
		}

		fileInfo, err = OpenAliOSS(aliCfg.Config, bucketName, objectName, relativePath)

	default:
		return nil, fmt.Errorf("unknown file type: %s", fileType)
	}

	if err != nil {
		return nil, tracing.Error(err)
	}
	return fileInfo, nil
}

func absFilePath(p string) string {
	if strings.HasPrefix(p, "~/") {
		usr, err := user.Current()
		if err != nil {
			panic(err)
		}

		return JoinUri(usr.HomeDir, p[2:])
	}
	if filepath.IsAbs(p) {
		return p
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		panic(err)
	}
	return abs
}
