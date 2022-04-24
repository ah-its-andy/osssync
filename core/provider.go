package core

import (
	"fmt"
	"os/user"
	"osssync/common/config"
	"osssync/common/tracing"
	"path/filepath"
	"strings"
)

func GetFile(filePath string) (FileInfo, error) {
	fileType := ResolveUriType(filePath)
	switch fileType {
	case FileType_Physical:
		return OpenPhysicalFile(absFilePath(filePath))

	case FileType_AliOSS:
		credentialFilePath := config.RequireString(Arg_CredentialsFile)
		aliCfg := AliOSSCfgWrapper{}
		err := config.BindYaml(credentialFilePath, &aliCfg)
		if err != nil {
			return nil, tracing.Error(err)
		}
		bucketName, err := ResolveBucketName(filePath)
		if err != nil {
			return nil, tracing.Error(err)
		}
		objectName, err := ResolveRelativePath(filePath)
		if err != nil {
			return nil, tracing.Error(err)
		}

		return OpenAliOSS(aliCfg.Config, bucketName, objectName)

	default:
		return nil, fmt.Errorf("unknown file type: %s", fileType)
	}
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
