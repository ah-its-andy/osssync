package core

import (
	"fmt"
	"osssync/common/config"
	"osssync/common/tracing"
)

func GetFile(fileType FileType, filePath string) (FileInfo, error) {
	switch fileType {
	case FileType_Physical:
		return OpenPhysicalFile(filePath)

	case FileType_AliOSS:
		credentialFilePath := config.RequireString(Arg_CredentialsFile)
		aliCfg := AliOSSConfig{}
		err := config.BindYaml(credentialFilePath, &aliCfg)
		if err != nil {
			return nil, tracing.Error(err)
		}
		bucketName := config.RequireString(Arg_BucketName)
		return OpenAliOSS(aliCfg, bucketName, filePath)

	default:
		return nil, fmt.Errorf("unknown file type: %s", fileType)
	}
}
