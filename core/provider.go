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
		aliCfg := AliOSSCfgWrapper{}
		err := config.BindYaml(credentialFilePath, &aliCfg)
		if err != nil {
			return nil, tracing.Error(err)
		}
		bucketName := config.RequireString(Arg_BucketName)

		sourceRoot := config.RequireString(Arg_SourcePath)
		relativePath := filePath[len(sourceRoot)+1:]

		return OpenAliOSS(aliCfg.Config, bucketName, fmt.Sprintf("%s/%s", config.RequireString(Arg_RemoteDir), relativePath))

	default:
		return nil, fmt.Errorf("unknown file type: %s", fileType)
	}
}
