package core

import (
	"fmt"
	"osssync/common/config"
	"osssync/common/tracing"
)

func GetFile(filePath string) (FileInfo, error) {
	fileType := ResolveUriType(filePath)
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
		bucketName, err := ResolveBucketName(filePath)
		if err != nil {
			return nil, tracing.Error(err)
		}
		objectName, err := ResolveRelativePath(filePath)
		if err != nil {
			return nil, tracing.Error(err)
		}
		sourceRoot := config.RequireString(Arg_SourcePath)
		relativePath := filePath[len(sourceRoot)+1:]

		return OpenAliOSS(aliCfg.Config, bucketName, fmt.Sprintf("%s/%s", objectName, relativePath))

	default:
		return nil, fmt.Errorf("unknown file type: %s", fileType)
	}
}
