package core

import (
	"fmt"
	"strings"
)

func ResolveUriType(uri string) FileType {
	if strings.HasPrefix(uri, "oss://") {
		return FileType_AliOSS
	}
	return FileType_Physical
}

func ResolveBucketName(uri string) (string, error) {
	indexOf := strings.Index(uri, "://")
	if indexOf == -1 {
		return "", fmt.Errorf("invalid uri: %s", uri)
	}
	lastIndexOf := strings.Index(uri[indexOf+3:], "/")

	if lastIndexOf == -1 {
		return "", fmt.Errorf("invalid uri: %s", uri)
	}

	return uri[indexOf+3 : indexOf+3+lastIndexOf], nil
}

func ResolveRelativePath(uri string) (string, error) {
	indexOf := strings.Index(uri, "://")
	if indexOf == -1 {
		return "", fmt.Errorf("invalid uri: %s", uri)
	}
	lastIndexOf := strings.LastIndex(uri[indexOf+3:], "/")

	if lastIndexOf == -1 {
		return "", fmt.Errorf("invalid uri: %s", uri)
	}

	return uri[indexOf+3+lastIndexOf+1:], nil
}
