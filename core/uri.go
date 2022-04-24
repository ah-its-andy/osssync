package core

import (
	"fmt"
	"strings"
)

func JoinUri(a ...string) string {
	parts := make([]string, len(a))
	for i, part := range a {
		p := part
		if i > 0 {
			p = strings.TrimPrefix(p, "/")
		}
		p = strings.TrimSuffix(p, "/")
		parts[i] = p
	}
	return strings.Join(parts, "/")
}

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
	path := uri[indexOf+3:]
	indexOf2 := strings.Index(path, "/")
	return uri[indexOf+indexOf2+4:], nil
}
