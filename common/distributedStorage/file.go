package distributedstorage

import (
	"io"
	"os"
	"strings"
)

type DistributedFile interface {
	io.Writer
	io.Reader
	io.Closer

	WriteHeader(version int64, fileSize int64) error
	RebuildBlk() error
}

func OpenFile(path string, createIfNotExists bool) (*os.File, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			if createIfNotExists {
				dirPath := path[:strings.LastIndex(path, "/")]
				if err := os.MkdirAll(dirPath, 0755); err != nil {
					return nil, err
				}
				return os.Create(path)
			} else {
				return nil, os.ErrNotExist
			}
		} else {
			return nil, err
		}
	} else {
		return os.OpenFile(path, os.O_RDWR, 0755)
	}
}
