package client

import (
	"fmt"
	"hash/crc64"
	"osssync/common/dataAccess/nosqlite"
	"osssync/common/logging"
	"osssync/core"
	"strconv"
	"strings"
)

func FindFileIndex(fileInfo core.FileInfo) (*ObjectIndexModel, error) {
	fileType := fileInfo.FileType()
	fileName := fileInfo.Name()
	fileSize := fileInfo.Size()
	fileLastModifyTime := fileInfo.Properties()[core.PropertyName_ContentModTime]
	indexName := ComputeIndexName(fileType, fileName, strconv.FormatInt(fileSize, 10), fileLastModifyTime)
	indexModel, err := nosqlite.Get[ObjectIndexModel](indexName)
	if err != nil {
		return nil, err
	}
	return &indexModel, nil
}

type ObjectIndexModel struct {
	Id             string `json:"id"`
	Name           string `json:"name"`
	FileType       string `json:"file_type"`
	FilePath       string `json:"file_path"`
	FileName       string `json:"file_name"`
	Size           int64  `json:"size"`
	LastModifyTime string `json:"last_modify_time"`

	CRC64 string `json:"crc64"`
}

func (e ObjectIndexModel) ID() string {
	return e.Id
}

func (ObjectIndexModel) TableName() string {
	return "object_index"
}

func ComputeIndexName(fileType, name, size, modifyTime string) string {
	args := []string{
		fileType, name, size, modifyTime,
	}
	payload := strings.Join(args, ",")
	crc64Hash := crc64.New(crc64.MakeTable(crc64.ISO))
	crc64Hash.Write([]byte(payload))
	return strconv.FormatUint(crc64Hash.Sum64(), 10)
}

func SetIndexModel(src core.FileInfo, dest core.FileInfo, crc64 uint64) error {
	fileIndex := &ObjectIndexModel{}
	fileIndex.Id = nosqlite.GenerateUUID()
	fileIndex.Name = ComputeIndexName(src.FileType(), src.Name(), strconv.FormatInt(src.Size(), 10), src.Properties()[core.PropertyName_ContentModTime])
	fileIndex.CRC64 = strconv.FormatUint(crc64, 10)
	fileIndex.Size = src.Size()
	fileIndex.LastModifyTime = src.Properties()[core.PropertyName_ContentModTime]
	fileIndex.FileName = src.Name()
	fileIndex.FileType = dest.FileType()
	fileIndex.FilePath = dest.Path()
	err := nosqlite.Set(fileIndex.Name, *fileIndex,
		nosqlite.KV{
			K: string(core.PropertyName_ContentCRC64),
			V: fileIndex.CRC64,
		},
		nosqlite.KV{
			K: "fileType",
			V: dest.FileType(),
		})
	if err != nil {
		logging.Warn(fmt.Sprintf("failed to set file index: %s", err.Error()), nil)
	}
	return nil
}
