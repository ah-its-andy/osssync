package client

type ObjectIndexModel struct {
	Id       string `json:"id"`
	Name     string `json:"name"`
	FilePath string `json:"file_path"`
	FileName string `json:"file_name"`
	Size     int64  `json:"size"`
	Synced   string `json:"synced"`

	CRC64 string `json:"crc64"`
	MD5   string `json:"md5"`
}

func (e ObjectIndexModel) ID() string {
	return e.Id
}

func (ObjectIndexModel) TableName() string {
	return "object_index"
}
