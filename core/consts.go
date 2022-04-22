package core

type PropertyName string

const (
	PropertyName_ContentLength  PropertyName = "x-content-length"
	PropertyName_ContentName    PropertyName = "x-content-name"
	PropertyName_ContentMD5     PropertyName = "x-content-md5"
	PropertyName_ContentCRC32   PropertyName = "x-content-crc32"
	PropertyName_ContentModTime PropertyName = "x-content-modtime"
	PropertyName_ContentType    PropertyName = "x-content-type"
)

type FileType string

const (
	FileType_Physical FileType = "physical"
	FileType_AliOSS   FileType = "alioss"
)

const (
	Arg_Config          = "config"
	Arg_SourcePath      = "source_path"
	Arg_DestPath        = "dest_path"
	Arg_CredentialsFile = "credentials_file"
	Arg_Operation       = "operation"
	Arg_FullIndex       = "full_index"
	Arg_Salt            = "salt"
	Arg_ChunkSizeMb     = "chunk_size_mb"
)
