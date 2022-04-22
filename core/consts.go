package core

type PropertyName string

const (
	PropertyName_ContentLength  PropertyName = "X-Content-Length"
	PropertyName_ContentName    PropertyName = "X-Content-Name"
	PropertyName_ContentMD5     PropertyName = "X-Content-MD5"
	PropertyName_ContentCRC32   PropertyName = "X-Content-CRC32"
	PropertyName_ContentModTime PropertyName = "X-Content-ModTime"
	PropertyName_ContentType    PropertyName = "X-Content-Type"
)

type FileType string

const (
	FileType_Physical FileType = "physical"
	FileType_AliOSS   FileType = "alioss"
)

const (
	Arg_Config          = "config"
	Arg_SourcePath      = "source_path"
	Arg_Provider        = "provider"
	Arg_BucketName      = "bucket_name"
	Arg_CredentialsFile = "credentials_file"
	Arg_Operation       = "operation"
	Arg_FullIndex       = "full_index"
	Arg_RemoteDir       = "remote_dir"
	Arg_Salt            = "salt"
	Arg_ChunkSizeMb     = "chunk_size_mb"
)
