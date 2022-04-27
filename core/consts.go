package core

import "fmt"

type PropertyName string

const (
	PropertyName_ContentLength  PropertyName = "x-content-length"
	PropertyName_ContentName    PropertyName = "x-content-name"
	PropertyName_ContentMD5     PropertyName = "x-content-md5"
	PropertyName_ContentCRC64   PropertyName = "x-content-CRC64"
	PropertyName_ContentModTime PropertyName = "x-content-modtime"
	PropertyName_ContentType    PropertyName = "x-content-type"
)

type FileType string

const (
	FileType_Physical FileType = "physical"
	FileType_AliOSS   FileType = "alioss"
)

const (
	Arg_Config          = "OSY_CONFIG_PATH"
	Arg_SourcePath      = "OSY_SOURCE_PATH"
	Arg_DestPath        = "OSY_DEST_PATH"
	Arg_CredentialsFile = "OSY_CREDENTIALS"
	Arg_Operation       = "OSY_OPERATION"
	Arg_FullIndex       = "OSY_FULL_INDEX"
	Arg_ChunkSizeMb     = "OSY_CHUNK_SIZE_MB"
	Arg_DbPath          = "OSY_DB_PATH"
	Arg_Zip             = "OSY_ZIP"
	Arg_Password        = "OSY_PASSWORD"
	Arg_Mnemonic        = "OSY_MNEMONIC"
	Arg_TmpDir          = "OSY_TMP_DIR"
)

var ErrCRC64NotMatch error = fmt.Errorf("crc64 not match")
var ErrIndexOutOfRange error = fmt.Errorf("index out of range")
