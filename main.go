package main

import (
	"flag"
	"osssync/app"
	"osssync/common/config"
	"osssync/core"
	"path/filepath"
)

func main() {
	var args Args
	flag.StringVar(&args.Config, "config", "", "config file path")
	flag.StringVar(&args.SourcePath, "source", "", "source path")
	flag.StringVar(&args.Provider, "provider", "", "object storage service provider. e.g. alioss")
	flag.StringVar(&args.BucketName, "bucket", "", "bucket name")
	flag.StringVar(&args.CredentialsFile, "credentials", "", "credentials file")
	flag.StringVar(&args.RemoteDir, "remote", "", "remote directory, default as root")
	//flag.BoolVar(&args.IndexOnly, "indexOnly", false, "only index files")
	flag.BoolVar(&args.FullIndex, "fullIndex", false, "full index")
	flag.StringVar(&args.Salt, "salt", "", "salt")
	flag.Int64Var(&args.ChunkSizeMb, "chunkSize", 0, "chunk size in MB")
	flag.StringVar(&args.Operation, "operation", "", "[index, push, pull, sync]")
	flag.Parse()

	if args.SourcePath == "" {
		panic("source path is required")
	}

	if args.Operation != "index" {
		if args.Provider == "" {
			panic("provider is required")
		}

		if args.BucketName == "" {
			panic("bucket name is required")
		}

		if args.CredentialsFile == "" {
			panic("credentials file is required")
		}
	}

	absSourcePath := args.SourcePath
	if !filepath.IsAbs(absSourcePath) {
		absSourcePath, _ = filepath.Abs(args.SourcePath)
	}
	config.AttachValue(core.Arg_SourcePath, absSourcePath)

	if args.Operation != "index" {
		absCredentialsFile := args.CredentialsFile
		if !filepath.IsAbs(absCredentialsFile) {
			absCredentialsFile, _ = filepath.Abs(args.CredentialsFile)
		}
		config.AttachValue(core.Arg_CredentialsFile, absCredentialsFile)
	}

	if args.Config != "" {
		absCfgFilePath := args.Config
		if !filepath.IsAbs(absCfgFilePath) {
			absCfgFilePath, _ = filepath.Abs(args.Config)
		}
		config.AttachValue(core.Arg_Config, absCfgFilePath)
	}

	config.AttachValue(core.Arg_Provider, args.Provider)
	config.AttachValue(core.Arg_BucketName, args.BucketName)
	config.AttachValue(core.Arg_Operation, args.Operation)
	config.AttachValue(core.Arg_FullIndex, args.FullIndex)
	config.AttachValue(core.Arg_RemoteDir, args.RemoteDir)
	config.AttachValue(core.Arg_Salt, args.Salt)
	config.AttachValue(core.Arg_ChunkSizeMb, args.ChunkSizeMb)

	err := app.Startup()
	if err != nil {
		panic(err)
	}

	err = app.Run()
	if err != nil {
		panic(err)
	}
}

type Args struct {
	Config string

	SourcePath string

	Provider        string
	BucketName      string
	CredentialsFile string
	// IndexOnly       bool
	FullIndex   bool
	RemoteDir   string
	Salt        string
	ChunkSizeMb int64

	Operation string
	Daemon    bool
}
