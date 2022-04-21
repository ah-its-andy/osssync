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
	flag.BoolVar(&args.IndexOnly, "indexOnly", false, "only index files")
	flag.BoolVar(&args.FullIndex, "fullIndex", false, "full index")
	flag.Parse()

	if args.SourcePath == "" {
		panic("source path is required")
	}

	if !args.IndexOnly {
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

	if !args.IndexOnly {
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
	config.AttachValue(core.Arg_IndexOnly, args.IndexOnly)
	config.AttachValue(core.Arg_FullIndex, args.FullIndex)

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
	IndexOnly       bool
	FullIndex       bool
}
