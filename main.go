package main

import (
	"flag"
	"os/user"
	"osssync/app"
	"osssync/common/config"
	"osssync/core"
	"path/filepath"
	"strings"
)

func main() {
	var args Args
	flag.StringVar(&args.Config, "config", "", "config file path")
	flag.StringVar(&args.SourcePath, "source", "", "source path")
	//flag.StringVar(&args.Provider, "provider", "", "object storage service provider. e.g. alioss")
	flag.StringVar(&args.DestPath, "dest", "", "dest path")
	flag.StringVar(&args.CredentialsFile, "credentials", "", "credentials file")
	//flag.BoolVar(&args.IndexOnly, "indexOnly", false, "only index files")
	flag.BoolVar(&args.FullIndex, "fullIndex", false, "full index")
	//flag.StringVar(&args.Salt, "salt", "", "salt")
	flag.Int64Var(&args.ChunkSizeMb, "chunkSize", 0, "chunk size in MB")
	flag.StringVar(&args.Operation, "operation", "", "[index, push, pull, sync]")
	flag.Parse()

	if args.SourcePath == "" {
		panic("source path is required")
	}

	if args.Operation != "index" {
		if args.DestPath == "" {
			panic("DestPath is required")
		}
	}

	absSourcePath := args.SourcePath
	if !filepath.IsAbs(absSourcePath) {
		absSourcePath, _ = filepath.Abs(args.SourcePath)
	}
	config.AttachValue(core.Arg_SourcePath, absSourcePath)

	if args.Operation != "index" {
		config.AttachValue(core.Arg_CredentialsFile, absFilePath(args.CredentialsFile))
	}

	if args.Config != "" {
		config.AttachValue(core.Arg_Config, absFilePath(args.Config))
	}

	if args.DestPath != "" {
		config.AttachValue(core.Arg_DestPath, absFilePath(args.DestPath))
	}

	config.AttachValue(core.Arg_Operation, args.Operation)
	config.AttachValue(core.Arg_FullIndex, args.FullIndex)
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

	DestPath        string
	CredentialsFile string
	// IndexOnly       bool
	FullIndex   bool
	Salt        string
	ChunkSizeMb int64

	Operation string
	Daemon    bool
}

func absFilePath(p string) string {
	if strings.HasPrefix(p, "~/") {
		usr, err := user.Current()
		if err != nil {
			panic(err)
		}

		return filepath.Join(usr.HomeDir, p[2:])
	}
	if filepath.IsAbs(p) {
		return p
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		panic(err)
	}
	return abs
}
