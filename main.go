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
	flag.StringVar(&args.DbPath, "db", "", "db path")
	flag.StringVar(&args.Password, "password", "", "password")
	flag.StringVar(&args.Mnemonic, "mnemonic", "", "mnemonic")
	flag.BoolVar(&args.Zip, "zip", false, "compress files to zip")
	flag.StringVar(&args.TmpDir, "tmpDir", "./.tmp", "tmp dir")
	flag.Parse()

	config.AttachValue(core.Arg_SourcePath, absFilePath(args.SourcePath))
	config.AttachValue(core.Arg_CredentialsFile, absFilePath(args.CredentialsFile))
	config.AttachValue(core.Arg_Config, absFilePath(args.Config))
	config.AttachValue(core.Arg_Config, absFilePath(args.Config))
	config.AttachValue(core.Arg_DestPath, absFilePath(args.DestPath))
	config.AttachValue(core.Arg_Operation, args.Operation)
	config.AttachValue(core.Arg_FullIndex, args.FullIndex)
	config.AttachValue(core.Arg_ChunkSizeMb, args.ChunkSizeMb)
	config.AttachValue(core.Arg_DbPath, absFilePath(args.DbPath))
	config.AttachValue(core.Arg_Zip, args.Zip)
	config.AttachValue(core.Arg_Password, args.Password)
	config.AttachValue(core.Arg_Mnemonic, strings.TrimPrefix(strings.TrimSuffix(args.Mnemonic, "'"), "'"))
	config.AttachValue(core.Arg_TmpDir, absFilePath(args.TmpDir))

	if args.Operation != "generateKey" {
		if config.GetStringOrDefault(core.Arg_SourcePath, "") == "" {
			panic("source path is required")
		}

		if config.GetStringOrDefault(core.Arg_DestPath, "") == "" {
			panic("DestPath is required")
		}
	}

	err := app.Startup()
	if err != nil {
		panic(err)
	}

	// print config
	config.Print()

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

	DbPath string

	Password string
	Mnemonic string

	Zip    bool
	TmpDir string
}

func absFilePath(p string) string {
	if p == "" {
		return p
	}
	if strings.Contains(p, "://") {
		return p
	}
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
