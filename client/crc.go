package client

import (
	"fmt"
	"osssync/common/logging"
	"osssync/common/tracing"
	"osssync/core"
)

func CheckCRC64(src core.FileInfo, dest core.FileInfo) error {
	srcCrc, err := src.CRC64()
	if err != nil {
		return tracing.Error(err)
	}

	destCrc, err := dest.CRC64()
	if err != nil {
		return tracing.Error(err)
	}

	if srcCrc != destCrc {
		return tracing.Error(core.ErrCRC64NotMatch)
	}

	logging.Debug(fmt.Sprintf("CRC64 check passed: %s", src.Name()), nil)
	return nil
}
