package distributedstorage

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestDFileV5(t *testing.T) {
	srcFilePath := "/mnt/c/test/1.bmp"
	destDir := "/mnt/c/test/"
	srcFile, err := os.Open(srcFilePath)
	if err != nil {
		t.Error(err)
	}
	defer srcFile.Close()
	srcStat, err := srcFile.Stat()
	if err != nil {
		t.Error(err)
	}

	dfile, err := OpenV5([3]string{
		filepath.Join(destDir, "slice.01"),
		filepath.Join(destDir, "slice.02"),
		filepath.Join(destDir, "slice.03"),
	}, true)
	if err != nil {
		t.Error(err)
	}
	defer dfile.Close()
	dfile.WriteHeader(5, srcStat.Size())
	n, err := io.Copy(dfile, srcFile)
	if err != nil {
		t.Error(err)
	}
	t.Logf("Copied %d bytes", n)
}

func TestReadFile(t *testing.T) {
	srcPath := "/mnt/c/test/"
	destPath := "/mnt/c/test/decoded2.bmp"
	dfile, err := OpenV5([3]string{
		filepath.Join(srcPath, "slice.01"),
		filepath.Join(srcPath, "slice.02"),
		filepath.Join(srcPath, "slice.03"),
	}, false)
	if err != nil {
		t.Error(err)
	}
	defer dfile.Close()
	destFile, err := os.Create(destPath)
	if err != nil {
		t.Error(err)
	}
	defer destFile.Close()

	n, err := io.Copy(destFile, dfile)
	if err != nil {
		t.Error(err)
	}
	t.Logf("Copied %d bytes", n)

}

func TestRebuildByteWithXor(t *testing.T) {
	b1 := byte(200)
	b2 := byte(55)
	xor := b1 ^ b2
	rebuild, _ := RebuildByte(xor, b2, true)
	if rebuild == b2 {
		t.Log("OK")
	} else {
		t.Error("FAIL")
	}
}

func TestRebuildBlk(t *testing.T) {
	srcPath := "/mnt/c/test/"
	dfile, err := OpenV5([3]string{
		filepath.Join(srcPath, "slice.01"),
		filepath.Join(srcPath, "slice.02"),
		filepath.Join(srcPath, "slice.03"),
	}, false)
	if err != nil {
		t.Error(err)
	}
	defer dfile.Close()

	err = dfile.RebuildBlk()
	if err != nil {
		t.Error(err)
	}

}
