package distributedstorage

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestDFileV5(t *testing.T) {
	srcFilePath := "/mnt/d/test/002Q78y5zy7kytcb5o12d.jfif"
	destDir := "/mnt/d/test/output/"
	srcFile, err := os.Open(srcFilePath)
	if err != nil {
		t.Error(err)
	}
	defer srcFile.Close()

	dfile, err := OpenV5([3]string{
		filepath.Join(destDir, "slice.01"),
		filepath.Join(destDir, "slice.02"),
		filepath.Join(destDir, "slice.03"),
	}, true)
	if err != nil {
		t.Error(err)
	}
	defer dfile.Close()
	n, err := io.Copy(dfile, srcFile)
	if err != nil {
		t.Error(err)
	}
	t.Logf("Copied %d bytes", n)
}
