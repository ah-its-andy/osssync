package distributedstorage

import (
	"io"
	"os"
	"sync"
)

type DistributedFileV5 struct {
	files []*os.File
}

func OpenV5(path [3]string, createIfNotExists bool) (DistributedFile, error) {
	dfile := &DistributedFileV5{
		files: make([]*os.File, 3),
	}
	var err error
	dfile.files[0], err = OpenFile(path[0], createIfNotExists)
	if err != nil {
		return nil, err
	}
	dfile.files[1], err = OpenFile(path[1], createIfNotExists)
	if err != nil {
		return nil, err
	}
	dfile.files[2], err = OpenFile(path[2], createIfNotExists)
	if err != nil {
		return nil, err
	}
	return dfile, nil
}

func (dfile *DistributedFileV5) Write(p []byte) (n int, err error) {
	offset := 0
	for {
		var sectorData []byte
		if offset >= len(p) {
			break
		}
		bufferSize := 4096
		if offset+4096 > len(p) {
			bufferSize = len(p) - offset
		}
		sectorData = make([]byte, bufferSize)
		copy(sectorData, p[offset:])

		sector, err := CreateSectorV5(sectorData)
		if err != nil {
			return offset, err
		}

		wg := &sync.WaitGroup{}

		wg.Add(3)
		go func(f *os.File, sector []byte) {
			defer wg.Done()
			if _, err := f.Write(sector); err != nil {
				panic(err)
			}
		}(dfile.files[0], sector[0])

		go func(f *os.File, sector []byte) {
			defer wg.Done()
			if _, err := f.Write(sector); err != nil {
				panic(err)
			}
		}(dfile.files[1], sector[1])

		go func(f *os.File, sector []byte) {
			defer wg.Done()
			if _, err := f.Write(sector); err != nil {
				panic(err)
			}
		}(dfile.files[2], sector[2])

		wg.Wait()

		if offset >= len(p) {
			break
		}
		offset += 4096
	}
	return len(p), nil
}

func (dfile *DistributedFileV5) Read(p []byte) (n int, err error) {
	offset := 0
	for {
		b1 := make([]byte, 2048)
		b2 := make([]byte, 2048)
		b3 := make([]byte, 2048)
		wg := &sync.WaitGroup{}
		wg.Add(3)
		go func(f *os.File, b []byte) {
			defer wg.Done()
			if n, err := f.Read(b); err != nil {
				if err == io.EOF {
					buf := make([]byte, n)
					copy(buf, b)
					b = buf
				}
				panic(err)
			}
		}(dfile.files[0], b1)

		go func(f *os.File, b []byte) {
			defer wg.Done()
			if _, err := f.Read(b); err != nil {
				if err == io.EOF {
					buf := make([]byte, n)
					copy(buf, b)
					b = buf
				}
				panic(err)
			}
		}(dfile.files[1], b2)
		go func(f *os.File, b []byte) {
			defer wg.Done()
			if _, err := f.Read(b); err != nil {
				if err == io.EOF {
					buf := make([]byte, n)
					copy(buf, b)
					b = buf
				}
				panic(err)
			}
		}(dfile.files[2], b3)
		wg.Wait()

		sector, err := DecodeSectorV5([][]byte{
			b1, b2, b3,
		})
		if err != nil {
			return 0, err
		}
		copy(p[offset:], sector)
		offset += 2056
		if offset >= len(p) {
			break
		}
	}
	return len(p), io.EOF
}

func (dfile *DistributedFileV5) Close() error {
	for _, f := range dfile.files {
		f.Close()
	}
	return nil
}
