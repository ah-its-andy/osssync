package distributedstorage

import (
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
			sectorData = make([]byte, len(p)-offset+4096)
		} else {
			sectorData = make([]byte, 4096)
		}
		copy(sectorData, p[offset:])

		sector, err := CreateSectorV5(sectorData)
		if err != nil {
			return offset, err
		}

		for _, frame := range sector {
			var wg sync.WaitGroup
			wg.Add(3)
			b1 := frame[0][:]
			b2 := frame[1][:]
			b3 := frame[2][:]
			go func() {
				defer wg.Done()
				dfile.files[0].Write(b1)
			}()
			go func() {
				defer wg.Done()
				dfile.files[1].Write(b2)
			}()
			go func() {
				defer wg.Done()
				dfile.files[2].Write(b3)
			}()
			wg.Wait()
		}

		if offset >= len(p) {
			break
		}
		offset += 4096
	}
	return offset, nil
}

func (dfile *DistributedFileV5) Read(p []byte) (n int, err error) {
	b1 := make([]byte, 2048)
	b2 := make([]byte, 2048)
	b3 := make([]byte, 2048)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		dfile.files[0].Read(b1)
	}()
	go func() {
		defer wg.Done()
		dfile.files[1].Read(b2)
	}()
	go func() {
		defer wg.Done()
		dfile.files[2].Read(b3)
	}()
	wg.Wait()
	sector, err := DecodeSectorV5([][]byte{
		b1, b2, b3,
	})
	if err != nil {
		return 0, err
	}
	return copy(p, sector), nil
}

func (dfile *DistributedFileV5) Close() error {
	for _, f := range dfile.files {
		f.Close()
	}
	return nil
}
