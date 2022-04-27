package distributedstorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type DistributedFileV5 struct {
	filePaths   [3]string
	files       []*os.File
	eof         bool
	eofLock     sync.Mutex
	offset      int64
	decodedSize int64

	missingPart int
	rebuildBlk  *os.File
}

func OpenV5(path [3]string, createIfNotExists bool) (DistributedFile, error) {
	dfile := &DistributedFileV5{
		filePaths: path,
		files:     make([]*os.File, 3),
		offset:    8,
	}
	var err error
	dfile.files[0], err = OpenFile(path[0], createIfNotExists)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}
	dfile.files[1], err = OpenFile(path[1], createIfNotExists)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}
	dfile.files[2], err = OpenFile(path[2], createIfNotExists)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}
	return dfile, nil
}

func (dfile *DistributedFileV5) Write(p []byte) (n int, err error) {
	offset := 0
	bufw := []*bytes.Buffer{
		bytes.NewBuffer([]byte{}),
		bytes.NewBuffer([]byte{}),
		bytes.NewBuffer([]byte{}),
	}
	for {
		var sectorData []byte

		sectorData = make([]byte, len(p))
		copy(sectorData, p[offset:])

		sector, err := CreateSectorV5(sectorData)
		if err != nil {
			return offset, err
		}

		for i, frame := range sector {
			dfile.writeDbg(p, i, frame)

			wg := &sync.WaitGroup{}
			wg.Add(3)
			go func(w io.Writer, frame [4]byte) {
				defer wg.Done()
				if _, err := w.Write(frame[:]); err != nil {
					panic(err)
				}
			}(bufw[0], frame[0])

			go func(w io.Writer, frame [4]byte) {
				defer wg.Done()
				if _, err := w.Write(frame[:]); err != nil {
					panic(err)
				}
			}(bufw[1], frame[1])

			go func(w io.Writer, frame [4]byte) {
				defer wg.Done()
				if _, err := w.Write(frame[:]); err != nil {
					panic(err)
				}
			}(bufw[2], frame[2])

			wg.Wait()
		}

		bts := [][]byte{
			bufw[0].Bytes(),
			bufw[1].Bytes(),
			bufw[2].Bytes(),
		}

		dfile.files[0].WriteAt(bts[0], dfile.offset)
		dfile.files[1].WriteAt(bts[1], dfile.offset)
		dfile.files[2].WriteAt(bts[2], dfile.offset)
		dfile.offset += int64(len(bts[2]))
		offset += len(p)
		if offset >= len(p) {
			break
		}
	}
	return len(p), nil
}

func (dfile *DistributedFileV5) writeDbg(p []byte, i int, frame [3][4]byte) {
	decodedFrame := DecodeFrameV5(frame)
	offset := i * 8
	data := make([]byte, 8)
	copy(data, p[offset:])
	if bytes.Compare(data, decodedFrame[:]) != 0 {
		panic("data != decodedFrame")
	}
}

func (dfile *DistributedFileV5) WriteHeader(version int64, fileSize int64) error {
	buf := []*bytes.Buffer{
		bytes.NewBuffer([]byte{}),
		bytes.NewBuffer([]byte{}),
		bytes.NewBuffer([]byte{}),
	}
	versionBinary := make([]byte, 8)
	binary.LittleEndian.PutUint64(versionBinary, uint64(version))
	versionFrame := CreateFrameV5([8]byte{
		versionBinary[0], versionBinary[1], versionBinary[2], versionBinary[3],
		versionBinary[4], versionBinary[5], versionBinary[6], versionBinary[7],
	})
	if _, err := buf[0].Write(versionFrame[0][:]); err != nil {
		return err
	}
	if _, err := buf[1].Write(versionFrame[1][:]); err != nil {
		return err
	}
	if _, err := buf[2].Write(versionFrame[2][:]); err != nil {
		return err
	}

	fileSizeBinary := make([]byte, 8)
	binary.LittleEndian.PutUint64(fileSizeBinary, uint64(fileSize))
	fileSizeFrame := CreateFrameV5([8]byte{
		fileSizeBinary[0], fileSizeBinary[1], fileSizeBinary[2], fileSizeBinary[3],
		fileSizeBinary[4], fileSizeBinary[5], fileSizeBinary[6], fileSizeBinary[7],
	})
	if _, err := buf[0].Write(fileSizeFrame[0][:]); err != nil {
		return err
	}
	if _, err := buf[1].Write(fileSizeFrame[1][:]); err != nil {
		return err
	}
	if _, err := buf[2].Write(fileSizeFrame[2][:]); err != nil {
		return err
	}

	bts := [][]byte{
		buf[0].Bytes(),
		buf[1].Bytes(),
		buf[2].Bytes(),
	}

	dfile.files[0].WriteAt(bts[0], 0)
	dfile.files[1].WriteAt(bts[1], 0)
	dfile.files[2].WriteAt(bts[2], 0)

	dfile.offset = 8
	return nil
}

func (dfile *DistributedFileV5) readHeader() (ver int64, fileSize int64, err error) {
	buf := [][]byte{
		make([]byte, 8),
		make([]byte, 8),
		make([]byte, 8),
	}
	dfile.files[0].ReadAt(buf[0], 0)
	dfile.files[1].ReadAt(buf[1], 0)
	dfile.files[2].ReadAt(buf[2], 0)
	frames := GetFrames(buf)
	if _, _, ok := CheckFrameV5(frames[0]); !ok {
		fmt.Println(fmt.Sprintf("frame %v is not valid", 0))
	}
	if _, _, ok := CheckFrameV5(frames[1]); !ok {
		fmt.Println(fmt.Sprintf("frame %v is not valid", 1))
	}
	verb := DecodeFrameV5(frames[0])
	sizeb := DecodeFrameV5(frames[1])
	verv := binary.LittleEndian.Uint64(verb[:])
	sizev := binary.LittleEndian.Uint64(sizeb[:])
	return int64(verv), int64(sizev), nil
}

func (dfile *DistributedFileV5) readBuf(bufferSize int) (int64, [][]byte, error) {
	if bufferSize%4 != 0 {
		return 0, nil, fmt.Errorf("bufferSize %v is not valid", bufferSize)
	}

	var n1 int64

	stripeData := [][]byte{
		make([]byte, bufferSize),
		make([]byte, bufferSize),
		make([]byte, bufferSize),
	}

	if dfile.files[0] != nil {
		n, err := dfile.files[0].ReadAt(stripeData[0], dfile.offset)
		if err != nil {
			if err == io.EOF {
				dfile.setEof(true)
				endBuf := make([]byte, n)
				copy(endBuf, stripeData[0])
				stripeData[0] = endBuf
			} else {
				return 0, stripeData, err
			}
		}
		n1 = int64(n)
	}

	if dfile.files[1] != nil {
		n, err := dfile.files[1].ReadAt(stripeData[1], dfile.offset)
		if err != nil {
			if err == io.EOF {
				dfile.setEof(true)
				endBuf := make([]byte, n)
				copy(endBuf, stripeData[1])
				stripeData[1] = endBuf
			} else {
				return 0, stripeData, err
			}
		}
	}

	if dfile.files[2] != nil {
		n, err := dfile.files[2].ReadAt(stripeData[2], dfile.offset)
		if err != nil {
			if err == io.EOF {
				dfile.setEof(true)
				endBuf := make([]byte, n)
				copy(endBuf, stripeData[2])
				stripeData[2] = endBuf
			} else {
				return 0, stripeData, err
			}
		}
	}

	dfile.offset += int64(n1)
	return n1, stripeData, nil
}

func (dfile *DistributedFileV5) CheckMissing() (int, error) {
	missings := []int{}
	for i := 0; i < 3; i++ {
		if dfile.files[i] == nil {
			missings = append(missings, i)
		}
	}
	if len(missings) > 1 {
		return -1, fmt.Errorf("more than one missing file")
	}
	if len(missings) > 0 {
		return missings[0], nil
	}
	return -1, nil
}
func (dfile *DistributedFileV5) Read(p []byte) (n int, err error) {
	miss, err := dfile.CheckMissing()
	if err != nil {
		return 0, err
	}
	if miss > -1 {
		panic(fmt.Sprintf("missing file %v", miss))
	}
	bufferSize := len(p) / 8 * 4
	bufw := bytes.NewBuffer([]byte{})

	_, fileSize, err := dfile.readHeader()
	if err != nil {
		return 0, err
	}

	var readNum int

	for {
		if readNum == len(p) || dfile.eof {
			break
		}
		// debug: set buffer size to 4, to test each frame decodes correctly
		//bufferSize = 4
		_, stripeData, err := dfile.readBuf(bufferSize)
		if err != nil {
			return 0, err
		}
		frames := GetFrames(stripeData)
		decodeBuf := bytes.NewBuffer([]byte{})
		for i, frame := range frames {
			if _, _, ok := CheckFrameV5(frame); !ok {
				fmt.Println(fmt.Sprintf("frame %v is not valid", i))
			}
			decodeFrame := DecodeFrameV5(frame)
			decodeBuf.Write(decodeFrame[:])
		}
		bufw.Write(decodeBuf.Bytes())
		readNum += len(decodeBuf.Bytes())
	}

	remainSize := fileSize - dfile.decodedSize
	if remainSize < int64(readNum) {
		readNum = int(remainSize)
	}

	copy(p, bufw.Bytes())

	if dfile.eof {
		dfile.decodedSize += int64(readNum)
		return readNum, io.EOF
	} else {
		dfile.decodedSize += int64(readNum)
		dfile.offset += int64(bufferSize)
		return readNum, nil
	}
}

func (dfile *DistributedFileV5) setEof(v bool) {
	dfile.eofLock.Lock()
	defer dfile.eofLock.Unlock()
	dfile.eof = v
}

func (dfile *DistributedFileV5) Close() error {
	for _, f := range dfile.files {
		f.Close()
	}
	return nil
}

func (dfile *DistributedFileV5) RebuildBlk() error {
	dfile.offset = 0
	miss, _ := dfile.CheckMissing()
	var err error
	dfile.rebuildBlk, err = OpenFile(dfile.filePaths[miss], true)
	if err != nil {
		return err
	}
	defer dfile.rebuildBlk.Close()
	bufferSize := 2048
	writeOffset := int64(0)
	for {
		n, buf, err := dfile.readBuf(bufferSize)
		if err != nil {
			return err
		}
		writeBuf := make([]byte, n)
		frames := GetFrames(buf)
		//     0 1 2 3
		// 0 : 0 1 2 3            0*4+0 = 0
		// 1 : 4 5 6 7            1*4+0 = 4
		// 2 : 8 9 10 11		  2*4+0 = 8
		for i, frame := range frames {
			for j := 0; j < 4; j++ {
				rebuildField := RebuildField(miss, j, frame)
				writeBuf[i*4+j] = rebuildField
			}
		}
		dfile.rebuildBlk.WriteAt(writeBuf, writeOffset)

		if dfile.eof {
			break
		}

		writeOffset += int64(bufferSize)
	}
	return nil
}
