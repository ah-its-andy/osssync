package distributedstorage

import (
	"encoding/binary"
)

func CreateFrameV5(data [8]byte) [3][4]byte {
	ret := make([][4]byte, 3)
	ret[0] = [4]byte{data[0], data[2] ^ data[3], data[5], data[6]}
	ret[1] = [4]byte{data[1], data[2], data[4] ^ data[5], data[7]}
	ret[2] = [4]byte{data[0] ^ data[1], data[3], data[4], data[6] ^ data[7]}
	return [3][4]byte{ret[0], ret[1], ret[2]}
}

func DecodeFrameV5(frame [3][4]byte) [8]byte {
	if x, y, ok := CheckFrameV5(frame); !ok {
		return DecodeFrameV5(RebuildFrameV5(x, y, frame))
	}
	return [8]byte{
		frame[0][0], frame[1][0],
		frame[1][1], frame[2][1],
		frame[2][2], frame[0][2],
		frame[0][3], frame[1][3],
	}
}

func CheckFrameV5(frame [3][4]byte) (x, y int8, ok bool) {
	if !CheckXor(frame[0][1], frame[1][1], frame[2][1]) {
		return 0, 1, false
	}
	if !CheckXor(frame[1][2], frame[2][2], frame[0][2]) {
		return 1, 2, false
	}
	if !CheckXor(frame[2][0], frame[0][0], frame[1][0]) {
		return 2, 0, false
	}
	if !CheckXor(frame[2][3], frame[0][3], frame[1][3]) {
		return 2, 3, false
	}
	return 0, 0, true
}

func RebuildFrameV5(x, y int8, frame [3][4]byte) [3][4]byte {
	var pos [2]int8
	if x == 0 {
		pos = [2]int8{1, 2}
	} else if x == 1 {
		pos = [2]int8{0, 2}
	} else {
		pos = [2]int8{0, 1}
	}
	// rebuild block
	b, ok := RebuildByte(frame[x][y], frame[pos[0]][y], true)
	if !ok {
		b, ok = RebuildByte(frame[x][y], frame[pos[1]][y], false)
		if !ok {
			panic("rebuild framev5 failed")
		} else {
			frame[x+2][y] = b
		}
	} else {
		frame[x+1][y] = b
	}
	return frame
}

func RebuildByte(xor, another byte, leftToRight bool) (byte, bool) {
	var result byte
	for {
		if xor == SumXor(another, result, leftToRight) {
			return result, true
		} else {
			if result == 255 {
				break
			}
			result++
		}
	}
	return 0, false
}

func SumXor(a, b byte, leftToRight bool) byte {
	if leftToRight {
		return a ^ b
	} else {
		return b ^ a
	}
}

func CheckXor(xor byte, left byte, right byte) bool {
	return xor == (left ^ right)
}

func CreateSectorV5(data []byte) ([][3][4]byte, error) {
	sectorSize := len(data)
	alignSize := sectorSize
	if sectorSize%8 != 0 {
		alignSize += 8 - sectorSize%8
	}

	sectorSizeBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(sectorSizeBuf, uint64(sectorSize))

	ret := make([][3][4]byte, alignSize/8+1)
	ret[0] = CreateFrameV5(
		[8]byte{
			sectorSizeBuf[0], sectorSizeBuf[1],
			sectorSizeBuf[2], sectorSizeBuf[3],
			sectorSizeBuf[4], sectorSizeBuf[5],
			sectorSizeBuf[6], sectorSizeBuf[7],
		})

	for i := 8; i < len(data); i += 8 {
		if i+7 > len(data) {
			remain := len(data) - i
			frameData := make([]byte, 8)
			for j := 0; j < 8; j++ {
				if j < remain {
					frameData[j] = data[i+j]
				} else {
					frameData[j] = 0
				}
			}
			ret[i/8] = CreateFrameV5(
				[8]byte{
					frameData[0], frameData[1],
					frameData[2], frameData[3],
					frameData[4], frameData[5],
					frameData[6], frameData[7]})
		} else {
			ret[i/8] = CreateFrameV5([8]byte{
				data[i], data[i+1],
				data[i+2], data[i+3],
				data[i+4], data[i+5],
				data[i+6], data[i+7]})
		}
	}
	return ret, nil
}

func DecodeSectorV5(sectorData [][]byte) ([]byte, error) {
	sectorSizeData := make([][4]byte, 3)
	copy(sectorSizeData[0][:], sectorData[0][:4])
	copy(sectorSizeData[1][:], sectorData[1][:4])
	copy(sectorSizeData[2][:], sectorData[2][:4])
	sectorFrame := DecodeFrameV5([3][4]byte{
		sectorSizeData[0],
		sectorSizeData[1],
		sectorSizeData[2],
	})
	sectorSize := int(binary.LittleEndian.Uint64(sectorFrame[:]))
	decodeData := make([]byte, 8*sectorSize)
	for i := 1; i < sectorSize; i++ {
		offset := i * 4
		frameData := make([][4]byte, 3)
		copy(frameData[0][:], sectorData[0][offset:4])
		copy(frameData[1][:], sectorData[1][offset:4])
		copy(frameData[2][:], sectorData[2][offset:4])
		sectorFrame = DecodeFrameV5([3][4]byte{
			frameData[0],
			frameData[1],
			frameData[2],
		})
		copy(decodeData[(i-1)*8:], sectorFrame[:])
	}
	return decodeData, nil
}
