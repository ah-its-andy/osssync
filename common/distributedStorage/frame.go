package distributedstorage

import (
	"fmt"
	"math"
)

func CreateFrameV5(data [8]byte) [3][4]byte {
	ret := make([][4]byte, 3)
	ret[0] = [4]byte{data[0], data[2] ^ data[3], data[5], data[6]}
	ret[1] = [4]byte{data[1], data[2], data[4] ^ data[5], data[7]}
	ret[2] = [4]byte{data[0] ^ data[1], data[3], data[4], data[6] ^ data[7]}
	return [3][4]byte{ret[0], ret[1], ret[2]}
}

func DecodeFrameV5(frame [3][4]byte) [8]byte {
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

func RebuildField(blkIdx int, lineIdx int, frame [3][4]byte) byte {
	var pos [2]int
	if blkIdx == 0 {
		pos = [2]int{1, 2}
	} else if blkIdx == 1 {
		pos = [2]int{0, 2}
	} else {
		pos = [2]int{0, 1}
	}
	xorBlkIdx := lineIdx
	if lineIdx == 0 || lineIdx == 3 {
		xorBlkIdx = 2
	}
	if xorBlkIdx == blkIdx {
		// this field is xor
		ret := frame[pos[0]][lineIdx] ^ frame[pos[1]][lineIdx]
		return ret
	} else {
		// this field is data, rebuild it with xor
		xor := frame[xorBlkIdx][lineIdx]
		anotherBlk := pos[0]
		if pos[0] == xorBlkIdx {
			anotherBlk = pos[1]
		}
		ret := SumXor(frame[anotherBlk][lineIdx], xor, true)

		return ret
	}
}

// compute direction of xor will have different result, because byte type will not always be 8 bit with zero-fill
func SumXor(a, b byte, leftToRight bool) byte {
	if leftToRight {
		return a ^ b
	} else {
		return b ^ a
	}
}

func CheckXor(xor byte, left byte, right byte) bool {
	sum := SumXor(left, right, true)
	return xor == sum
}

func CreateSectorV5(data []byte) ([][3][4]byte, error) {
	sectorSize := len(data)
	// alignSize := sectorSize
	// if sectorSize%8 != 0 {
	// 	alignSize += 8 - sectorSize%8
	// }

	frameSize := int(math.Ceil(float64(sectorSize) / 8))
	frames := make([][3][4]byte, frameSize)

	for i := 0; i < len(data); i += 8 {
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
			frames[i/8] = CreateFrameV5(
				[8]byte{
					frameData[0], frameData[1],
					frameData[2], frameData[3],
					frameData[4], frameData[5],
					frameData[6], frameData[7]})
		} else {
			frames[i/8] = CreateFrameV5([8]byte{
				data[i], data[i+1],
				data[i+2], data[i+3],
				data[i+4], data[i+5],
				data[i+6], data[i+7]})
		}
	}
	return frames, nil
}

// func FlatFramesV5(frames [][3][4]byte) [3][]byte {
// 	ret := make([][]byte, 3)
// 	ret[0] = make([]byte, len(frames)*4)
// 	ret[1] = make([]byte, len(frames)*4)
// 	ret[2] = make([]byte, len(frames)*4)

// 	for i, frame := range frames {
// 		copy(ret[0][i*4:i*4+4], frame[0][:])
// 		copy(ret[1][i*4:i*4+4], frame[1][:])
// 		copy(ret[2][i*4:i*4+4], frame[2][:])
// 	}
// 	return [3][]byte{ret[0], ret[1], ret[2]}
// }

func GetFrames(sectorData [][]byte) [][3][4]byte {
	var frames [][3][4]byte
	for i := 0; i < len(sectorData[0][:]); i += 4 {
		frameData := make([][4]byte, 3)
		copy(frameData[0][:], sectorData[0][i:i+4])
		copy(frameData[1][:], sectorData[1][i:i+4])
		copy(frameData[2][:], sectorData[2][i:i+4])

		frame := [3][4]byte{
			frameData[0],
			frameData[1],
			frameData[2],
		}

		frames = append(frames, frame)
	}
	return frames
}

func Bytes2Bits(data []byte) []int {
	dst := make([]int, 0)
	for _, v := range data {
		for i := 0; i < 8; i++ {
			move := uint(7 - i)
			dst = append(dst, int((v>>move)&1))
		}
	}
	fmt.Println(len(dst))
	return dst
}
