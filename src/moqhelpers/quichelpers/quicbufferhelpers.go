/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package quichelpers

import (
	"errors"
	"fmt"
)

type IWtReadableStream interface {
	Read(p []byte) (int, error)
}

type IWtWritableStream interface {
	Write(p []byte) (int, error)
}

// taken from the QUIC draft
const (
	maxVarInt1 = 63
	maxVarInt2 = 16383
	maxVarInt4 = 1073741823
	maxVarInt8 = 4611686018427387903
)

func ReadBytes(stream IWtReadableStream, buffer []byte) error {
	readSize := 0
	totalSize := len(buffer)
	var err error = nil
	for readSize < totalSize && err == nil {
		n := 0
		tmpBuffer := make([]byte, totalSize-readSize)
		n, err = stream.Read(tmpBuffer)
		copy(buffer[readSize:readSize+n], tmpBuffer[0:n])

		readSize += n
	}
	return err
}

func ReadByte(stream IWtReadableStream) (ret byte, err error) {
	tmpBuffer := []byte{0}
	err = ReadBytes(stream, tmpBuffer)
	if err == nil {
		ret = tmpBuffer[0]
	}
	return
}

// Var int helpers

func ReadVarint(stream IWtReadableStream) (uint64, error) {
	firstByte, err := ReadByte(stream)
	if err != nil {
		return 0, err
	}
	// the first two bits of the first byte encode the length
	len := 1 << ((firstByte & 0xc0) >> 6)
	b1 := firstByte & (0xff - 0xc0)
	if len == 1 {
		return uint64(b1), nil
	}
	b2, err := ReadByte(stream)
	if err != nil {
		return 0, err
	}
	if len == 2 {
		return uint64(b2) + uint64(b1)<<8, nil
	}
	b3, err := ReadByte(stream)
	if err != nil {
		return 0, err
	}
	b4, err := ReadByte(stream)
	if err != nil {
		return 0, err
	}
	if len == 4 {
		return uint64(b4) + uint64(b3)<<8 + uint64(b2)<<16 + uint64(b1)<<24, nil
	}
	b5, err := ReadByte(stream)
	if err != nil {
		return 0, err
	}
	b6, err := ReadByte(stream)
	if err != nil {
		return 0, err
	}
	b7, err := ReadByte(stream)
	if err != nil {
		return 0, err
	}
	b8, err := ReadByte(stream)
	if err != nil {
		return 0, err
	}
	return uint64(b8) + uint64(b7)<<8 + uint64(b6)<<16 + uint64(b5)<<24 + uint64(b4)<<32 + uint64(b3)<<40 + uint64(b2)<<48 + uint64(b1)<<56, nil
}

func writeSafe(stream IWtWritableStream, data []byte) error {
	remainingBytes := len(data)
	start := 0
	for remainingBytes > 0 {
		n, err := stream.Write(data[start:remainingBytes])
		if err != nil {
			return err
		}
		remainingBytes = remainingBytes - n
		start = start + n
	}
	return nil
}

func VarIntLength(i uint64) (size uint, err error) {
	if i <= maxVarInt1 {
		size = 1
	} else if i <= maxVarInt2 {
		size = 2
	} else if i <= maxVarInt4 {
		size = 4
	} else if i <= maxVarInt8 {
		size = 8
	} else {
		err = errors.New(fmt.Sprintf("%#x doesn't fit into 62 bits", i))
	}
	return
}

func WriteVarint(stream IWtWritableStream, i uint64) error {
	if i <= maxVarInt1 {
		return writeSafe(stream, []byte{uint8(i)})
	}
	if i <= maxVarInt2 {
		return writeSafe(stream, []byte{uint8(i>>8) | 0x40, uint8(i)})
	}
	if i <= maxVarInt4 {
		return writeSafe(stream, []byte{uint8(i>>24) | 0x80, uint8(i >> 16), uint8(i >> 8), uint8(i)})
	}
	if i <= maxVarInt8 {
		return writeSafe(stream, []byte{uint8(i>>56) | 0xc0, uint8(i >> 48), uint8(i >> 40), uint8(i >> 32),
			uint8(i >> 24), uint8(i >> 16), uint8(i >> 8), uint8(i),
		})
	}
	return errors.New(fmt.Sprintf("%#x doesn't fit into 62 bits", i))
}

// MOQT String

func ReadString(stream IWtReadableStream, max_allowed_length uint) (string, error) {
	strLength, errStrLength := ReadVarint(stream)
	if errStrLength != nil {
		return "", errStrLength
	}
	if uint(strLength) > max_allowed_length {
		return "", errors.New(fmt.Sprintf("String length exceeds limit of %d, received: %d", max_allowed_length, strLength))
	}

	strBytes := make([]byte, strLength)
	errStr := ReadBytes(stream, strBytes)
	if errStr != nil {
		return "", errStr
	}
	return string(strBytes), nil
}

func WriteString(stream IWtWritableStream, str string) error {
	strBytes := []byte(str)
	WriteVarint(stream, uint64(len(strBytes)))
	return writeSafe(stream, strBytes)
}
