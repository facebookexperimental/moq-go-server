/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package moqobject

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Object header
type MoqObjectHeader struct {
	TrackId        uint64
	GroupSequence  uint64
	ObjectSequence uint64
	SendOrder      uint64
}

type MoqObject struct {
	MoqObjectHeader

	ReceivedAt time.Time
	MaxAgeS    uint64

	// Mutable (protected)
	buffer []byte

	// Mutable (protected)
	eof bool

	// Lock to protect mutable fields
	lock *sync.RWMutex
}

func (m *MoqObjectHeader) GetDebugStr() string {
	return fmt.Sprintf("TrackId: %d, groupSeq: %d, dbjSeq: %d, sendOrder: %d", m.TrackId, m.GroupSequence, m.ObjectSequence, m.SendOrder)
}

// FileReader Defines a reader
type moqMessageObjectReader struct {
	offset int
	*MoqObject
}

// New message object
func New(objHeader MoqObjectHeader, maxAgeS uint64) *MoqObject {
	moqtObj := MoqObject{MoqObjectHeader: MoqObjectHeader{TrackId: objHeader.TrackId, GroupSequence: objHeader.GroupSequence, ObjectSequence: objHeader.ObjectSequence, SendOrder: objHeader.SendOrder}, ReceivedAt: time.Now(), MaxAgeS: maxAgeS, eof: false, buffer: []byte{}, lock: new(sync.RWMutex)}

	return &moqtObj
}

func (m *MoqObject) GetDebugStr() string {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return fmt.Sprintf("%s, bytesRead: %d", m.MoqObjectHeader.GetDebugStr(), len(m.buffer))
}

// Write bytes
func (m *MoqObject) PayloadWrite(p []byte) int {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.buffer = append(m.buffer, p...)
	return len(p)
}

// NO more bytes will be added
func (m *MoqObject) SetEof() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.eof = true
}

// Get EOF
func (m *MoqObject) GetEof() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return m.eof
}

// Returns a new reader
func (m *MoqObject) NewReader() io.Reader {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return &moqMessageObjectReader{
		offset:    0,
		MoqObject: m,
	}
}

// Read Reads bytes from object
func (r *moqMessageObjectReader) Read(p []byte) (int, error) {
	r.MoqObject.lock.RLock()
	defer r.MoqObject.lock.RUnlock()

	if r.offset >= len(r.MoqObject.buffer) {
		if r.MoqObject.eof {
			return 0, io.EOF
		}
		return 0, nil
	}
	n := copy(p, r.MoqObject.buffer[r.offset:])
	r.offset += n
	return n, nil
}
