// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package relay

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/juju/errors"
)

type RecordType byte

const (
	ConnCreateRecord RecordType = iota + 1
	ConnDeleteRecord
	ConnRequestRecord
	ConnResponseRecord
)

func (r RecordType) String() string {
	switch r {
	case ConnCreateRecord:
		return "create"
	case ConnDeleteRecord:
		return "delete"
	case ConnRequestRecord:
		return "request"
	case ConnResponseRecord:
		return "response"
	default:
		return "none"
	}
}

// Record format:
//  type, id, data len, data ...
type Record struct {
	Type         RecordType
	ConnectionID uint32
	Data         []byte
}

func (rec *Record) Encode(w io.Writer) error {
	recLen := len(rec.Data) + 5
	buf := make([]byte, recLen+8)
	n := 0
	binary.BigEndian.PutUint32(buf[n:n+4], uint32(recLen))
	n += 4
	// save xor rec len to guarantee length validation.
	binary.BigEndian.PutUint32(buf[n:n+4], ^uint32(recLen))
	n += 4
	buf[n] = byte(rec.Type)
	n++
	binary.BigEndian.PutUint32(buf[n:n+4], rec.ConnectionID)
	n += 4
	copy(buf[n:], rec.Data)

	_, err := w.Write(buf)
	return errors.Trace(err)
}

func (rec *Record) Decode(r io.Reader) error {
	var recLenBuf [8]byte
	nn, err := io.ReadFull(r, recLenBuf[0:8])
	if err != nil {
		return errors.Trace(err)
	} else if nn == 0 {
		// no more data to read
		return errors.Trace(io.EOF)
	}

	recLen := binary.BigEndian.Uint32(recLenBuf[0:4])
	if l := binary.BigEndian.Uint32(recLenBuf[4:8]); l != ^recLen {
		return errors.Errorf("invalid buffer to decode %q", recLenBuf)
	}

	buf := make([]byte, recLen)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return errors.Trace(err)
	}

	n := 0
	rec.Type = RecordType(buf[n])
	n++
	rec.ConnectionID = binary.BigEndian.Uint32(buf[n : n+4])
	n += 4
	rec.Data = buf[n:]

	return errors.Trace(err)
}

func (rec *Record) String() string {
	return fmt.Sprintf("conn: %d, type: %s, data: %q", rec.ConnectionID, rec.Type, rec.Data)
}

type recordWriter interface {
	Write(tp RecordType, id uint32, data []byte) error
}

type defaultRecordWriter struct {
	w io.Writer
}

func (w *defaultRecordWriter) Write(tp RecordType, id uint32, data []byte) error {
	rec := &Record{
		Type:         tp,
		ConnectionID: id,
		Data:         data,
	}

	err := rec.Encode(w.w)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

type dummyRecordWriter struct {
}

func (w *dummyRecordWriter) Write(tp RecordType, id uint32, data []byte) error {
	return nil
}
