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
	buf := make([]byte, len(rec.Data)+9)
	n := 0
	buf[n] = byte(rec.Type)
	n++
	binary.BigEndian.PutUint32(buf[n:n+4], rec.ConnectionID)
	n += 4
	binary.BigEndian.PutUint32(buf[n:n+4], uint32(len(rec.Data)))
	n += 4
	copy(buf[n:], rec.Data)

	_, err := w.Write(buf)
	return errors.Trace(err)
}

func (rec *Record) Decode(r io.Reader) error {
	buf := make([]byte, 9)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return errors.Trace(err)
	}
	n := 0
	rec.Type = RecordType(buf[n])
	n++
	rec.ConnectionID = binary.BigEndian.Uint32(buf[n : n+4])
	n += 4
	payloadSize := binary.BigEndian.Uint32(buf[n : n+4])

	rec.Data = make([]byte, payloadSize)
	_, err = io.ReadFull(r, rec.Data)
	return errors.Trace(err)
}

func (rec *Record) String() string {
	return fmt.Sprintf("conn: %d, type: %s, data: %q", rec.ConnectionID, rec.Type, rec.Data)
}

func WriteRecord(w io.Writer, tp RecordType, id uint32, data []byte) error {
	rec := &Record{
		Type:         tp,
		ConnectionID: id,
		Data:         data,
	}
	return errors.Trace(rec.Encode(w))
}

func ReadRecord(r io.Reader) (*Record, error) {
	rec := new(Record)
	err := rec.Decode(r)
	return rec, errors.Trace(err)
}
