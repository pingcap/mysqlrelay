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
	"bytes"
	"io"
	"io/ioutil"
	"os"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/arena"
)

type Replayer struct {
	rec *os.File

	recWriter io.Writer
	clients   map[uint32]*ClientConn

	OnRecordRead func(*Record)

	driver IDriver
}

func (r *Replayer) Run() error {
	defer r.rec.Close()

	for {
		err := r.replayRecord()
		if terror.ErrorEqual(err, io.EOF) {
			return nil
		} else if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

type replayChecker struct {
	r *Replayer
}

func (r *Replayer) readRecord() (*Record, error) {
	record, err := ReadRecord(r.rec)
	if err != nil {
		return nil, errors.Trace(err)
	}

	r.OnRecordRead(record)
	return record, nil
}

func (r *replayChecker) Write(data []byte) (int, error) {
	record, err := r.r.readRecord()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if record.Type != ConnResponseRecord {
		return 0, errors.Errorf("next record must be response type, but %s", record.Type)
	}

	if !bytes.Equal(data, record.Data) {
		return 0, errors.Errorf("mismatch record data, need %q, but got %q", record.Data, data)
	}

	return len(data), nil
}

func (r *Replayer) replayRecord() error {
	record, err := r.readRecord()
	if err != nil {
		return errors.Trace(err)
	}

	switch record.Type {
	case ConnCreateRecord:
		if _, ok := r.clients[record.ConnectionID]; ok {
			return errors.Errorf("duplicated connection id for %s", record)
		}
		conn := new(ClientConn)
		err = conn.Unmarshal(record.Data)
		if err != nil {
			return errors.Trace(err)
		}
		conn.pkg = &dummyPacket{}
		conn.recWriter = r.recWriter
		conn.alloc = arena.NewAllocator(32 * 1024)
		conn.ctx, err = r.driver.OpenCtx(conn.Capability, conn.Collation, conn.DBName)
		if err != nil {
			return errors.Trace(err)
		}
		r.clients[conn.ConnectionID] = conn
	case ConnDeleteRecord:
		conn, ok := r.clients[record.ConnectionID]
		if !ok {
			return errors.Errorf("missing connection for %s", record)
		}
		delete(r.clients, record.ConnectionID)
		conn.Close()
	case ConnRequestRecord:
		conn, ok := r.clients[record.ConnectionID]
		if !ok {
			return errors.Errorf("missing connection for %s", record)
		}
		err = conn.Dispatch(record.Data)
		return errors.Trace(err)
	case ConnResponseRecord:
		_, ok := r.clients[record.ConnectionID]
		if !ok {
			return errors.Errorf("missing connection for %s", record)
		}
		// nothing to do.
	default:
		return errors.Errorf("invalid record %s", record)
	}

	return nil
}

func NewReplayer(driver IDriver, path string, check bool) (*Replayer, error) {
	r := new(Replayer)
	r.driver = driver
	r.clients = make(map[uint32]*ClientConn, 1)

	var err error
	r.rec, err = os.Open(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if !check {
		r.recWriter = ioutil.Discard
	} else {
		r.recWriter = &replayChecker{r: r}
	}

	r.OnRecordRead = func(*Record) {}

	return r, nil
}
