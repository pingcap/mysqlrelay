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
	"io"
	"os"
	"reflect"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/arena"
)

type Replayer struct {
	rec *os.File

	recWriter recordWriter
	clients   map[uint32]*ClientConn

	OnRecordRead func(*Record)

	driver IDriver

	err error
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
	record := new(Record)
	err := record.Decode(r.rec)
	if err != nil {
		return nil, errors.Trace(err)
	}

	r.OnRecordRead(record)
	return record, nil
}

func (r *replayChecker) Write(tp RecordType, id uint32, data []byte) error {
	if r.r.err != nil {
		return nil
	}

	if tp != ConnResponseRecord {
		r.r.err = errors.Errorf("next record must be response type, but %s", tp)
		return nil
	}

	rec1, err := r.r.readRecord()
	if err != nil {
		r.r.err = errors.Trace(err)
		return nil
	}

	rec2 := &Record{
		Type:         tp,
		ConnectionID: id,
		Data:         data,
	}

	if !reflect.DeepEqual(rec1, rec2) {
		r.r.err = errors.Errorf("mismatch record got %s, but record %s", rec2, rec1)
		return nil
	}

	return nil
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

		conn.HandleRequest(record.Data)
	case ConnResponseRecord:
		_, ok := r.clients[record.ConnectionID]
		if !ok {
			return errors.Errorf("missing connection for %s", record)
		}
	default:
		return errors.Errorf("invalid record %s", record)
	}

	return r.err
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
		r.recWriter = &dummyRecordWriter{}
	} else {
		r.recWriter = &replayChecker{r: r}
	}

	r.OnRecordRead = func(rec *Record) {}

	return r, nil
}
