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
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
)

func TestRelay(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testRelaySuite{})

type testRelaySuite struct {
	server *Server
	store  kv.Storage

	db *sql.DB

	path string
}

func (s *testRelaySuite) SetUpSuite(c *C) {
	log.SetLevelByString("error")

	store, err := tidb.NewStore("memory://test/test")
	c.Assert(err, IsNil)

	s.store = store
	s.path = "/tmp/test_mysqlrelay.log"

	server, err := NewServer(NewTiDBDriver(store), ":4001", s.path)
	c.Assert(err, IsNil)
	s.server = server

	go server.Run()

	time.Sleep(time.Millisecond * 100)

	dsn := "root@tcp(localhost:4001)/test?strict=true"
	db, err := sql.Open("mysql", dsn)
	c.Assert(err, IsNil)
	s.db = db
}

func (s *testRelaySuite) TearDownSuite(c *C) {
	if s.server != nil {
		s.server.Close()
	}

	if s.db != nil {
		err := s.db.Close()
		c.Assert(err, IsNil)
	}

	if s.store != nil {
		err := s.store.Close()
		c.Assert(err, IsNil)
	}
}

func (s *testRelaySuite) mustQuery(c *C, query string, args ...interface{}) {
	r, err := s.db.Query(query, args...)
	c.Assert(err, IsNil)
	defer r.Close()
	s.discardRows(c, r)
}

func (s *testRelaySuite) discardRows(c *C, r *sql.Rows) {
	// we don't care query result.
	for r.Next() {
	}

	err := r.Err()
	c.Assert(err, IsNil)
}

func (s *testRelaySuite) mustExec(c *C, query string, args ...interface{}) {
	_, err := s.db.Exec(query, args...)
	c.Assert(err, IsNil)
}

func (s *testRelaySuite) TestRelay(c *C) {
	s.mustExec(c, "create table t (c1 int, c2 int, primary key(c1))")
	s.mustExec(c, "insert into t values (1, 1), (2, 2), (3, 3)")
	s.mustQuery(c, "select * from t")
	s.mustQuery(c, "select * from t where c1 = 1")

	stmt, err := s.db.Prepare("select * from t where c1 = ?")
	c.Assert(err, IsNil)
	r, err := stmt.Query(1)
	c.Assert(err, IsNil)
	s.discardRows(c, r)
	r, err = stmt.Query(2)
	c.Assert(err, IsNil)
	s.discardRows(c, r)
	r, err = stmt.Query(10)
	c.Assert(err, IsNil)
	s.discardRows(c, r)
	stmt.Close()

	store, err := tidb.NewStore("memory://test_replay/test_replay")
	c.Assert(err, IsNil)
	defer store.Close()

	replayer, err := NewReplayer(NewTiDBDriver(store), s.path, false)
	c.Assert(err, IsNil)
	replayer.OnRecordRead = func(rec *Record) {
		log.Errorf("record %s", rec)
	}
	err = replayer.Run()
	c.Assert(err, IsNil, Commentf("%s", errors.ErrorStack(err)))
}
