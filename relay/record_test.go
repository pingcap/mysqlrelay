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

	. "github.com/pingcap/check"
)

var _ = Suite(&testRecordSuite{})

type testRecordSuite struct {
}

func (s *testRecordSuite) TestRecord(c *C) {
	var buf bytes.Buffer

	err := WriteRecord(&buf, ConnRequestRecord, 0, []byte("hello world"))
	c.Assert(err, IsNil)

	rec, err := ReadRecord(&buf)
	c.Assert(err, IsNil)
	c.Assert(rec, DeepEquals, &Record{
		Type:         ConnRequestRecord,
		ConnectionID: 0,
		Data:         []byte("hello world"),
	})
}
