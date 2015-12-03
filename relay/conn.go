// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/hack"
)

var defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows

type ClientConn struct {
	pkg          packet
	Addr         string `json:"addr"`
	Capability   uint32 `json:"capability"`
	ConnectionID uint32 `json:"connection_id"`
	Collation    uint8  `json:"collation"`
	Charset      string `json:"charset"`
	User         string `json:"user"`
	DBName       string `json:"dbname"`
	alloc        arena.Allocator
	lastCmd      string
	ctx          IContext

	recWriter io.Writer
}

func (cc *ClientConn) String() string {
	return fmt.Sprintf("conn: %s, status: %d, charset: %s, user: %s, lastInsertId: %d",
		cc.Addr, cc.ctx.Status(), cc.Charset, cc.User, cc.ctx.LastInsertID(),
	)
}

func (cc *ClientConn) Handshake(driver IDriver) error {
	if err := cc.writeInitialHandshake(); err != nil {
		return errors.Trace(err)
	}
	if err := cc.readHandshakeResponse(driver); err != nil {
		cc.writeError(err)
		return errors.Trace(err)
	}
	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, 0, 0)
	if cc.Capability&mysql.ClientProtocol41 > 0 {
		data = append(data, dumpUint16(mysql.ServerStatusAutocommit)...)
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	cc.pkg.Reset()
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}

func (cc *ClientConn) Close() error {
	if cc.ctx != nil {
		return cc.ctx.Close()
	}
	return nil
}

func (cc *ClientConn) Marshal() ([]byte, error) {
	return json.Marshal(cc)
}

func (cc *ClientConn) Unmarshal(data []byte) error {
	return json.Unmarshal(data, cc)
}

func (cc *ClientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	// min version 10
	data = append(data, 10)
	// server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(cc.ConnectionID), byte(cc.ConnectionID>>8), byte(cc.ConnectionID>>16), byte(cc.ConnectionID>>24))
	// auth-plugin-data-part-1
	data = append(data, salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(defaultCapability), byte(defaultCapability>>8))
	// charset, utf-8 default
	data = append(data, uint8(mysql.DefaultCollationID))
	//status
	data = append(data, dumpUint16(mysql.ServerStatusAutocommit)...)
	// below 13 byte may not be used
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(defaultCapability>>16), byte(defaultCapability>>24))
	// filler [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, salt[8:]...)
	// filler [00]
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(cc.flush())
}

func (cc *ClientConn) readPacket() ([]byte, error) {
	data, err := cc.pkg.Read()
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = WriteRecord(cc.recWriter, ConnRequestRecord, cc.ConnectionID, data)

	return data, errors.Trace(err)
}

func (cc *ClientConn) writePacket(data []byte) error {
	err := WriteRecord(cc.recWriter, ConnResponseRecord, cc.ConnectionID, data)
	if err != nil {
		return errors.Trace(err)
	}

	return cc.pkg.Write(data)
}

func (cc *ClientConn) readHandshakeResponse(driver IDriver) error {
	data, err := cc.readPacket()
	if err != nil {
		return errors.Trace(err)
	}

	pos := 0
	// capability
	cc.Capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4
	// skip max packet size
	pos += 4
	// charset, skip, if you want to use another charset, use set names
	cc.Collation = data[pos]
	pos++
	// skip reserved 23[00]
	pos += 23
	// user name
	cc.User = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(cc.User) + 1
	// auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]
	pos += authLen
	if cc.Capability&mysql.ClientConnectWithDB > 0 {
		if len(data[pos:]) > 0 {
			idx := bytes.IndexByte(data[pos:], 0)
			cc.DBName = string(data[pos : pos+idx])
		}
	}
	// Open session and do auth
	cc.ctx, err = driver.OpenCtx(cc.Capability, uint8(cc.Collation), cc.DBName)
	if err != nil {
		cc.Close()
		return errors.Trace(err)
	}

	host, _, err1 := net.SplitHostPort(cc.Addr)
	if err1 != nil {
		return errors.Trace(mysql.NewErr(mysql.ErrAccessDenied, cc.User, cc.Addr, "Yes"))
	}
	user := fmt.Sprintf("%s@%s", cc.User, host)
	if !cc.ctx.Auth(user, auth, salt) {
		return errors.Trace(mysql.NewErr(mysql.ErrAccessDenied, cc.User, host, "Yes"))
	}

	return nil
}

func (cc *ClientConn) Run(l *TokenLimiter) {
	defer func() {
		r := recover()
		if r != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			log.Errorf("lastCmd %s, %v, %s", cc.lastCmd, r, buf)
		}
		cc.Close()
	}()

	for {
		err := cc.runPacket(l)
		if err != nil {
			return
		}
	}
}

func (cc *ClientConn) runPacket(l *TokenLimiter) error {
	tk := l.Get()
	defer l.Put(tk)

	cc.alloc.Reset()
	data, err := cc.readPacket()
	if err != nil {
		if terror.ErrorNotEqual(err, io.EOF) {
			log.Error(err)
		}
		return errors.Trace(err)
	}

	if err := cc.Dispatch(data); err != nil {
		if terror.ErrorEqual(err, io.EOF) {
			return errors.Trace(err)
		}
		log.Errorf("dispatch error %s, %s", errors.ErrorStack(err), cc)
		log.Errorf("cmd: %s", string(data[1:]))
		cc.writeError(err)
	}

	cc.pkg.Reset()
	return nil
}

func (cc *ClientConn) Dispatch(data []byte) error {
	cmd := data[0]
	data = data[1:]
	cc.lastCmd = hack.String(data)

	switch cmd {
	case mysql.ComQuit:
		return io.EOF
	case mysql.ComQuery:
		return cc.handleQuery(hack.String(data))
	case mysql.ComPing:
		return cc.writeOK()
	case mysql.ComInitDB:
		log.Debug("init db", hack.String(data))
		if err := cc.useDB(hack.String(data)); err != nil {
			return errors.Trace(err)
		}
		return cc.writeOK()
	case mysql.ComFieldList:
		return cc.handleFieldList(hack.String(data))
	case mysql.ComStmtPrepare:
		return cc.handleStmtPrepare(hack.String(data))
	case mysql.ComStmtExecute:
		return cc.handleStmtExecute(data)
	case mysql.ComStmtClose:
		return cc.handleStmtClose(data)
	case mysql.ComStmtSendLongData:
		return cc.handleStmtSendLongData(data)
	case mysql.ComStmtReset:
		return cc.handleStmtReset(data)
	default:
		return mysql.NewErrf(mysql.ErrUnknown, "command %d not supported now", cmd)
	}
}

func (cc *ClientConn) useDB(db string) (err error) {
	_, err = cc.ctx.Execute("use " + db)
	if err != nil {
		return errors.Trace(err)
	}
	cc.DBName = db
	return
}

func (cc *ClientConn) flush() error {
	return cc.pkg.Flush()
}

func (cc *ClientConn) writeOK() error {
	data := cc.alloc.AllocWithLen(4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, dumpLengthEncodedInt(uint64(cc.ctx.AffectedRows()))...)
	data = append(data, dumpLengthEncodedInt(uint64(cc.ctx.LastInsertID()))...)
	if cc.Capability&mysql.ClientProtocol41 > 0 {
		data = append(data, dumpUint16(cc.ctx.Status())...)
		data = append(data, dumpUint16(cc.ctx.WarningCount())...)
	}

	err := cc.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}

func (cc *ClientConn) writeError(e error) error {
	var m *mysql.SQLError
	var ok bool
	originErr := errors.Cause(e)
	if m, ok = originErr.(*mysql.SQLError); !ok {
		m = mysql.NewErrf(mysql.ErrUnknown, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))
	data = append(data, mysql.ErrHeader)
	data = append(data, byte(m.Code), byte(m.Code>>8))
	if cc.Capability&mysql.ClientProtocol41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	err := cc.writePacket(data)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(cc.flush())
}

func (cc *ClientConn) writeEOF() error {
	data := cc.alloc.AllocWithLen(4, 9)

	data = append(data, mysql.EOFHeader)
	if cc.Capability&mysql.ClientProtocol41 > 0 {
		data = append(data, dumpUint16(cc.ctx.WarningCount())...)
		data = append(data, dumpUint16(cc.ctx.Status())...)
	}

	err := cc.writePacket(data)
	return errors.Trace(err)
}

func (cc *ClientConn) handleQuery(sql string) (err error) {
	rs, err := cc.ctx.Execute(sql)
	if err != nil {
		return errors.Trace(err)
	}
	if rs != nil {
		err = cc.writeResultset(rs, false)
	} else {
		err = cc.writeOK()
	}
	return errors.Trace(err)
}

func (cc *ClientConn) handleFieldList(sql string) (err error) {
	parts := strings.Split(sql, "\x00")
	columns, err := cc.ctx.FieldList(parts[0])
	if err != nil {
		return errors.Trace(err)
	}
	data := make([]byte, 4, 1024)
	for _, v := range columns {
		data = data[0:4]
		data = append(data, v.Dump(cc.alloc)...)
		if err := cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}
	if err := cc.writeEOF(); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(cc.flush())
}

func (cc *ClientConn) writeResultset(rs ResultSet, binary bool) error {
	defer rs.Close()
	// We need to call Next before we get columns.
	// Otherwise, we will get incorrect columns info.
	row, err := rs.Next()
	if err != nil {
		return errors.Trace(err)
	}

	columns, err := rs.Columns()
	if err != nil {
		return errors.Trace(err)
	}
	columnLen := dumpLengthEncodedInt(uint64(len(columns)))
	data := cc.alloc.AllocWithLen(4, 1024)
	data = append(data, columnLen...)
	if err := cc.writePacket(data); err != nil {
		return errors.Trace(err)
	}

	for _, v := range columns {
		data = data[0:4]
		data = append(data, v.Dump(cc.alloc)...)
		if err := cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
	}

	if err = cc.writeEOF(); err != nil {
		return errors.Trace(err)
	}

	for {
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		data = data[0:4]
		if binary {
			var rowData []byte
			rowData, err = dumpRowValuesBinary(cc.alloc, columns, row)
			if err != nil {
				return errors.Trace(err)
			}
			data = append(data, rowData...)
		} else {
			for i, value := range row {
				if value == nil {
					data = append(data, 0xfb)
					continue
				}
				var valData []byte
				valData, err = dumpTextValue(columns[i].Type, value)
				if err != nil {
					return errors.Trace(err)
				}
				data = append(data, dumpLengthEncodedString(valData, cc.alloc)...)
			}
		}

		if err := cc.writePacket(data); err != nil {
			return errors.Trace(err)
		}
		row, err = rs.Next()
	}

	err = cc.writeEOF()
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(cc.flush())
}
