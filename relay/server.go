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
	"io/ioutil"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/arena"
)

var (
	baseConnID uint32 = 10000
)

// Server is the MySQL protocol server
type Server struct {
	driver            IDriver
	listener          net.Listener
	rwlock            *sync.RWMutex
	concurrentLimiter *TokenLimiter
	clients           map[uint32]net.Conn
	wg                sync.WaitGroup

	rec *os.File
}

// use a const salt for handshake
var salt = []byte("01234567890123456789")

func (s *Server) newConn(conn net.Conn) (cc *ClientConn, err error) {
	addr := conn.RemoteAddr().String()
	log.Info("newConn", addr)
	cc = &ClientConn{
		pkg:          newDefaultPacket(conn),
		ConnectionID: atomic.AddUint32(&baseConnID, 1),
		Collation:    mysql.DefaultCollationID,
		Charset:      mysql.DefaultCharset,
		alloc:        arena.NewAllocator(32 * 1024),
		Addr:         addr,
		recWriter:    ioutil.Discard,
	}
	return
}

// NewServer creates a new Server.
func NewServer(driver IDriver, addr string, path string) (*Server, error) {
	s := &Server{
		driver:            driver,
		concurrentLimiter: NewTokenLimiter(1),
		rwlock:            &sync.RWMutex{},
		clients:           make(map[uint32]net.Conn),
	}

	var err error

	if len(path) > 0 {
		s.rec, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0755)
	} else {
		// if empty, we will use DevNull file.
		s.rec, err = os.Open(os.DevNull)
	}

	if err != nil {
		return nil, errors.Trace(err)
	}

	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("Server run MySql Protocol Listen at [%s]", addr)
	return s, nil
}

// Run runs the server.
func (s *Server) Run() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("accept error %s", err.Error())
			return errors.Trace(err)
		}

		s.wg.Add(1)
		go s.onConn(conn)
	}
}

// Close closes the server.
func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}

	s.rwlock.Lock()
	for id, c := range s.clients {
		c.Close()
		delete(s.clients, id)
	}
	s.rwlock.Unlock()

	s.wg.Wait()

	if s.rec != nil {
		s.rec.Close()
		s.rec = nil
	}
}

func (s *Server) onConn(c net.Conn) {
	defer func() {
		c.Close()
		s.wg.Done()
	}()

	conn, err := s.newConn(c)
	if err != nil {
		log.Errorf("newConn error %s", errors.ErrorStack(err))
		return
	}
	if err := conn.Handshake(s.driver); err != nil {
		log.Errorf("handshake error %s", errors.ErrorStack(err))
		return
	}

	// record this connection
	buf, err := conn.Marshal()
	if err != nil {
		log.Errorf("encode connection %s err %s", conn, err)
		return
	}

	err = WriteRecord(s.rec, ConnCreateRecord, conn.ConnectionID, buf)
	if err != nil {
		log.Errorf("record create conneciont %s err %s", conn, err)
		return
	}

	// after handshake, we will record every request/response
	conn.recWriter = s.rec

	s.rwlock.Lock()
	s.clients[conn.ConnectionID] = c
	s.rwlock.Unlock()

	defer func() {
		log.Infof("close %s", conn)

		s.rwlock.Lock()
		delete(s.clients, conn.ConnectionID)
		s.rwlock.Unlock()

		WriteRecord(s.rec, ConnDeleteRecord, conn.ConnectionID, nil)
	}()

	conn.Run(s.concurrentLimiter)
}
