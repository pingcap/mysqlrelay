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

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/mysqlrelay/relay"
	"github.com/pingcap/tidb"
)

var (
	store     = flag.String("store", "memory", "registered store name, [memory, goleveldb, boltdb]")
	storePath = flag.String("path", "/tmp/tidb", "tidb storage path")
	logLevel  = flag.String("L", "error", "log level: info, debug, warn, error, fatal")
	port      = flag.String("P", "4000", "mp server port")
	lease     = flag.Int("lease", 1, "schema lease seconds, very dangerous to change only if you know what you do")
	relayPath = flag.String("relay_path", "/tmp/tidb_relay.log", "log file to record commands")
)

func main() {
	flag.Parse()

	if *lease < 0 {
		log.Fatalf("invalid lease seconds %d", *lease)
	}

	tidb.SetSchemaLease(time.Duration(*lease) * time.Second)

	log.SetLevelByString(*logLevel)
	store, err := tidb.NewStore(fmt.Sprintf("%s://%s", *store, *storePath))
	if err != nil {
		log.Fatal(err)
	}

	var driver relay.IDriver
	driver = relay.NewTiDBDriver(store)
	var svr *relay.Server
	svr, err = relay.NewServer(driver, fmt.Sprintf(":%s", *port), *relayPath)
	if err != nil {
		log.Fatal(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)
		svr.Close()
		time.Sleep(1 * time.Second)
		os.Exit(0)
	}()

	log.Error(svr.Run())
}
