// Copyright 2017 RapidLoop, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/rapidloop/sop/sopdb/rdb"
)

var (
	flagDBPath = flag.String("path", "data/index", "path to the index database")
	flagDump   = flag.Bool("dump", true, "dump the database")
	flagVerify = flag.Bool("verify", true, "verify the database")
)

func main() {
	flag.Parse()

	db, err := rdb.OpenIndexDB(*flagDBPath)
	if err != nil {
		log.Fatal(err)
	}

	if *flagDump {
		if errCount := db.Dump(); errCount > 0 {
			fmt.Printf("\nDump failed with %d errors.\n", errCount)
			db.Close()
			os.Exit(1)
		}
	}

	if *flagVerify {
		if *flagDump {
			fmt.Println()
		}
		if errCount := db.Verify(); errCount > 0 {
			fmt.Printf("\nVerify failed with %d errors.\n", errCount)
			db.Close()
			os.Exit(1)
		}
	}

	db.Close()
	os.Exit(0)
}
