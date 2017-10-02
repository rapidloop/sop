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
