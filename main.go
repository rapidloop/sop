package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/rapidloop/sop/api"
	"github.com/rapidloop/sop/input"
	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/output"
	"github.com/rapidloop/sop/sopdb"
	"github.com/rapidloop/sop/sopdb/rdb"
)

const VERSION = "1.0-beta1"

var config model.Config

func main() {
	if len(os.Args) != 2 {
		help(os.Stderr)
		os.Exit(1)
	}
	switch os.Args[1] {
	case "-h", "--help":
		help(os.Stdout)
		return
	case "--version":
		version()
		return
	case "-p":
		model.PrintExampleConfig()
		return
	default:
		if err := config.Load(os.Args[1]); err != nil {
			fmt.Fprintf(os.Stderr, "sop: failed to load config: %v\n", err)
			os.Exit(1)
		}
	}

	os.Exit(realmain())
}

func help(w io.Writer) {
	fmt.Fprintf(w, `Usage: sop path/to/config

sop is a multi-purpose metrics manipulation tool * https://github.com/rapidloop/sop
sop version %v * (c) RapidLoop, Inc. 2017 * Apache 2.0 License

  sop path/to/config      start sop with the specified config
  sop -p                  print detailed example configuration to stdout
  sop -h, sop --help      show this help
  sop --version           show version info

To get started, try:
  $ sop -p > sop.cfg
  $ vim sop.cfg
  $ sop sop.cfg
`, VERSION)
}

func version() {
	fmt.Println(VERSION)
}

//------------------------------------------------------------------------------

func realmain() int {
	// setup log
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Lshortfile)
	log.Printf("sop starting: version=%s, pid=%d", VERSION, os.Getpid())
	defer log.Print("sop shutdown complete")

	// start outputs
	outputs := make([]output.Output, 0, len(config.Output))
	for _, outcfg := range config.Output {
		out, err := output.NewOutput(outcfg)
		if err != nil {
			log.Printf("failed to create output (type %s): %v", outcfg.Type, err)
			log.Printf("configuration was: %+v", outcfg)
			return 1
		}
		if err = out.Start(); err != nil {
			log.Printf("failed to start output %s: %v", out.Info(), err)
			return 1
		}
		log.Printf("started output: %s", out.Info())
		outputs = append(outputs, out)
	}
	defer func() {
		for _, out := range outputs {
			if err := out.Stop(); err != nil {
				log.Printf("failed to stop output %s: %v", out.Info(), err)
			} else {
				log.Printf("stopped output: %s", out.Info())
			}
		}
	}()

	// open db
	tdb := time.Now()
	db, err := rdb.NewDB(config.General.DataPath)
	if err != nil {
		log.Printf("failed to open database at %q: %v", config.General.DataPath,
			err)
		return 1
	}
	elapsed := time.Since(tdb)
	log.Printf("started database: path=%s (took %v)", config.General.DataPath,
		elapsed)
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("failed to stop database: %v", err)
		} else {
			log.Print("stopped database")
		}
	}()

	// create storer
	storer := sopdb.NewStorer(db, config.General, outputs)
	storer.Start()
	log.Printf("started storer: ttl=%v, downsample=%v", storer.TTL(),
		storer.Downsample())
	defer func() {
		storer.Stop()
		log.Print("stopped storer")
	}()

	// create reaper
	if config.General.RetentionDays <= 0 || config.General.RetentionGCHours <= 0 {
		log.Print("retention/gc not set, old data will not be scrubbed")
	} else {
		reaper := sopdb.NewReaper(db, config.General)
		reaper.Start()
		log.Printf("started reaper: retain=%v, gc=%v", reaper.Retain(),
			reaper.Interval())
		defer func() {
			reaper.Stop()
			log.Print("stopped reaper")
		}()
	}

	// start inputs
	inputs := make([]input.Input, 0, len(config.Input))
	for _, inpcfg := range config.Input {
		inp, err := input.NewInput(inpcfg, storer)
		if err != nil {
			log.Printf("failed to create input (type %s): %v", inpcfg.Type, err)
			log.Printf("configuration was: %+v", inpcfg)
			return 1
		}
		if err = inp.Start(); err != nil {
			log.Printf("failed to start input %s: %v", inp.Info(), err)
			return 1
		}
		log.Printf("started input: %s", inp.Info())
		inputs = append(inputs, inp)
	}
	defer func() {
		for _, inp := range inputs {
			if err := inp.Stop(); err != nil {
				log.Printf("failed to stop input %s: %v", inp.Info(), err)
			} else {
				log.Printf("stopped input: %s", inp.Info())
			}
		}
	}()

	// start APIs
	apis := make([]api.API, 0, len(config.API))
	for _, apicfg := range config.API {
		oneAPI, err := api.NewAPI(apicfg, config.General, db)
		if err != nil {
			log.Printf("failed to create api (type %s): %v", apicfg.Type, err)
			log.Printf("configuration was: %+v", apicfg)
			return 1
		}
		if err = oneAPI.Start(); err != nil {
			log.Printf("failed to start api %s: %v", oneAPI.Info(), err)
			return 1
		}
		log.Printf("started api: %s", oneAPI.Info())
		apis = append(apis, oneAPI)
	}
	defer func() {
		for _, oneAPI := range apis {
			if err := oneAPI.Stop(); err != nil {
				log.Printf("failed to stop api %s: %v", oneAPI.Info(), err)
			} else {
				log.Printf("stopped api: %s", oneAPI.Info())
			}
		}
	}()

	// ready
	log.Print("sop open for business")

	// wait for signal
	log.Printf("exiting on signal: %v", waitForInterrupt())
	return 0
}

func waitForInterrupt() (s os.Signal) {
	ch := make(chan os.Signal, 5)
	signal.Notify(ch, os.Interrupt)
	s = <-ch
	signal.Stop(ch)
	close(ch)
	return
}
