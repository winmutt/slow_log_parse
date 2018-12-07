/*
	Copyright (c) 2018, Rolf Martin-Hoster. All rights reserved.
	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.
	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.
	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package main

import (
	"encoding/json"
	"errors"
	"flag"
	f "fmt"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/percona/go-mysql/event"
	"github.com/percona/go-mysql/log"
	parser "github.com/percona/go-mysql/log/slow"
	"github.com/percona/go-mysql/query"
)

func handleErr(e error) {
	if e != nil {
		f.Println(e)
		os.Exit(1)
	}
}

func loadSlowLog(fname *string) *os.File {

	fh, err := os.Open(*fname)
	handleErr(err)

	return fh

}

func parseSlowLog(fh *os.File) {
	// event wg
	eg := sync.WaitGroup{}
	// result wg
	rg := sync.WaitGroup{}

	p := parser.NewSlowLogParser(fh, log.Options{})

	// start parsing slow log
	go p.Start()

	logChannels := make(map[uint64]chan *log.Event)
	resultChannel := make(chan event.Result, 10000)

	// ensure we wait for the resultChannel to close
	rg.Add(1)

	// let's setup the result channel first to listen for output from the per thread parser
	go func(resultChannel chan event.Result) {
		defer rg.Done()

	result_loop:
		for {
			select {
			case r, open := <-resultChannel:
				if !open {
					break result_loop
				}
				gotJSON, err := json.Marshal(r)

				handleErr(err)

				f.Println(string(gotJSON))
			case <-time.After(time.Millisecond):
			default:
			}
		}

	}(resultChannel)

	// let's start receiving parsed log events
	for e := range p.EventChan() {
		threadID := e.NumberMetrics["Thread_id"]
		// if global threadID is not set grab it from the first event
		if threadID == 0 {
			threadID = e.ThreadID
		}

		// check to see if we are ready to send events to the log channel
		if logchan, ok := logChannels[threadID]; ok {
			logchan <- e
		} else {
			// otherwise let's create a log channel
			logChannels[threadID] = make(chan *log.Event, 1000)
			eg.Add(1)

			// this listener aggregates sessions stats.
			go func(threadID uint64, log_events <-chan *log.Event, resultChannel chan<- event.Result) {
				defer eg.Done()

				// TODO - add args for these params
				a := event.NewAggregator(true /* we need context */, 0 /* utc offset */, 0 /* long query time */)

				open := true
				var e *log.Event

				// leave channel open until an QUIT (disconnect) is seen or log parsing is completed
				for open == true {
					select {
					case e, open = <-log_events:
						if open {
							fp := query.Fingerprint(e.Query)

							//helps us manage goroutine counts
							if fp == "quit" {
								open = false
							}

							//need to add filtering support ???
							id := query.Id(fp)
							a.AddEvent(e, id, fp)

						}
					case <-time.After(time.Millisecond):
						// sleep for a ms if nothing is ready
						// TODO figure out if increasing the wait time incrementally decreases total time
					}
				}

				// aggregate the results back into the result channel
				resultChannel <- a.Finalize()

			}(threadID, logChannels[threadID], resultChannel)
		}
	}

	// at the end of processing the log, we force the log channels to close so
	// that they are notified that is all the data they are going to receive
	// and call finalize
	for _, v := range logChannels {
		close(v)
	}
	// wait for all the slow log go routines to close
	eg.Wait()

	// implicitly close the resultChannel as all log events have been processed
	close(resultChannel)

	// wait for all the results to spit out
	rg.Wait()

}

func main() {
	flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	flag.ErrHelp = errors.New("flag: help requested")

	flag.Usage = func() {
		f.Printf("Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	cpuprofile := flag.String("cpuprofile", "", "useful for debugging only, write cpu profile to file")
	fname := flag.String("f", "", "File name of slow log to parse, REQUIRED")

	flag.Parse()
	if *fname == "" {
		flag.Usage()
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			handleErr(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	parseSlowLog(loadSlowLog(fname))
}
