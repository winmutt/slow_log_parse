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

// # Time: 2018-09-11T18:47:04.652709Z
// # User@Host: mc_20171004[mc_20171004] @  [10.130.6.16]  Id: 172519641
// # Schema: mc_7678901  Last_errno: 0  Killed: 0
// # Query_time: 0.002067  Lock_time: 0.000286  Rows_sent: 0  Rows_examined: 506  Rows_affected: 0
// # Bytes_sent: 5424  Tmp_tables: 0  Tmp_disk_tables: 0  Tmp_table_sizes: 0
// # QC_Hit: No  Full_scan: Yes  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
// # Filesort: No  Filesort_on_disk: No  Merge_passes: 0
// #   InnoDB_IO_r_ops: 0  InnoDB_IO_r_bytes: 0  InnoDB_IO_r_wait: 0.000000
// #   InnoDB_rec_lock_wait: 0.000000  InnoDB_queue_wait: 0.000000
// #   InnoDB_pages_distinct: 16
// SET timestamp=1536691624;
// SELECT `Campaign_Automation`.`campaign_id` AS `Campaign_Automation__campaign_id`, ......

func parseSlowLog(fh *os.File) {
	// event wg
	eg := sync.WaitGroup{}
	// result wg
	rg := sync.WaitGroup{}

	p := parser.NewSlowLogParser(fh, log.Options{})

	go p.Start()

	logChannels := make(map[uint64]chan *log.Event)
	resultChannel := make(chan event.Result, 10000)

	for e := range p.EventChan() {
		threadID := e.NumberMetrics["Thread_id"]
		if threadID == 0 {
			threadID = e.ThreadID
		}
		if val, ok := logChannels[threadID]; ok {
			val <- e
		} else {
			// f.Println("Creating channel for ", threadID)
			logChannels[threadID] = make(chan *log.Event, 1000)
			eg.Add(1)

			// this listener aggregates sessions stats.
			go func(threadID uint64, log_events <-chan *log.Event, resultChannel chan<- event.Result) {
				defer eg.Done()

				a := event.NewAggregator(false /* we dont want PII */, 0 /* utc offset */, 0 /* long query time */)

				var open bool
				open = true
				var e *log.Event

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
					}
				}

				// aggregate the results back into the result channel
				resultChannel <- a.Finalize()

			}(threadID, logChannels[threadID], resultChannel)
		}
	}

	// ensure we wait for the resultChannel to close
	rg.Add(1)
	go func(resultChannel chan event.Result) {
		defer rg.Done()

		results := make([]event.Result, 1)

	result_loop:
		for {
			select {
			case r, open := <-resultChannel:
				if !open {
					break result_loop
				}
				results = append(results, r)
			case <-time.After(time.Millisecond):
			default:
			}
		}

		gotJSON, err := json.Marshal(results)

		handleErr(err)

		f.Println(string(gotJSON))
	}(resultChannel)

	// at the end of processing the log, we force the log channels to close so
	// that they are notified that is all the data they are going to receive
	// and call finalize
	for _, v := range logChannels {
		close(v)
	}
	// wait for all the slow log go routines to close
	eg.Wait()

	close(resultChannel)
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
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	fname := flag.String("f", "", "File name of slow log to parse, required")

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
