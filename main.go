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
	"flag"
	f "fmt"
	"github.com/percona/go-mysql/log"
	parser "github.com/percona/go-mysql/log/slow"
	"github.com/percona/go-mysql/event"
	"github.com/percona/go-mysql/query"
	"encoding/json"
	"os"
	"time"
  "sync"
)

func handleErr(e error) {
	if e != nil {
		f.Println(e)
		os.Exit(1)
	}
}

func loadSlowLog() *os.File {
	fname := flag.String("f", "", "filename")
	flag.Parse()
	if *fname == "" {
		f.Println("Please pass a filename to parse")
		os.Exit(1)
	}

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
	wg := sync.WaitGroup{}
	o := log.Options{}

	p := parser.NewSlowLogParser(fh, o)

	go p.Start()

	// sorted_log := make(map[uint64][]*log.Event)
	log_channels := make(map[uint64]chan *log.Event)

	for e := range p.EventChan() {
		if val, ok := log_channels[e.NumberMetrics["Thread_id"]]; ok {
			val <- e
		} else {
      // f.Println("Creating channel for ", e.NumberMetrics["Thread_id"])
			log_channels[e.NumberMetrics["Thread_id"]] = make(chan *log.Event, 100)
      wg.Add(1)

			// this listener aggregates sessions stats.
			go func (thread_id uint64, log_events chan *log.Event) {
        wg.Done()

      	a := event.NewAggregator(false /* we dont want PII */, 0 /* utc offset */, 0 /* long query time */)
        ch_open := true
      	for ch_open == true {
      		select {
      		case e, open := <-log_events:
              ch_open = open
              if ch_open {
                fp := query.Fingerprint(e.Query)
                if fp == "quit" {
                  ch_open = false;
                }

                if fp != "set @sql_context_injection=?" {
              		id := query.Id(fp)
          		    a.AddEvent(e, id, fp)
                }
              }
      		case <-time.After(time.Millisecond):
      		}
      	}
      	got := a.Finalize()
      	gotJson, err := json.MarshalIndent(got, "", "  ")

      	handleErr(err)

      	f.Printf("%d: %s\n", thread_id, gotJson)
      } (e.NumberMetrics["Thread_id"], log_channels[e.NumberMetrics["Thread_id"]])
		}
	}

	for _, v := range log_channels {
    close(v)
  }
  wg.Wait()
}

func main() {
	parseSlowLog(loadSlowLog())
}
