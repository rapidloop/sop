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

package sopdb

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/rapidloop/sop/model"
)

type Reaper struct {
	db       DB
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	interval time.Duration
	retain   time.Duration
}

func NewReaper(db DB, config model.GeneralConfig) *Reaper {
	retain := time.Duration(int64(config.RetentionDays*24)) * time.Hour
	interval := time.Duration(int64(config.RetentionGCHours)) * time.Hour
	return &Reaper{
		db:       db,
		interval: interval,
		retain:   retain,
	}
}

func (r *Reaper) Interval() time.Duration {
	return r.interval
}

func (r *Reaper) Retain() time.Duration {
	return r.retain
}

func (r *Reaper) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(1)
	go r.reaper(ctx)
}

func (r *Reaper) Stop() {
	r.cancel()
	r.wg.Wait()
}

func (r *Reaper) reaper(ctx context.Context) {
	t := time.NewTicker(r.interval)
	count := 1
	for {
		select {
		case <-t.C:
			r.reap(count)
			count++
		case <-ctx.Done():
			t.Stop()
			r.wg.Done()
			return
		}
	}
}

func (r *Reaper) reap(count int) {
	log.Printf("reaper run #%d starting", count)

	until := time.Now().Add(-r.retain)
	untilMs := uint64(until.UnixNano() / 1e6)
	log.Printf("deleting from tsdb until %s", until.Format(time.RFC3339Nano))

	n, err := r.db.TS().DeleteUntil(untilMs)
	if err != nil {
		log.Printf("failed: %v", err)
	} else {
		log.Printf("deleted %d records", n)
	}

	log.Printf("reaper run #%d done", count)
}
