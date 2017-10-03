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

package rdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/rapidloop/sop/sopdb"
)

func TestLatestDBBasic(t *testing.T) {
	os.RemoveAll("/tmp/testlatest")
	db, err := OpenLatestDB("/tmp/testlatest")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLatestDBPutGet(t *testing.T) {
	os.RemoveAll("/tmp/testlatest")
	db, err := OpenLatestDB("/tmp/testlatest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	err = db.Put(100, 1234, 4567.8)
	if err != nil {
		t.Fatal(err)
	}

	ts, val, err := db.Get(100)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 1234 {
		t.Fatalf("got %d, expected 1234", ts)
	}
	if val != 4567.8 {
		t.Fatalf("got %v, expected 4567.8", val)
	}

	ts, val, err = db.Get(200)
	if err == nil {
		t.Fatal("expected error")
	}
	if err != sopdb.ErrNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestLatestDBOpenFail(t *testing.T) {
	ioutil.WriteFile("/tmp/test2", []byte{0}, 0755)
	_, err := OpenLatestDB("/tmp/test2")
	if err == nil {
		t.Fatal(err)
	}
	os.Remove("/tmp/test2")
}

func TestLatestDBIterate(t *testing.T) {
	os.RemoveAll("/tmp/testlatest")
	db, err := OpenLatestDB("/tmp/testlatest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	for i := 1; i <= 1000; i++ {
		err = db.Put(i, uint64(i), float64(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	ids := make([]int, 0, 500)
	err = db.Iterate(500, func(seriesID int, value float64) error {
		if float64(seriesID) != value {
			t.Fatalf("series id %d, value %v", seriesID, value)
		}
		ids = append(ids, seriesID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 500 {
		t.Fatalf("expected 500 entries, got %d", len(ids))
	}
	for _, id := range ids {
		if id <= 500 || id > 1000 {
			t.Fatalf("unexpected id %d", id)
		}
	}
}
