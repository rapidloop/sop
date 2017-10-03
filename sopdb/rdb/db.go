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
	"os"
	"path/filepath"

	"github.com/rapidloop/sop/sopdb"
)

type rdbt struct {
	idb      *IndexDB
	latestdb *LatestDB
	tsdb     *TSDB
}

func NewDB(dir string) (sopdb.DB, error) {
	os.MkdirAll(dir, 0775)

	idb, err := OpenIndexDB(filepath.Join(dir, "index"))
	if err != nil {
		return nil, err
	}

	latestdb, err := OpenLatestDB(filepath.Join(dir, "latest"))
	if err != nil {
		idb.Close()
		return nil, err
	}

	tsdb, err := OpenTSDB(filepath.Join(dir, "tsdb"))
	if err != nil {
		return nil, err
	}

	return &rdbt{idb, latestdb, tsdb}, nil
}

func (r *rdbt) Index() sopdb.IndexDB {
	return r.idb
}

func (r *rdbt) Latest() sopdb.LatestDB {
	return r.latestdb
}

func (r *rdbt) TS() sopdb.TSDB {
	return r.tsdb
}

func (r *rdbt) Close() error {
	r.idb.Close()
	r.latestdb.Close()
	r.tsdb.Close()
	return nil
}
