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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"runtime"

	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
	"github.com/tecbot/gorocksdb"
)

type LatestDB struct {
	rdb   *gorocksdb.DB
	defro *gorocksdb.ReadOptions
	defwo *gorocksdb.WriteOptions
}

func OpenLatestDB(dir string) (db *LatestDB, err error) {
	os.MkdirAll(dir, 0775)

	//filter := gorocksdb.NewBloomFilter(10) // 10 bits
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	//bbto.SetFilterPolicy(filter)
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	//opts.OptimizeForPointLookup(10)              // 10MB cache
	//opts.SetAllowConcurrentMemtableWrites(false) // reqd for previous
	opts.IncreaseParallelism(runtime.NumCPU())
	rdb, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return
	}

	defro := gorocksdb.NewDefaultReadOptions()
	defwo := gorocksdb.NewDefaultWriteOptions()
	//defwo.SetSync(true)

	db = &LatestDB{rdb: rdb, defro: defro, defwo: defwo}
	return
}

func (db *LatestDB) Close() error {
	db.defro.Destroy()
	db.defwo.Destroy()
	db.rdb.Close()
	return nil
}

func (db *LatestDB) Put(seriesID int, metric model.Metric, timestamp uint64,
	value float64) error {

	lv := &latestValue{
		Metric:    metric,
		Timestamp: timestamp,
		Value:     value,
	}
	val, err := lv.Encode()
	if err != nil {
		return err
	}
	key := int2bytes(seriesID)
	return db.rdb.Put(db.defwo, key, val)
}

func (db *LatestDB) Get(seriesID int) (metric model.Metric, timestamp uint64,
	value float64, err error) {

	key := int2bytes(seriesID)
	slice, err := db.rdb.Get(db.defro, key)
	if slice != nil {
		defer slice.Free()
	}
	if err != nil {
		return
	}
	if slice.Size() == 0 && err == nil { // not found
		err = sopdb.ErrNotFound
		return
	}
	var lv latestValue
	if err = lv.Decode(slice.Data()); err != nil {
		return
	}
	metric = lv.Metric
	timestamp = lv.Timestamp
	value = lv.Value
	return
}

func (db *LatestDB) Iterate(cb sopdb.LatestDBProcessorFunc) error {

	// create iterator
	it := db.rdb.NewIterator(db.defro)
	defer it.Close()

	// iterate
	it.SeekToFirst()
	for ; it.Valid(); it.Next() {

		// get key
		k := it.Key()
		key, err := bytes2int(k.Data())
		if err != nil {
			k.Free()
			return err
		}

		// get value
		v := it.Value()
		var lv latestValue
		if err := lv.Decode(v.Data()); err != nil {
			k.Free()
			v.Free()
			return err
		}

		// process
		err = cb(key, lv.Metric, lv.Timestamp, lv.Value)
		k.Free()
		v.Free()
		if err != nil {
			return err
		}
	}
	return it.Err()
}

func int2bytes(id int) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, uint64(id))
	return result
}

func bytes2int(in []byte) (id int, err error) {
	if len(in) != 8 {
		err = fmt.Errorf("bad key, got %d bytes", len(in))
		return
	}
	id = int(binary.BigEndian.Uint64(in))
	return
}

type latestValue struct {
	Metric    model.Metric
	Timestamp uint64
	Value     float64
}

func (lv *latestValue) Decode(in []byte) error {
	return gob.NewDecoder(bytes.NewReader(in)).Decode(lv)
}

func (lv *latestValue) Encode() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(lv); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func init() {
	gob.Register(latestValue{})
}
