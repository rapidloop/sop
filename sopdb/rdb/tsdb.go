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
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"

	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
	"github.com/tecbot/gorocksdb"
)

type TSDB struct {
	rdb   *gorocksdb.DB
	defro *gorocksdb.ReadOptions
	defwo *gorocksdb.WriteOptions
}

func OpenTSDB(dir string) (db *TSDB, err error) {
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
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(8))
	rdb, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return
	}

	defro := gorocksdb.NewDefaultReadOptions()
	defwo := gorocksdb.NewDefaultWriteOptions()
	//defwo.SetSync(true)

	db = &TSDB{rdb: rdb, defro: defro, defwo: defwo}
	return
}

func (db *TSDB) Close() error {
	db.defro.Destroy()
	db.defwo.Destroy()
	db.rdb.Close()
	return nil
}

func (db *TSDB) Put(seriesID int, timestamp uint64, value float64) error {
	key := sidts2bytes(seriesID, timestamp)
	val := value2bytes(value)
	return db.rdb.Put(db.defwo, key, val)
}

// DeleteUntil deletes all records that have a timestamp less than the
// specified one. It probably needs to trigger a compaction after this.
func (db *TSDB) DeleteUntil(timestamp uint64) (int, error) {
	// create iterator
	it := db.rdb.NewIterator(db.defro)
	defer it.Close()

	// make batch
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	// iterate
	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		k := it.Key()
		_, ts, err := bytes2sidts(k.Data())
		if err == nil && ts < timestamp {
			batch.Delete(k.Data())
		}
		k.Free()
	}
	if err := it.Err(); err != nil {
		return 0, err
	}
	if batch.Count() == 0 {
		return 0, nil
	}

	if err := db.rdb.Write(db.defwo, batch); err != nil {
		return 0, err
	}
	return batch.Count(), nil
}

func (db *TSDB) Query(seriesID int, from, to uint64) (result []model.Sample, err error) {
	// create iterator
	it := db.rdb.NewIterator(db.defro)
	defer it.Close()

	// iterate
	first := sidts2bytes(seriesID, from)
	it.Seek(first)
	for ; it.Valid(); it.Next() {

		// get key
		k := it.Key()
		sid, ts, err := bytes2sidts(k.Data())
		if err != nil {
			k.Free()
			return nil, err
		}
		if sid != seriesID || ts > to {
			k.Free()
			break
		}

		// get value
		v := it.Value()
		val, err := bytes2value(v.Data())
		if err != nil {
			k.Free()
			v.Free()
			return nil, err
		}

		// process
		result = append(result, model.Sample{Timestamp: ts, Value: val})
		k.Free()
		v.Free()
	}
	if err = it.Err(); err != nil {
		result = nil
		return
	}
	model.SortSamples(result)
	return
}

type Iterator struct {
	id    int
	it    *gorocksdb.Iterator
	currt uint64
	currv float64
}

func NewIterator(id int, it *gorocksdb.Iterator) *Iterator {
	return &Iterator{id: id, it: it}
}

func (i *Iterator) Seek(t uint64) bool {
	key := sidts2bytes(i.id, t)
	i.it.Seek(key)
	if err := i.it.Err(); err != nil || !i.it.Valid() {
		return false
	}
	return i.load()
}

func (i *Iterator) load() bool {
	// get key
	i.currt = 0
	k := i.it.Key()
	defer k.Free()
	sid, ts, err := bytes2sidts(k.Data())
	if err != nil { // corrupt data
		log.Print(err)
		return false
	}
	if sid != i.id { // ran out of the required id
		return false
	}
	i.currt = ts

	// get value
	v := i.it.Value()
	defer v.Free()
	val, err := bytes2value(v.Data())
	if err != nil { // corrupt data
		log.Print(err)
		return false
	}
	i.currv = val
	return true
}

func (i *Iterator) At() (uint64, float64) {
	return i.currt, i.currv
}

func (i *Iterator) Next() bool {
	i.it.Next()
	if err := i.it.Err(); err != nil || !i.it.Valid() {
		return false
	}
	return i.load()
}

func (i *Iterator) Close() {
	i.it.Close()
}

func (db *TSDB) Iterate(seriesID int) sopdb.Iterator {
	return NewIterator(seriesID, db.rdb.NewIterator(db.defro))
}

func sidts2bytes(seriesID int, ts uint64) []byte {
	result := make([]byte, 8+8)
	binary.BigEndian.PutUint64(result[0:8], uint64(seriesID))
	binary.BigEndian.PutUint64(result[8:16], ts)
	return result
}

func bytes2sidts(in []byte) (sid int, ts uint64, err error) {
	if len(in) != 16 {
		err = fmt.Errorf("bad key, got %d bytes", len(in))
		return
	}
	sid = int(binary.BigEndian.Uint64(in[0:8]))
	ts = binary.BigEndian.Uint64(in[8:16])
	return
}

func value2bytes(val float64) []byte {
	val2 := math.Float64bits(val)
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result[0:8], val2)
	return result
}

func bytes2value(in []byte) (val float64, err error) {
	if len(in) != 8 {
		err = fmt.Errorf("bad data, got %d bytes", len(in))
		return
	}
	val2 := binary.BigEndian.Uint64(in)
	val = math.Float64frombits(val2)
	return
}
