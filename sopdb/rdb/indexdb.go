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
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
	"github.com/rapidloop/sop/util"
	"github.com/tecbot/gorocksdb"
	"golang.org/x/tools/container/intsets"
)

type IndexDB struct {
	rdb    *gorocksdb.DB
	defro  *gorocksdb.ReadOptions
	defwo  *gorocksdb.WriteOptions
	nextID uint64
}

var idKey = []byte("@id")

func OpenIndexDB(dir string) (db *IndexDB, err error) {
	os.MkdirAll(dir, 0775)

	filter := gorocksdb.NewBloomFilter(10) // 10 bits
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetFilterPolicy(filter)
	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.OptimizeForPointLookup(20)              // 20MB cache
	opts.SetAllowConcurrentMemtableWrites(false) // reqd for previous
	opts.IncreaseParallelism(runtime.NumCPU())
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(1))
	rdb, err := gorocksdb.OpenDb(opts, dir)
	if err != nil {
		return
	}

	defro := gorocksdb.NewDefaultReadOptions()
	defwo := gorocksdb.NewDefaultWriteOptions()
	//defwo.SetSync(true)

	// read nextID from collection
	var nextID uint64
	val, err := rdb.Get(defro, idKey)
	if err == nil && val != nil && val.Size() == 8 {
		nextID = binary.BigEndian.Uint64(val.Data())
	}
	if val != nil {
		val.Free()
	}

	db = &IndexDB{rdb: rdb, defro: defro, defwo: defwo, nextID: nextID}
	return
}

func (db *IndexDB) Close() error {
	db.defro.Destroy()
	db.defwo.Destroy()
	db.rdb.Close()
	return nil
}

func (db *IndexDB) makeNextID() int {
	return int(atomic.AddUint64(&db.nextID, 1))
}

func (db *IndexDB) Query(expr model.LabelExpr) ([]int, error) {
	// the set that the ids must belong to
	pos := &intsets.Sparse{}
	neg := &intsets.Sparse{}

	// mark initial state
	pos.Insert(0)
	neg.Insert(0)

	// for each term
	for i, t := range expr.Terms {
		var err error
		switch t.Op {
		case model.MatchEq:
			err = db.matchEq(t.Name, t.Value, pos)
		case model.MatchNotEq:
			err = db.matchEq(t.Name, t.Value, neg)
		case model.MatchReg:
			err = db.matchReg(t.Name, expr.Res[i], pos)
		case model.MatchNotReg:
			err = db.matchReg(t.Name, expr.Res[i], neg)
		}
		if err != nil {
			return nil, err
		}
	}

	// take difference and return
	pos.DifferenceWith(neg)
	ids := make([]int, 0, pos.Len())
	ids = pos.AppendTo(ids)
	//log.Printf("%v => %d", expr, ids)
	return ids, nil
}

func (db *IndexDB) matchEq(name, value string, s *intsets.Sparse) error {
	k := []byte("=" + name + "=" + value)
	v, err := db.rdb.Get(db.defro, k)
	if err != nil {
		return err
	}
	vs, err := util.SparseFromBytes(v.Data())
	if err != nil {
		return err
	}
	v.Free()
	if s.Has(0) {
		s.Remove(0)
		s.UnionWith(vs)
	} else {
		s.IntersectionWith(vs)
	}
	return nil
}

func (db *IndexDB) matchReg(name string, re *regexp.Regexp, s *intsets.Sparse) error {

	// create iterator
	pfx := []byte("=" + name + "=")
	it := db.rdb.NewIterator(db.defro)
	defer it.Close()

	it.Seek(pfx)
	for ; it.Valid(); it.Next() {

		// get current item
		k := it.Key()
		kd := k.Data()
		if !bytes.HasPrefix(kd, pfx) {
			k.Free()
			break
		}
		v := it.Value()
		vs, err := util.SparseFromBytes(v.Data())
		if err != nil {
			k.Free()
			v.Free()
			return err
		}

		// process it
		if re.Match(kd[len(pfx):]) {
			if s.Has(0) {
				s.Remove(0)
				s.UnionWith(vs)
			} else {
				s.IntersectionWith(vs)
			}
		}
		k.Free()
		v.Free()
	}
	return it.Err()
}

func (db *IndexDB) matchRegCB(name string, re *regexp.Regexp, cb func(k, v []byte) error) error {

	// create iterator
	pfx := []byte(name + "=")
	it := db.rdb.NewIterator(db.defro)
	defer it.Close()

	it.Seek(pfx)
	for ; it.Valid(); it.Next() {

		// get current item
		k := it.Key()
		kd := k.Data()
		if !bytes.HasPrefix(kd, pfx) {
			k.Free()
			break
		}
		v := it.Value()
		vd := v.Data()

		// process it
		if re.Match(kd[len(pfx):]) {
			if err := cb(kd, vd); err != nil {
				return err
			}
		}
		k.Free()
		v.Free()
	}
	return it.Err()
}

func (db *IndexDB) Make(metric model.Metric) (id int, err error) {
	sortedMetric := metric.Copy()
	sortedMetric.Sort()

	// first do lookup
	id, err = db.LookupMetric(sortedMetric)
	if err == nil {
		return
	}

	// series not found, need to add:

	// serialize the metric
	metricBytes, err := sortedMetric.Encode()
	if err != nil {
		return
	}

	// start batch
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	// the new series id
	newID := db.makeNextID()
	newIDBytes := intToBytes(newID)
	batch.Put(idKey, newIDBytes)

	// the full string for the id for id->metric lookup
	recKey := make([]byte, 1+8)
	recKey[0] = '#'
	copy(recKey[1:], newIDBytes)
	batch.Put(recKey, metricBytes)

	// for metric->id lookup
	metKey := []byte("~" + sortedMetric.String())
	batch.Put(metKey, newIDBytes)

	// populate batch
	for _, l := range sortedMetric {
		key := []byte("=" + l.Name + "=" + l.Value)
		slice, err := db.rdb.Get(db.defro, key)
		if err != nil {
			return 0, err
		}
		s, err := util.SparseFromBytes(slice.Data())
		slice.Free()
		if err != nil {
			return 0, err
		}
		s.Insert(newID)
		newVal, err := util.SparseToBytes(s)
		if err != nil {
			return 0, err
		}
		batch.Put(key, newVal)
	}

	// execute batch
	if err = db.rdb.Write(db.defwo, batch); err != nil {
		return
	}
	return newID, nil
}

func (db *IndexDB) Lookup(id int) (metric model.Metric, err error) {
	key := make([]byte, 1+8)
	key[0] = '#'
	copy(key[1:], int2bytes(id))
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
	err = metric.Decode(slice.Data())
	return
}

func (db *IndexDB) LookupMetric(metric model.Metric) (id int, err error) {
	sortedMetric := metric.Copy()
	sortedMetric.Sort()

	key := []byte("~" + sortedMetric.String())
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
	return bytes2int(slice.Data())
}

/*
NOTTESTED
func (db *IndexDB) Delete(expr model.LabelExpr) error {
	// query and get ids
	ids, err := db.Query(expr)
	if err != nil {
		return err
	}

	// make idset
	idSet := &intsets.Sparse{}
	for _, id := range ids {
		idSet.Insert(id)
	}

	// start batch
	batch := gorocksdb.NewWriteBatch()
	defer batch.Destroy()

	// for each id
	for _, id := range ids {
		key := []byte("#" + strconv.Itoa(id))
		batch.Delete(key)
	}

	// for each term
	for i, t := range expr.Terms {
		switch t.Op {
		case model.MatchEq:
			key := []byte(t.Name + "=" + t.Value)
			slice, err := db.rdb.Get(db.defro, key)
			if err != nil {
				return err
			}
			vs, err := util.SparseFromBytes(slice.Data())
			slice.Free()
			if err != nil {
				return err
			}
			vs.DifferenceWith(idSet)
			newval, err := util.SparseToBytes(vs)
			if err != nil {
				return err
			}
			batch.Put(key, newval)
		case model.MatchReg:
			err := db.matchRegCB(t.Name, expr.Res[i], func(k, v []byte) error {
				vs, err := util.SparseFromBytes(v)
				if err != nil {
					return err
				}
				vs.DifferenceWith(idSet)
				newval, err := util.SparseToBytes(vs)
				if err != nil {
					return err
				}
				batch.Put(k, newval)
				return nil
			})
			if err != nil {
				return err
			}
		}
	}

	// execute batch
	return db.rdb.Write(db.defwo, batch)
}
*/

func (db *IndexDB) LabelValues(name string) (values []string, err error) {
	// create iterator
	pfx := []byte("=" + name + "=")
	it := db.rdb.NewIterator(db.defro)
	defer it.Close()

	it.Seek(pfx)
	for ; it.Valid(); it.Next() {

		// get current item
		k := it.Key()
		kd := k.Data()
		if !bytes.HasPrefix(kd, pfx) {
			k.Free()
			break
		}
		values = append(values, string(kd[len(pfx):]))
		k.Free()
	}
	err = it.Err()
	return
}

func intToBytes(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	return b
}

func bytesToInt(b []byte) (int, error) {
	if len(b) == 8 {
		return int(binary.BigEndian.Uint64(b)), nil
	}
	return 0, fmt.Errorf("bad data: expected 8 bytes, got %d", len(b))
}

func (db *IndexDB) Dump() (errors int) {
	it := db.rdb.NewIterator(db.defro)
	defer it.Close()
	it.SeekToFirst()
	for ; it.Valid(); it.Next() {
		k := it.Key()
		kd := k.Data()
		v := it.Value()
		vd := v.Data()

		if k.Size() == 0 {
			fmt.Println("ERR zero-length key")
			errors++
		} else if string(kd) == string(idKey) {
			if v.Size() == 8 {
				fmt.Printf("OK  id = %d\n", binary.BigEndian.Uint64(vd))
			} else {
				fmt.Printf("ERR id key but wrong size %d", v.Size())
				errors++
			}
		} else if kd[0] == '#' {
			// key is #<id>
			// value is metric gob
			if k.Size() != 9 {
				fmt.Printf("ERR map# wrong key size %d\n", k.Size())
				errors++
			} else {
				id := binary.BigEndian.Uint64(kd[1:])
				var metric model.Metric
				if err := metric.Decode(vd); err != nil {
					fmt.Printf("ERR map# %d bad value: %v", id, err)
					errors++
				} else {
					fmt.Printf("OK  map# %d -> %s\n", id, metric)
				}
			}
		} else if kd[0] == '~' {
			// key is ~metric_str
			// value is id, 8 bytes
			if v.Size() == 8 {
				id := binary.BigEndian.Uint64(vd)
				fmt.Printf("OK  map~ %s -> %d\n", string(kd[1:]), id)
			} else {
				fmt.Printf("ERR map~ %s wrong value size %d\n", string(kd[1:]), v.Size())
				errors++
			}
		} else if kd[0] == '=' {
			// key is name=value
			// value is sparse
			parts := strings.Split(string(kd), "=")
			if len(parts) != 3 {
				fmt.Printf("ERR map= bad key %q\n", string(kd))
				errors++
			} else {
				vs, err := util.SparseFromBytes(vd)
				if err != nil {
					fmt.Printf("ERR map= for key %s bad value: %v\n", string(kd), err)
					errors++
				} else {
					fmt.Printf("OK  map= %s %s %v\n", parts[1], parts[2], vs)
				}
			}
		} else {
			fmt.Printf("ERR unknown key %v\n", kd)
			errors++
		}

		k.Free()
		v.Free()
	}
	if err := it.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "ERR iteration error: %v", err)
		errors++
	}
	return
}

func (db *IndexDB) Verify() (errors int) {
	// get next id
	var nextID uint64
	if val, err := db.rdb.Get(db.defro, idKey); err != nil {
		fmt.Printf("ERR failed to get @id: %v\n", err)
		errors++
	} else if val.Size() == 0 && err == nil {
		fmt.Print("OK  @id not found, assuming 0\n")
		val.Free()
	} else if val.Size() != 8 {
		fmt.Printf("ERR size of @id's value is %d (expected 8)\n", val.Size())
		val.Free()
	} else {
		nextID = binary.BigEndian.Uint64(val.Data())
		val.Free()
	}
	fmt.Printf("OK  next id is %d\n", nextID)

	// load all id->metrics
	id2metric := make(map[int]model.Metric)
	it1 := db.rdb.NewIterator(db.defro)
	it1.Seek([]byte{'#'})
	done := false
	totalLabelCount := 0
	for ; !done && it1.Valid(); it1.Next() {
		k := it1.Key()
		kd := k.Data()
		v := it1.Value()
		vd := v.Data()

		if k.Size() > 1 && kd[0] == '#' {
			// key is #<id>
			// value is metric gob
			if k.Size() != 9 {
				fmt.Printf("ERR map# wrong key size %d\n", k.Size())
				errors++
			} else {
				id := binary.BigEndian.Uint64(kd[1:])
				var metric model.Metric
				if err := metric.Decode(vd); err != nil {
					fmt.Printf("ERR map# %d bad value: %v", id, err)
					errors++
				} else {
					totalLabelCount += len(metric)
					id2metric[int(id)] = metric
				}
			}
		} else {
			done = true
		}
		k.Free()
		v.Free()
	}
	if err := it1.Err(); err != nil {
		fmt.Printf("ERR iterator error: %v\n", err)
		errors++
	}
	it1.Close()

	// load all metrics->id
	metric2id := make(map[string]int)
	it2 := db.rdb.NewIterator(db.defro)
	it2.Seek([]byte{'~'})
	done = false
	for ; !done && it2.Valid(); it2.Next() {
		k := it2.Key()
		kd := k.Data()
		v := it2.Value()
		vd := v.Data()

		if k.Size() > 1 && kd[0] == '~' {
			// key is ~metric_str
			// value is id, 8 bytes
			if v.Size() != 8 {
				fmt.Printf("ERR map~ wrong value size %d for key %s\n",
					v.Size(), string(k.Data()))
				errors++
			} else {
				id := binary.BigEndian.Uint64(vd)
				metric2id[string(kd[1:])] = int(id)
			}
		} else {
			done = true
		}
		k.Free()
		v.Free()
	}
	if err := it2.Err(); err != nil {
		fmt.Printf("ERR iterator error: %v\n", err)
		errors++
	}
	it2.Close()

	// compare count
	if len(metric2id) != len(id2metric) {
		fmt.Printf("ERR %d metric->id but %d id->metric\n",
			len(metric2id), len(id2metric))
		errors++
	} else {
		fmt.Printf("OK  %d metric<->id\n", len(metric2id))
	}

	// check id->metric->id
	oldErrors := errors
	for id, metric := range id2metric {
		id2, ok := metric2id[metric.String()]
		if !ok {
			fmt.Printf("ERR id %d metric %v not found in map~\n", id, metric)
			errors++
		} else if id != id2 {
			fmt.Printf("ERR id %d metric %v has different id %d in map~\n",
				id, metric, id2)
			errors++
		}
		if id > int(nextID) {
			fmt.Printf("ERR id %d for metric %v is > nextID %d in map~\n", id,
				metric, nextID)
			errors++
		}
	}
	if oldErrors == errors {
		fmt.Print("OK  id->metric->id\n")
	}

	// check metric->id->metric
	oldErrors = errors
	for metric, id := range metric2id {
		metric2, ok := id2metric[id]
		if !ok {
			fmt.Printf("ERR metric %v id %d not found in map#\n", metric, id)
			errors++
		} else if metric != metric2.String() {
			fmt.Printf("ERR metric %v id %d has different metric %v in map#\n",
				metric, id, metric2)
			errors++
		}
		if id > int(nextID) {
			fmt.Printf("ERR id %d for metric %s is > nextID %d in map#\n", id,
				metric, nextID)
			errors++
		}
	}
	if oldErrors == errors {
		fmt.Print("OK  metric->id->metric\n")
	}

	// load all the intsets
	lv2ids := make(map[string]*intsets.Sparse)
	it3 := db.rdb.NewIterator(db.defro)
	it3.Seek([]byte{'='})
	done = false
	totalSparseLenCount := 0
	for ; !done && it3.Valid(); it3.Next() {
		k := it3.Key()
		kd := k.Data()
		v := it3.Value()
		vd := v.Data()

		if k.Size() > 1 && kd[0] == '=' {
			// key is =name=value
			// value is []uint64
			parts := strings.Split(string(kd), "=")
			if len(parts) != 3 {
				fmt.Printf("ERR map= bad key %q\n", string(kd))
				errors++
			} else {
				vs, err := util.SparseFromBytes(vd)
				if err != nil {
					fmt.Printf("ERR map= for key %s bad value: %v\n", string(kd), err)
					errors++
				} else {
					lv2ids[string(kd)] = vs
					totalSparseLenCount += vs.Len()
				}
			}
		} else {
			done = true
		}
		k.Free()
		v.Free()
	}
	if err := it3.Err(); err != nil {
		fmt.Printf("ERR iterator error: %v\n", err)
		errors++
	}
	it3.Close()

	// compare label counts
	if totalSparseLenCount != totalLabelCount {
		fmt.Printf("ERR %d map= labels but %d metric labels\n",
			totalSparseLenCount, totalLabelCount)
	} else {
		fmt.Printf("OK  %d labels\n", totalSparseLenCount)
	}

	// compare id2metric -> lv2ids
	oldErrors = errors
	for id, metric := range id2metric {
		for _, label := range metric {
			lv := fmt.Sprintf("=%s=%s", label.Name, label.Value)
			ids, ok := lv2ids[lv]
			if !ok {
				fmt.Printf("ERR metric id %d %v label %s not found in map=\n",
					id, metric, lv)
				errors++
			} else if !ids.Has(id) {
				fmt.Printf("ERR metric id %d %v label %s not found in value of map=\n",
					id, metric, lv)
				errors++
			}
		}
	}
	if oldErrors == errors {
		fmt.Print("OK  id->metric = lv->ids\n")
	}

	// compare lv2ids -> id2metric
	oldErrors = errors
	for lv, ids := range lv2ids {
		parts := strings.Split(lv, "=")
		idsSlice := make([]int, 0, ids.Len())
		ids.AppendTo(idsSlice)
		for _, id := range idsSlice {
			metric, ok := id2metric[id]
			if !ok {
				fmt.Printf("ERR map= key %s value %d does not exist in map#\n",
					lv, id)
				errors++
			} else {
				found := false
				for _, label := range metric {
					if label.Name == parts[0] && label.Value == parts[1] {
						found = true
						break
					}
				}
				if !found {
					fmt.Printf("ERR map= key %s value %d but metric is %v\n",
						lv, id, metric)
					errors++
				}
			}
		}
	}
	if oldErrors == errors {
		fmt.Print("OK  lv->ids = id->metric\n")
	}

	return
}
