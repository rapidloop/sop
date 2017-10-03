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

package bridge

import (
	"log"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
)

// Closer is an object that has a Close() method that needs to be called to
// free up resources.
type Closer interface {
	Close() error
}

//------------------------------------------------------------------------------

/*
// SeriesIterator iterates over the data of a time series.
type SeriesIterator interface {
	// Seek advances the iterator forward to the value at or after
	// the given timestamp.
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	At() (t int64, v float64)
	// Next advances the iterator by one.
	Next() bool
	// Err returns the current error.
	Err() error
}
*/

// SeriesIterator iterates over the data of a time series.
type SeriesIterator struct {
	id   int
	tsdb sopdb.TSDB
	it   sopdb.Iterator
}

func NewSeriesIterator(seriesID int, tsdb sopdb.TSDB) *SeriesIterator {
	return &SeriesIterator{
		id:   seriesID,
		tsdb: tsdb,
		it:   tsdb.Iterate(seriesID),
	}
}

// Seek advances the iterator forward to the value at or after
// the given timestamp.
func (s *SeriesIterator) Seek(t int64) bool {
	return s.it.Seek(uint64(t))
}

// At returns the current timestamp/value pair.
func (s *SeriesIterator) At() (t int64, v float64) {
	var tt uint64
	tt, v = s.it.At()
	t = int64(tt)
	return
}

// Next advances the iterator by one.
func (s *SeriesIterator) Next() bool {
	return s.it.Next()
}

// Err returns the current error.
func (s *SeriesIterator) Err() error {
	return nil
}

// Close frees up allocated resources.
func (s *SeriesIterator) Close() error {
	s.it.Close()
	return nil
}

//------------------------------------------------------------------------------

/*
// Series represents a single time series.
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	Labels() labels.Labels

	// Iterator returns a new iterator of the data of the series.
	Iterator() SeriesIterator
}
*/

// Series represents a single time series.
type Series struct {
	tsdb    sopdb.TSDB
	labs    labels.Labels
	id      int
	closers []Closer
}

func NewSeries(metric model.Metric, seriesID int, tsdb sopdb.TSDB) *Series {
	m := make(map[string]string)
	for _, lv := range metric {
		m[lv.Name] = lv.Value
	}
	return &Series{
		tsdb: tsdb,
		labs: labels.FromMap(m),
		id:   seriesID,
	}
}

// Labels returns the complete set of labels identifying the series.
func (s *Series) Labels() labels.Labels {
	return s.labs
}

// Iterator returns a new iterator of the data of the series.
func (s *Series) Iterator() storage.SeriesIterator {
	sit := NewSeriesIterator(s.id, s.tsdb)
	s.closers = append(s.closers, sit)
	return sit
}

func (s *Series) Close() error {
	for _, c := range s.closers {
		c.Close()
	}
	return nil
}

//------------------------------------------------------------------------------

// SeriesSet contains a set of series.
type SeriesSet struct {
	metrics []model.Metric
	ids     []int
	index   int
	tsdb    sopdb.TSDB
	err     error
	closers []Closer
}

func NewSeriesSet(metrics []model.Metric, ids []int, tsdb sopdb.TSDB,
	err error) *SeriesSet {

	if len(metrics) != len(ids) {
		panic("metrics and ids are of unequal cardinality")
	}
	return &SeriesSet{
		metrics: metrics,
		ids:     ids,
		tsdb:    tsdb,
		index:   -1,
		err:     err,
	}
}

func (ss *SeriesSet) Next() bool {
	if (ss.index+1) >= 0 && (ss.index+1) < len(ss.metrics) {
		ss.index++
		return true
	}
	return false
}

func (ss *SeriesSet) At() storage.Series {
	if ss.index >= len(ss.metrics) {
		return nil
	}
	series := NewSeries(ss.metrics[ss.index], ss.ids[ss.index], ss.tsdb)
	ss.closers = append(ss.closers, series)
	return series
}

func (ss *SeriesSet) Err() error {
	return ss.err
}

func (ss *SeriesSet) Close() error {
	for _, c := range ss.closers {
		c.Close()
	}
	return nil
}

//------------------------------------------------------------------------------

// Querier provides reading access to time series data.
type Querier struct {
	indexdb sopdb.IndexDB
	tsdb    sopdb.TSDB
	closers []Closer
}

func NewQuerier(indexdb sopdb.IndexDB, tsdb sopdb.TSDB) *Querier {
	return &Querier{
		indexdb: indexdb,
		tsdb:    tsdb,
	}
}

// Select returns a set of series that matches the given label matchers.
func (q *Querier) Select(mm ...*labels.Matcher) storage.SeriesSet {
	// compile matchers to a label expression
	terms := make([]model.LabelOp, len(mm))
	for i, m := range mm {
		terms[i].Name = m.Name
		terms[i].Value = m.Value
		terms[i].Op = int(m.Type) // just happen to have the same values.. :-)
	}
	var expr model.LabelExpr
	if err := expr.CompileFromTerms(terms); err != nil {
		log.Printf("bad query %v: %v", terms, err)
		return NewSeriesSet(nil, nil, q.tsdb, err)
	}

	// execute query
	ids, err := q.indexdb.Query(expr)
	if err != nil {
		log.Printf("query failed: %v: %v", expr, err)
		return NewSeriesSet(nil, nil, q.tsdb, err)
	}

	// lookup ids
	metrics := make([]model.Metric, len(ids))
	for i, id := range ids {
		metrics[i], err = q.indexdb.Lookup(id)
		if err != nil {
			log.Printf("failed to lookup id %d: %v", id, err)
			return NewSeriesSet(nil, nil, q.tsdb, err)
		}
	}

	s := NewSeriesSet(metrics, ids, q.tsdb, nil)
	q.closers = append(q.closers, s)
	return s
}

// LabelValues returns all potential values for a label name.
func (q *Querier) LabelValues(name string) ([]string, error) {
	panic("Not Implemented!")
}

// Close releases the resources of the Querier.
func (q *Querier) Close() error {
	for _, c := range q.closers {
		c.Close()
	}
	return nil
}

//------------------------------------------------------------------------------

/*
// Queryable allows opening a storage querier.
type Queryable interface {
	Querier(mint, maxt int64) (storage.Querier, error)
}
*/

//------------------------------------------------------------------------------

type QueryEngine struct {
	db sopdb.DB
	e  *promql.Engine
}

func NewQueryEngine(db sopdb.DB) *QueryEngine {
	self := &QueryEngine{db: db}
	self.e = promql.NewEngine(self, nil)
	return self
}

func (q *QueryEngine) Querier(mint, maxt int64) (storage.Querier, error) {
	return NewQuerier(q.db.Index(), q.db.TS()), nil
}

func (q *QueryEngine) PromEngine() *promql.Engine {
	return q.e
}
