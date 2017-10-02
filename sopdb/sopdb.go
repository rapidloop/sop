package sopdb

import (
	"errors"

	"github.com/rapidloop/sop/model"
)

// IndexDB is used to map a time-series name to an integer and vice-versa. It
// can also return a list of ids that match a given label expression.
type IndexDB interface {
	// Query returns the ids of the series that match the given expression.
	Query(expr model.LabelExpr) (ids []int, err error)

	// Make gets the id of the series identified by the given fully-qualified
	// metric. If the series does not exist, it is created and a new id is
	// returned.
	Make(metric model.Metric) (id int, err error)

	// Delete the given metric. NOTTESTED
	//Delete(expr model.LabelExpr) error

	// Lookup
	Lookup(id int) (metric model.Metric, err error)

	LookupMetric(metric model.Metric) (id int, err error)

	LabelValues(name string) (values []string, err error)

	// Close the database. No further operations must be attempted.
	Close() error
}

/*
NOTTESTED
// DeleteMetric is a utility function for deleting a single metric from an
// IndexDB. Helpful wrapper over IndexDB.Delete() method.
func DeleteMetric(idb IndexDB, labels []model.Label) error {
	var expr model.LabelExpr
	if err := expr.CompileFromLabels(labels); err != nil {
		return err
	}
	return idb.Delete(expr)
}
*/

type LatestDBProcessorFunc func(seriesID int, metric model.Metric, timestamp uint64,
	value float64) error

// LatestDB stores the latest value for a metric (identified by an integer).
// The timestamp for the value is also stored, to support staleness feature.
type LatestDB interface {
	Put(seriesID int, metric model.Metric, timestamp uint64, value float64) error

	Get(seriesID int) (metric model.Metric, timestamp uint64, value float64, err error)

	Iterate(cb LatestDBProcessorFunc) error

	Close() error
}

type TSDB interface {
	Put(seriesID int, timestamp uint64, value float64) error

	Query(seriesID int, from, to uint64) (result []model.Sample, err error)

	// NOTIMPLEMENTED
	//Delete(seriesID int) error

	Iterate(seriesID int) Iterator

	Close() error

	DeleteUntil(timestamp uint64) (count int, err error)
}

type Iterator interface {
	Seek(t uint64) bool

	At() (t uint64, v float64)

	Next() bool

	Close()
}

var (
	// ErrNotFound is the generic error value to indicate absence of things
	// asked for.
	ErrNotFound = errors.New("not found")

	ErrBadMetric = errors.New("bad metric")

	ErrBadSample = errors.New("bad sample(s)")
)

// DB is the main storage interface.
type DB interface {
	Index() IndexDB
	Latest() LatestDB
	TS() TSDB
	Close() error
}
