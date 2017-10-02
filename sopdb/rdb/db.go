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
