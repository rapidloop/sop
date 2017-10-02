package rdb

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
)

func TestIndexDBBasic(t *testing.T) {
	os.RemoveAll("/tmp/test1")
	db, err := OpenIndexDB("/tmp/test1")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexDBMake(t *testing.T) {
	os.RemoveAll("/tmp/test1")
	db, err := OpenIndexDB("/tmp/test1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	labels := []model.Label{
		{"__name__", "up"},
		{"job", "node"},
		{"instance", "host:9001"},
	}
	id, err := db.Make(labels)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("got id %d, expected 1", id)
	}

	metric, err := db.Lookup(1)
	if err != nil {
		t.Fatal(err)
	}
	exp := `up{job="node", instance="host:9001"}`
	if metric.String() != exp {
		t.Fatalf("got entry `%s`, expected `%s`", metric.String(), exp)
	}

	// make the same one again
	id, err = db.Make(labels)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("got id %d, expected 1", id)
	}
}

func TestIndexDBLookup(t *testing.T) {
	os.RemoveAll("/tmp/test1")
	db, err := OpenIndexDB("/tmp/test1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	labels := []model.Label{
		{"__name__", "up"},
		{"job", "node"},
		{"instance", "host:9001"},
	}
	id, err := db.Make(labels)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("got id %d, expected 1", id)
	}

	_, err = db.Lookup(2)
	if err == nil {
		t.Fatal("error was expected")
	} else if err != sopdb.ErrNotFound {
		t.Fatalf("unexpected error, got: `%v`", err)
	}
}

func TestIndexDBID(t *testing.T) {
	os.RemoveAll("/tmp/test1")
	db, err := OpenIndexDB("/tmp/test1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// add a label
	labels := []model.Label{
		{"__name__", "up"},
		{"job", "node"},
		{"instance", "host:9001"},
	}
	id, err := db.Make(labels)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("got id %d, expected 1", id)
	}

	// close the db
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// reopen the db
	db, err = OpenIndexDB("/tmp/test1")
	if err != nil {
		t.Fatal(err)
	}

	// add another label
	labels2 := []model.Label{
		{"__name__", "up"},
		{"job", "node"},
		{"instance", "anotherhost:9001"},
	}
	id2, err := db.Make(labels2)
	if err != nil {
		t.Fatal(err)
	}
	if id2 != 2 {
		t.Fatalf("got id %d, expected 2", id2)
	}
	metric, err := db.Lookup(1)
	if err != nil {
		t.Fatal(err)
	}
	exp := `up{job="node", instance="host:9001"}`
	if metric.String() != exp {
		t.Fatalf("got entry `%s`, expected `%s`", metric.String(), exp)
	}
	metric, err = db.Lookup(2)
	if err != nil {
		t.Fatal(err)
	}
	exp2 := `up{job="node", instance="anotherhost:9001"}`
	if metric.String() != exp2 {
		t.Fatalf("got entry `%s`, expected `%s`", metric.String(), exp2)
	}
}

func TestIndexDBDelete(t *testing.T) {
	os.RemoveAll("/tmp/test1")
	db, err := OpenIndexDB("/tmp/test1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// add a label
	labels := []model.Label{
		{"__name__", "up"},
		{"job", "node"},
		{"instance", "host:9001"},
	}
	id, err := db.Make(labels)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("got id %d, expected 1", id)
	}

	// add another label
	labels2 := []model.Label{
		{"__name__", "up"},
		{"job", "node"},
		{"instance", "anotherhost:9001"},
	}
	id2, err := db.Make(labels2)
	if err != nil {
		t.Fatal(err)
	}
	if id2 != 2 {
		t.Fatalf("got id %d, expected 2", id2)
	}

	// make expr
	var expr model.LabelExpr
	if err = expr.CompileFromLabels(labels); err != nil {
		t.Fatal(err)
	}

	// delete the first one
	err = db.Delete(expr)
	if err != nil {
		t.Fatal(err)
	}

	// lookup the first one
	m, err := db.Lookup(1)
	if err == nil {
		t.Fatal("error was expected")
	} else if err != sopdb.ErrNotFound {
		t.Fatalf("unexpected error, got: `%v`", err)
	}

	m, err = db.Lookup(2)
	if err != nil {
		t.Fatal(err)
	}
	exp := `up{job="node", instance="anotherhost:9001"}`
	if m.String() != exp {
		t.Fatalf("got entry `%s`, expected `%s`", m.String(), exp)
	}
}

func TestIndexDBOpenFail(t *testing.T) {
	ioutil.WriteFile("/tmp/test2", []byte{0}, 0755)
	_, err := OpenIndexDB("/tmp/test2")
	if err == nil {
		t.Fatal(err)
	}
	os.Remove("/tmp/test2")
}
