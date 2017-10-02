package rdb

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestTSDBBasic(t *testing.T) {
	os.RemoveAll("/tmp/testtsdb")
	db, err := OpenTSDB("/tmp/testtsdb")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTSDBOpenFail(t *testing.T) {
	ioutil.WriteFile("/tmp/test2", []byte{0}, 0755)
	_, err := OpenTSDB("/tmp/test2")
	if err == nil {
		t.Fatal(err)
	}
	os.Remove("/tmp/test2")
}

func TestTSDBPutGet(t *testing.T) {
	os.RemoveAll("/tmp/testtsdb")
	db, err := OpenTSDB("/tmp/testtsdb")
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

	result, err := db.Query(100, 0, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(result))
	}
	if result[0].Timestamp != 1234 {
		t.Fatalf("bad timestamp, got %d", result[0].Timestamp)
	}
	if result[0].Value != 4567.8 {
		t.Fatalf("bad vale, got %v", result[0].Value)
	}
}

func TestTSDBQuery(t *testing.T) {
	os.RemoveAll("/tmp/testtsdb")
	db, err := OpenTSDB("/tmp/testtsdb")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	for i := 1; i <= 1100; i++ {
		err = db.Put(100, uint64(i), float64(i))
		if err != nil {
			t.Fatal(err)
		}
		err = db.Put(101, uint64(2000+i), float64(2000+i))
		if err != nil {
			t.Fatal(err)
		}
	}

	result, err := db.Query(100, 500, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 501 {
		t.Fatalf("expected 501 entries, got %d", len(result))
	}
	for i, s := range result {
		if s.Timestamp != uint64(500+i) {
			t.Fatalf("timestamp expected %d got %d", s.Timestamp, uint64(500+i))
		}
		if s.Value != float64(500+i) {
			t.Fatalf("timestamp expected %v got %v", s.Value, float64(500+i))
		}
	}
}
