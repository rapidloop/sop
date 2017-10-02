package util

import (
	"bytes"
	"encoding/binary"

	"golang.org/x/tools/container/intsets"
)

// TODO: evaluate if map[int]bool is better suited to our needs than
// intsets.Sparse.

// SparseToBytes serializes a Sparse object to a byte array.
func SparseToBytes(s *intsets.Sparse) ([]byte, error) {
	vals := make([]int, 0, s.Len())
	vals = s.AppendTo(vals)
	vals2 := make([]int64, s.Len())
	for i, v := range vals {
		vals2[i] = int64(v)
	}
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, binary.BigEndian, vals2); err != nil {
		return nil, err
	}
	data := buf.Bytes()
	return data, nil
}

// SparseFromBytes deserializes the given byte array into a Sparse object.
func SparseFromBytes(b []byte) (*intsets.Sparse, error) {
	vals := make([]int64, len(b)/8)
	if err := binary.Read(bytes.NewBuffer(b), binary.BigEndian, vals); err != nil {
		return nil, err
	}
	s := &intsets.Sparse{}
	for _, v := range vals {
		s.Insert(int(v))
	}
	return s, nil
}
