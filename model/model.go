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

package model

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/promql"
)

var (
	MatchOpString = [4]string{"=", "!=", "=~", "!~"}
	RxLabel       = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]{0,254}`)

	// MetricNameLabel is the label name indicating the metric name of a
	// timeseries.
	MetricNameLabel = "__name__"
)

// Label represents a key-value pair that qualifies a time series.
type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func (l Label) String() string {
	return fmt.Sprintf(`%s="%s"`, l.Name, l.Value)
}

// These are the LabelOp operators.
const (
	MatchEq = iota
	MatchNotEq
	MatchReg
	MatchNotReg
)

// LabelOp represents a label name, an operator (one of =, !=, =~, !~) and
// a value. A LabelOp forms part of an expression to select time series.
type LabelOp struct {
	Name  string
	Op    int
	Value string
}

func (lop LabelOp) String() string {
	var ops string
	if lop.Op >= MatchEq && lop.Op <= MatchNotReg {
		ops = MatchOpString[lop.Op]
	} else {
		ops = "?"
	}
	return fmt.Sprintf(`%s%s"%s"`, lop.Name, ops, lop.Value)
}

// LabelExpr is an expression consisting of zero or more LabelOp terms. The
// expression is an implicit AND condition.
type LabelExpr struct {
	Terms []LabelOp
	Res   []*regexp.Regexp
}

func (le LabelExpr) String() string {
	var name string
	ops := make([]string, 0, len(le.Terms))
	for _, t := range le.Terms {
		if t.Name == MetricNameLabel {
			name = t.Value
		} else {
			ops = append(ops, t.String())
		}
	}
	return name + "{" + strings.Join(ops, ",") + "}"
}

// CompileFromTerms compiles a LabelExpr from a set of LabelOp entries.
func (le *LabelExpr) CompileFromTerms(terms []LabelOp) error {
	res := make([]*regexp.Regexp, len(terms))
	hasEq := false
	for i, t := range terms {
		if t.Op == MatchReg || t.Op == MatchNotReg {
			r, err := regexp.Compile("^(?:" + t.Value + ")$")
			if err != nil {
				return err
			}
			res[i] = r
		} else if t.Op != MatchEq && t.Op != MatchNotEq {
			return errors.New("unknown operator")
		}
		if !RxLabel.MatchString(t.Name) {
			return errors.New("bad label name " + t.Name)
		}
		if t.Op == MatchReg || t.Op == MatchEq {
			hasEq = true
		}
	}
	if !hasEq {
		return errors.New("must contain at least one non-empty matcher")
	}
	le.Terms = terms
	le.Res = res
	return nil
}

// CompileFromLabels is similar to CompileFromTerms, expect that each label
// is treated as a LabelOp with the = operator.
func (le *LabelExpr) CompileFromLabels(labels []Label) error {
	le.Terms = make([]LabelOp, len(labels))
	le.Res = make([]*regexp.Regexp, len(labels))
	seen := make(map[string]bool)
	for i, l := range labels {
		if !RxLabel.MatchString(l.Name) {
			return errors.New("bad label name " + l.Name)
		}
		if _, ok := seen[l.Name]; ok {
			return errors.New("repeated label name")
		}
		seen[l.Name] = true
		le.Terms[i].Name = l.Name
		le.Terms[i].Op = MatchEq
		le.Terms[i].Value = l.Value
	}
	return nil
}

func (le *LabelExpr) CompileFromString(input string) error {
	matchers, err := promql.ParseMetricSelector(input)
	if err != nil {
		return err
	}
	le.Terms = make([]LabelOp, len(matchers))
	le.Res = make([]*regexp.Regexp, len(matchers))
	for i, m := range matchers {
		le.Terms[i].Name = m.Name
		le.Terms[i].Op = int(m.Type)
		le.Terms[i].Value = m.Value
		if le.Terms[i].Op == MatchReg || le.Terms[i].Op == MatchNotReg {
			le.Res[i], err = regexp.Compile("^(?:" + le.Terms[i].Value + ")$")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (le *LabelExpr) Match(metric Metric) bool {
	for i, t := range le.Terms {
		// find label in metric
		found := false
		for _, l := range metric {
			if l.Name == t.Name {
				found = true
				switch t.Op {
				case MatchEq:
					if l.Value != t.Value {
						return false
					}
				case MatchNotEq:
					if l.Value == t.Value {
						return false
					}
				case MatchReg:
					if !le.Res[i].MatchString(l.Value) {
						return false
					}
				case MatchNotReg:
					if le.Res[i].MatchString(l.Value) {
						return false
					}
				}
				break
			}
		}
		if !found && (t.Op == MatchEq || t.Op == MatchReg) {
			return false
		}
	}
	return true
}

func FilterInclude(metric Metric, exprs []LabelExpr) bool {
	for _, e := range exprs {
		if e.Match(metric) {
			return true
		}
	}
	return false
}

func FilterExclude(metric Metric, exprs []LabelExpr) bool {
	for _, e := range exprs {
		if e.Match(metric) {
			return false
		}
	}
	return true
}

func FilterMatch(metric Metric, includes, excludes []LabelExpr) bool {
	if len(includes) > 0 {
		return FilterInclude(metric, includes)
	}
	if len(excludes) > 0 {
		return FilterExclude(metric, excludes)
	}
	return true
}

type Filter struct {
	incl []LabelExpr
	excl []LabelExpr
}

func (f *Filter) Compile(includes, excludes []string) error {
	if len(includes) > 0 && len(excludes) > 0 {
		return errors.New("only an inclusion or an exclusion filter may be specified, not both")
	}
	f.incl = make([]LabelExpr, len(includes))
	for i, incl := range includes {
		if err := f.incl[i].CompileFromString(incl); err != nil {
			return fmt.Errorf("bad include filter %q: %v", incl, err)
		}
	}
	f.excl = make([]LabelExpr, len(excludes))
	for i, excl := range excludes {
		if err := f.excl[i].CompileFromString(excl); err != nil {
			return fmt.Errorf("bad exclude filter %q: %v", excl, err)
		}
	}
	return nil
}

func (f *Filter) Match(metric Metric) bool {
	return FilterMatch(metric, f.incl, f.excl)
}

// IsValidMetricName checks if the given string is a valid metric name or not.
func IsValidMetricName(n string) bool {
	if len(n) == 0 {
		return false
	}
	for i, b := range n {
		if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || b == ':' || (b >= '0' && b <= '9' && i > 0)) {
			return false
		}
	}
	return true
}

// Metric is a qualified time series name,
// like 'up{job="node", instance="server1:9000"}'.
type Metric []Label

func (m Metric) String() string {
	var name string
	parts := make([]string, 0, len(m))
	for _, label := range m {
		if label.Name == MetricNameLabel {
			name = label.Value
		} else {
			parts = append(parts, label.String())
		}
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

func (m Metric) IsValid() bool {
	if len(m) == 0 {
		return false
	}
	for _, l := range m {
		if l.Name == MetricNameLabel && IsValidMetricName(l.Value) {
			return true
		}
	}
	return false
}

func (m Metric) Name() string {
	for _, label := range m {
		if label.Name == MetricNameLabel {
			return label.Value
		}
	}
	return ""
}

func (m Metric) Sort() {
	sort.Slice(m, func(i, j int) bool {
		return m[i].Name < m[j].Name
	})
}

func (m Metric) Copy() Metric {
	out := make([]Label, len(m))
	copy(out, m)
	return out
}

func (m Metric) Encode() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *Metric) Decode(b []byte) error {
	return gob.NewDecoder(bytes.NewBuffer(b)).Decode(m)
}

// Sample is a single datapoint of a time series.
type Sample struct {
	Timestamp uint64
	Value     float64
}

type byTime []Sample

func (a byTime) Len() int           { return len(a) }
func (a byTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTime) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }

// SortSamples sorts a slice of samples in the canonical way -- in the
// increasing order of timestamp.
func SortSamples(samples []Sample) {
	sort.Sort(byTime(samples))
}
