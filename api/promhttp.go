// Some of the code in the file is adapted from Prometheus source code, which
// contained the following license header:

// Copyright 2016 The Prometheus Authors
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

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/mdevan/httprouter"
	"github.com/prometheus/client_golang/prometheus"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/rapidloop/sop/bridge"
	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
	"github.com/rapidloop/sop/util"
)

//------------------------------------------------------------------------------

const (
	httpStopTimeout  = 10 * time.Second
	httpReadTimeout  = 5 * time.Second
	httpWriteTimeout = 5 * time.Second
	httpIdleTimeout  = 5 * time.Minute
)

var (
	index = 0

	metRequest = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "sop_api_promhttp_request",
		Help:       "Time taken for processing HTTP requests.",
		Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
	}, []string{"index", "name"})
)

type promHTTPAPI struct {
	db        sopdb.DB
	qe        *bridge.QueryEngine
	listen    string
	server    *http.Server
	ttlSecs   uint64
	extlabels map[string]string
	indexStr  string
}

func newPromHTTPAPI(config model.APIConfig, general model.GeneralConfig,
	db sopdb.DB) (*promHTTPAPI, error) {

	listen := config.Listen
	if _, _, err := util.ParseHostPort(listen); err != nil {
		return nil, err
	}
	indexStr := strconv.Itoa(index)
	index++
	self := &promHTTPAPI{
		db:        db,
		qe:        bridge.NewQueryEngine(db),
		listen:    listen,
		ttlSecs:   uint64(general.TTLSec),
		extlabels: config.FederationLabels,
		indexStr:  indexStr,
	}
	return self, nil
}

func (h *promHTTPAPI) Info() string {
	return fmt.Sprintf("prometheus_http (listen=%s)", h.listen)
}

func (h *promHTTPAPI) Start() error {
	ln, err := net.Listen("tcp", h.listen)
	if err != nil {
		return err
	}
	mux := httprouter.New()
	mux.GET("/api/v1/query", h.handleQuery)
	mux.GET("/api/v1/query_range", h.handleQueryRange)
	mux.GET("/api/v1/series", h.handleSeries)
	mux.GET("/api/v1/label/:name/values", h.handleLabelValues)
	mux.OPTIONS("/api/v1/*path", h.handleOptions)
	mux.GET("/federate", h.federation)
	mux.GET("/metrics", prometheus.Handler().ServeHTTP)
	h.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  httpReadTimeout,
		WriteTimeout: httpWriteTimeout,
		IdleTimeout:  httpIdleTimeout,
	}
	go h.server.Serve(ln)
	return nil
}

func (h *promHTTPAPI) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), httpStopTimeout)
	defer cancel()
	return h.server.Shutdown(ctx)
}

func (h *promHTTPAPI) handleQuery(w http.ResponseWriter, r *http.Request) {
	h.handleCommon(w, r, h.query, "query")
}

func (h *promHTTPAPI) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	h.handleCommon(w, r, h.queryRange, "query_range")
}

func (h *promHTTPAPI) handleSeries(w http.ResponseWriter, r *http.Request) {
	h.handleCommon(w, r, h.series, "series")
}

func (h *promHTTPAPI) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	h.handleCommon(w, r, h.labelValues, "label")
}

func (h *promHTTPAPI) handleOptions(w http.ResponseWriter, r *http.Request) {
	h.handleCommon(w, r, h.options, "options")
}

func (h *promHTTPAPI) handleCommon(w http.ResponseWriter, r *http.Request,
	inner func(*http.Request) (interface{}, *apiError), name string) {

	t := time.Now()
	o := metRequest.WithLabelValues(h.indexStr, name)
	setCORS(w)
	if data, err := inner(r); err != nil {
		respondError(w, err, data)
	} else if data != nil {
		respond(w, data)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
	o.Observe(time.Since(t).Seconds())
}

//------------------------------------------------------------------------------

type queryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`
}

func (h *promHTTPAPI) query(r *http.Request) (interface{}, *apiError) {
	var ts time.Time
	if t := r.FormValue("time"); t != "" {
		var err error
		ts, err = parseTime(t)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}
	} else {
		ts = time.Now()
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	qry, err := h.qe.PromEngine().NewInstantQuery(r.FormValue("query"), ts)
	if err != nil {
		return nil, &apiError{errorBadData, err}
	}

	res := qry.Exec(ctx)
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			return nil, &apiError{errorCanceled, res.Err}
		case promql.ErrQueryTimeout:
			return nil, &apiError{errorTimeout, res.Err}
		case promql.ErrStorage:
			return nil, &apiError{errorInternal, res.Err}
		}
		return nil, &apiError{errorExec, res.Err}
	}
	return &queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
	}, nil
}

//------------------------------------------------------------------------------

func (h *promHTTPAPI) queryRange(r *http.Request) (interface{}, *apiError) {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		return nil, &apiError{errorBadData, err}
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		return nil, &apiError{errorBadData, err}
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start time")
		return nil, &apiError{errorBadData, err}
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		return nil, &apiError{errorBadData, err}
	}

	if step <= 0 {
		err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
		return nil, &apiError{errorBadData, err}
	}

	// For safety, limit the number of returned points per timeseries.
	// This is sufficient for 60s resolution for a week or 1h resolution for a year.
	if end.Sub(start)/step > 11000 {
		err := errors.New("exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)")
		return nil, &apiError{errorBadData, err}
	}

	ctx := r.Context()
	if to := r.FormValue("timeout"); to != "" {
		var cancel context.CancelFunc
		timeout, err := parseDuration(to)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}

		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	qry, err := h.qe.PromEngine().NewRangeQuery(r.FormValue("query"), start, end, step)
	if err != nil {
		return nil, &apiError{errorBadData, err}
	}

	res := qry.Exec(ctx)
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			return nil, &apiError{errorCanceled, res.Err}
		case promql.ErrQueryTimeout:
			return nil, &apiError{errorTimeout, res.Err}
		}
		return nil, &apiError{errorExec, res.Err}
	}

	return &queryData{
		ResultType: res.Value.Type(),
		Result:     res.Value,
	}, nil
}

//------------------------------------------------------------------------------

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0)
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999)
)

func (h *promHTTPAPI) series(r *http.Request) (interface{}, *apiError) {
	r.ParseForm()
	if len(r.Form["match[]"]) == 0 {
		return nil, &apiError{errorBadData, fmt.Errorf("no match[] parameter provided")}
	}

	var start time.Time
	if t := r.FormValue("start"); t != "" {
		var err error
		start, err = parseTime(t)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}
	} else {
		start = minTime
	}

	var end time.Time
	if t := r.FormValue("end"); t != "" {
		var err error
		end, err = parseTime(t)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}
	} else {
		end = maxTime
	}

	var matcherSets [][]*labels.Matcher
	for _, s := range r.Form["match[]"] {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			return nil, &apiError{errorBadData, err}
		}
		matcherSets = append(matcherSets, matchers)
	}

	q, err := h.qe.Querier(timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		return nil, &apiError{errorExec, err}
	}
	defer q.Close()

	var set storage.SeriesSet

	for _, mset := range matcherSets {
		set = storage.DeduplicateSeriesSet(set, q.Select(mset...))
	}

	metrics := []labels.Labels{}

	for set.Next() {
		metrics = append(metrics, set.At().Labels())
	}
	if set.Err() != nil {
		return nil, &apiError{errorExec, set.Err()}
	}

	return metrics, nil
}

//------------------------------------------------------------------------------

func (h *promHTTPAPI) labelValues(r *http.Request) (interface{}, *apiError) {
	name := httprouter.Param(r, "name")
	values, err := h.db.Index().LabelValues(name)
	if err != nil {
		return nil, &apiError{errorExec, err}
	}
	return values, nil
}

//------------------------------------------------------------------------------

func (h *promHTTPAPI) options(r *http.Request) (interface{}, *apiError) {
	return nil, nil
}

//------------------------------------------------------------------------------

// This is only a basic /federate implementation. Only plain text format,
// without type information and with unsorted metrics.
func (h *promHTTPAPI) federation(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()

	// compile the form input to []expr
	f := req.Form["match[]"]
	exprs := make([]model.LabelExpr, 0, len(f))
	for _, s := range f {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			log.Printf("federate: promql: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		terms := make([]model.LabelOp, len(matchers))
		for i, m := range matchers {
			terms[i].Name = m.Name
			terms[i].Value = m.Value
			terms[i].Op = int(m.Type) // just happen to have the same values.. :-)
		}
		var expr model.LabelExpr
		if err := expr.CompileFromTerms(terms); err != nil {
			log.Printf("federate: compile: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		exprs = append(exprs, expr)
	}

	// only this format for now
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	var after uint64
	if h.ttlSecs > 0 {
		after = uint64(time.Now().UnixNano()/1e6) - h.ttlSecs*1000
	}
	err := h.db.Latest().Iterate(func(seriesID int, metric model.Metric,
		timestamp uint64, value float64) error {
		if after == 0 || timestamp > after {
			for _, e := range exprs {
				if e.Match(metric) {
					for extn, extv := range h.extlabels {
						metric = append(metric, model.Label{
							Name:  extn,
							Value: extv,
						})
					}
					metric.Sort()
					fmt.Fprintf(w, "%v %v %d\n", metric, value, timestamp)
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("federate: iterate: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

//------------------------------------------------------------------------------

type status string

const (
	statusSuccess status = "success"
	statusError          = "error"
)

type errorType string

const (
	errorNone     errorType = ""
	errorTimeout            = "timeout"
	errorCanceled           = "canceled"
	errorExec               = "execution"
	errorBadData            = "bad_data"
	errorInternal           = "internal"
)

var corsHeaders = map[string]string{
	"Access-Control-Allow-Headers":  "Accept, Authorization, Content-Type, Origin",
	"Access-Control-Allow-Methods":  "GET, OPTIONS",
	"Access-Control-Allow-Origin":   "*",
	"Access-Control-Expose-Headers": "Date",
}

type apiError struct {
	typ errorType
	err error
}

func (e *apiError) Error() string {
	return fmt.Sprintf("%s: %s", e.typ, e.err)
}

type response struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// Enables cross-site script calls.
func setCORS(w http.ResponseWriter) {
	for h, v := range corsHeaders {
		w.Header().Set(h, v)
	}
}

func respond(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	b, err := json.Marshal(&response{
		Status: statusSuccess,
		Data:   data,
	})
	if err != nil {
		return
	}
	w.Write(b)
}

func respondError(w http.ResponseWriter, apiErr *apiError, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	var code int
	switch apiErr.typ {
	case errorBadData:
		code = http.StatusBadRequest
	case errorExec:
		code = 422
	case errorCanceled, errorTimeout:
		code = http.StatusServiceUnavailable
	case errorInternal:
		code = http.StatusInternalServerError
	default:
		code = http.StatusInternalServerError
	}
	w.WriteHeader(code)

	b, err := json.Marshal(&response{
		Status:    statusError,
		ErrorType: apiErr.typ,
		Error:     apiErr.err.Error(),
		Data:      data,
	})
	if err != nil {
		return
	}
	w.Write(b)
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := prommodel.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func init() {
	prometheus.MustRegister(metRequest)
}
