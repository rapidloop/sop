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

package influxdb

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
	"github.com/rapidloop/sop/util"
)

const (
	influxdbServerStopTimeout = 10 * time.Second
	influxdbHTTPReadTimeout   = 5 * time.Second
	influxdbHTTPWriteTimeout  = 5 * time.Second
	influxdbHTTPIdleTimeout   = 5 * time.Minute
)

var (
	index = 0

	metInput = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "sop_input_influxdb_input_seconds",
		Help:       "Time taken for processing write requests.",
		Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
	}, []string{"index"})
	metSamplesCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sop_input_influxdb_samples_total",
		Help: "The number of successful samples stored.",
	}, []string{"index"})
)

type impl struct {
	listen string
	server *http.Server
	storer *sopdb.Storer
	f      model.Filter

	metInput        prometheus.Observer
	metSamplesCount prometheus.Counter
}

func NewInput(config model.InputConfig, storer *sopdb.Storer) (*impl, error) {
	listen := config.Listen
	if _, _, err := util.ParseHostPort(listen); err != nil {
		return nil, err
	}
	var filter model.Filter
	if err := filter.Compile(config.FilterInclude, config.FilterExclude); err != nil {
		return nil, err
	}
	indexStr := strconv.Itoa(index)
	index++
	im := &impl{
		listen:          listen,
		storer:          storer,
		f:               filter,
		metInput:        metInput.WithLabelValues(indexStr),
		metSamplesCount: metSamplesCount.WithLabelValues(indexStr),
	}
	return im, nil
}

func (im *impl) Info() string {
	return fmt.Sprintf("influxdb (listen=%s)", im.listen)
}

func (im *impl) Start() error {
	ln, err := net.Listen("tcp", im.listen)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/write", im.handleWrite)
	im.server = &http.Server{
		Handler:      mux,
		ReadTimeout:  influxdbHTTPReadTimeout,
		WriteTimeout: influxdbHTTPWriteTimeout,
		IdleTimeout:  influxdbHTTPIdleTimeout,
	}
	go im.server.Serve(ln)
	return nil
}

func (im *impl) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), influxdbServerStopTimeout)
	defer cancel()
	return im.server.Shutdown(ctx)
}

func sanitize(value []byte) string {
	buf := &bytes.Buffer{}
	buf.Grow(len(value))
	for _, c := range value {
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			(c == ':') {
			buf.WriteByte(c)
		} else {
			buf.WriteByte('_')
		}
	}
	return buf.String()
}

func (im *impl) handleWrite(w http.ResponseWriter, r *http.Request) {
	t := time.Now()
	defer func() {
		im.metInput.Observe(time.Since(t).Seconds())
	}()

	precision := r.FormValue("precision")
	if len(precision) == 0 {
		precision = "n"
	}

	data, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		log.Printf("influxdb input: HTTP read error: %v", err)
		w.WriteHeader(400)
		return
	}

	points, err := models.ParsePointsWithPrecision(data, time.Now().UTC(), precision)
	if len(points) == 0 && err != nil {
		log.Printf("influxdb input: %v", err)
		w.WriteHeader(400)
		return
	}

	for _, point := range points {
		// make all labels for the metric, except the tags
		tags := point.Tags()
		tagLabels := make([]model.Label, len(tags))
		for i, t := range tags {
			tagLabels[i].Name = sanitize(t.Key)
			tagLabels[i].Value = string(t.Value)
		}

		// get the fields
		fields, err := point.Fields()
		if err != nil {
			log.Printf("influxdb input: error fetching fields: %v", err)
			continue
		}
		for fn, fv := range fields {

			// check if this field can be represented as a float
			value, ok := fv.(float64)
			if !ok {
				ivalue, ok := fv.(int64)
				if !ok {
					continue
				} else {
					value = float64(ivalue)
				}
			}

			// make full metric
			labels := make([]model.Label, len(tagLabels)+1)
			copy(labels, tagLabels)
			labels[len(labels)-1].Name = model.MetricNameLabel
			labels[len(labels)-1].Value = sanitize([]byte(string(point.Name()) + "_" + fn))

			sample := model.Sample{
				Timestamp: uint64(point.Time().UnixNano() / 1e6),
				Value:     value,
			}
			if err := im.storer.Store(labels, []model.Sample{sample}); err != nil {
				w.WriteHeader(500)
				return
			}
			im.metSamplesCount.Inc()
		}
	}

	w.WriteHeader(204)
}

func init() {
	prometheus.MustRegister(metInput)
	prometheus.MustRegister(metSamplesCount)
}
