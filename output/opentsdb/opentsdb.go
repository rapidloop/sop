// Some of the code in the file is adapted from Prometheus source code, which
// contained the following license header:

// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opentsdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/util"
)

var (
	batchMaxCount = 100
	batchMaxWait  = time.Minute

	// Send only these many samples per each OpenTSDB call. Request body should
	// not exceed OpenTSDB config "tsd.http.request.max_chunk".
	opentsdbPerCall = 10

	index = 0

	metPut = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "sop_output_opentsdb_put_seconds",
		Help:       "The time taken for doing put API calls.",
		Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
	}, []string{"index"})
	metSamplesCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sop_output_opentsdb_put_samples_total",
		Help: "The total number of samples stored into OpenTSDB.",
	}, []string{"index", "result"})
)

type impl struct {
	b      *util.Batcher
	out    chan []util.BatchItem
	f      model.Filter
	cancel context.CancelFunc
	wg     sync.WaitGroup

	parsedURL *url.URL
	timeout   time.Duration
	client    *http.Client

	// metrics
	metPut          prometheus.Observer
	metSuccessCount prometheus.Counter
	metFailCount    prometheus.Counter
}

func NewOutput(config model.OutputConfig) (*impl, error) {
	// batcher out channel
	out := make(chan []util.BatchItem, 1)

	// url
	if len(config.URL) == 0 {
		return nil, errors.New("'url' must be specified for opentsdb output")
	}
	parsedURL, err := url.Parse(config.URL)
	if err != nil {
		return nil, fmt.Errorf("opentsdb url: %v", err)
	}
	parsedURL.Path = "/api/put"
	parsedURL.RawQuery = "details"

	// timeout
	timeout := 5 * time.Second
	if config.TimeoutSec > 0 {
		timeout = time.Duration(config.TimeoutSec) * time.Second
	}

	// filter
	var filter model.Filter
	if err := filter.Compile(config.FilterInclude, config.FilterExclude); err != nil {
		return nil, err
	}

	// index
	indexStr := strconv.Itoa(index)
	index++

	// done
	im := &impl{
		b:         util.NewBatcher(batchMaxCount, batchMaxWait, out),
		out:       out,
		f:         filter,
		parsedURL: parsedURL,
		timeout:   timeout,
		client: &http.Client{
			Timeout: timeout,
		},
		metPut:          metPut.WithLabelValues(indexStr),
		metSuccessCount: metSamplesCount.WithLabelValues(indexStr, "success"),
		metFailCount:    metSamplesCount.WithLabelValues(indexStr, "fail"),
	}
	return im, nil
}

func (im *impl) Info() string {
	return fmt.Sprintf("opentsdb (url=%v)", im.parsedURL)
}

func (im *impl) Start() error {
	im.b.Start()
	newCtx, cancel := context.WithCancel(context.Background())
	im.cancel = cancel
	im.wg.Add(1)
	go im.sender(newCtx)
	return nil
}

func (im *impl) Stop() error {
	im.b.Stop()
	im.cancel()
	im.wg.Wait()
	close(im.out)
	return nil
}

func (im *impl) Store(metric model.Metric, samples []model.Sample) {
	im.b.Enqueue(metric, samples)
}

func (im *impl) sender(ctx context.Context) {
	for {
		select {
		case items := <-im.out:
			if err := im.sendWithRetry(items); err != nil {
				log.Printf("opentsdb send: %v", err)
			}
		case <-ctx.Done():
			im.wg.Done()
			return
		}
	}
}

// StoreSamplesRequest is used for building a JSON request for storing samples
// via the OpenTSDB.
type StoreSamplesRequest struct {
	Metric    TagValue            `json:"metric"`
	Timestamp uint64              `json:"timestamp"`
	Value     float64             `json:"value"`
	Tags      map[string]TagValue `json:"tags"`
}

// tagsFromMetric translates Prometheus metric into OpenTSDB tags.
func tagsFromMetric(m model.Metric) map[string]TagValue {
	tags := make(map[string]TagValue, len(m)-1)
	for _, lv := range m {
		if lv.Name == model.MetricNameLabel {
			continue
		}
		tags[lv.Name] = TagValue(lv.Value)
	}
	return tags
}

func (im *impl) sendWithRetry(batch []util.BatchItem) (err error) {

	// each put is retried thrice
	backoffLogic := backoff.WithMaxTries(backoff.NewExponentialBackOff(), 3)

	// send in chunks of opentsdbPerCall
	for i := 0; i < len(batch); i += opentsdbPerCall {
		// get a chunk
		end := i + opentsdbPerCall
		if end > len(batch) {
			end = len(batch)
		}
		one := batch[i:end]

		// convert to json
		var data []byte
		var sampleCount int
		sampleCount, data, err = reqtoJSON(one)
		if err != nil {
			return
		}

		// send it
		backoff.Retry(func() error {
			errSend := im.send(data, sampleCount)
			if _, ok := errSend.(recoverableError); ok {
				return errSend // is recoverable, retry
			}
			err = errSend // updates outer err
			return nil    // this will stop the retry
		}, backoffLogic)
		if err != nil {
			return
		}
	}
	return
}

type recoverableError struct {
	error
}

func reqtoJSON(batch []util.BatchItem) (int, []byte, error) {
	reqs := make([]StoreSamplesRequest, 0, 100*len(batch))
	for _, it := range batch {
		metric := TagValue(it.Metric.Name())
		for _, s := range it.Samples {
			v := float64(s.Value)
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			reqs = append(reqs, StoreSamplesRequest{
				Metric:    metric,
				Timestamp: s.Timestamp / 1000,
				Value:     v,
				Tags:      tagsFromMetric(it.Metric),
			})
		}
	}
	data, err := json.Marshal(reqs)
	return len(reqs), data, err
}

// Write sends a batch of samples to OpenTSDB via its HTTP API.
func (im *impl) send(data []byte, n int) error {
	t := time.Now()
	defer func() {
		im.metPut.Observe(time.Since(t).Seconds())
	}()

	resp, err := im.client.Post(im.parsedURL.String(), "application/json",
		bytes.NewReader(data))
	if err != nil {
		return recoverableError{err}
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode/100 != 2 {
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		var r struct {
			Success int `json:"success"`
			Failed  int `json:"failed"`
		}
		if err := json.Unmarshal(buf, &r); err != nil {
			return err
		}
		im.metFailCount.Add(float64(r.Failed))
		im.metSuccessCount.Add(float64(r.Success))
		return fmt.Errorf("%d samples failed: %v", r.Failed, string(buf))
	}

	im.metSuccessCount.Add(float64(n))
	return nil
}

func init() {
	prometheus.MustRegister(metPut)
	prometheus.MustRegister(metSamplesCount)
}
