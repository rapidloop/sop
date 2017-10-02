// Some of the code in the file is adapted from Prometheus source code, which
// contained the following license header:

// Copyright 2016 The Prometheus Authors
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

package promv2

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
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

	index = 0

	metWrite = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "sop_output_promv2_seconds",
		Help:       "Time taken for performing remote writes.",
		Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
	}, []string{"index"})
	metWritesCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sop_output_promv2_writes_total",
		Help: "The number of total remote writes.",
	}, []string{"index", "result"})
	metSamplesCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sop_output_promv2_samples_total",
		Help: "The number of successful samples sent.",
	}, []string{"index"})
)

type impl struct {
	b      *util.Batcher
	out    chan []util.BatchItem
	f      model.Filter
	cancel context.CancelFunc
	wg     sync.WaitGroup

	url     string
	timeout time.Duration
	client  *http.Client

	metWrite              prometheus.Observer
	metSuccessWritesCount prometheus.Counter
	metFailWritesCount    prometheus.Counter
	metSamplesCount       prometheus.Counter
}

func NewOutput(config model.OutputConfig) (*impl, error) {
	out := make(chan []util.BatchItem, 1)
	if len(config.URL) == 0 {
		return nil, errors.New("'url' must be specified for prometheus remote write")
	}
	timeout := 5 * time.Second
	if config.TimeoutSec > 0 {
		timeout = time.Duration(config.TimeoutSec) * time.Second
	}
	var filter model.Filter
	if err := filter.Compile(config.FilterInclude, config.FilterExclude); err != nil {
		return nil, err
	}
	indexStr := strconv.Itoa(index)
	index++
	im := &impl{
		b:       util.NewBatcher(batchMaxCount, batchMaxWait, out),
		out:     out,
		f:       filter,
		url:     config.URL,
		timeout: timeout,
		client: &http.Client{
			Timeout: timeout,
		},
		metWrite:              metWrite.WithLabelValues(indexStr),
		metSuccessWritesCount: metWritesCount.WithLabelValues(indexStr, "success"),
		metFailWritesCount:    metWritesCount.WithLabelValues(indexStr, "fail"),
		metSamplesCount:       metSamplesCount.WithLabelValues(indexStr),
	}
	return im, nil
}

func (im *impl) Info() string {
	return fmt.Sprintf("prometheus remote write (url=%s)", im.url)
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
				log.Printf("prometheus remote write send: %v", err)
			}
		case <-ctx.Done():
			im.wg.Done()
			return
		}
	}
}

func (im *impl) sendWithRetry(batch []util.BatchItem) error {
	count, data, err := util.ToWriteRequest(batch)
	if err != nil {
		return err
	}

	backoffLogic := backoff.WithMaxTries(backoff.NewExponentialBackOff(), 3)
	backoff.Retry(func() error {
		errSend := im.send(data, count)
		if _, ok := errSend.(recoverableError); ok { // is recoverable, retry
			return errSend // retry
		}
		err = errSend
		return nil // this will stop the retry
	}, backoffLogic)
	return err
}

type recoverableError struct {
	error
}

const maxErrMsgLen = 256

// Store sends a batch of samples to the HTTP endpoint.
func (im *impl) send(data []byte, n int) error {
	t := time.Now()
	defer func() {
		im.metWrite.Observe(time.Since(t).Seconds())
	}()

	httpReq, err := http.NewRequest("POST", im.url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	httpResp, err := im.client.Do(httpReq)
	if err != nil {
		im.metFailWritesCount.Inc()
		return recoverableError{err}
	}
	if httpResp.Body != nil {
		defer httpResp.Body.Close()
	}

	if httpResp.StatusCode/100 != 2 {
		if httpResp.Body != nil {
			scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
			line := ""
			if scanner.Scan() {
				line = scanner.Text()
			}
			im.metFailWritesCount.Inc()
			return fmt.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
		}
		im.metFailWritesCount.Inc()
		return fmt.Errorf("server returned HTTP status %s", httpResp.Status)
	}
	if httpResp.StatusCode/100 == 5 {
		im.metFailWritesCount.Inc()
		return recoverableError{err}
	}

	im.metSuccessWritesCount.Inc()
	im.metSamplesCount.Add(float64(n))
	return nil
}

func init() {
	prometheus.MustRegister(metWrite)
	prometheus.MustRegister(metWritesCount)
	prometheus.MustRegister(metSamplesCount)
}
