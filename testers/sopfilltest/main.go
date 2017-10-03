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

package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

var (
	flagSeriesCount = flag.Uint("n", 10000, "number of unique time series")
	flagDays        = flag.Uint("d", 1, "backfill these many day's worth of fake data")
	flagFillType    = flag.String("t", "random", "values to fill one of 'random', or 'zero'")
	flagInterval    = flag.Duration("i", time.Minute, "simulated collection interval")
	flagDims        = flag.Uint("m", 5, "dimensions per series")
	flagURL         = flag.String("url", "http://localhost:9096/", "sop's prometheus remote write endpoint")
)

var (
	isRandom    bool
	labelCount  int
	client      *http.Client
	elapsed     int64
	bytesSent   int
	samplesSent int
)

func main() {
	flag.Parse()

	samplesPerSeries := uint(24 * float64(*flagDays) / float64(flagInterval.Hours()))
	totalSamples := samplesPerSeries * (*flagSeriesCount)
	fmt.Printf("Will create %d series and %d samples each. ",
		*flagSeriesCount, samplesPerSeries)
	fmt.Printf("Total %d samples. Hit enter to start:", totalSamples)
	fmt.Scanln()

	rand.Seed(time.Now().UnixNano())
	if *flagFillType == "random" {
		isRandom = true
	}
	labelCount = int(*flagDims) + 1
	client = &http.Client{
		Timeout: 50 * time.Second,
	}
	samples := make([]*prompb.TimeSeries, 0, 100)
	onePC := int(totalSamples / 100)
	quotient := 0
	count := 0
	totalInterval := time.Duration(int64(*flagInterval) * int64(samplesPerSeries))
	t := time.Now().Add(-totalInterval)
	for i := 0; i < int(samplesPerSeries); i++ {
		for seriesID := 0; seriesID < int(*flagSeriesCount); seriesID++ {
			sample := makeSample(seriesID, t)
			samples = append(samples, sample)
			if len(samples) == 100 {
				send(samples)
				samples = make([]*prompb.TimeSeries, 0, 100)
			}
			count++
		}
		t = t.Add(*flagInterval)
		if onePC != 0 && count/onePC != quotient {
			quotient = count / onePC
			fmt.Printf("%d%%", quotient)
		}
	}
	if len(samples) > 0 {
		send(samples)
	}
	fmt.Printf(`
Timings:
	Total time    %v
	Total bytes   %d
	Total samples %d

	Rate = %.2f samples/sec
`,
		time.Duration(elapsed),
		bytesSent,
		samplesSent,
		float64(samplesSent)/(time.Duration(elapsed).Seconds()),
	)
}

func makeSample(seriesID int, t time.Time) *prompb.TimeSeries {
	ts := &prompb.TimeSeries{
		Labels: make([]*prompb.Label, labelCount),
		Samples: []*prompb.Sample{
			&prompb.Sample{
				Timestamp: t.Unix() / 1e6,
				Value:     0,
			},
		},
	}
	ts.Labels[0] = &prompb.Label{
		Name:  "__name__",
		Value: fmt.Sprintf("series%d", seriesID),
	}
	for i := 1; i < len(ts.Labels); i++ {
		ts.Labels[i] = &prompb.Label{
			Name:  fmt.Sprintf("dim%d", i),
			Value: "dimvalue",
		}
	}
	return ts
}

func send(batch []*prompb.TimeSeries) {
	wr := prompb.WriteRequest{
		Timeseries: batch,
	}
	data, err := wr.Marshal()
	if err != nil {
		log.Fatalf("failed to serialize data: %v", err)
	}
	compressed := snappy.Encode(nil, data)

	httpReq, err := http.NewRequest("POST", *flagURL, bytes.NewReader(compressed))
	if err != nil {
		log.Fatalf("failed to create request: %v", err)
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	t := time.Now()
	httpResp, err := client.Do(httpReq)
	elapsed += int64(time.Since(t))
	if err != nil {
		log.Fatalf("failed to POST to url: %v", err)
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode/100 != 2 {
		log.Fatalf("HTTP request failed with status code: %d", httpResp.StatusCode)
	}
	fmt.Print(".")
	bytesSent += len(compressed)
	samplesSent += len(batch)
}
