package util

import (
	"context"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rapidloop/sop/model"
)

type BatchItem struct {
	Metric  model.Metric
	Samples []model.Sample
}

type Batcher struct {
	maxCount int
	maxWait  time.Duration
	in       chan BatchItem
	out      chan []BatchItem
	wg       sync.WaitGroup
	cancel   context.CancelFunc
}

func NewBatcher(maxCount int, maxWait time.Duration, outChan chan []BatchItem) *Batcher {
	return &Batcher{
		maxCount: maxCount,
		maxWait:  maxWait,
		in:       make(chan BatchItem),
		out:      outChan,
	}
}

func (b *Batcher) batcher(ctx context.Context) {
	items := make([]BatchItem, 0, b.maxCount)
	flush := func() {
		b.out <- items
		items = make([]BatchItem, 0, b.maxCount)
	}
	t := time.NewTicker(b.maxWait)
	for {
		select {
		case item := <-b.in:
			items = append(items, item)
			if len(items) == b.maxCount {
				flush()
			}
		case <-t.C:
			if len(items) > 0 {
				flush()
			}
		case <-ctx.Done():
			if len(items) > 0 {
				b.out <- items
			}
			t.Stop()
			b.wg.Done()
			return
		}
	}
}

func (b *Batcher) Start() {
	newCtx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.wg.Add(1)
	go b.batcher(newCtx)
}

func (b *Batcher) Stop() {
	b.cancel()
	b.wg.Wait()
	close(b.in)
}

func (b *Batcher) Enqueue(metric model.Metric, samples []model.Sample) {
	b.in <- BatchItem{metric, samples}
}

func ToWriteRequest(items []BatchItem) (sampleCount int, data []byte, err error) {
	// serialize protobuf
	var wr prompb.WriteRequest
	wr.Timeseries = make([]*prompb.TimeSeries, len(items))
	for i, item := range items {
		wr.Timeseries[i] = &prompb.TimeSeries{
			Labels:  make([]*prompb.Label, len(item.Metric)),
			Samples: make([]*prompb.Sample, len(item.Samples)),
		}
		for j, label := range item.Metric {
			wr.Timeseries[i].Labels[j] = &prompb.Label{
				Name:  label.Name,
				Value: label.Value,
			}
		}
		for j, sample := range item.Samples {
			wr.Timeseries[i].Samples[j] = &prompb.Sample{
				Timestamp: int64(sample.Timestamp),
				Value:     sample.Value,
			}
			sampleCount++
		}
	}
	pbData, err := wr.Marshal()
	if err != nil {
		return
	}

	// compress
	data = snappy.Encode(nil, pbData)
	return
}
