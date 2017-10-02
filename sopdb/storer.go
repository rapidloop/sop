package sopdb

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/output"
)

type Storer struct {
	db          DB
	downsample  time.Duration
	ttl         time.Duration
	downsampler *Downsampler
	outputs     []output.Output
}

func NewStorer(db DB, config model.GeneralConfig, outputs []output.Output) *Storer {
	downsample := time.Duration(config.DownsampleSec) * time.Second
	ttl := time.Duration(config.TTLSec) * time.Second
	s := &Storer{
		db:         db,
		downsample: downsample,
		ttl:        ttl,
		outputs:    outputs,
	}
	if downsample > 0 {
		s.downsampler = NewDownsampler(db, downsample, ttl, outputs)
	}
	return s
}

func (s *Storer) Downsample() time.Duration {
	return s.downsample
}

func (s *Storer) TTL() time.Duration {
	return s.ttl
}

func (s *Storer) Start() {
	if s.downsampler != nil {
		s.downsampler.Start()
	}
}

func (s *Storer) Stop() {
	if s.downsampler != nil {
		s.downsampler.Stop()
	}
}

func (s *Storer) Store(metric model.Metric, samples []model.Sample) error {
	// validate input
	if !metric.IsValid() {
		return ErrBadMetric
	}
	if len(samples) == 0 {
		return ErrBadSample
	}

	// get series id for metric
	index := s.db.Index()
	id, err := index.Make(metric)
	if err != nil {
		return err
	}

	// store the latest sample in latest DB
	recent := samples[len(samples)-1]
	err = s.db.Latest().Put(id, metric, recent.Timestamp, recent.Value)
	if err != nil {
		return err
	}

	// if downsample is disabled, store all the samples in tsdb also
	if s.downsampler == nil {
		tsdb := s.db.TS()
		for _, s := range samples {
			if err := tsdb.Put(id, s.Timestamp, s.Value); err != nil {
				return err
			}
		}

		// push to outputs
		for _, o := range s.outputs {
			// this assumes that the Output will behave sensibly and not block
			// for any longer than necessary
			o.Store(metric, samples)
		}
	}

	return nil
}

//------------------------------------------------------------------------------

type Downsampler struct {
	db      DB
	dur     time.Duration
	ttl     time.Duration
	outputs []output.Output
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func NewDownsampler(db DB, dur, ttl time.Duration, outputs []output.Output) *Downsampler {
	return &Downsampler{
		db:      db,
		dur:     dur,
		ttl:     ttl,
		outputs: outputs,
	}
}

func (d *Downsampler) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel
	d.wg.Add(1)
	go d.downsampler(ctx)
}

func (d *Downsampler) downsampler(ctx context.Context) {
	t := time.NewTicker(d.dur)
	for {
		select {
		case <-t.C:
			d.downsample()
		case <-ctx.Done():
			t.Stop()
			d.wg.Done()
			return
		}
	}
}

func (d *Downsampler) downsample() {
	t := time.Now()
	var put, reject int
	latest := d.db.Latest()
	tsdb := d.db.TS()

	var after uint64
	now := uint64(time.Now().UnixNano() / 1e6)
	if d.ttl > 0 {
		after = now - uint64(d.ttl/1e6)
	}

	err := latest.Iterate(func(seriesID int, metric model.Metric, timestamp uint64, value float64) error {
		if timestamp <= after { // ignore expired entries
			reject++
			return nil
		}
		if err := tsdb.Put(seriesID, now, value); err != nil {
			return err
		}
		put++
		for _, o := range d.outputs {
			o.Store(metric, []model.Sample{
				{Timestamp: now, Value: value},
			})
		}
		return nil
	})
	if err != nil {
		log.Printf("downsample failed: %v", err)
	}

	elapsed := time.Since(t)
	log.Printf("downsample run: stored %d rejected %d samples in %v",
		put, reject, elapsed)
}

func (d *Downsampler) Stop() {
	d.cancel()
	d.wg.Wait()
}
