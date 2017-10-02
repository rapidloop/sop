package influxdb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"time"

	influxdb "github.com/influxdata/influxdb/client/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/util"
)

var (
	batchMaxCount = 100
	batchMaxWait  = time.Minute

	index = 0

	metWrite = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "sop_output_influxdb_write_seconds",
		Help:       "Time taken for performing batch writes.",
		Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
	}, []string{"index"})
	metWritesCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sop_output_influxdb_writes_total",
		Help: "The number of total remote writes.",
	}, []string{"index", "result"})
	metSamplesCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sop_output_influxdb_samples_total",
		Help: "The number of successful samples sent.",
	}, []string{"index"})
)

type impl struct {
	b      *util.Batcher
	cancel context.CancelFunc
	wg     sync.WaitGroup
	out    chan []util.BatchItem
	f      model.Filter

	db     string
	rp     string
	client influxdb.Client
	addr   string

	metWrite              prometheus.Observer
	metSuccessWritesCount prometheus.Counter
	metFailWritesCount    prometheus.Counter
	metSamplesCount       prometheus.Counter
}

func NewOutput(config model.OutputConfig) (*impl, error) {
	out := make(chan []util.BatchItem, 1)
	if len(config.Database) == 0 {
		return nil, errors.New("'database' must be specified for InfluxDB")
	}
	var filter model.Filter
	if err := filter.Compile(config.FilterInclude, config.FilterExclude); err != nil {
		return nil, err
	}
	if _, _, err := util.ParseHostPort(config.Address); err != nil {
		return nil, fmt.Errorf("bad address %q for influxdb: %v", config.Address, err)
	}
	if config.Protocol != "http" && config.Protocol != "https" &&
		config.Protocol != "udp" {
		return nil, fmt.Errorf("'protocol' must be one of 'http', 'https' or 'udp'")
	}
	var client influxdb.Client
	var addr string
	var err error
	if config.Protocol == "udp" {
		addr = "udp://" + config.Address
		client, err = influxdb.NewUDPClient(influxdb.UDPConfig{
			Addr: config.Address,
		})
	} else {
		addr = fmt.Sprintf("%s://%s", config.Protocol, config.Address)
		client, err = influxdb.NewHTTPClient(influxdb.HTTPConfig{
			Addr:               addr,
			Username:           config.Username,
			Password:           config.Password,
			Timeout:            time.Duration(config.TimeoutSec) * time.Second,
			InsecureSkipVerify: config.NoCertCheck,
		})
	}
	if err != nil {
		return nil, err
	}
	indexStr := strconv.Itoa(index)
	index++
	im := &impl{
		b:                     util.NewBatcher(batchMaxCount, batchMaxWait, out),
		out:                   out,
		db:                    config.Database,
		rp:                    config.RetentionPolicy,
		f:                     filter,
		client:                client,
		addr:                  addr,
		metWrite:              metWrite.WithLabelValues(indexStr),
		metSuccessWritesCount: metWritesCount.WithLabelValues(indexStr, "success"),
		metFailWritesCount:    metWritesCount.WithLabelValues(indexStr, "fail"),
		metSamplesCount:       metSamplesCount.WithLabelValues(indexStr),
	}
	return im, nil
}

func (im *impl) Info() string {
	return fmt.Sprintf("influxdb (addr=%s, db=%s, rp=%s)", im.addr, im.db, im.rp)
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
			if err := im.send(items); err != nil {
				log.Printf("influxdb send: %v", err)
			}
		case <-ctx.Done():
			im.wg.Done()
			return
		}
	}
}

func (im *impl) send(batch []util.BatchItem) error {
	t := time.Now()
	defer func() {
		im.metWrite.Observe(time.Since(t).Seconds())
	}()

	points := make([]*influxdb.Point, 0, 100*len(batch))
	// for each item
	for _, it := range batch {
		if !im.f.Match(it.Metric) {
			continue // skip if it does not pass the filter
		}
		// for each sample
		for _, s := range it.Samples {
			v := float64(s.Value)
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			p, err := influxdb.NewPoint(
				it.Metric.Name(),
				tagsFromMetric(it.Metric),
				map[string]interface{}{"value": v},
				time.Unix(0, int64(s.Timestamp)*1e6),
			)
			if err != nil {
				return err
			}
			points = append(points, p)
		}
	}
	if len(points) == 0 {
		return nil
	}

	bps, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:        im.db,
		RetentionPolicy: im.rp,
	})
	if err != nil {
		im.metFailWritesCount.Inc()
		return err
	}
	bps.AddPoints(points)
	if err := im.client.Write(bps); err != nil {
		im.metFailWritesCount.Inc()
		return err
	}
	im.metSamplesCount.Add(float64(len(points)))
	im.metSuccessWritesCount.Inc()
	return nil
}

// tagsFromMetric extracts InfluxDB tags from a metric.
func tagsFromMetric(m model.Metric) map[string]string {
	tags := make(map[string]string, len(m)-1)
	for _, lv := range m {
		if lv.Name != model.MetricNameLabel {
			tags[lv.Name] = lv.Value
		}
	}
	return tags
}

func init() {
	prometheus.MustRegister(metWrite)
	prometheus.MustRegister(metWritesCount)
	prometheus.MustRegister(metSamplesCount)
}
