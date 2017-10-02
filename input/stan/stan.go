package stan

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/nats-io/go-nats-streaming"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
)

var (
	index = 0

	metInput = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "sop_input_stan_input_seconds",
		Help:       "Time taken for processing messages.",
		Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
	}, []string{"index"})
	metSamplesCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sop_input_stan_samples_total",
		Help: "The number of successful samples stored.",
	}, []string{"index"})
)

type impl struct {
	f       model.Filter
	storer  *sopdb.Storer
	stopped bool

	natsURL      string
	stanCluster  string
	stanClientID string
	subject      string
	durableName  string
	conn         stan.Conn
	subn         stan.Subscription

	metInput        prometheus.Observer
	metSamplesCount prometheus.Counter
}

func NewInput(config model.InputConfig, storer *sopdb.Storer) (*impl, error) {
	if len(config.NATSURL) == 0 {
		return nil, errors.New("'nats_url' must be specified for nats streaming")
	}
	if len(config.STANCluster) == 0 {
		return nil, errors.New("'stan_cluster' must be specified for nats streaming")
	}
	if len(config.STANClientID) == 0 {
		return nil, errors.New("'stan_clientid' must be specified for nats streaming")
	}
	if len(config.Subject) == 0 {
		return nil, errors.New("'subject' must be specified for nats streaming")
	}
	if len(config.STANDurableName) == 0 {
		return nil, errors.New("'stan_durablename' must be specified for nats streaming")
	}
	var filter model.Filter
	if err := filter.Compile(config.FilterInclude, config.FilterExclude); err != nil {
		return nil, err
	}
	indexStr := strconv.Itoa(index)
	index++
	im := &impl{
		f:               filter,
		storer:          storer,
		natsURL:         config.NATSURL,
		stanCluster:     config.STANCluster,
		stanClientID:    config.STANClientID,
		subject:         config.Subject,
		durableName:     config.STANDurableName,
		metInput:        metInput.WithLabelValues(indexStr),
		metSamplesCount: metSamplesCount.WithLabelValues(indexStr),
	}
	return im, nil
}

func (im *impl) Info() string {
	return fmt.Sprintf("nats streaming (url=%s, cluster=%s, subject=%s)",
		im.natsURL, im.stanCluster, im.subject)
}

func (im *impl) Start() error {
	conn, err := stan.Connect(im.stanCluster, im.stanClientID, stan.NatsURL(im.natsURL))
	if err != nil {
		return err
	}
	subn, err := conn.Subscribe(im.subject, im.handler,
		stan.DurableName(im.durableName), stan.SetManualAckMode(),
		stan.StartWithLastReceived())
	if err != nil {
		conn.Close()
		return err
	}
	im.conn = conn
	im.subn = subn
	return nil
}

func (im *impl) Stop() error {
	im.stopped = true
	return im.conn.Close()
}

func (im *impl) handler(msg *stan.Msg) {
	t := time.Now()
	defer func() {
		im.metInput.Observe(time.Since(t).Seconds())
	}()

	if im.stopped {
		return // without acking
	}
	reqBuf, err := snappy.Decode(nil, msg.Data)
	if err != nil {
		log.Printf("nats streaming input: decode error: %v", err)
		return
	}

	var req prompb.WriteRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		log.Printf("nats streaming input: unmarshal error: %v", err)
		return
	}

	for _, ts := range req.Timeseries {
		labels := make([]model.Label, len(ts.Labels))
		for i, l := range ts.Labels {
			labels[i].Name = l.Name
			labels[i].Value = l.Value
		}

		// check if it matches filter
		if !im.f.Match(labels) {
			continue
		}

		samples := make([]model.Sample, 0, len(ts.Samples))
		for _, s := range ts.Samples {
			if !value.IsStaleNaN(s.Value) {
				samples = append(samples, model.Sample{
					Timestamp: uint64(s.Timestamp),
					Value:     s.Value,
				})
			}
		}

		if len(samples) > 0 {
			if err := im.storer.Store(labels, samples); err != nil {
				log.Printf("nats streaming input: %v", err)
			} else {
				im.metSamplesCount.Add(float64(len(samples)))
			}
		}
	}

	if err := msg.Ack(); err != nil {
		log.Printf("nats streaming input: ack failed: %v", err)
	}
}

func init() {
	prometheus.MustRegister(metInput)
	prometheus.MustRegister(metSamplesCount)
}
