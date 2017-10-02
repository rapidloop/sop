package stan

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/go-nats-streaming"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/util"
)

var (
	batchMaxCount = 100
	batchMaxWait  = time.Minute

	index = 0

	metWrite = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "sop_output_stan_seconds",
		Help:       "Time taken for publishing.",
		Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
	}, []string{"index"})
	metSamplesCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sop_output_stan_samples_total",
		Help: "The number of samples published.",
	}, []string{"index"})
)

type impl struct {
	b      *util.Batcher
	cancel context.CancelFunc
	wg     sync.WaitGroup
	out    chan []util.BatchItem
	f      model.Filter

	natsURL      string
	stanCluster  string
	stanClientID string
	subject      string
	conn         stan.Conn

	metWrite        prometheus.Observer
	metSamplesCount prometheus.Counter
}

func NewOutput(config model.OutputConfig) (*impl, error) {
	out := make(chan []util.BatchItem, 1)
	if len(config.NATSURL) == 0 {
		return nil, errors.New("'nats_url' must be specified for NATS streaming")
	}
	if len(config.STANCluster) == 0 {
		return nil, errors.New("'stan_cluster' must be specified for NATS streaming")
	}
	if len(config.STANClientID) == 0 {
		return nil, errors.New("'stan_clientid' must be specified for NATS streaming")
	}
	if len(config.Subject) == 0 {
		return nil, errors.New("'subject' must be specified for NATS streaming")
	}
	var filter model.Filter
	if err := filter.Compile(config.FilterInclude, config.FilterExclude); err != nil {
		return nil, err
	}
	indexStr := strconv.Itoa(index)
	index++
	im := &impl{
		b:               util.NewBatcher(batchMaxCount, batchMaxWait, out),
		out:             out,
		f:               filter,
		natsURL:         config.NATSURL,
		stanCluster:     config.STANCluster,
		stanClientID:    config.STANClientID,
		subject:         config.Subject,
		metWrite:        metWrite.WithLabelValues(indexStr),
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
	im.conn = conn
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
	return im.conn.Close()
}

func (im *impl) Store(metric model.Metric, samples []model.Sample) {
	im.b.Enqueue(metric, samples)
}

func (im *impl) sender(ctx context.Context) {
	for {
		select {
		case items := <-im.out:
			if err := im.send(items); err != nil {
				log.Printf("stan send: %v", err)
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

	count, data, err := util.ToWriteRequest(batch)
	if err != nil {
		return err
	}
	if err := im.conn.Publish(im.subject, data); err != nil {
		return err
	}
	im.metSamplesCount.Add(float64(count))
	return nil
}

func init() {
	prometheus.MustRegister(metWrite)
	prometheus.MustRegister(metSamplesCount)
}
