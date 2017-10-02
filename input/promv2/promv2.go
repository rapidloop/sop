package promv2

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
	"github.com/rapidloop/sop/util"
)

const (
	promServerStopTimeout = 10 * time.Second
	promHTTPReadTimeout   = 5 * time.Second
	promHTTPWriteTimeout  = 5 * time.Second
	promHTTPIdleTimeout   = 5 * time.Minute
)

var (
	index = 0

	metInput = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "sop_input_promv2_input_seconds",
		Help:       "Time taken for processing write requests.",
		Objectives: map[float64]float64{0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
	}, []string{"index"})
	metSamplesCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sop_input_promv2_samples_total",
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
	return fmt.Sprintf("prometheus v2 remote write (listen=%s)", im.listen)
}

func (im *impl) Start() error {
	ln, err := net.Listen("tcp", im.listen)
	if err != nil {
		return err
	}
	im.server = &http.Server{
		Handler:      http.HandlerFunc(im.handler),
		ReadTimeout:  promHTTPReadTimeout,
		WriteTimeout: promHTTPWriteTimeout,
		IdleTimeout:  promHTTPIdleTimeout,
	}
	go im.server.Serve(ln)
	return nil
}

func (im *impl) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), promServerStopTimeout)
	defer cancel()
	return im.server.Shutdown(ctx)
}

func (im *impl) handler(w http.ResponseWriter, r *http.Request) {
	t := time.Now()
	defer func() {
		im.metInput.Observe(time.Since(t).Seconds())
	}()

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
				log.Printf("promv2 remote write input: %v", err)
			} else {
				im.metSamplesCount.Add(float64(len(samples)))
			}
		}
	}
}

func init() {
	prometheus.MustRegister(metInput)
	prometheus.MustRegister(metSamplesCount)
}
