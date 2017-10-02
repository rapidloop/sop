package output

import (
	"fmt"

	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/output/influxdb"
	"github.com/rapidloop/sop/output/opentsdb"
	"github.com/rapidloop/sop/output/promv2"
	"github.com/rapidloop/sop/output/stan"
)

type Output interface {
	Info() string
	Start() error
	Stop() error
	Store(metric model.Metric, samples []model.Sample)
}

func NewOutput(config model.OutputConfig) (Output, error) {
	switch config.Type {
	case model.OutputTypePrometheus2RemoteWrite:
		return promv2.NewOutput(config)
	case model.OutputTypeInfluxDB:
		return influxdb.NewOutput(config)
	case model.OutputTypeNatsStreaming:
		return stan.NewOutput(config)
	case model.OutputTypeOpenTSDB:
		return opentsdb.NewOutput(config)
	default:
		return nil, fmt.Errorf("unknown output type %q", config.Type)
	}
}
