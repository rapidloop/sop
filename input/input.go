package input

import (
	"fmt"

	"github.com/rapidloop/sop/input/influxdb"
	"github.com/rapidloop/sop/input/promv1"
	"github.com/rapidloop/sop/input/promv2"
	"github.com/rapidloop/sop/input/stan"
	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
)

type Input interface {
	Info() string
	Start() error
	Stop() error
}

func NewInput(config model.InputConfig, storer *sopdb.Storer) (Input, error) {
	switch config.Type {
	case model.InputTypePrometheus1RemoteWrite:
		return promv1.NewInput(config, storer)
	case model.InputTypePrometheus2RemoteWrite:
		return promv2.NewInput(config, storer)
	case model.InputTypeNatsStreaming:
		return stan.NewInput(config, storer)
	case model.InputTypeInfluxDB:
		return influxdb.NewInput(config, storer)
	default:
		return nil, fmt.Errorf("unknown input type %q", config.Type)
	}
}
