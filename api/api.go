package api

import (
	"fmt"

	"github.com/rapidloop/sop/model"
	"github.com/rapidloop/sop/sopdb"
)

type API interface {
	Info() string
	Start() error
	Stop() error
}

func NewAPI(config model.APIConfig, general model.GeneralConfig,
	db sopdb.DB) (API, error) {

	switch config.Type {
	case model.APITypePrometheusHTTP:
		return newPromHTTPAPI(config, general, db)
	default:
		return nil, fmt.Errorf("unknown api type %q", config.Type)
	}
}
