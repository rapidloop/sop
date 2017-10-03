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
