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
