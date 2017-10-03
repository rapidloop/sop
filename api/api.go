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
