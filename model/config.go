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

package model

import (
	"io"
	"os"

	"github.com/BurntSushi/toml"
)

type Config struct {
	General GeneralConfig  `toml:"general"`
	Input   []InputConfig  `toml:"input"`
	Output  []OutputConfig `toml:"output"`
	API     []APIConfig    `toml:"api"`
}

type GeneralConfig struct {
	DataPath         string `toml:"data_path"`
	DownsampleSec    uint   `toml:"downsample_seconds"`
	TTLSec           uint   `toml:"ttl_seconds"`
	RetentionDays    uint   `toml:"retention_days"`
	RetentionGCHours uint   `toml:"retention_gc_hours"`
}

type APIConfig struct {
	Type             string            `toml:"type"`
	Listen           string            `toml:"listen"`
	FederationLabels map[string]string `toml:"federation_labels"`
}

type InputConfig struct {
	Type            string   `toml:"type"`
	Listen          string   `toml:"listen"`
	FilterInclude   []string `toml:"filter_include"`
	FilterExclude   []string `toml:"filter_exclude"`
	NATSURL         string   `toml:"nats_url"`
	STANCluster     string   `toml:"stan_cluster"`
	STANClientID    string   `toml:"stan_clientid"`
	STANDurableName string   `toml:"stan_durablename"`
	Subject         string   `toml:"subject"`
}

type OutputConfig struct {
	Type            string   `toml:"type"`
	Address         string   `toml:"address"`
	Username        string   `toml:"username"`
	Password        string   `toml:"password"`
	TimeoutSec      uint     `toml:"timeout_seconds"`
	NoCertCheck     bool     `toml:"no_cert_check"`
	Database        string   `toml:"database"`
	RetentionPolicy string   `toml:"retention_policy"`
	Protocol        string   `toml:"protocol"`
	FilterInclude   []string `toml:"filter_include"`
	FilterExclude   []string `toml:"filter_exclude"`
	NATSURL         string   `toml:"nats_url"`
	STANCluster     string   `toml:"stan_cluster"`
	STANClientID    string   `toml:"stan_clientid"`
	Subject         string   `toml:"subject"`
	URL             string   `toml:"url"`
}

// Valid types for input.
const (
	InputTypePrometheus1RemoteWrite = "prometheusv1_remote_write"
	InputTypePrometheus2RemoteWrite = "prometheusv2_remote_write"
	InputTypeNatsStreaming          = "nats_streaming"
	InputTypeInfluxDB               = "influxdb"
)

// Valid types for API.
const (
	APITypePrometheusHTTP = "prometheus_http"
)

// Valid types for output.
const (
	OutputTypePrometheus2RemoteWrite = "prometheusv2_remote_write"
	OutputTypeInfluxDB               = "influxdb"
	OutputTypeNatsStreaming          = "nats_streaming"
	OutputTypeOpenTSDB               = "opentsdb"
)

var defaultConfig = Config{
	General: GeneralConfig{
		DataPath:         "data",
		TTLSec:           240, // 240 seconds = 4 minutes
		RetentionDays:    180,
		RetentionGCHours: 24,
	},
}

func (c *Config) Load(file string) error {
	newConfig := defaultConfig
	if _, err := toml.DecodeFile(file, &newConfig); err != nil {
		return err
	}
	*c = newConfig
	return nil
}

func PrintExampleConfig() {
	io.WriteString(os.Stdout, exampleConfig)
}

const exampleConfig = `
# This is the default configuration file for sop. For more information see
# https://github.com/rapidloop/sop.


#-------------------------------------------------------------------------------
# General
#-------------------------------------------------------------------------------

[general]

# data_path specifies the directory where database files are created. Defaults
# to "data" under the current directory.
data_path = "data"

# downsample_seconds controls downsampling. By default it is set to 0, which
# disables downsampling. To enable, set it to a duration value in seconds. sop
# will then sample the incoming data every these many seconds and store it in
# it's main database.
downsample_seconds = 0

# Any metric that does not receive an updated sample before this TTL expires
# will not show up in downsampling, API or output. Related to Prometheus'
# "staleness" concept. You can this to 0 to disable, but promql queries may
# not work as expected.
ttl_seconds = 240

# The data is retained in the main time series database for these many days.
retention_days = 180

# A job is run every these many hours to ensure that samples older than the
# retention specified above are deleted from the time series database.
# NOTE: This job is *disk intensive*!
retention_gc_hours = 24


#-------------------------------------------------------------------------------
# Input
#-------------------------------------------------------------------------------
# Input configuration.
#
# There can be any number of "input" sections. Each input section will start
# an input handler of a specific type. Currently there are 4 input types, an
# example of each is shown below.
#
# Filters can be used to select a subset of the incoming data to be stored.
# Use an "exclude" filter to accept *all but the* specified metrics selectors.
# Use an "include" filter to accept *only the* specified metrics selectors.
# Only an include or an exclude filter may be specified for an input, not both.
# A metric selector is a string like 'foo{env=~"staging|testing|development"}'.

[[input]]
type = "prometheusv2_remote_write"
listen = "0.0.0.0:9096"
# filter_exclude = [ '{job="prometheus"}' ]
# filter_include = []

# [[input]]
# type = "prometheusv1_remote_write"
# listen = "0.0.0.0:9097"
# filter_exclude = [ '{job="prometheus"}' ]
# filter_include = []

# [[input]]
# type = "nats_streaming"
# nats_url = ""         # required
# stan_cluster = ""     # required
# stan_clientid = ""    # required
# stan_durablename = "" # required
# subject = ""          # required

# [[input]]
# type = "influxdb"
# listen = "0.0.0.0:8086"


#-------------------------------------------------------------------------------
# API
#-------------------------------------------------------------------------------
# Enable APIs to query the data stored in sop.

# The prometheus_http API provides interfaces for Grafana and Prometheus
# federation. 'federation_labels' are extra labels added only to the metrics
# returned by the '/federate' endpoint.
[[api]]
type = "prometheus_http"
listen = "0.0.0.0:9095"
federation_labels = {generator="sop",foo="bar"}


#-------------------------------------------------------------------------------
# Output
#-------------------------------------------------------------------------------
# Configure where all to push the received data to (apart from storing it
# internally).

# [[output]]
# type = "influxdb"
# address = "localhost:8086"    # required
# protocol = "http"             # required, one of: http, https, udp
# database = "prometheus"       # required
# retention_policy = ""
# username = "foo"
# password = "bar"
# filter_include = []
# filter_exclude = ["up"]

# [[output]]
# type = "nats_streaming"
# nats_url = ""      # required, e.g. nats://127.0.0.1:4222
# stan_cluster = ""  # required, must match what was passed to nats-streaming-server
# stan_clientid = "" # required, a name unique among all clients of this cluster
# subject = ""       # required, match to another sop's input

# [[output]]
# type = "prometheusv2_remote_write"
# url = "http://10.11.12.13:9096/write"  # required
# timeout_secs = 5

# [[output]]
# type = "opentsdb"
# url = "http://127.0.0.1:4242/"   # required
# timeout_secs = 5
`
