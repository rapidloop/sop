
**sop** is a multi-purpose metrics storage and manipulation tool based on the
Prometheus metrics data model.

## Features

### Inputs

sop can accept inputs from various sources, simultaneously. The incoming metrics
can be **downsampled**. You can also use **filters** to store only a subset of
the incoming data. Time series data is stored in RocksDB, and is **compressed**
by default. Metrics can be set to expire, to support **staleness handling**.

These input sources are implemented currently:

* *Prometheus v1.x remote write*: An HTTP endpoint that can be set as the
  remote write URL for Prometheus v1.x. Tested with 1.7.2.
* *Prometheus v2.x remote write*: An HTTP endpoint that can be set as the
  remote write URL for Prometheus v2.x. Tested with 2.0.0-beta.5; subject to change
  as 2.0 evolves.
* *InfluxDB*: An HTTP "/write" endpoint that can accept InfluxDB line protocol. 
  Tags are mapped to Prometheus-style labels and field values to multiple
  metrics.
* *NATS streaming server*: A durable NATS streaming client, which can ingest
  and store metrics published to a NATS streaming server by another sop instance.
  (See outputs section below.)

### APIs

sop supports APIs that are used to get at the data stored inside it's database.
One API is implemented currently:

* *Prometheus HTTP API*: provides the standard Prometheus APIs, including
  PromQL and federation support. sop can be configured as a Prometheus
  data source in Grafana. Federation out of sop using the "/federate" endpoint
  is also possible.

### Outputs

Apart from storing internally, sop can stream the incoming data (after
downsampling, if downsampling is configured) to other servers. **Filters** can
be used to send only a subset of the data. Multiple outputs can be operated
simultaneously.

These output destinations are implemented currently:

* *Prometheus v2.x remote write*: Write to a Prometheus v2.x remote write
  HTTP endpoint.
* *InfluxDB*: Write to an InfluxDB server using the HTTP "/write" endpoint.
* *OpenTSDB*: Write to an OpenTSDB server using the HTTP "/api/put" endpoint.
* *NATS streaming server*: Publish metrics to a NATS streaming server. The NATS
  streaming server is used as a durable queue. The published messages can then
  be read in by another sop instance. It can also be read in by multiple sop
  instances, thereby redundantly duplicating the metrics stream. Using such a
  queue also allows the sop output to on occasion burst above rates that
  can be sustainedly ingested by the sop input.

## Uses

* *Long-term archiving*: downsample and store metrics for a configurable
  retention period. Stored data can be queried anytime with Grafana and APIs.
* *Unified storage*: metrics from multiple metrics system into sop. Or use sop
  to send them all to another system, like OpenTSDB.
* *Centralized storage*: collect and store metrics from multiple datacenters
  or locations into a centralized store.

## Status

sop is currently in beta (1.0-beta.1).

Performance-wise, it can do sustained write rates of 13k+ samples/sec on a
4-core 8GB DigitalOcean node, as measured using the `sopfilltest` tool.

## Getting Started

sop is available as a zero-dependency, single-binary executable for 64-bit
Linux platforms. Pre-built binaries are available as [GitHub releases](https://github.com/rapidloop/sop/releases);
start off by downloading the latest release.

sop is invoked with the path to a configuration file. You can generate the
default configuration file from sop itself using the "-p" flag:

```
$ sop -p > sop.cfg
```

You can then edit the configuration file "sop.cfg" to setup inputs, APIs and
outputs. The comments within the file should be self-explanatory. If not, ask!

You can run sop by invoking it with the path to the configuration file:

```
$ sop sop.cfg
2017/10/03 09:11:26.190018 main.go:77: sop starting: version=1.0-beta.1, pid=11333
2017/10/03 09:11:26.653607 main.go:115: started database: path=data (took 463.453866ms)
2017/10/03 09:11:26.653681 main.go:128: started storer: ttl=4m0s, downsample=0s
2017/10/03 09:11:26.653729 main.go:141: started reaper: retain=4320h0m0s, gc=24h0m0s
2017/10/03 09:11:26.654129 main.go:162: started input: prometheus v2 remote write (listen=0.0.0.0:9096)
2017/10/03 09:11:26.654366 main.go:188: started api: prometheus_http (listen=0.0.0.0:9095)
2017/10/03 09:11:26.654386 main.go:202: sop open for business
```

Use `^C` to exit.

See [here](https://prometheus.io/docs/operating/configuration/#%3Cremote_write%3E)
for documentation about Prometheus' remote storage configuration.

If you want to build sop yourself, see the [sop-build](github.com/rapidloop/sop-build)
repo.

## Contributing

sop is released under Apache 2.0 license, copyright (c) 2017 RapidLoop, Inc.

sop is written in Go. Data is stored in RocksDB. It reuses parts of Prometheus,
especially PromQL. The build scripts are maintained separately in the
sop-build repository. To hack on sop yourself, start by cloning sop-build and
building sop. It's fairly straight forward to add a new input, api or output.

Your contributions are welcome. Start off by raising a issue here in GitHub.

sop is developed and maintained by [RapidLoop](https://www.rapidloop.com/),
the makers of [OpsDash](https://www.opsdash.com/). Talk to us for support or
customized monitoring solutions at hello@rapidloop.com.

## So, hey, is _sop_ an acronym?

Ah. Um. Yeah, well, actually yes: _Son of Prometheus_. (ducks)
