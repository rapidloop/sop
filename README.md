
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

## Getting Started

> To be documented.

See [here](https://prometheus.io/docs/operating/configuration/#%3Cremote_write%3E)
for documentation about Prometheus' remote storage configuration.

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
