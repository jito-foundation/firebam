# Metrics

## Overview
Firedancer maintains many internal performance counters for use by
developers and monitoring tools, and exposes them via a
[Prometheus](https://prometheus.io/docs/introduction/overview/) HTTP
endpoint:

::: code-group

```toml [config.toml]
[tiles.metric]
    prometheus_listen_port = 7999
```

:::

```sh [bash]
$ curl http://localhost:7999/metrics
# HELP tile_pid The process ID of the tile.
# TYPE tile_pid gauge
tile_pid{kind="net",kind_id="0"} 1527373
tile_pid{kind="quic",kind_id="0"} 1527370
tile_pid{kind="quic",kind_id="1"} 1527371
tile_pid{kind="verify",kind_id="0"} 1527369
tile_pid{kind="verify",kind_id="1"} 1527374
tile_pid{kind="dedup",kind_id="0"} 1527365
...
```

## Streaming Updates (Websocket)
The GUI tile exposes data over its WebSocket at the `/websocket` endpoint. You can use a tool like [websocat](https://github.com/vi/websocat) to connect to it:

```sh [bash]
$ websocat ws://127.0.0.1:80/websocket | jq -c 'select(.topic=="slot")'
{"topic":"slot","key":"update","value":{"publish":{"slot":4892,"mine":true,"start_timestamp_nanos":"1763952870737140785","target_end_timestamp_nanos":"1763952871087140864","skipped":false,"duration_nanos":349917888,"completed_time_nanos":"1763952871092165326","level":"optimistically_confirmed","success_nonvote_transaction_cnt":0,"failed_nonvote_transaction_cnt":0,"success_vote_transaction_cnt":1,"failed_vote_transaction_cnt":0,"max_compute_units":60000000,"compute_units":3428,"shreds": null,"transaction_fee":"2500","priority_fee":"0","tips":"0"}}}
...
```

::: warning WARNING

Metrics are currently only provided for developer and diagnostic use,
and the endpoint or data provided may break or change in incompatible
ways at any time.

:::

There are three metric types reported by Firedancer, following the
[Prometheus data model](https://prometheus.io/docs/concepts/metric_types/):

 - `counter` &mdash; A cumulative metric representing a monotonically increasing counter.
 - `gauge` &mdash; A single numerical value that can go arbitrarily up or down.
 - `histogram` &mdash; Samples observations like packet sizes and counts them in buckets.

<!--@include: ./metrics-generated.md-->
