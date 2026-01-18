BAM Debug
=========

This page collects quick BAM-specific runtime checks for Firedancer.

Metrics
-------

The BAM tile emits Prometheus metrics. 

Key counters/gauges to watch:
- `bam_bundle_received`: Cumulative bundles received from the BAM node.
- `bam_results_sent`: Cumulative bundle result messages forwarded to the BAM node.
- `bam_bundle_results_dropped`: Results dropped before publication.
- `bam_results_queue_depth`: Results buffered for feedback.

Commands
--------

Live watch:

```sh
watch -n 1 'curl -s http://127.0.0.1:7999/metrics | grep -E "^(bam_bundle_received|bam_results_sent|bam_bundle_results_dropped|bam_results_queue_depth)\\b"'
```

If the HTTP endpoint is disabled, you can still dump metrics via `fdctl`
(update the config path as needed):

```sh
fdctl metrics --config local.toml | grep -E "^(bam_bundle_received|bam_results_sent|bam_bundle_results_dropped|bam_results_queue_depth)\\b"
```
