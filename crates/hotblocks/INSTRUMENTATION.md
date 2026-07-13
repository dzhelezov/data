# Hotblocks RocksDB and slow-response instrumentation

## RocksDB metrics

The RocksDB collector is always registered. Integer properties are exported whenever RocksDB
returns them; statistics metrics require `--rocksdb-stats`. Missing properties and disabled
statistics are skipped without failing the metrics scrape.

All per-column-family property gauges carry `cf`, whose value is one of `DATASETS`, `CHUNKS`,
`TABLES`, `DIRTY_TABLES`, or `DELETED_TABLES`:

- `hotblocks_rocksdb_num_running_compactions`
- `hotblocks_rocksdb_num_running_flushes`
- `hotblocks_rocksdb_compaction_pending`
- `hotblocks_rocksdb_estimate_pending_compaction_bytes`
- `hotblocks_rocksdb_mem_table_flush_pending`
- `hotblocks_rocksdb_cur_size_all_mem_tables`
- `hotblocks_rocksdb_size_all_mem_tables`
- `hotblocks_rocksdb_num_live_versions`
- `hotblocks_rocksdb_live_sst_files_size`
- `hotblocks_rocksdb_total_sst_files_size`
- `hotblocks_rocksdb_estimate_num_keys`
- `hotblocks_rocksdb_estimate_table_readers_mem`
- `hotblocks_rocksdb_block_cache_usage`
- `hotblocks_rocksdb_block_cache_pinned_usage`
- `hotblocks_rocksdb_num_immutable_mem_table`
- `hotblocks_rocksdb_estimate_live_data_size`

The database-wide write-stall gauges have no labels:

- `hotblocks_rocksdb_actual_delayed_write_rate`
- `hotblocks_rocksdb_is_write_stopped`

The following statistics tickers are cumulative values exported as gauges. Use `rate()` in
PromQL; a process restart resets them:

- `hotblocks_rocksdb_block_cache_hit_total`
- `hotblocks_rocksdb_block_cache_miss_total`
- `hotblocks_rocksdb_block_cache_data_hit_total`
- `hotblocks_rocksdb_block_cache_data_miss_total`
- `hotblocks_rocksdb_stall_micros_total`
- `hotblocks_rocksdb_bytes_read_total`
- `hotblocks_rocksdb_bytes_written_total`
- `hotblocks_rocksdb_compact_read_bytes_total`
- `hotblocks_rocksdb_compact_write_bytes_total`

The selected RocksDB statistics histograms are exposed as cumulative, unlabeled count and sum
gauges:

- `hotblocks_rocksdb_db_write_stall_count`
- `hotblocks_rocksdb_db_write_stall_sum`
- `hotblocks_rocksdb_compaction_times_micros_count`
- `hotblocks_rocksdb_compaction_times_micros_sum`

## Slow-response metrics

`hotblocks_slow_responses_total{dataset,reason}` counts non-long-poll responses that exceed a
configured threshold. `reason` is `ttfb` when time to first byte is too high or `throughput` when
a completed stream of at least 10,000 bytes falls below the configured bytes-per-second floor.

The existing `hotblocks_http_seconds_to_first_byte` histogram now has a `long_poll` boolean label.
A request is a long-poll when its query enters the dataset waiter because the requested first
block is not available yet. Dashboards and alerts can select `long_poll="false"` for ordinary
query latency.

Slow detections also emit a warning with `dataset`, `reason`, `ttfb_ms`, `bytes`,
`bytes_per_sec`, and `is_long_poll` fields.

## CLI flags

- `--slow-response-ttfb-ms <MS>` sets the non-long-poll TTFB threshold. Default: `2000`.
- `--slow-response-min-bps <BYTES_PER_SECOND>` sets the minimum completed-stream throughput.
  Default: `50000`.
- `--rocksdb-stats` enables the RocksDB cumulative ticker and histogram metrics. Property gauges
  do not require it.
