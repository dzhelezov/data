use std::{fmt::Write, sync::LazyLock, time::Duration};

use anyhow::bail;
use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeLabelSet, EncodeLabelValue, LabelValueEncoder},
    metrics::{
        MetricType,
        counter::Counter,
        family::Family,
        histogram::{Histogram, exponential_buckets}
    },
    registry::Registry
};
use sqd_storage::db::{DatasetId, ReadSnapshot};
use tracing::{error, warn};

use crate::{query::QueryExecutorCollector, types::DBRef};

#[derive(Copy, Clone, Hash, Debug, Default, Ord, PartialOrd, Eq, PartialEq, EncodeLabelSet)]
struct DatasetLabel {
    dataset: DatasetValue
}

#[derive(Copy, Clone, Hash, Debug, Default, Ord, PartialOrd, Eq, PartialEq)]
struct DatasetValue(DatasetId);

impl EncodeLabelValue for DatasetValue {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        encoder.write_str(self.0.as_str())
    }
}

macro_rules! dataset_label {
    ($dataset_id:expr) => {
        DatasetLabel {
            dataset: DatasetValue($dataset_id)
        }
    };
}

type Labels = Vec<(&'static str, String)>;

const ROCKSDB_CF_PROPERTIES: &[(&str, &str)] = &[
    ("num_running_compactions", "rocksdb.num-running-compactions"),
    ("num_running_flushes", "rocksdb.num-running-flushes"),
    ("compaction_pending", "rocksdb.compaction-pending"),
    (
        "estimate_pending_compaction_bytes",
        "rocksdb.estimate-pending-compaction-bytes"
    ),
    ("mem_table_flush_pending", "rocksdb.mem-table-flush-pending"),
    ("cur_size_all_mem_tables", "rocksdb.cur-size-all-mem-tables"),
    ("size_all_mem_tables", "rocksdb.size-all-mem-tables"),
    ("num_live_versions", "rocksdb.num-live-versions"),
    ("live_sst_files_size", "rocksdb.live-sst-files-size"),
    ("total_sst_files_size", "rocksdb.total-sst-files-size"),
    ("estimate_num_keys", "rocksdb.estimate-num-keys"),
    ("estimate_table_readers_mem", "rocksdb.estimate-table-readers-mem"),
    ("block_cache_usage", "rocksdb.block-cache-usage"),
    ("block_cache_pinned_usage", "rocksdb.block-cache-pinned-usage"),
    ("num_immutable_mem_table", "rocksdb.num-immutable-mem-table"),
    ("estimate_live_data_size", "rocksdb.estimate-live-data-size")
];

const ROCKSDB_DB_PROPERTIES: &[(&str, &str)] = &[
    ("actual_delayed_write_rate", "rocksdb.actual-delayed-write-rate"),
    ("is_write_stopped", "rocksdb.is-write-stopped")
];

const ROCKSDB_TICKERS: &[(&str, &str)] = &[
    ("rocksdb.block.cache.hit", "block_cache_hit_total"),
    ("rocksdb.block.cache.miss", "block_cache_miss_total"),
    ("rocksdb.block.cache.data.hit", "block_cache_data_hit_total"),
    ("rocksdb.block.cache.data.miss", "block_cache_data_miss_total"),
    ("rocksdb.stall.micros", "stall_micros_total"),
    ("rocksdb.bytes.read", "bytes_read_total"),
    ("rocksdb.bytes.written", "bytes_written_total"),
    ("rocksdb.compact.read.bytes", "compact_read_bytes_total"),
    ("rocksdb.compact.write.bytes", "compact_write_bytes_total")
];

const ROCKSDB_HISTOGRAMS: &[(&str, &str)] = &[
    ("rocksdb.db.write.stall", "db_write_stall"),
    ("rocksdb.compaction.times.micros", "compaction_times_micros")
];

const MIN_SLOW_RESPONSE_BYTES: u64 = 10_000;

#[derive(Debug, Copy, Clone)]
pub struct SlowResponseConfig {
    ttfb_threshold: Duration,
    min_bytes_per_second: u64
}

impl SlowResponseConfig {
    pub fn new(ttfb_threshold_ms: u64, min_bytes_per_second: u64) -> Self {
        Self {
            ttfb_threshold: Duration::from_millis(ttfb_threshold_ms),
            min_bytes_per_second
        }
    }
}

impl Default for SlowResponseConfig {
    fn default() -> Self {
        Self::new(2_000, 50_000)
    }
}

fn buckets(start: f64, count: usize) -> impl Iterator<Item = f64> {
    std::iter::successors(Some(start), |x| Some(x * 10.))
        .flat_map(|x| [x, x * 1.5, x * 2.5, x * 5.0])
        .take(count)
}

pub static HTTP_STATUS: LazyLock<Family<Labels, Counter>> = LazyLock::new(Default::default);
pub static HTTP_TTFB: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(buckets(0.001, 20))));
pub static SLOW_RESPONSES: LazyLock<Family<Labels, Counter>> = LazyLock::new(Default::default);

pub static QUERY_ERROR_TOO_MANY_TASKS: LazyLock<Counter> = LazyLock::new(Default::default);
pub static QUERY_ERROR_TOO_MANY_DATA_WAITERS: LazyLock<Counter> = LazyLock::new(Default::default);

pub static COMPLETED_QUERIES: LazyLock<Counter> = LazyLock::new(Default::default);

pub static STREAM_DURATIONS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(0.01, 2.0, 20))));
pub static STREAM_BYTES: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(1000., 2.0, 20))));
pub static STREAM_BLOCKS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 2.0, 30))));
pub static STREAM_CHUNKS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(buckets(1., 20))));
pub static STREAM_BYTES_PER_SECOND: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(100., 3.0, 20))));
pub static STREAM_BLOCKS_PER_SECOND: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 3.0, 20))));

pub static QUERIED_BLOCKS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(exponential_buckets(1., 2.0, 30))));
pub static QUERIED_CHUNKS: LazyLock<Family<Labels, Histogram>> =
    LazyLock::new(|| Family::new_with_constructor(|| Histogram::new(buckets(1., 20))));

pub fn report_query_too_many_tasks_error() {
    QUERY_ERROR_TOO_MANY_TASKS.inc();
}

pub fn report_query_too_many_data_waiters_error() {
    QUERY_ERROR_TOO_MANY_DATA_WAITERS.inc();
}

pub fn report_http_response(
    labels: &Vec<(&'static str, String)>,
    to_first_byte: Duration,
    long_poll: bool,
    slow_response_config: SlowResponseConfig
) {
    HTTP_STATUS.get_or_create(&labels).inc();

    let mut ttfb_labels = labels.clone();
    ttfb_labels.push(("long_poll", long_poll.to_string()));
    HTTP_TTFB
        .get_or_create(&ttfb_labels)
        .observe(to_first_byte.as_secs_f64());

    if !long_poll && to_first_byte > slow_response_config.ttfb_threshold {
        let Some(dataset) = response_dataset(labels) else {
            return;
        };

        report_slow_response(dataset, "ttfb");
        warn!(
            dataset,
            ttfb_ms = duration_millis(to_first_byte),
            bytes = 0_u64,
            bytes_per_sec = 0.0,
            is_long_poll = long_poll,
            reason = "ttfb",
            "slow response detected"
        );
    }
}

pub fn report_stream_slow_response(
    dataset_id: DatasetId,
    to_first_byte: Duration,
    bytes: u64,
    duration: Duration,
    long_poll: bool,
    slow_response_config: SlowResponseConfig
) {
    if long_poll || bytes < MIN_SLOW_RESPONSE_BYTES || duration.is_zero() {
        return;
    }

    let bytes_per_sec = bytes as f64 / duration.as_secs_f64();
    if bytes_per_sec >= slow_response_config.min_bytes_per_second as f64 {
        return;
    }

    report_slow_response(dataset_id.as_str(), "throughput");
    warn!(
        dataset = dataset_id.as_str(),
        ttfb_ms = duration_millis(to_first_byte),
        bytes,
        bytes_per_sec,
        is_long_poll = long_poll,
        reason = "throughput",
        "slow response detected"
    );
}

fn response_dataset(labels: &Labels) -> Option<&str> {
    labels
        .iter()
        .find_map(|(key, value)| (*key == "dataset_name").then_some(value.as_str()))
}

fn report_slow_response(dataset: &str, reason: &'static str) {
    let labels = vec![("dataset", dataset.to_owned()), ("reason", reason.to_owned())];
    SLOW_RESPONSES.get_or_create(&labels).inc();
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[derive(Debug)]
pub struct RocksDbCollector {
    pub db: DBRef
}

impl Collector for RocksDbCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        for &(metric_suffix, property) in ROCKSDB_CF_PROPERTIES {
            let values: Vec<_> = self
                .db
                .column_families()
                .iter()
                .filter_map(|&cf| self.db.rocksdb_int_property_cf(cf, property).map(|value| (cf, value)))
                .collect();

            if values.is_empty() {
                continue;
            }

            let metric_name = format!("hotblocks_rocksdb_{metric_suffix}");
            let help = format!("RocksDB {property} property");
            let mut metric = encoder.encode_descriptor(&metric_name, &help, None, MetricType::Gauge)?;
            for (cf, value) in values {
                let labels = vec![("cf", cf.to_owned())];
                metric.encode_family(&labels)?.encode_gauge(&value)?;
            }
        }

        for &(metric_suffix, property) in ROCKSDB_DB_PROPERTIES {
            let Some(value) = self.db.rocksdb_int_property(property) else {
                continue;
            };
            let metric_name = format!("hotblocks_rocksdb_{metric_suffix}");
            let help = format!("RocksDB {property} property");
            encoder
                .encode_descriptor(&metric_name, &help, None, MetricType::Gauge)?
                .encode_gauge(&value)?;
        }

        if let Some(statistics) = self.db.rocksdb_statistics() {
            encode_rocksdb_statistics(&mut encoder, &statistics)?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
struct ParsedStatistic<'a> {
    name: &'a str,
    count: Option<u64>,
    sum: Option<f64>
}

fn parse_rocksdb_statistic(line: &str) -> Option<ParsedStatistic<'_>> {
    let tokens: Vec<_> = line.split_whitespace().collect();
    let name = *tokens.first()?;
    let mut count = None;
    let mut sum = None;
    let mut index = 1;

    while index < tokens.len() {
        let key = tokens[index].trim_end_matches(':');
        if key != "COUNT" && key != "SUM" {
            index += 1;
            continue;
        }

        index += 1;
        if tokens.get(index) == Some(&":") {
            index += 1;
        }
        let value = tokens.get(index)?.trim_end_matches(',');
        match key {
            "COUNT" => count = value.parse().ok(),
            "SUM" => sum = value.parse().ok(),
            // Guarded by the COUNT/SUM check above; never panic on the scrape path.
            _ => return None
        }
        index += 1;
    }

    (count.is_some() || sum.is_some()).then_some(ParsedStatistic { name, count, sum })
}

fn encode_rocksdb_statistics(encoder: &mut DescriptorEncoder, statistics: &str) -> Result<(), std::fmt::Error> {
    // Statistics values are cumulative. They are exported as gauges to preserve the raw
    // cumulative value (apply rate() in PromQL). Converting to the Counter metric type is a
    // follow-up: the OpenMetrics `_total` suffix must be handled to avoid a doubled suffix on
    // the already-`_total`-named tickers, and the histogram count/sum need proper typing.
    for line in statistics.lines() {
        let Some(statistic) = parse_rocksdb_statistic(line) else {
            continue;
        };

        if let Some((_, metric_suffix)) = ROCKSDB_TICKERS.iter().find(|(name, _)| *name == statistic.name) {
            if let Some(value) = statistic.count {
                let metric_name = format!("hotblocks_rocksdb_{metric_suffix}");
                encoder
                    .encode_descriptor(
                        &metric_name,
                        "Cumulative RocksDB statistics ticker",
                        None,
                        MetricType::Gauge
                    )?
                    .encode_gauge(&value)?;
            }
            continue;
        }

        let Some((_, metric_suffix)) = ROCKSDB_HISTOGRAMS.iter().find(|(name, _)| *name == statistic.name) else {
            continue;
        };

        if let Some(value) = statistic.count {
            let metric_name = format!("hotblocks_rocksdb_{metric_suffix}_count");
            encoder
                .encode_descriptor(
                    &metric_name,
                    "Cumulative RocksDB statistics histogram count",
                    None,
                    MetricType::Gauge
                )?
                .encode_gauge(&value)?;
        }
        if let Some(value) = statistic.sum {
            let metric_name = format!("hotblocks_rocksdb_{metric_suffix}_sum");
            encoder
                .encode_descriptor(
                    &metric_name,
                    "Cumulative RocksDB statistics histogram sum",
                    None,
                    MetricType::Gauge
                )?
                .encode_gauge(&value)?;
        }
    }

    Ok(())
}

#[derive(Debug)]
pub struct DatasetMetricsCollector {
    pub db: DBRef,
    pub datasets: Vec<DatasetId>
}

impl Collector for DatasetMetricsCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let db = self.db.snapshot();

        for dataset_id in self.datasets.iter().copied() {
            if let Err(err) = collect_dataset_metrics(&mut encoder, &db, dataset_id) {
                return if err.is::<std::fmt::Error>() {
                    Err(err.downcast().unwrap())
                } else {
                    // subsequent metric collection most likely will fail as well,
                    // hence let's terminate metric collection entirely
                    error!(
                        err =? err,
                        "failed to collect metrics for dataset {}",
                        dataset_id
                    );
                    Ok(())
                };
            }
        }

        Ok(())
    }
}

fn collect_dataset_metrics(
    encoder: &mut DescriptorEncoder,
    db: &ReadSnapshot,
    dataset_id: DatasetId
) -> anyhow::Result<()> {
    let Some(label) = db.get_label(dataset_id)? else {
        return Ok(());
    };

    let Some(first_chunk) = db.get_first_chunk(dataset_id)? else {
        return Ok(());
    };

    let Some(last_chunk) = db.get_last_chunk(dataset_id)? else {
        bail!("first chunk exists, while last does not")
    };

    encoder
        .encode_descriptor("hotblocks_first_block", "First block", None, MetricType::Gauge)?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&first_chunk.first_block())?;

    encoder
        .encode_descriptor("hotblocks_last_block", "Last block", None, MetricType::Gauge)?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&last_chunk.last_block())?;

    encoder
        .encode_descriptor(
            "hotblocks_last_block_timestamp_ms",
            "Timestamp of the last block",
            None,
            MetricType::Gauge
        )?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&last_chunk.last_block_time().unwrap_or(0))?;

    encoder
        .encode_descriptor(
            "hotblocks_last_finalized_block",
            "Last finalized block",
            None,
            MetricType::Gauge
        )?
        .encode_family(&dataset_label!(dataset_id))?
        .encode_gauge(&label.finalized_head().map_or(0, |h| h.number))?;

    Ok(())
}

pub fn build_metrics_registry() -> Registry {
    let mut top_registry = Registry::default();
    let registry = top_registry.sub_registry_with_prefix("hotblocks");

    registry.register(
        "query_error_too_many_tasks",
        "Number of query tasks rejected due to task queue overflow",
        QUERY_ERROR_TOO_MANY_TASKS.clone()
    );

    registry.register(
        "query_error_too_many_data_waiters",
        "Number of queries rejected, because data is not yet available and there are too many data waiters",
        QUERY_ERROR_TOO_MANY_DATA_WAITERS.clone()
    );

    registry.register("http_status", "Number of sent HTTP responses", HTTP_STATUS.clone());
    registry.register(
        "http_seconds_to_first_byte",
        "Time to first byte of HTTP responses",
        HTTP_TTFB.clone()
    );
    registry.register(
        "slow_responses",
        "Number of non-long-poll responses that exceeded a latency or throughput threshold",
        SLOW_RESPONSES.clone()
    );

    registry.register("stream_bytes", "Number of bytes per stream", STREAM_BYTES.clone());
    registry.register("stream_blocks", "Number of blocks per stream", STREAM_BLOCKS.clone());
    registry.register("stream_chunks", "Number of chunks per stream", STREAM_CHUNKS.clone());
    registry.register(
        "stream_bytes_per_second",
        "Completed streams bandwidth",
        STREAM_BYTES_PER_SECOND.clone()
    );
    registry.register(
        "stream_blocks_per_second",
        "Completed streams speed in blocks",
        STREAM_BLOCKS_PER_SECOND.clone()
    );
    registry.register(
        "stream_duration_seconds",
        "Durations of completed streams",
        STREAM_DURATIONS.clone()
    );
    registry.register(
        "queried_blocks",
        "Number of blocks per running query",
        QUERIED_BLOCKS.clone()
    );
    registry.register(
        "queried_chunks",
        "Number of chunks per running query",
        QUERIED_CHUNKS.clone()
    );
    registry.register(
        "completed_queries",
        "Number of completed queries",
        COMPLETED_QUERIES.clone()
    );

    top_registry
}

impl Collector for QueryExecutorCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let active_queries = self.get_active_queries();

        encoder
            .encode_descriptor(
                "hotblocks_active_queries",
                "Number of currently active queries",
                None,
                MetricType::Gauge
            )?
            .encode_gauge(&active_queries)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{ParsedStatistic, parse_rocksdb_statistic};

    #[test]
    fn parses_ticker_statistic() {
        assert_eq!(
            parse_rocksdb_statistic("rocksdb.block.cache.hit COUNT : 12345"),
            Some(ParsedStatistic {
                name: "rocksdb.block.cache.hit",
                count: Some(12_345),
                sum: None
            })
        );
    }

    #[test]
    fn parses_histogram_statistic() {
        assert_eq!(
            parse_rocksdb_statistic("rocksdb.db.write.stall P50 : 1.0 P95 : 2.0 P99 : 3.0 COUNT : 7 SUM : 42.5"),
            Some(ParsedStatistic {
                name: "rocksdb.db.write.stall",
                count: Some(7),
                sum: Some(42.5)
            })
        );
    }
}
