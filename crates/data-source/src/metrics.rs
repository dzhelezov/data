use std::sync::LazyLock;

use prometheus_client::metrics::{counter::Counter, family::Family};

type Labels = Vec<(&'static str, String)>;

/// Upstream data-source ingestion errors by `source` host and `kind`.
/// Registered in the hotblocks metrics registry.
pub static INGEST_SOURCE_ERRORS: LazyLock<Family<Labels, Counter>> = LazyLock::new(Default::default);

pub(crate) fn record_ingest_source_error(source: &str, kind: &'static str) {
    INGEST_SOURCE_ERRORS
        .get_or_create(&vec![("source", source.to_string()), ("kind", kind.to_string())])
        .inc();
}
