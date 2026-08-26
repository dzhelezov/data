use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Instant
};

use anyhow::{Context, anyhow};
use futures::{FutureExt, StreamExt};
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_storage::db::{CF_TABLES, DatasetId};
use tracing::{error, info, warn};

use crate::{
    dataset_config::{DatasetConfig, RetentionConfig},
    dataset_controller::DatasetController,
    errors::UnknownDataset,
    types::{DBRef, RetentionStrategy}
};

pub type DataServiceRef = Arc<DataService>;

pub struct DataService {
    datasets: HashMap<DatasetId, Arc<DatasetController>>
}

impl DataService {
    pub async fn start(
        db: DBRef,
        datasets: BTreeMap<DatasetId, DatasetConfig>,
        disk_reclaim: bool,
        spill_bound_bytes: usize
    ) -> anyhow::Result<Self> {
        let unconfigured: Vec<DatasetId> = db
            .get_all_datasets()?
            .into_iter()
            .filter(|ds| !datasets.contains_key(&ds.id))
            .map(|ds| ds.id)
            .collect();

        // Must run before any controller spawns -- see `startup_disk_recovery`.
        {
            let db = db.clone();
            let recovery = tokio::task::spawn_blocking(move || startup_disk_recovery(&db, &unconfigured, disk_reclaim));
            if let Err(err) = recovery.await {
                error!(error =? err, "startup disk recovery panicked");
            }
        }

        let configured_datasets = datasets.len();
        let controller_init_started = Instant::now();
        info!(configured_datasets, "dataset controller initialization started");

        let mut controllers = futures::stream::iter(datasets.into_iter())
            .map(|(dataset_id, cfg)| {
                let db = db.clone();

                let http_client = sqd_data_client::reqwest::default_http_client();

                let data_sources = cfg
                    .data_sources
                    .into_iter()
                    .map(|url| ReqwestDataClient::new(http_client.clone(), url))
                    .collect();

                let (retention, max_blocks) = match &cfg.retention_strategy {
                    RetentionConfig::FromBlock { number, parent_hash } => (
                        RetentionStrategy::FromBlock {
                            number: *number,
                            parent_hash: parent_hash.clone()
                        },
                        None
                    ),
                    RetentionConfig::Head(n) => (RetentionStrategy::Head(*n), None),
                    RetentionConfig::Api { max_blocks } => (RetentionStrategy::None, *max_blocks),
                    RetentionConfig::None => (RetentionStrategy::None, None)
                };

                tokio::task::spawn_blocking(move || {
                    DatasetController::new(
                        db,
                        dataset_id,
                        cfg.kind,
                        retention,
                        max_blocks,
                        data_sources,
                        spill_bound_bytes
                    )
                    .map(|c| {
                        c.enable_compaction(!cfg.disable_compaction);
                        Arc::new(c)
                    })
                })
                // Flatten the blocking task's JoinError (a panic or cancellation of the init) into
                // this dataset's own result, tagged with its id, so one dataset's failure is
                // attributed and isolated below -- never a fail-all via `?` (audit N2 / FM-a).
                .map(move |res| {
                    let ctl = match res {
                        Ok(inner) => inner,
                        Err(join_err) => Err(anyhow::Error::new(join_err))
                    }
                    .with_context(|| anyhow!("failed to initialize dataset {}", dataset_id));
                    (dataset_id, ctl)
                })
            })
            .buffered(5);

        // Drain every controller outcome before deciding anything: a single dataset's init failure
        // must not take down the others (INV-36 / CN-10). The split is deliberate -- `partition`
        // attributes the outcomes, then the readiness gauges and the completion log are emitted, and
        // only then is the all-fail floor enforced, so a total outage is fully observed (every
        // failure gauge + the summary line) before the process refuses to start.
        let mut outcomes = Vec::with_capacity(configured_datasets);
        while let Some(outcome) = controllers.next().await {
            outcomes.push(outcome);
        }

        let (ready, failed) = Self::partition_boot_outcomes(outcomes);

        let mut datasets = HashMap::with_capacity(ready.len());
        for (dataset_id, ctl) in ready {
            crate::metrics::report_dataset_boot_ready(dataset_id, true);
            datasets.insert(ctl.dataset_id(), ctl);
        }
        for &dataset_id in &failed {
            crate::metrics::report_dataset_boot_ready(dataset_id, false);
        }

        info!(
            configured_datasets,
            datasets_ready = datasets.len(),
            datasets_failed = failed.len(),
            elapsed_ms = controller_init_started.elapsed().as_millis() as u64,
            "dataset controller initialization complete"
        );

        Self::enforce_all_fail_floor(configured_datasets, datasets.len())?;

        Ok(Self { datasets })
    }

    /// Partition per-dataset boot outcomes into the controllers that initialized and the ids that
    /// failed. Pure over the controller type `T` and free of DB/metric side effects, so the
    /// isolation policy -- one dataset's failure never drops another -- is unit-testable without
    /// storage. Each failure is logged with its dataset id; the caller emits the readiness gauge
    /// from the returned lists and then applies `enforce_all_fail_floor`.
    fn partition_boot_outcomes<T>(
        outcomes: Vec<(DatasetId, anyhow::Result<T>)>
    ) -> (Vec<(DatasetId, T)>, Vec<DatasetId>) {
        let mut ready = Vec::new();
        let mut failed = Vec::new();

        for (dataset_id, result) in outcomes {
            match result {
                Ok(ctl) => ready.push((dataset_id, ctl)),
                Err(err) => {
                    error!(
                        dataset = %dataset_id,
                        error =? err,
                        "dataset controller initialization failed; other datasets keep serving"
                    );
                    failed.push(dataset_id);
                }
            }
        }

        (ready, failed)
    }

    /// The narrow all-fail floor: refuse to start only when every configured dataset failed, so a
    /// total init outage cannot masquerade as a healthy empty service (INV-43 permits a dataset's
    /// own startup failure, not the service's). `start` applies this *after* the readiness gauges
    /// and the completion log are emitted, so an all-fail boot is fully observed before it bails. An
    /// empty configuration still boots, as it does today -- an orthogonal config-policy question,
    /// deliberately out of scope here.
    fn enforce_all_fail_floor(configured_datasets: usize, ready_datasets: usize) -> anyhow::Result<()> {
        if configured_datasets > 0 && ready_datasets == 0 {
            anyhow::bail!(
                "all {} configured dataset(s) failed to initialize; refusing to start",
                configured_datasets
            );
        }
        Ok(())
    }

    pub fn get_dataset(&self, dataset_id: DatasetId) -> Result<Arc<DatasetController>, UnknownDataset> {
        self.datasets
            .get(&dataset_id)
            .map(Arc::clone)
            .ok_or(UnknownDataset { dataset_id })
    }
}

/// Startup-only disk recovery; must run before any ingest or query exists (the file
/// unlink ignores snapshots, and the orphan purge treats every dirty marker as an orphan
/// from a dead build).
///
/// `disk_reclaim` gates both reclaim steps (`--startup-disk-reclaim`, off by default);
/// deleting unconfigured datasets always runs. Ordering matters on a near-full disk:
/// 1. an unlink pass first -- it needs no scratch space, so it frees below-watermark space
///    where every write below would fail with ENOSPC;
/// 2. bookkeeping writes that lift the reclaim watermark: purge orphan dirty markers,
///    delete unconfigured datasets;
/// 3. a second unlink pass to free whatever step 2 unpinned.
///
/// Every step is best-effort: a failure leaves the watermark pinned until a later startup
/// succeeds, but never blocks startup.
///
/// Does not rescue a volume at literally zero free bytes: the database has already opened
/// by the time we get here, and opening replays the WAL and flushes it to L0.
fn startup_disk_recovery(db: &DBRef, unconfigured: &[DatasetId], disk_reclaim: bool) {
    let started = Instant::now();
    let bytes_before = table_sst_bytes(db);

    info!(
        reclaim_enabled = disk_reclaim,
        unconfigured_datasets = unconfigured.len(),
        table_sst_bytes_before = bytes_before,
        "startup disk recovery started"
    );

    let mut orphans_purged = 0usize;
    let mut unconfigured_deleted = 0usize;

    if disk_reclaim {
        if let Err(err) = db.reclaim_disk_space() {
            error!(error =? err, "startup disk reclaim (first pass) failed");
        }

        match db.purge_orphan_dirty_tables() {
            Ok(n) => orphans_purged = n,
            Err(err) => warn!(error =? err, "failed to purge orphan dirty tables")
        }
    }

    for dataset_id in unconfigured {
        match db.delete_dataset(*dataset_id) {
            Ok(()) => unconfigured_deleted += 1,
            Err(err) => {
                error!(
                    dataset_id = %dataset_id,
                    error =? err,
                    "failed to delete dataset; its chunks keep pinning the reclaim watermark"
                )
            }
        }
    }

    if disk_reclaim {
        if let Err(err) = db.reclaim_disk_space() {
            error!(error =? err, "startup disk reclaim (second pass) failed");
        }
    }

    let bytes_after = table_sst_bytes(db);
    // Only meaningful when both probes answered; a missing probe must not read as "freed 0".
    let freed_bytes = bytes_before
        .zip(bytes_after)
        .map(|(before, after)| before.saturating_sub(after));

    info!(
        reclaim_enabled = disk_reclaim,
        orphans_purged,
        unconfigured_datasets = unconfigured.len(),
        unconfigured_deleted,
        table_sst_bytes_before = bytes_before,
        table_sst_bytes_after = bytes_after,
        freed_bytes,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "startup disk recovery complete"
    );

    if !disk_reclaim {
        // FUTURE: ungate the orphan purge once the rollout measurement is done. It is safe
        // without the unlink, and while gated an interrupted build leaks its data for good;
        // it shares the flag only so `reclaim-measure` can still contrast the two watermarks.
        info!("startup disk reclaim is off; enable with --startup-disk-reclaim");
    }
}

/// Live SST bytes of `CF_TABLES`, straight from RocksDB metadata -- no SST reads. `None`
/// when the property is unavailable, so a probe failure stays distinguishable from zero.
fn table_sst_bytes(db: &DBRef) -> Option<u64> {
    match db.get_property(CF_TABLES, "rocksdb.live-sst-files-size") {
        Ok(value) => value.and_then(|v| v.parse().ok()),
        Err(err) => {
            warn!(error =? err, "failed to read live SST size");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use sqd_storage::db::DatasetId;

    use super::DataService;

    // A controller stand-in: `partition_boot_outcomes` is pure over the controller type, so the
    // boot-supervision policy is exercised without a DB, network, or spawned tasks. Each test walks
    // `start`'s exact sequence -- partition, then the floor over the ready count -- so the two
    // together prove the same decision `start` makes.
    fn ds(name: &str) -> DatasetId {
        DatasetId::from_str(name)
    }

    #[test]
    fn partition_isolates_one_dataset_failure_from_the_healthy_ones() {
        // Middle dataset fails; the two around it must still boot (INV-36 / CN-10).
        let outcomes = vec![
            (ds("alpha"), Ok(1u32)),
            (ds("bravo"), Err(anyhow!("controller init blew up"))),
            (ds("charlie"), Ok(3u32)),
        ];

        let (ready, failed) = DataService::partition_boot_outcomes(outcomes);

        assert_eq!(ready, vec![(ds("alpha"), 1u32), (ds("charlie"), 3u32)]);
        assert_eq!(failed, vec![ds("bravo")]);
        // Two survivors clear the floor.
        DataService::enforce_all_fail_floor(3, ready.len()).expect("a partial failure must still boot");
    }

    #[test]
    fn all_fail_trips_the_floor() {
        // Every configured dataset failed: the service must bail rather than come up empty and
        // masquerade as healthy.
        let outcomes: Vec<(DatasetId, anyhow::Result<u32>)> =
            vec![(ds("alpha"), Err(anyhow!("boom"))), (ds("bravo"), Err(anyhow!("boom")))];

        let (ready, failed) = DataService::partition_boot_outcomes(outcomes);
        assert!(ready.is_empty());
        assert_eq!(failed, vec![ds("alpha"), ds("bravo")]);

        let err =
            DataService::enforce_all_fail_floor(2, ready.len()).expect_err("an all-fail boot must refuse to start");
        assert!(err.to_string().contains("refusing to start"), "{err}");
    }

    #[test]
    fn one_survivor_clears_the_floor() {
        // A single healthy dataset among failures is enough to boot -- the floor is *all*-fail.
        let outcomes = vec![(ds("alpha"), Err(anyhow!("boom"))), (ds("bravo"), Ok(7u32))];

        let (ready, failed) = DataService::partition_boot_outcomes(outcomes);
        assert_eq!(ready, vec![(ds("bravo"), 7u32)]);
        assert_eq!(failed, vec![ds("alpha")]);
        DataService::enforce_all_fail_floor(2, ready.len()).expect("one survivor must clear the floor");
    }

    #[test]
    fn empty_configuration_still_boots() {
        // No configured datasets: the floor does not fire (it is scoped to `configured > 0`), so an
        // empty service comes up exactly as it does today.
        let (ready, failed) = DataService::partition_boot_outcomes::<u32>(vec![]);
        assert!(ready.is_empty());
        assert!(failed.is_empty());
        DataService::enforce_all_fail_floor(0, ready.len()).expect("empty config must boot");
    }
}
