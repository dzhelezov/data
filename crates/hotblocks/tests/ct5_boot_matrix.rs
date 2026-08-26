//! CT-5 boot matrix — one configured dataset's init failure must not down the others.
//!
//! Audit finding N2 (P1): at boot, a single `DatasetController::new` error used to propagate via
//! `?` and abort the whole `DataService::start`, so one bad dataset took every healthy dataset down
//! with it. The fix supervises init per dataset (`partition_boot_outcomes` isolates the failure;
//! `enforce_all_fail_floor` refuses to start only when *every* configured dataset failed) and
//! reports the outcome as `hotblocks_dataset_boot_ready{dataset=…}` — 1 served, 0 skipped-and-alarmed.
//!
//! A **kind mismatch on reboot** is the deterministic single-dataset boot failure among the three
//! equivalent N2 triggers (corrupt label / retention-gap bail / kind mismatch): the first boot
//! writes each dataset's storage label under kind `evm`; re-declaring one dataset under a different
//! kind makes `create_dataset_if_not_exists` reject the reopen (an existing dataset cannot change
//! kind), so exactly that controller's init fails while the others initialize and serve.

use anyhow::{Result, ensure};
use sqd_hotblocks_harness::{
    chain::{Chain, Evm, Solana},
    driver::Client,
    sut::{DatasetSpec, Retention, Sut, SutConfig}
};

const HEALTHY: &str = "healthy";
const BROKEN: &str = "broken";

/// An EVM dataset whose only source is a dead port: the controller initializes (writing the storage
/// label) while ingestion fails in the background — CT-5 cares about boot supervision, not ingest.
fn evm_dataset(id: &str, dead_port: u16) -> DatasetSpec {
    DatasetSpec {
        id: id.to_string(),
        kind: Evm.config_kind().to_string(),
        retention: Retention::Head(100),
        disable_compaction: false,
        sources: vec![format!("http://127.0.0.1:{dead_port}/{id}")]
    }
}

/// The per-dataset boot outcome gauge — the signal an alert reads. `None` means no series at all.
async fn boot_ready(client: &Client, dataset: &str) -> Result<Option<f64>> {
    Ok(client
        .metrics()
        .await?
        .get("hotblocks_dataset_boot_ready", Some(("dataset", dataset))))
}

#[tokio::test(flavor = "multi_thread")]
async fn ct5_one_datasets_boot_failure_is_isolated_and_alarmed() -> Result<()> {
    // Claim a port and release it: source probes are refused rather than left hanging, so both
    // datasets boot without waiting on ingestion.
    let dead = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?.port();

    let mut sut = Sut::start(SutConfig::new(
        env!("CARGO_BIN_EXE_sqd-hotblocks"),
        vec![evm_dataset(HEALTHY, dead), evm_dataset(BROKEN, dead)]
    ))
    .await?;

    // Baseline: the clean boot initialized both configured datasets (both labels written as `evm`).
    let m = Client::new(sut.base_url(), HEALTHY)?;
    ensure!(
        boot_ready(&m, HEALTHY).await? == Some(1.0),
        "healthy dataset must report boot-ready 1 on the clean boot"
    );
    ensure!(
        boot_ready(&m, BROKEN).await? == Some(1.0),
        "the to-be-broken dataset must report boot-ready 1 on the clean boot"
    );

    // Stop, then re-declare BROKEN under a mismatched kind. The database still holds its `evm`
    // label, so the storage-layer identity guard rejects the reopen and its controller init fails.
    sut.stop().await?;
    let mut broken = evm_dataset(BROKEN, dead);
    broken.kind = Solana.config_kind().to_string();
    sut.set_datasets(vec![evm_dataset(HEALTHY, dead), broken])?;

    // The service still comes up: a per-dataset init failure is isolated, not an all-fail floor.
    // (`restart` awaits readiness; a fail-all boot would bail before serving and this would error.)
    sut.restart().await?;

    let m = Client::new(sut.base_url(), HEALTHY)?;
    ensure!(
        boot_ready(&m, BROKEN).await? == Some(0.0),
        "the broken dataset must report boot-ready 0 — the alarm signal, not a silent whole-service exit"
    );
    ensure!(
        boot_ready(&m, HEALTHY).await? == Some(1.0),
        "the healthy dataset must keep serving (boot-ready 1) despite its neighbour's failure"
    );

    // The isolation is real end to end, not just a gauge: the healthy dataset answers (an empty
    // dataset still serves `/head` as 200/null), while the broken one is omitted (404 → `Err`).
    ensure!(
        Client::new(sut.base_url(), HEALTHY)?.head().await.is_ok(),
        "the healthy dataset must serve requests after the isolated failure"
    );
    ensure!(
        Client::new(sut.base_url(), BROKEN)?.head().await.is_err(),
        "the broken dataset must be omitted from the service, not served"
    );

    Ok(())
}
