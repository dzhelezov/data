//! Directive #67 — bounded, side-effect-safe optimistic transaction retries.
//!
//! Contention is injected deterministically WITHOUT any production test hook:
//! a sabotaging `update_dataset` runs INSIDE the victim callback's window
//! (after the victim's snapshot, before its commit) and commits a conflicting
//! label write. The victim's commit then fails optimistic validation and the
//! retry loop must recreate the transaction and replay the callback.

use std::{
    cell::Cell,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant}
};

use sqd_primitives::BlockRef;
use sqd_storage::db::{get_global_tx_restarts, Database, DatabaseSettings, RetryPolicy, TxRetryExhausted};
use tempfile::tempdir;

/// The retry counters are deliberately process-global (bounded-cardinality
/// metrics), so tests that measure counter deltas must not overlap.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    match SERIAL.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner()
    }
}

fn open_db(policy: RetryPolicy) -> sqd_storage::db::Database {
    // Same convention as tests/utils.rs `setup_db`: the TempDir handle drops
    // here; the open RocksDB keeps working on the unlinked path.
    let dir = tempdir().unwrap();
    DatabaseSettings::default()
        .with_tx_retry_policy(policy)
        .open(dir.path())
        .unwrap()
}

fn dataset(db: &Database) -> sqd_storage::db::DatasetId {
    let id = sqd_storage::db::DatasetId::from_str("evm");
    db.create_dataset_if_not_exists(id, sqd_storage::db::DatasetKind::from_str("evm"))
        .unwrap();
    id
}

/// ORDERING IS LOAD-BEARING: the sabotage must run BEFORE the victim stages
/// its own writes in the callback. Optimistic transactions track reads for
/// conflict detection; the victim's label read in `DatasetUpdate::new` is what
/// makes this competing commit conflict at the victim's commit time.
fn sabotage(db: &Database, id: sqd_storage::db::DatasetId, marker: u64) {
    db.update_dataset(id, |tx| {
        tx.set_finalized_head(BlockRef {
            number: marker,
            hash: format!("sabotage-{}", marker)
        });
        Ok(())
    })
    .unwrap();
}

fn finalized_number(db: &Database, id: sqd_storage::db::DatasetId) -> Option<u64> {
    db.update_dataset(id, |tx| Ok(tx.label().finalized_head().map(|h| h.number)))
        .unwrap()
}

#[test]
fn no_contention_commits_on_the_first_attempt() {
    let _serial = serial();
    let db = open_db(RetryPolicy::default());
    let id = dataset(&db);

    let restarts_before = get_global_tx_restarts();
    db.update_dataset(id, |tx| {
        tx.set_finalized_head(BlockRef {
            number: 7,
            hash: "clean".to_string()
        });
        Ok(())
    })
    .unwrap();

    assert_eq!(finalized_number(&db, id), Some(7));
    assert_eq!(
        get_global_tx_restarts(),
        restarts_before,
        "no retries without contention"
    );
}

#[test]
fn n_conflicts_commit_exactly_once_after_n_replays() {
    let _serial = serial();
    const CONFLICTS: u32 = 3;
    let db = open_db(RetryPolicy::default().with_base_backoff(Duration::from_millis(1)));
    let id = dataset(&db);

    let seen_attempts = Cell::new(0u32);
    let restarts_before = get_global_tx_restarts();
    let attempts_before = sqd_storage::db::get_global_tx_retry_attempts();

    db.update_dataset(id, |tx| {
        let attempt = seen_attempts.get();
        seen_attempts.set(attempt + 1);
        if attempt < CONFLICTS {
            // Commits inside the victim's attempt window: the victim's
            // already-tracked label read conflicts with this write at commit.
            sabotage(&db, id, 100 + attempt as u64);
        }
        tx.set_finalized_head(BlockRef {
            number: 42,
            hash: "winner".to_string()
        });
        Ok(())
    })
    .unwrap();

    assert_eq!(
        seen_attempts.get(),
        CONFLICTS + 1,
        "callback replayed once per conflict"
    );
    assert_eq!(
        get_global_tx_restarts() - restarts_before,
        CONFLICTS as u64,
        "each conflict recorded exactly one restart"
    );
    assert_eq!(
        sqd_storage::db::get_global_tx_retry_attempts() - attempts_before,
        CONFLICTS as u64,
        "retry-attempt accounting matches restarts"
    );
    assert_eq!(finalized_number(&db, id), Some(42), "exactly one commit became visible");
}

#[test]
fn persistent_contention_fails_with_typed_exhaustion() {
    let _serial = serial();
    let policy = RetryPolicy::default()
        .with_max_attempts(3)
        .with_base_backoff(Duration::from_millis(1))
        .with_max_backoff(Duration::from_millis(2));
    let db = open_db(policy);
    let id = dataset(&db);

    let exhausted_before = sqd_storage::db::get_global_tx_exhausted();

    let err = db
        .update_dataset(id, |tx| {
            sabotage(&db, id, 7); // every attempt conflicts
            tx.set_finalized_head(BlockRef {
                number: 42,
                hash: "never-commits".to_string()
            });
            Ok(())
        })
        .unwrap_err();

    let exhausted = err
        .downcast_ref::<TxRetryExhausted>()
        .expect("exhaustion must be a typed, downcastable error");
    assert_eq!(exhausted.attempts, 3, "budget of attempts respected");
    assert!(!exhausted.last.is_empty());
    assert_eq!(sqd_storage::db::get_global_tx_exhausted(), exhausted_before + 1);
    assert_ne!(
        finalized_number(&db, id),
        Some(42),
        "the contested mutation never committed"
    );
}

#[test]
fn deadline_exhausts_even_with_a_generous_attempt_budget() {
    let _serial = serial();
    // Generous attempt count, tight wall clock: the deadline must terminate
    // the run, and the typed error must say so.
    let policy = RetryPolicy::default()
        .with_max_attempts(1000)
        .with_deadline(Duration::from_millis(100))
        .with_base_backoff(Duration::from_millis(30))
        .with_max_backoff(Duration::from_millis(40));
    let db = open_db(policy);
    let id = dataset(&db);

    let err = db
        .update_dataset(id, |tx| {
            sabotage(&db, id, 9); // every attempt conflicts
            tx.set_finalized_head(BlockRef {
                number: 42,
                hash: "deadline-bound".to_string()
            });
            Ok(())
        })
        .unwrap_err();

    let exhausted = err
        .downcast_ref::<TxRetryExhausted>()
        .expect("deadline exhaustion must be typed");
    assert!(
        exhausted.last.contains("deadline"),
        "the deadline branch must be reported, got: {}",
        exhausted.last
    );
    assert!(
        exhausted.elapsed < Duration::from_secs(2),
        "deadline must actually bound wall time"
    );
    assert_ne!(finalized_number(&db, id), Some(42));
}

#[test]
fn backoff_is_bounded_by_the_policy() {
    let _serial = serial();
    const CONFLICTS: u32 = 3;
    let policy = RetryPolicy::default()
        .with_base_backoff(Duration::from_millis(20))
        .with_max_backoff(Duration::from_millis(30));
    let db = open_db(policy);
    let id = dataset(&db);

    let backoff_before = sqd_storage::db::get_global_tx_backoff_ms();
    let started = Instant::now();

    let attempts = Cell::new(0u32);
    db.update_dataset(id, |_tx| {
        let attempt = attempts.get();
        attempts.set(attempt + 1);
        if attempt < CONFLICTS {
            sabotage(&db, id, 500 + attempt as u64);
        }
        Ok(())
    })
    .unwrap();

    let elapsed = started.elapsed();
    let slept = sqd_storage::db::get_global_tx_backoff_ms() - backoff_before;
    // Full jitter in [0, 30ms) per retry: never more than cap * retries.
    assert!(
        slept <= (CONFLICTS as u64) * 30,
        "recorded backoff {} ms exceeds cap",
        slept
    );
    assert!(elapsed < Duration::from_secs(2), "no unbounded stall on the happy path");
}

#[test]
fn racing_writers_stay_consistent_under_contention() {
    let _serial = serial();
    // Deliberate violation of the single-writer assumption, absorbed by the
    // budget: both writers must either commit or fail typed — never corrupt.
    let policy = RetryPolicy::default()
        .with_max_attempts(64)
        .with_deadline(Duration::from_secs(60))
        .with_base_backoff(Duration::from_millis(1))
        .with_max_backoff(Duration::from_millis(5));
    let db = Arc::new(open_db(policy));
    let id = dataset(&db);

    // Release both writers at the same instant so their transactions genuinely overlap;
    // without the barrier one thread can finish before the other starts and the test
    // passes on data that was never actually contended.
    let start = Arc::new(std::sync::Barrier::new(2));
    let restarts_before = get_global_tx_restarts();
    let mut handles = Vec::new();
    for writer in 0u64..2 {
        let db = Arc::clone(&db);
        let start = Arc::clone(&start);
        handles.push(std::thread::spawn(move || {
            start.wait();
            for i in 0..20u64 {
                let number = 1000 * (writer + 1) + i;
                db.update_dataset(id, |tx| {
                    tx.set_finalized_head(BlockRef {
                        number,
                        hash: format!("w{}-{}", writer, i)
                    });
                    Ok(())
                })?;
            }
            anyhow::Ok(())
        }));
    }
    for h in handles {
        h.join().unwrap().unwrap();
    }

    // The two writers really did collide: optimistic conflicts forced replays. Without
    // this the "consistency under contention" claim could hold vacuously on an
    // uncontended run.
    assert!(
        get_global_tx_restarts() > restarts_before,
        "racing writers must produce at least one commit conflict / restart"
    );

    // The final label must be one whole write from one writer, not a mix.
    let final_head = db
        .update_dataset(id, |tx| Ok(tx.label().finalized_head().cloned()))
        .unwrap()
        .expect("a committed head must exist");
    let writer = final_head.number / 1000 - 1;
    let i = final_head.number % 1000;
    assert!(
        writer < 2 && i < 20,
        "final head {:?} is not one of the raced writes",
        final_head
    );
    assert_eq!(final_head.hash, format!("w{}-{}", writer, i));
}
