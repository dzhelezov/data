//! Directive #67 — bounded, side-effect-safe optimistic transaction retries.
//!
//! Contention is injected deterministically WITHOUT any production test hook:
//! a sabotaging `update_dataset` runs INSIDE the victim callback's window
//! (after the victim's snapshot, before its commit) and commits a conflicting
//! label write. The victim's commit then fails optimistic validation and the
//! retry loop must recreate the transaction and replay the callback.

use std::{
    cell::Cell,
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant}
};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, UInt32Array, UInt64Array},
    datatypes::{DataType, Field, Schema}
};
use sqd_primitives::{BlockRef, TransactionRef};
use sqd_storage::db::{
    get_global_tx_restarts, Chunk, Database, DatabaseSettings, DatasetId, RetryPolicy, TxRetryExhausted
};
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
fn deadline_before_backoff_is_attributed_at_the_pre_backoff_exit() {
    let _serial = serial();
    // F67-4: when the callback+commit work ALONE overruns the wall clock, the
    // pre-backoff exit must attribute the DEADLINE, not generic attempt-budget
    // exhaustion — even though the attempt budget is nowhere near spent. The
    // sibling test above asserts only `contains("deadline")`, which passes on
    // EITHER deadline branch (pre- or post-backoff); it does not pin the
    // pre-backoff attribution that 1afabea added. Here a callback that sleeps
    // well past a tiny deadline forces `deadline_hit` true on the very first
    // commit conflict — before any backoff sleep — so the run can ONLY exit via
    // the pre-backoff branch, and its message must say so. A regression that
    // dropped the pre-backoff deadline check would mislabel this run "attempt
    // budget exhausted" and this test would fail.
    let policy = RetryPolicy::default()
        .with_max_attempts(8) // generous: attempts are NOT the limiter here
        .with_deadline(Duration::from_millis(5));
    let db = open_db(policy);
    let id = dataset(&db);

    let err = db
        .update_dataset(id, |tx| {
            sabotage(&db, id, 11); // force the commit conflict
                                   // Overrun the 5 ms deadline within the attempt itself,
                                   // deterministically (60 ms ≫ 5 ms holds under CPU load too).
            std::thread::sleep(Duration::from_millis(60));
            tx.set_finalized_head(BlockRef {
                number: 42,
                hash: "pre-backoff-deadline".to_string()
            });
            Ok(())
        })
        .unwrap_err();

    let exhausted = err
        .downcast_ref::<TxRetryExhausted>()
        .expect("deadline exhaustion must be typed");
    assert_eq!(
        exhausted.attempts, 1,
        "the deadline ended the run on the first attempt, before any retry"
    );
    assert!(
        exhausted.last.contains("before backoff"),
        "the pre-backoff deadline branch must be attributed, got: {}",
        exhausted.last
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

// --- F67-5(c): indexed-write atomicity across the retry boundary -----------
//
// Every other sabotage test mutates only the label with both hash indexes
// disabled, so a regression that dropped the index-write flags when the retry
// loop recreates the transaction (`Tx::run*` re-derives them from the caller)
// would pass unnoticed. These two tests open a database with BOTH hash indexes
// on and insert, inside the contended callback, a chunk carrying `blocks` and
// `transactions` tables. The label, the chunk metadata, the block-hash index
// and the transaction-hash index must either all commit together after the
// replays, or all be absent when the budget is exhausted — never a partial mix.

fn open_indexed_db(policy: RetryPolicy) -> Database {
    let dir = tempdir().unwrap();
    DatabaseSettings::default()
        .with_block_hash_index(true)
        .with_transaction_hash_index(true)
        .with_tx_retry_policy(policy)
        .open(dir.path())
        .unwrap()
}

fn indexed_block_hash(n: u64) -> String {
    format!("0x{n:064x}")
}

fn indexed_tx_hash(block_number: u64, transaction_index: u32) -> String {
    format!("0x7c{block_number:048x}{transaction_index:012x}")
}

/// An EVM chunk carrying both a `blocks` table (feeds `CF_BLOCK_HASHES`) and a
/// `transactions` table (feeds `CF_TRANSACTION_HASHES`), plus the exact entries
/// each index is expected to produce.
struct DualIndexChunk {
    chunk: Chunk,
    block_hashes: Vec<(u64, String)>,
    tx_rows: Vec<(u64, u32, String)>
}

fn make_dual_index_chunk(db: &Database, first: u64, last: u64, per_block: u32) -> DualIndexChunk {
    // blocks table: (number, hash)
    let block_schema = Arc::new(Schema::new(vec![
        Field::new("number", DataType::UInt64, false),
        Field::new("hash", DataType::Utf8, false),
    ]));
    let block_hashes: Vec<(u64, String)> = (first..=last).map(|n| (n, indexed_block_hash(n))).collect();
    let block_numbers = Arc::new(UInt64Array::from(
        block_hashes.iter().map(|(n, _)| *n).collect::<Vec<_>>()
    )) as ArrayRef;
    let block_hash_arr = Arc::new(StringArray::from(
        block_hashes.iter().map(|(_, h)| h.as_str()).collect::<Vec<_>>()
    )) as ArrayRef;
    let mut blocks_builder = db.new_table_builder(block_schema.clone());
    blocks_builder
        .write_record_batch(&RecordBatch::try_new(block_schema, vec![block_numbers, block_hash_arr]).unwrap())
        .unwrap();

    // transactions table: (block_number, transaction_index, hash)
    let tx_schema = Arc::new(Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("hash", DataType::Utf8, false),
    ]));
    let mut tx_rows: Vec<(u64, u32, String)> = Vec::new();
    for n in first..=last {
        for ti in 0..per_block {
            tx_rows.push((n, ti, indexed_tx_hash(n, ti)));
        }
    }
    let bn_arr = Arc::new(UInt64Array::from(
        tx_rows.iter().map(|(b, _, _)| *b).collect::<Vec<_>>()
    )) as ArrayRef;
    let ti_arr = Arc::new(UInt32Array::from(
        tx_rows.iter().map(|(_, t, _)| *t).collect::<Vec<_>>()
    )) as ArrayRef;
    let th_arr = Arc::new(StringArray::from(
        tx_rows.iter().map(|(_, _, h)| h.as_str()).collect::<Vec<_>>()
    )) as ArrayRef;
    let mut tx_builder = db.new_table_builder(tx_schema.clone());
    tx_builder
        .write_record_batch(&RecordBatch::try_new(tx_schema, vec![bn_arr, ti_arr, th_arr]).unwrap())
        .unwrap();

    let mut tables = BTreeMap::new();
    tables.insert("blocks".to_owned(), blocks_builder.finish().unwrap());
    tables.insert("transactions".to_owned(), tx_builder.finish().unwrap());

    DualIndexChunk {
        chunk: Chunk::V1 {
            first_block: first,
            last_block: last,
            last_block_hash: indexed_block_hash(last),
            parent_block_hash: "base".to_owned(),
            first_block_time: None,
            last_block_time: None,
            tables
        },
        block_hashes,
        tx_rows
    }
}

fn last_chunk_present(db: &Database, id: DatasetId) -> bool {
    db.snapshot().get_last_chunk(id).unwrap().is_some()
}

#[test]
fn indexed_write_commits_label_chunk_and_both_indexes_after_replays() {
    let _serial = serial();
    const CONFLICTS: u32 = 3;
    let db = open_indexed_db(RetryPolicy::default().with_base_backoff(Duration::from_millis(1)));
    let id = dataset(&db);
    let dual = make_dual_index_chunk(&db, 0, 4, 2);

    let seen_attempts = Cell::new(0u32);
    let restarts_before = get_global_tx_restarts();

    db.update_dataset(id, |tx| {
        let attempt = seen_attempts.get();
        seen_attempts.set(attempt + 1);
        if attempt < CONFLICTS {
            // Conflict on the label read, forcing the retry loop to recreate the
            // transaction and replay this closure — index flags and all.
            sabotage(&db, id, 700 + attempt as u64);
        }
        tx.insert_chunk(&dual.chunk)?;
        tx.set_finalized_head(BlockRef {
            number: 4,
            hash: indexed_block_hash(4)
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

    // All four artifacts of the single committed transaction are visible.
    assert_eq!(finalized_number(&db, id), Some(4), "label finalized head committed");
    let snapshot = db.snapshot();
    assert!(
        snapshot.get_last_chunk(id).unwrap().is_some(),
        "chunk metadata committed with the label"
    );
    for (n, hash) in &dual.block_hashes {
        assert_eq!(
            snapshot.find_block_by_hash(id, hash).unwrap(),
            Some(BlockRef {
                number: *n,
                hash: hash.clone()
            }),
            "block {n} must resolve — its hash index committed atomically with the chunk"
        );
    }
    for (bn, ti, hash) in &dual.tx_rows {
        assert_eq!(
            snapshot.find_transaction_by_hash(id, hash).unwrap(),
            Some(TransactionRef {
                block_number: *bn,
                transaction_index: *ti,
                hash: hash.clone()
            }),
            "tx {hash} must resolve — its hash index committed atomically with the chunk"
        );
    }
}

#[test]
fn indexed_write_leaves_no_partial_state_on_exhaustion() {
    let _serial = serial();
    let policy = RetryPolicy::default()
        .with_max_attempts(3)
        .with_base_backoff(Duration::from_millis(1))
        .with_max_backoff(Duration::from_millis(2));
    let db = open_indexed_db(policy);
    let id = dataset(&db);
    let dual = make_dual_index_chunk(&db, 0, 4, 2);

    let err = db
        .update_dataset(id, |tx| {
            sabotage(&db, id, 7); // every attempt conflicts
            tx.insert_chunk(&dual.chunk)?;
            tx.set_finalized_head(BlockRef {
                number: 4,
                hash: indexed_block_hash(4)
            });
            Ok(())
        })
        .unwrap_err();

    err.downcast_ref::<TxRetryExhausted>()
        .expect("exhaustion must be a typed, downcastable error");

    // Nothing the contended closure staged survived: not the chunk, and not a
    // single entry in either hash index. Atomicity holds on the failure path too.
    assert!(
        !last_chunk_present(&db, id),
        "no chunk may commit when the budget is exhausted"
    );
    let snapshot = db.snapshot();
    for (_n, hash) in &dual.block_hashes {
        assert_eq!(
            snapshot.find_block_by_hash(id, hash).unwrap(),
            None,
            "block-hash index entry {hash} leaked despite exhaustion"
        );
    }
    for (_bn, _ti, hash) in &dual.tx_rows {
        assert_eq!(
            snapshot.find_transaction_by_hash(id, hash).unwrap(),
            None,
            "transaction-hash index entry {hash} leaked despite exhaustion"
        );
    }
    // The victim's label write never became visible; only the sabotage's did.
    assert_ne!(
        finalized_number(&db, id),
        Some(4),
        "the contested label never committed"
    );
}
