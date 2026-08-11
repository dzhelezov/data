use std::{
    cell::RefCell,
    cmp::{max, min},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant}
};

use anyhow::{anyhow, bail, ensure, Context};
use rocksdb::ColumnFamily;
use sqd_primitives::{BlockNumber, ItemIndex};

use crate::db::{
    data::{ChunkId, HashIndexKey},
    db::{
        RocksDB, RocksIterator, RocksTransaction, RocksTransactionOptions, CF_BLOCK_HASHES, CF_CHUNKS, CF_DATASETS,
        CF_DELETED_TABLES, CF_DIRTY_TABLES, CF_TRANSACTION_HASHES
    },
    read::{
        blocks_table::{find_block_hash, for_each_block_hash, get_parent_block_hash},
        chunk::ChunkIterator,
        transactions_table::for_each_transaction_hash
    },
    table_id::TableId,
    Chunk, DatasetId, DatasetKind, DatasetLabel, ReadSnapshot
};

static GLOBAL_RESTARTS: AtomicU64 = AtomicU64::new(0);
/// Retry attempts beyond the first across all transactions (restart bookkeeping).
static GLOBAL_RETRY_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
/// Operations that used up their retry budget without committing.
static GLOBAL_EXHAUSTED: AtomicU64 = AtomicU64::new(0);
/// Aggregate time spent in retry backoff, in milliseconds.
static GLOBAL_BACKOFF_MS: AtomicU64 = AtomicU64::new(0);
/// Cheap xorshift64* state for backoff jitter; zero seeds itself on first use.
static JITTER_STATE: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static LOCAL_RESTARTS: RefCell<u64> = RefCell::new(0);
}

pub fn get_global_tx_restarts() -> u64 {
    GLOBAL_RESTARTS.load(Ordering::Relaxed)
}

pub fn get_local_tx_restarts() -> u64 {
    LOCAL_RESTARTS.with_borrow(|val| *val)
}

pub fn get_global_tx_retry_attempts() -> u64 {
    GLOBAL_RETRY_ATTEMPTS.load(Ordering::Relaxed)
}

pub fn get_global_tx_exhausted() -> u64 {
    GLOBAL_EXHAUSTED.load(Ordering::Relaxed)
}

pub fn get_global_tx_backoff_ms() -> u64 {
    GLOBAL_BACKOFF_MS.load(Ordering::Relaxed)
}

fn record_restart() {
    GLOBAL_RESTARTS.fetch_add(1, Ordering::SeqCst);
    GLOBAL_RETRY_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
    LOCAL_RESTARTS.with_borrow_mut(|val| *val = val.wrapping_add(1))
}

// Telemetry counters are independent (no cross-counter invariant), so new
// additions use Relaxed; the pre-existing GLOBAL_RESTARTS SeqCst is kept as-is.
fn record_exhausted() {
    GLOBAL_EXHAUSTED.fetch_add(1, Ordering::Relaxed);
}

fn record_backoff(duration: Duration) {
    GLOBAL_BACKOFF_MS.fetch_add(duration.as_millis() as u64, Ordering::Relaxed);
}

/// Uniform-ish jitter in `[0, bound)` without a rand dependency: one xorshift64*
/// step per call. Backoff sleeps are the only consumer, so correlation between
/// successive draws is acceptable here.
fn jitter(bound: Duration) -> Duration {
    let mut state = JITTER_STATE.load(Ordering::Relaxed);
    loop {
        let next = if state == 0 {
            // Never let the state settle at the zero fixed point.
            0x9E3779B97F4A7C15u64
        } else {
            let x = state ^ (state >> 12);
            let x = x ^ (x << 25);
            let x = x ^ (x >> 27);
            x.wrapping_mul(0x2545F4914F6CDD1D)
        };
        match JITTER_STATE.compare_exchange_weak(state, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => {
                // Nanosecond resolution: a sub-millisecond cap must still yield
                // a proportional sleep. `as_millis()` truncated any bound < 1ms
                // to zero, turning a positive backoff into a zero-sleep hot spin
                // until the attempt/deadline budget was burned.
                let bound_ns = bound.as_nanos().min(u64::MAX as u128) as u64;
                return if bound_ns == 0 {
                    Duration::ZERO
                } else {
                    Duration::from_nanos(next % bound_ns)
                };
            }
            Err(actual) => state = actual
        }
    }
}

/// Bounded retry budget for optimistic storage transactions.
///
/// The store is designed for a single writer per dataset; `Busy`/`TryAgain`
/// commit conflicts are expected to be rare (retention/compaction racing the
/// ingest writer). The budget therefore exists to fail loudly when that
/// assumption is violated, not to absorb unbounded contention: exhaustion is a
/// typed error the caller must handle, never a silent spin.
#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    /// Total attempts including the first. Values below 1 are clamped to 1.
    pub max_attempts: u32,
    /// Wall-clock budget for the whole run, counted from the first attempt.
    pub deadline: Duration,
    /// Base backoff before a retry; doubles each attempt up to `max_backoff`.
    pub base_backoff: Duration,
    /// Cap on a single backoff sleep.
    pub max_backoff: Duration
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            deadline: Duration::from_secs(30),
            base_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1)
        }
    }
}

impl RetryPolicy {
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts.max(1);
        self
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    pub fn with_base_backoff(mut self, base_backoff: Duration) -> Self {
        self.base_backoff = base_backoff;
        self
    }

    pub fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Full-jitter backoff for `retry` (0-based retry number, i.e. attempt-1).
    fn backoff(&self, retry: u32) -> Duration {
        // The exponential term doubles each retry and saturates, rather than
        // pinning at an arbitrary 2^16 ceiling. A raw `1 << retry` panics for
        // `retry >= 32`, so the shift is computed with `checked_shl` and
        // saturates to `u32::MAX`; `max_backoff` is what actually caps the
        // sleep. This keeps the documented "doubles up to max_backoff"
        // contract instead of silently flattening the curve past attempt 16.
        let factor = 1u32.checked_shl(retry).unwrap_or(u32::MAX);
        let exp = self.base_backoff.saturating_mul(factor);
        let cap = exp.min(self.max_backoff);
        jitter(cap)
    }
}

/// A transaction run gave up after exhausting its [`RetryPolicy`] without
/// committing. No mutation became visible; the operation may be retried by the
/// caller as a whole, but forward progress is no longer guaranteed and the
/// single-writer assumption should be investigated.
#[derive(Debug)]
pub struct TxRetryExhausted {
    pub attempts: u32,
    pub elapsed: Duration,
    /// Why the last commit failed (`Busy`, `TryAgain`, or deadline details).
    pub last: String
}

impl std::fmt::Display for TxRetryExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "storage transaction retry budget exhausted after {} attempts in {:.3}s: {}",
            self.attempts,
            self.elapsed.as_secs_f64(),
            self.last
        )
    }
}

impl std::error::Error for TxRetryExhausted {}

/// Dataset kinds covered by the derived hash indexes. EVM-only for now;
/// hyperliquid must stay out because its `hash` is not a crypto hash and can
/// collide.
fn is_indexed_kind(kind: DatasetKind) -> bool {
    kind == DatasetKind::from_str("evm")
}

/// Aggregate time spent staging derived hash-index changes in an optimistic
/// storage transaction, including work repeated after transaction conflicts.
/// Timings are collected once per index scan, never once per entry.
#[derive(Clone, Copy, Debug, Default)]
pub struct HashIndexWriteMetrics {
    block_hash_index_duration: Duration,
    block_hash_index_operations: u64,
    transaction_hash_index_duration: Duration,
    transaction_hash_index_operations: u64
}

impl HashIndexWriteMetrics {
    /// Total time spent staging block-hash index changes, if the update
    /// performed any block-hash index scans.
    pub fn block_hash_index_duration(&self) -> Option<Duration> {
        (self.block_hash_index_operations > 0).then_some(self.block_hash_index_duration)
    }

    /// Number of block-hash index scans, including retried transaction attempts.
    pub fn block_hash_index_operations(&self) -> u64 {
        self.block_hash_index_operations
    }

    /// Total time spent staging transaction-hash index changes, if the update
    /// performed any transaction-hash index scans.
    pub fn transaction_hash_index_duration(&self) -> Option<Duration> {
        (self.transaction_hash_index_operations > 0).then_some(self.transaction_hash_index_duration)
    }

    /// Number of transaction-hash index scans, including retried attempts.
    pub fn transaction_hash_index_operations(&self) -> u64 {
        self.transaction_hash_index_operations
    }

    fn record(&mut self, index: HashIndex, duration: Duration) {
        match index {
            HashIndex::Block => {
                self.block_hash_index_duration = self.block_hash_index_duration.saturating_add(duration);
                self.block_hash_index_operations = self.block_hash_index_operations.saturating_add(1);
            }
            HashIndex::Transaction => {
                self.transaction_hash_index_duration = self.transaction_hash_index_duration.saturating_add(duration);
                self.transaction_hash_index_operations = self.transaction_hash_index_operations.saturating_add(1);
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.block_hash_index_duration = self
            .block_hash_index_duration
            .saturating_add(other.block_hash_index_duration);
        self.block_hash_index_operations = self
            .block_hash_index_operations
            .saturating_add(other.block_hash_index_operations);
        self.transaction_hash_index_duration = self
            .transaction_hash_index_duration
            .saturating_add(other.transaction_hash_index_duration);
        self.transaction_hash_index_operations = self
            .transaction_hash_index_operations
            .saturating_add(other.transaction_hash_index_operations);
    }
}

#[derive(Clone, Copy)]
enum HashIndex {
    Block,
    Transaction
}

pub struct Tx<'a> {
    db: &'a RocksDB,
    transaction: RocksTransaction<'a>,
    block_hash_index: bool,
    transaction_hash_index: bool,
    retry_policy: RetryPolicy,
    hash_index_write_metrics: RefCell<HashIndexWriteMetrics>
}

impl<'a> Tx<'a> {
    pub fn new(db: &'a RocksDB) -> Self {
        Self::with_retry_policy(db, RetryPolicy::default())
    }

    pub fn with_retry_policy(db: &'a RocksDB, retry_policy: RetryPolicy) -> Self {
        let mut tx_options = RocksTransactionOptions::default();
        tx_options.set_snapshot(true);

        let transaction = db.transaction_opt(&rocksdb::WriteOptions::default(), &tx_options);

        Self {
            db,
            transaction,
            block_hash_index: false,
            transaction_hash_index: false,
            retry_policy,
            hash_index_write_metrics: RefCell::new(HashIndexWriteMetrics::default())
        }
    }

    /// Enables block hash indexing for chunks written through this transaction.
    /// Set by [`Database::update_dataset`] from the database-level setting.
    pub fn with_block_hash_index(mut self, yes: bool) -> Self {
        self.block_hash_index = yes;
        self
    }

    /// Enables transaction hash indexing for chunks written through this
    /// transaction. The switch is intentionally independent of block hashes.
    pub fn with_transaction_hash_index(mut self, yes: bool) -> Self {
        self.transaction_hash_index = yes;
        self
    }

    /// Runs `cb` and commits, retrying the whole run on optimistic-commit
    /// conflicts (`Busy`/`TryAgain`) under the configured [`RetryPolicy`].
    ///
    /// Blocking: retry backoff sleeps on the calling thread. Callers on async
    /// runtimes must route through a blocking pool (hotblocks runs these
    /// writes on `tokio::task::spawn_blocking`).
    ///
    /// # Callback contract
    ///
    /// `cb` may execute MORE THAN ONCE — once per attempt — and only the
    /// staging of this transaction is rolled back between attempts. It MUST
    /// therefore be deterministic and replay-safe: no logging that represents
    /// committed state, no metric increments, no channel/watch publication,
    /// no file or network effects before commit. Effects that depend on the
    /// mutation having committed belong on the success path of the caller.
    ///
    /// On exhaustion the run fails with [`TxRetryExhausted`] (downcastable
    /// from the `anyhow::Error`); it never spins indefinitely and never
    /// reports success without a commit.
    pub fn run<R, F>(self, cb: F) -> anyhow::Result<R>
    where
        F: FnMut(&Self) -> anyhow::Result<R>
    {
        let mut metrics = HashIndexWriteMetrics::default();
        self.run_with_hash_index_metrics(&mut metrics, cb)
    }

    pub(crate) fn run_with_hash_index_metrics<R, F>(
        self,
        metrics: &mut HashIndexWriteMetrics,
        mut cb: F
    ) -> anyhow::Result<R>
    where
        F: FnMut(&Self) -> anyhow::Result<R>
    {
        let db = self.db;
        let block_hash_index = self.block_hash_index;
        let transaction_hash_index = self.transaction_hash_index;
        let policy = self.retry_policy;
        let started = Instant::now();
        let mut tx = self;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let result = match cb(&tx) {
                Ok(result) => result,
                Err(err) => {
                    metrics.merge(tx.hash_index_write_metrics.into_inner());
                    return Err(err);
                }
            };
            let (commit_result, attempt_metrics) = tx.commit();
            metrics.merge(attempt_metrics);
            match commit_result {
                Ok(_) => return Ok(result),
                Err(err) if err.kind() == rocksdb::ErrorKind::TryAgain || err.kind() == rocksdb::ErrorKind::Busy => {
                    let kind = format!("{:?}", err.kind());
                    // Attribute the exhausting bound. The deadline can be
                    // crossed by the callback+commit work itself (not only by a
                    // backoff sleep), so this pre-backoff exit must report the
                    // deadline when it is the cause — otherwise a run that a
                    // tight wall clock terminated is mislabelled as generic
                    // attempt exhaustion, hiding the real reason from callers.
                    let elapsed = started.elapsed();
                    let deadline_hit = elapsed >= policy.deadline;
                    let budget_left = attempt < policy.max_attempts && !deadline_hit;
                    if !budget_left {
                        record_exhausted();
                        let last = if deadline_hit {
                            format!("deadline before backoff after commit conflict: {}", kind)
                        } else {
                            format!("attempt budget exhausted after commit conflict: {}", kind)
                        };
                        return Err(anyhow!(TxRetryExhausted {
                            attempts: attempt,
                            elapsed,
                            last
                        }));
                    }
                    // `tx.commit()` above consumed the failed transaction (and
                    // its snapshot): nothing is held alive across the backoff,
                    // which yields to the competing writer.
                    // Full jitter, clamped to the remaining deadline so the run
                    // never oversleeps its wall-clock budget by a full backoff.
                    let remaining = policy.deadline.saturating_sub(started.elapsed());
                    let sleep = policy.backoff(attempt - 1).min(remaining);
                    std::thread::sleep(sleep);
                    record_backoff(sleep);
                    if started.elapsed() >= policy.deadline {
                        record_exhausted();
                        return Err(anyhow!(TxRetryExhausted {
                            attempts: attempt,
                            elapsed: started.elapsed(),
                            last: format!("deadline after commit conflict: {}", kind)
                        }));
                    }
                    // Count the restart only now that a replacement transaction is
                    // actually created and the callback will re-run. A run that the
                    // post-backoff deadline check terminated above never re-ran, so
                    // counting it earlier inflated the restart/attempt telemetry with
                    // attempts that never happened.
                    record_restart();
                    tx = Self::with_retry_policy(db, policy)
                        .with_block_hash_index(block_hash_index)
                        .with_transaction_hash_index(transaction_hash_index)
                }
                Err(err) => return Err(err.into())
            }
        }
    }

    fn commit(self) -> (Result<(), rocksdb::Error>, HashIndexWriteMetrics) {
        let Self {
            transaction,
            hash_index_write_metrics,
            ..
        } = self;
        (transaction.commit(), hash_index_write_metrics.into_inner())
    }

    fn measure_hash_index<R>(&self, index: HashIndex, cb: impl FnOnce() -> anyhow::Result<R>) -> anyhow::Result<R> {
        let started = Instant::now();
        let result = cb();
        self.hash_index_write_metrics
            .borrow_mut()
            .record(index, started.elapsed());
        result
    }

    pub fn find_label_for_update(&self, dataset_id: DatasetId) -> anyhow::Result<Option<DatasetLabel>> {
        let maybe_bytes = self
            .transaction
            .get_pinned_for_update_cf(self.cf_handle(CF_DATASETS), dataset_id, true)?;
        Ok(if let Some(bytes) = maybe_bytes {
            let label = borsh::from_slice(bytes.as_ref())?;
            Some(label)
        } else {
            None
        })
    }

    pub fn get_label_for_update(&self, dataset_id: DatasetId) -> anyhow::Result<DatasetLabel> {
        self.find_label_for_update(dataset_id)
            .and_then(|maybe_chunk| maybe_chunk.ok_or_else(|| anyhow!("dataset {} not found", dataset_id)))
    }

    pub fn write_label(&self, dataset_id: DatasetId, label: &DatasetLabel) -> anyhow::Result<()> {
        self.transaction
            .put_cf(self.cf_handle(CF_DATASETS), dataset_id, &borsh::to_vec(label).unwrap())?;
        Ok(())
    }

    pub fn delete_label(&self, dataset_id: DatasetId) -> anyhow::Result<()> {
        self.transaction.delete_cf(self.cf_handle(CF_DATASETS), dataset_id)?;
        Ok(())
    }

    pub fn write_chunk(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        self.transaction.put_cf(
            self.cf_handle(CF_CHUNKS),
            ChunkId::new_for_chunk(dataset_id, chunk),
            &borsh::to_vec(chunk).unwrap()
        )?;
        for table in chunk.tables().values() {
            self.transaction.delete_cf(self.cf_handle(CF_DIRTY_TABLES), table)?;
        }
        Ok(())
    }

    pub fn delete_chunk(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        self.transaction
            .delete_cf(self.cf_handle(CF_CHUNKS), ChunkId::new_for_chunk(dataset_id, chunk))?;
        for table_id in chunk.tables().values() {
            self.delete_table(table_id)?
        }
        Ok(())
    }

    pub fn delete_table(&self, table_id: &TableId) -> anyhow::Result<()> {
        // Value unused; the key's presence is the signal. `ops::logical_cleanup`
        // point-deletes the table's data and drops this entry.
        self.transaction
            .put_cf(self.cf_handle(CF_DELETED_TABLES), table_id, [])?;
        Ok(())
    }

    /// Adds the enabled derived-index entries for `chunk`. Both indexes are
    /// staged in the same optimistic transaction as chunk metadata.
    pub(crate) fn index_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        if !self.block_hash_index && !self.transaction_hash_index {
            return Ok(());
        }

        let Some(label) = self.find_label_for_update(dataset_id)? else {
            return Ok(()); // dataset does not exist - nothing to index
        };
        if !is_indexed_kind(label.kind()) {
            return Ok(());
        }

        if self.block_hash_index {
            self.measure_hash_index(HashIndex::Block, || self.index_block_hashes(dataset_id, chunk))?;
        }
        if self.transaction_hash_index {
            self.measure_hash_index(HashIndex::Transaction, || {
                self.index_transaction_hashes(dataset_id, chunk)
            })?;
        }
        Ok(())
    }

    fn index_block_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let Some(blocks_table_id) = chunk.tables().get("blocks").copied() else {
            return Ok(()); // defensively skip chunks without a blocks table
        };

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(blocks_table_id)?;
        let cf = self.cf_handle(CF_BLOCK_HASHES);
        let mut key = HashIndexKey::new(dataset_id, "");
        for_each_block_hash(&reader, |number, hash| {
            key.set_hash(hash);
            self.transaction.put_cf(cf, &key, number.to_be_bytes())?;
            Ok(())
        })
    }

    fn index_transaction_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let Some(transactions_table_id) = chunk.tables().get("transactions").copied() else {
            return Ok(());
        };

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(transactions_table_id)?;
        let cf = self.cf_handle(CF_TRANSACTION_HASHES);
        let mut key = HashIndexKey::new(dataset_id, "");
        for_each_transaction_hash(&reader, |block_number, transaction_index, hash| {
            key.set_hash(hash);
            self.transaction
                .put_cf(cf, &key, encode_transaction_position(block_number, transaction_index))?;
            Ok(())
        })
    }

    /// Removes every hash-index entry contributed by `chunk`.
    ///
    /// Removal is gated on neither flag nor dataset kind: entries written while
    /// a flag was on must still be removed when their chunk is pruned, or they
    /// would be stranded forever. Each column family is first probed with one
    /// prefix seek, making never-indexed chunks cheap and idempotent.
    pub(crate) fn unindex_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        self.measure_hash_index(HashIndex::Block, || self.unindex_block_hashes(dataset_id, chunk))?;
        self.measure_hash_index(HashIndex::Transaction, || {
            self.unindex_transaction_hashes(dataset_id, chunk)
        })
    }

    fn unindex_block_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let cf = self.cf_handle(CF_BLOCK_HASHES);
        if !self.has_hash_entries(cf, dataset_id)? {
            return Ok(());
        }

        let Some(blocks_table_id) = chunk.tables().get("blocks").copied() else {
            return Ok(());
        };

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(blocks_table_id)?;
        let mut key = HashIndexKey::new(dataset_id, "");
        for_each_block_hash(&reader, |_number, hash| {
            key.set_hash(hash);
            self.transaction.delete_cf(cf, &key)?;
            Ok(())
        })
    }

    fn unindex_transaction_hashes(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let cf = self.cf_handle(CF_TRANSACTION_HASHES);
        if !self.has_hash_entries(cf, dataset_id)? {
            return Ok(());
        }

        let Some(transactions_table_id) = chunk.tables().get("transactions").copied() else {
            return Ok(());
        };

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(transactions_table_id)?;
        let mut key = HashIndexKey::new(dataset_id, "");
        for_each_transaction_hash(&reader, |_block_number, _transaction_index, hash| {
            key.set_hash(hash);
            self.transaction.delete_cf(cf, &key)?;
            Ok(())
        })
    }

    /// Whether `dataset_id` holds at least one entry in `cf`: a single seek.
    /// Iterating the transaction (not the bare DB) keeps the answer accurate
    /// part-way through a multi-chunk `insert_fork`.
    fn has_hash_entries(&self, cf: &ColumnFamily, dataset_id: DatasetId) -> anyhow::Result<bool> {
        let (start, end) = HashIndexKey::dataset_range(dataset_id);

        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_snapshot(&self.transaction.snapshot());
        read_opts.set_iterate_upper_bound(end);

        let mut cursor = self.transaction.raw_iterator_cf_opt(cf, read_opts);

        cursor.seek(&start);
        cursor.status()?;

        Ok(cursor.valid())
    }

    pub fn insert_fork(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        let existing = self.list_chunks(dataset_id, 0, None).into_reversed();

        for head_result in existing {
            let head = head_result?;
            if chunk.first_block() <= head.first_block() {
                self.unindex_hashes(dataset_id, &head)?;
                self.delete_chunk(dataset_id, &head)?;
            } else if head.last_block() + 1 == chunk.first_block() {
                ensure!(
                    head.last_block_hash() == chunk.parent_block_hash(),
                    "chain continuity is violated between new chunk {} and its existing parent {}, expected parent hash was {}",
                    chunk,
                    head,
                    chunk.parent_block_hash()
                );
                break;
            } else if head.last_block() < chunk.first_block() {
                bail!(
                    "there is a gap between new chunk {} and existing {}, that is just below",
                    chunk,
                    head
                )
            } else {
                bail!("new chunk {} overlaps with existing {}", chunk, head)
            }
        }

        self.write_chunk(dataset_id, chunk)?;
        self.index_hashes(dataset_id, chunk)?;

        Ok(())
    }

    pub fn validate_chunk_insertion(&self, dataset_id: DatasetId, chunk: &Chunk) -> anyhow::Result<()> {
        ensure!(chunk.first_block() <= chunk.last_block());

        let existing = self
            .list_chunks(dataset_id, 0, Some(chunk.last_block() + 1))
            .into_reversed()
            .take(2);

        for chunk_result in existing {
            let n = chunk_result.context("failed to get neighbors")?;

            let is_disjoint = min(n.last_block(), chunk.last_block()) < max(n.first_block(), chunk.first_block());
            ensure!(is_disjoint, "new chunk {} overlaps with existing {}", chunk, n);

            if chunk.last_block() + 1 == n.first_block() {
                ensure!(
                    chunk.last_block_hash() == n.parent_block_hash(),
                    "chain continuity was violated between new {} and existing {}",
                    chunk,
                    n
                );
            }

            if n.last_block() + 1 == chunk.first_block() {
                ensure!(
                    n.last_block_hash() == chunk.parent_block_hash(),
                    "chain continuity was violated between new {} and existing {}",
                    chunk,
                    n
                );
            }
        }

        Ok(())
    }

    pub fn validate_parent_block_hash(
        &self,
        chunk: &Chunk,
        block_number: BlockNumber,
        expected_parent_hash: &str
    ) -> anyhow::Result<Result<(), String>> {
        if chunk.first_block() == block_number {
            return if chunk.parent_block_hash() == expected_parent_hash {
                Ok(Ok(()))
            } else {
                Ok(Err(chunk.parent_block_hash().to_string()))
            };
        }

        if chunk.last_block() + 1 == block_number {
            return if chunk.last_block_hash() == expected_parent_hash {
                Ok(Ok(()))
            } else {
                Ok(Err(chunk.last_block_hash().to_string()))
            };
        }

        ensure!(
            chunk.first_block() < block_number && block_number <= chunk.last_block(),
            "chunk {} does not have information about parent hash of block {}",
            chunk,
            block_number
        );

        let blocks_table_id = chunk
            .tables()
            .get("blocks")
            .copied()
            .ok_or_else(|| anyhow!("'blocks' table does not exist in chunk {}", chunk))?;

        let parent_hash = get_parent_block_hash(
            &ReadSnapshot::new(self.db).create_table_reader(blocks_table_id)?,
            block_number
        )?;

        if parent_hash == expected_parent_hash {
            Ok(Ok(()))
        } else {
            Ok(Err(parent_hash))
        }
    }

    /// The hash `chunk` itself carries for `block_number`, `None` when it holds no such block. Lets
    /// the write path check a finality report against the blocks that same response served.
    pub fn find_block_hash_in_chunk(&self, chunk: &Chunk, block_number: BlockNumber) -> anyhow::Result<Option<String>> {
        let blocks_table_id = chunk
            .tables()
            .get("blocks")
            .copied()
            .ok_or_else(|| anyhow!("'blocks' table does not exist in chunk {}", chunk))?;

        find_block_hash(
            &ReadSnapshot::new(self.db).create_table_reader(blocks_table_id)?,
            block_number
        )
    }

    /// The hash stored history carries for `block_number`, `None` when no stored chunk covers it:
    /// below the window, above the head, or a height the chain skips.
    pub fn find_stored_block_hash(
        &self,
        dataset_id: DatasetId,
        block_number: BlockNumber
    ) -> anyhow::Result<Option<String>> {
        let Some(chunk) = self
            .list_chunks(dataset_id, block_number, Some(block_number))
            .next()
            .transpose()?
        else {
            return Ok(None);
        };
        self.find_block_hash_in_chunk(&chunk, block_number)
    }

    /// Compares `chunk` against stored history block for block over
    /// `[chunk.first_block(), up_to]`, describing the first divergence.
    ///
    /// `up_to` is the finalized head (INV-13). Checking its hash alone would not
    /// do: hashes come from the source, so a reproduced boundary says nothing
    /// about the interior. Identical hashes over different payload are likewise
    /// invisible here — content is the source's word at every height.
    ///
    /// Peak memory is one stored chunk's worth of pairs: the replacement streams
    /// past a cursor that pulls stored chunks in one at a time.
    pub fn validate_finalized_prefix(
        &self,
        dataset_id: DatasetId,
        chunk: &Chunk,
        up_to: BlockNumber
    ) -> anyhow::Result<Result<(), String>> {
        let from = chunk.first_block();

        let stored_chunks = self
            .list_chunks(dataset_id, from, Some(up_to))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut stored_chunks = stored_chunks.iter();

        let mut stored: Vec<(BlockNumber, String)> = Vec::new();
        let mut pos = 0;
        let mut divergence = None;

        // `true` while a stored block is available at `stored[pos]`.
        let mut seek_stored = |stored: &mut Vec<(BlockNumber, String)>, pos: &mut usize| -> anyhow::Result<bool> {
            while *pos >= stored.len() {
                let Some(next) = stored_chunks.next() else {
                    return Ok(false);
                };
                *stored = self.read_block_hashes(next, from, up_to)?;
                *pos = 0;
            }
            Ok(true)
        };

        let blocks_table_id = chunk
            .tables()
            .get("blocks")
            .copied()
            .ok_or_else(|| anyhow!("'blocks' table does not exist in chunk {}", chunk))?;

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(blocks_table_id)?;

        for_each_block_hash(&reader, |number, hash| {
            if divergence.is_some() || number > up_to {
                return Ok(());
            }
            if !seek_stored(&mut stored, &mut pos)? {
                divergence = Some(format!(
                    "block {}#{} is not part of the stored finalized history",
                    number, hash
                ));
                return Ok(());
            }
            let (stored_number, stored_hash) = &stored[pos];
            if *stored_number == number && stored_hash == hash {
                pos += 1;
            } else {
                divergence = Some(format!(
                    "expected finalized block {}#{}, got {}#{}",
                    stored_number, stored_hash, number, hash
                ));
            }
            Ok(())
        })?;

        if divergence.is_none() && seek_stored(&mut stored, &mut pos)? {
            let (stored_number, stored_hash) = &stored[pos];
            divergence = Some(format!(
                "finalized block {}#{} is missing from the replacement",
                stored_number, stored_hash
            ));
        }

        Ok(divergence.map_or(Ok(()), Err))
    }

    fn read_block_hashes(
        &self,
        chunk: &Chunk,
        from: BlockNumber,
        to: BlockNumber
    ) -> anyhow::Result<Vec<(BlockNumber, String)>> {
        let blocks_table_id = chunk
            .tables()
            .get("blocks")
            .copied()
            .ok_or_else(|| anyhow!("'blocks' table does not exist in chunk {}", chunk))?;

        let snapshot = ReadSnapshot::new(self.db);
        let reader = snapshot.create_table_reader(blocks_table_id)?;

        let mut hashes = Vec::new();
        for_each_block_hash(&reader, |number, hash| {
            if from <= number && number <= to {
                hashes.push((number, hash.to_string()));
            }
            Ok(())
        })?;
        Ok(hashes)
    }

    pub fn list_chunks(
        &self,
        dataset_id: DatasetId,
        from_block: BlockNumber,
        to_block: Option<BlockNumber>
    ) -> ChunkIterator<RocksIterator<'_, RocksTransaction<'_>>> {
        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_snapshot(&self.transaction.snapshot());

        let cursor = self
            .transaction
            .raw_iterator_cf_opt(self.cf_handle(CF_CHUNKS), read_opts);

        ChunkIterator::new(cursor, dataset_id, from_block, to_block)
    }

    fn cf_handle(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).unwrap()
    }
}

fn encode_transaction_position(block_number: BlockNumber, transaction_index: ItemIndex) -> [u8; 12] {
    let mut bytes = [0; 12];
    bytes[..8].copy_from_slice(&block_number.to_be_bytes());
    bytes[8..].copy_from_slice(&transaction_index.to_be_bytes());
    bytes
}

#[cfg(test)]
mod backoff_math_tests {
    use std::time::Duration;

    use super::{jitter, RetryPolicy};

    /// F67-2: a sub-millisecond cap must not systematically collapse to a zero
    /// sleep. The old `as_millis()` truncation floored any bound `< 1ms` to
    /// zero, turning a positive backoff into a zero-sleep hot spin that burned
    /// the whole retry budget instantly. Nanosecond resolution yields
    /// proportional, positive draws.
    #[test]
    fn subms_jitter_bound_is_not_floored_to_zero() {
        let bound = Duration::from_micros(500);
        let mut saw_positive = false;
        for _ in 0..1000 {
            let d = jitter(bound);
            assert!(d < bound, "draw {d:?} must stay inside [0, {bound:?})");
            if d > Duration::ZERO {
                saw_positive = true;
            }
        }
        assert!(
            saw_positive,
            "a positive sub-ms cap must produce positive backoff draws"
        );
    }

    #[test]
    fn zero_bound_jitter_is_zero() {
        assert_eq!(jitter(Duration::ZERO), Duration::ZERO);
    }

    /// F67-6: retry numbers past 16 — and past the u32 shift width — must not
    /// panic (a raw `1u32 << retry` overflows for `retry >= 32`) and must stay
    /// capped by `max_backoff`, rather than the old arbitrary 2^16 pin.
    #[test]
    fn backoff_saturates_past_the_shift_width_without_panicking() {
        let policy = RetryPolicy::default(); // base 10ms, max 1s
        for retry in [0u32, 15, 16, 31, 32, 33, 64, 200] {
            let d = policy.backoff(retry);
            assert!(
                d <= policy.max_backoff,
                "retry {retry}: {d:?} must be capped by max_backoff {:?}",
                policy.max_backoff
            );
        }
    }

    /// The exponential actually climbs to the `max_backoff` cap instead of a
    /// smaller flattened plateau: sampled large-retry draws reach the upper
    /// half of the window.
    #[test]
    fn backoff_reaches_the_max_backoff_cap() {
        let policy = RetryPolicy::default();
        let half = policy.max_backoff / 2;
        let mut saw_high = false;
        for _ in 0..1000 {
            if policy.backoff(20) >= half {
                saw_high = true;
                break;
            }
        }
        assert!(
            saw_high,
            "large-retry backoff should span up to max_backoff, not a lower plateau"
        );
    }

    /// F67-6 (discriminating): the previous `1u32 << retry.min(16)` flattened the
    /// exponential at `base * 2^16` for every retry past 16. With the default
    /// policy that pin sits far *above* `max_backoff`, so it was invisible — the
    /// cap dominated either way. The regression only shows when the exponential
    /// would legitimately keep climbing past `2^16` before reaching the cap, i.e.
    /// when `base * 2^16 < max_backoff`. Here the old pin caps every draw at
    /// `base * 2^16`; the new saturating shift keeps doubling toward `max_backoff`.
    #[test]
    fn backoff_climbs_past_the_2_pow_16_pin_when_the_cap_allows_it() {
        let base = Duration::from_micros(1);
        let max = Duration::from_secs(10);
        let policy = RetryPolicy::default().with_base_backoff(base).with_max_backoff(max);

        // The old `retry.min(16)` ceiling: `base * 2^16`. Well below `max`, so a
        // correct saturating curve must be able to exceed it at a high retry.
        let old_pin = base * (1 << 16);
        assert!(
            old_pin < max,
            "test premise: the 2^16 pin must sit below the cap to be observable"
        );

        // retry 24 wants `base * 2^24` (~16.8s), clamped to the 10s cap — so draws
        // span [0, 10s). Under the old pin the cap would instead be `old_pin`
        // (~65ms) and no draw could exceed it.
        let mut saw_past_pin = false;
        for _ in 0..1000 {
            if policy.backoff(24) > old_pin {
                saw_past_pin = true;
                break;
            }
        }
        assert!(
            saw_past_pin,
            "a high-retry backoff must be able to exceed the old base*2^16 pin \
             ({old_pin:?}) when max_backoff ({max:?}) allows it"
        );
    }
}
