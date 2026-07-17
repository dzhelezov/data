use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering}
    },
    time::{Duration, Instant}
};

use anyhow::{bail, ensure};
use sqd_query::Query;

use super::{executor::QueryExecutor, response::QueryResponse};
use crate::{
    dataset_controller::DatasetController,
    encoding::ContentEncoding,
    errors::{Busy, QueryIsAboveTheHead, QueryKindMismatch},
    metrics::SlowResponseConfig,
    query::QueryExecutorCollector,
    types::{ClientId, DBRef, DatasetKind}
};

pub type QueryServiceRef = Arc<QueryService>;

pub struct QueryOutcome {
    pub response: anyhow::Result<QueryResponse>,
    pub long_poll: bool
}

pub struct QueryServiceBuilder {
    db: DBRef,
    max_data_waiters: usize,
    max_pending_tasks: usize,
    urgency: usize,
    slow_response_config: SlowResponseConfig
}

impl QueryServiceBuilder {
    pub fn new(db: DBRef) -> Self {
        Self {
            db,
            max_data_waiters: 64_000,
            max_pending_tasks: sqd_polars::POOL.current_num_threads() * 200,
            urgency: 500,
            slow_response_config: SlowResponseConfig::default()
        }
    }

    /// Max number of pending queries, waiting for new block arrival
    pub fn set_max_data_waiters(&mut self, count: usize) -> &mut Self {
        self.max_data_waiters = count;
        self
    }

    /// Max number of pending query tasks (work units).
    ///
    /// When exceeded, existing streams terminate and for new queries `Busy` error is returned.
    pub fn set_max_pending_query_tasks(&mut self, count: usize) -> &mut Self {
        self.max_pending_tasks = count;
        self
    }

    /// Roughly corresponds to a maximum time
    /// any query task can spend waiting in a work queue
    pub fn set_urgency(&mut self, ms: usize) -> &mut Self {
        self.urgency = ms;
        self
    }

    pub fn set_slow_response_config(&mut self, config: SlowResponseConfig) -> &mut Self {
        self.slow_response_config = config;
        self
    }

    pub fn build(&self) -> QueryService {
        QueryService {
            db: self.db.clone(),
            executor: QueryExecutor::new(self.max_pending_tasks, self.urgency),
            wait_slots: WaitSlots {
                waiters: AtomicUsize::new(0),
                limit: self.max_data_waiters
            },
            slow_response_config: self.slow_response_config
        }
    }
}

pub struct QueryService {
    db: DBRef,
    executor: QueryExecutor,
    wait_slots: WaitSlots,
    slow_response_config: SlowResponseConfig
}

impl QueryService {
    pub fn builder(db: DBRef) -> QueryServiceBuilder {
        QueryServiceBuilder::new(db)
    }

    pub async fn query(
        &self,
        dataset: &DatasetController,
        query: Query,
        client_id: ClientId,
        encoding: ContentEncoding
    ) -> QueryOutcome {
        self.query_internal(dataset, query, false, client_id, encoding).await
    }

    pub async fn query_finalized(
        &self,
        dataset: &DatasetController,
        query: Query,
        client_id: ClientId,
        encoding: ContentEncoding
    ) -> QueryOutcome {
        self.query_internal(dataset, query, true, client_id, encoding).await
    }

    async fn query_internal(
        &self,
        dataset: &DatasetController,
        query: Query,
        finalized: bool,
        client_id: ClientId,
        encoding: ContentEncoding
    ) -> QueryOutcome {
        let start = Instant::now();
        let long_poll = match self.is_long_poll(dataset, &query, finalized) {
            Ok(long_poll) => long_poll,
            Err(err) => {
                return QueryOutcome {
                    response: Err(err),
                    long_poll: false
                };
            }
        };

        let response = async {
            if long_poll {
                let Some(_wait_slot) = self.wait_slots.get() else {
                    bail!(Busy)
                };
                tokio::time::timeout(Duration::from_secs(5), async {
                    if finalized {
                        dataset.wait_for_finalized_block(query.first_block()).await
                    } else {
                        dataset.wait_for_block(query.first_block()).await
                    }
                })
                .await
                .map_err(|_| QueryIsAboveTheHead { finalized_head: None })?;
            }

            let mut response = QueryResponse::new(
                self.executor.clone(),
                self.db.clone(),
                dataset.dataset_id(),
                query,
                finalized,
                None,
                client_id,
                encoding,
                long_poll,
                self.slow_response_config
            )
            .await?;
            response.set_time_to_first_byte(start.elapsed());
            Ok(response)
        }
        .await;

        QueryOutcome { response, long_poll }
    }

    // `&self` is reserved for a future config-driven long-poll threshold; the body
    // reads only its arguments today.
    fn is_long_poll(&self, dataset: &DatasetController, query: &Query, finalized: bool) -> anyhow::Result<bool> {
        // Adopt upstream 546c1ac: DatasetKind::from_query is now fallible; bind once.
        let query_kind = DatasetKind::from_query(query)?;
        ensure!(
            dataset.dataset_kind() == query_kind,
            QueryKindMismatch {
                query_kind: query_kind.storage_kind(),
                dataset_kind: dataset.dataset_kind().storage_kind()
            }
        );

        let target_head = if finalized {
            dataset.get_finalized_head()
        } else {
            dataset.get_head()
        };

        let should_wait = match target_head {
            Some(head) if head.number >= query.first_block() => false,
            Some(head) if head.number + 1 == query.first_block() => {
                if let Some(parent_hash) = query.parent_block_hash() {
                    ensure!(
                        head.hash == parent_hash,
                        sqd_query::UnexpectedBaseBlock {
                            prev_blocks: vec![head],
                            expected_hash: parent_hash.to_string()
                        }
                    );
                }
                true
            }
            Some(_) | None => true
        };
        Ok(should_wait)
    }

    pub fn metrics_collector(&self) -> QueryExecutorCollector {
        self.executor.metrics_collector()
    }
}

struct WaitSlots {
    waiters: AtomicUsize,
    limit: usize
}

impl WaitSlots {
    fn get(&self) -> Option<WaitingSlot<'_>> {
        let previously_waiting = self.waiters.fetch_add(1, Ordering::SeqCst);
        let slot = WaitingSlot { waiters: &self.waiters };
        if previously_waiting < self.limit {
            Some(slot)
        } else {
            crate::metrics::report_query_too_many_data_waiters_error();
            None
        }
    }
}

struct WaitingSlot<'a> {
    waiters: &'a AtomicUsize
}

impl<'a> Drop for WaitingSlot<'a> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}
