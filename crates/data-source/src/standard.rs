use std::{future::Future, pin::Pin, task::Poll, time::Duration};

use anyhow::Context;
use futures::{future::BoxFuture, stream::BoxStream, FutureExt, Stream, StreamExt};
use sqd_data_client::{BlockStreamRequest, BlockStreamResponse, DataClient};
use sqd_primitives::{Block, BlockNumber, BlockRef};
use tokio::time::{Instant, Sleep};
use tracing::{info, warn};

use crate::types::{DataEvent, DataSource};

struct Endpoint<C: DataClient> {
    client: C,
    state: EndpointState<C::Block>,
    error_counter: usize,
    /// Consecutive parent-hash linkage rejections at the current position. Reset on a committed
    /// block or an external reposition (`set_position`), so the strike stays scoped to the position
    /// it was accrued at; drives an exponential backoff so a source stuck streaming a non-linking
    /// block cannot spin re-requesting the same position hot. Unlike `error_counter`, a mere
    /// successful stream *response* does not clear it — only real forward progress does.
    reject_counter: usize,
    last_committed_block: Option<BlockNumber>
}

enum EndpointState<B> {
    Ready,
    Req {
        req: BlockStreamRequest,
        future: BoxFuture<'static, anyhow::Result<BlockStreamResponse<B>>>
    },
    Stream {
        finalized_head: BlockNumber,
        blocks: BoxStream<'static, anyhow::Result<B>>
    },
    Fork {
        req: BlockStreamRequest,
        prev_blocks: Vec<BlockRef>
    },
    Backoff(Pin<Box<Sleep>>)
}

pub struct StandardDataSource<C: DataClient, F> {
    endpoints: Vec<Endpoint<C>>,
    state: DataSourceState<F>
}

struct DataSourceState<F> {
    parse: F,
    finalized_head: Option<BlockRef>,
    position: BlockStreamRequest,
    position_is_canonical: bool,
    max_seen_finalized_block: BlockNumber,
    fork_consensus_timeout: Option<Pin<Box<Sleep>>>,
    fork_consensus_started_at: Option<Instant>
}

impl<F> DataSourceState<F> {
    fn poll_endpoint<B, C>(&mut self, ep: &mut Endpoint<C>, cx: &mut std::task::Context<'_>) -> Poll<DataEvent<B>>
    where
        B: Block,
        C: DataClient,
        F: Fn(C::Block) -> anyhow::Result<B>
    {
        loop {
            match &mut ep.state {
                EndpointState::Ready => {
                    ep.state = EndpointState::Req {
                        req: self.position.clone(),
                        future: ep.client.stream(self.position.clone())
                    }
                }
                EndpointState::Req { req, future } => match future.poll_unpin(cx) {
                    Poll::Ready(Ok(BlockStreamResponse::Stream { finalized_head, blocks })) => {
                        let finalized_head_updated = self.on_new_finalized_head(finalized_head.as_ref());

                        ep.error_counter = 0;
                        ep.state = EndpointState::Stream {
                            finalized_head: finalized_head.as_ref().map_or(0, |b| b.number),
                            blocks
                        };

                        if finalized_head_updated {
                            return Poll::Ready(DataEvent::FinalizedHead(finalized_head.unwrap()));
                        }
                    }
                    Poll::Ready(Ok(BlockStreamResponse::Fork(prev_blocks))) => {
                        let req = req.clone();
                        self.fork_consensus_started_at.get_or_insert_with(Instant::now);
                        ep.on_fork_signal(req.first_block, &prev_blocks);
                        ep.error_counter = 0;
                        ep.state = EndpointState::Fork { req, prev_blocks };
                    }
                    Poll::Ready(Err(err)) => ep.on_error(err),
                    Poll::Pending => return Poll::Pending
                },
                EndpointState::Stream { finalized_head, blocks } => match blocks.poll_next_unpin(cx) {
                    Poll::Ready(None) => {
                        ep.error_counter = 0;
                        ep.state = EndpointState::Ready;
                        let prev_block = self.position.first_block.saturating_sub(1);
                        if prev_block >= self.max_seen_finalized_block && ep.last_committed_block == Some(prev_block) {
                            return Poll::Ready(DataEvent::MaybeOnHead);
                        }
                    }
                    Poll::Ready(Some(Ok(new_block))) => {
                        match (self.parse)(new_block).context("failed to parse a block") {
                            Ok(block) => {
                                ep.error_counter = 0;
                                if block.number() >= self.position.first_block {
                                    let is_final = *finalized_head >= block.number();
                                    if self.accept_new_block(&block, is_final) {
                                        ep.reject_counter = 0;
                                        ep.last_committed_block = Some(block.number());
                                        return Poll::Ready(DataEvent::Block { block, is_final });
                                    } else {
                                        ep.on_reject("parent_hash_mismatch");
                                    }
                                }
                            }
                            Err(err) => ep.on_error(err)
                        }
                    }
                    Poll::Ready(Some(Err(err))) => ep.on_error(err),
                    Poll::Pending => return Poll::Pending
                },
                EndpointState::Fork { req, .. } => {
                    if req == &self.position {
                        return Poll::Pending;
                    } else {
                        ep.state = EndpointState::Ready;
                    }
                }
                EndpointState::Backoff(sleep) => match sleep.as_mut().poll(cx) {
                    Poll::Ready(_) => ep.state = EndpointState::Ready,
                    Poll::Pending => return Poll::Pending
                }
            }
        }
    }

    fn accept_new_block(&mut self, block: &impl Block, is_final: bool) -> bool {
        assert!(self.position.first_block <= block.number());

        if let Some(parent_hash) = self.position.parent_block_hash.as_mut() {
            if block.parent_hash() != parent_hash {
                return false;
            }
            parent_hash.clear();
            parent_hash.push_str(block.hash());
        } else {
            self.position.parent_block_hash = Some(block.hash().to_string());
        }
        self.position.first_block = block.number() + 1;
        self.position_is_canonical = true;
        self.reset_fork_consensus();

        if is_final {
            set_head(&mut self.finalized_head, block.number(), block.hash());
        }

        true
    }

    fn reset_fork_consensus(&mut self) {
        self.fork_consensus_timeout = None;
        self.fork_consensus_started_at = None;
    }

    fn on_new_finalized_head(&mut self, new_head: Option<&BlockRef>) -> bool {
        let Some(new_head) = new_head else { return false };

        self.max_seen_finalized_block = std::cmp::max(self.max_seen_finalized_block, new_head.number);

        if self.position.first_block == 0 {
            return false;
        }

        let Some(current_parent_hash) = self.position.parent_block_hash.as_ref() else {
            return false;
        };

        let is_behind = self
            .finalized_head
            .as_ref()
            .map_or(false, |c| c.number >= new_head.number);

        if is_behind {
            return false;
        }

        let mut new_number = new_head.number;
        let mut new_hash = &new_head.hash;

        if new_head.number >= self.position.first_block {
            if !self.position_is_canonical {
                return false;
            }
            new_number = self.position.first_block - 1;
            new_hash = current_parent_hash;
        }

        set_head(&mut self.finalized_head, new_number, new_hash);

        true
    }
}

fn set_head(head: &mut Option<BlockRef>, number: BlockNumber, hash: &str) {
    if let Some(current) = head.as_mut() {
        current.number = number;
        current.hash.clear();
        current.hash.push_str(hash);
    } else {
        *head = Some(BlockRef {
            number,
            hash: hash.to_string()
        })
    }
}

impl<C: DataClient> Endpoint<C> {
    fn is_on_fork(&self) -> bool {
        match self.state {
            EndpointState::Fork { .. } => true,
            _ => false
        }
    }

    fn is_active(&self) -> bool {
        match self.state {
            EndpointState::Backoff(_) => false,
            _ => true
        }
    }

    // A source answers 409 only at `head + 1 == from`, so in-spec hints top out at `from - 1`.
    fn on_fork_signal(&self, from: BlockNumber, prev_blocks: &[BlockRef]) {
        let standing = match prev_blocks.last() {
            Some(top) if top.number < from.saturating_sub(1) => "above_tip",
            // Empty hints are a malformed 409 the reqwest client already rejects; counting them
            // as `at_tip` keeps the defect bucket clean of shapes it cannot speak to.
            _ => "at_tip"
        };
        crate::metrics::record_ingest_fork_signal(&self.client.source_label(), standing);
    }

    fn on_error(&mut self, error: anyhow::Error) {
        crate::metrics::record_ingest_source_error(&self.client.source_label(), self.client.error_kind(&error));

        let pause = backoff_ms(self.error_counter);
        if pause > 0 {
            warn!(
                error =? error,
                data_source =? self.client,
                "data ingestion error, will disable the data source for {} ms",
                pause
            )
        } else {
            warn!(
                error =? error,
                data_source =? self.client,
                "data ingestion error",
            )
        }
        self.state = if pause > 0 {
            let sleep = tokio::time::sleep(Duration::from_millis(pause));
            EndpointState::Backoff(Box::pin(sleep))
        } else {
            EndpointState::Ready
        };
        self.error_counter += 1;
    }

    /// A well-formed block that did not extend our chain (parent-hash mismatch). Unlike `on_error`
    /// this is not a transport/parse fault, but it re-requests the same position, so a source stuck
    /// emitting a non-linking block would spin hot. Escalate the same backoff ladder on consecutive
    /// rejects (reset by the next committed block) so the re-request rate is bounded, and record it
    /// so the loop is visible. The first reject keeps the original zero-latency retry.
    fn on_reject(&mut self, reason: &'static str) {
        crate::metrics::record_ingest_linkage_reject(&self.client.source_label(), reason);

        let pause = backoff_ms(self.reject_counter);
        if pause > 0 {
            warn!(
                reason,
                data_source =? self.client,
                "data source streamed a non-linking block, will disable it for {} ms",
                pause
            )
        } else {
            warn!(
                reason,
                data_source =? self.client,
                "data source streamed a non-linking block",
            )
        }
        self.state = if pause > 0 {
            let sleep = tokio::time::sleep(Duration::from_millis(pause));
            EndpointState::Backoff(Box::pin(sleep))
        } else {
            EndpointState::Ready
        };
        // Increment AFTER deriving `pause` from the pre-increment counter: the first reject
        // (counter 0 → pause 0) keeps the original free zero-latency retry, and only the second
        // consecutive reject onward installs a backoff.
        self.reject_counter += 1;
    }
}

/// Shared exponential backoff ladder (ms) for a single endpoint's consecutive faults. Index is
/// clamped to the last rung, so the pause tops out at 10s however long a fault persists.
fn backoff_ms(consecutive: usize) -> u64 {
    const BACKOFF_MS: [u64; 8] = [0, 100, 200, 500, 1000, 2000, 5000, 10000];
    BACKOFF_MS[std::cmp::min(consecutive, BACKOFF_MS.len() - 1)]
}

impl<B, C, F> StandardDataSource<C, F>
where
    B: Block,
    C: DataClient,
    F: Fn(C::Block) -> anyhow::Result<B>
{
    pub fn new(clients: Vec<C>, parse: F) -> Self {
        let endpoints = clients
            .into_iter()
            .map(|client| Endpoint {
                client,
                error_counter: 0,
                reject_counter: 0,
                state: EndpointState::Ready,
                last_committed_block: None
            })
            .collect();

        let state = DataSourceState {
            parse,
            finalized_head: None,
            position: BlockStreamRequest {
                first_block: 0,
                parent_block_hash: None
            },
            position_is_canonical: false,
            max_seen_finalized_block: 0,
            fork_consensus_timeout: None,
            fork_consensus_started_at: None
        };

        Self { endpoints, state }
    }

    fn poll_next_event(&mut self, cx: &mut std::task::Context<'_>) -> Poll<DataEvent<B>> {
        let mut committed = None;
        for (i, ep) in self.endpoints.iter_mut().enumerate() {
            let event = self.state.poll_endpoint(ep, cx);
            if event.is_ready() {
                committed = Some((i, event));
                break;
            }
        }
        if let Some((committed_idx, event)) = committed {
            // A committed block advances the shared position (`accept_new_block`), and a
            // linkage-reject strike is scoped to the position it was accrued at — so once the
            // position moves forward, every *other* endpoint's strike is stale. Clear their
            // counters and cancel any parked backoff; otherwise an endpoint that rejected at the
            // old position stays asleep at the new one for up to the full 10 s rung, needlessly
            // delaying it from rejoining a position it may now link (a multi-endpoint failover
            // regression). The committing endpoint already reset its own counter and must keep its
            // live stream, so it is skipped.
            if matches!(event, Poll::Ready(DataEvent::Block { .. })) {
                for (j, other) in self.endpoints.iter_mut().enumerate() {
                    if j == committed_idx {
                        continue;
                    }
                    other.reject_counter = 0;
                    if matches!(other.state, EndpointState::Backoff(_)) {
                        other.state = EndpointState::Ready;
                    }
                }
            }
            return event;
        }

        let forks = self.endpoints.iter().filter(|ep| ep.is_on_fork()).count();
        if forks > 0 {
            let active = self.endpoints.iter().filter(|ep| ep.is_active()).count();
            let decision = if forks > self.endpoints.len() / 2 {
                "majority"
            } else if forks == active {
                "all_active"
            } else if self.fork_consensus_timeout(cx) {
                "timeout"
            } else {
                return Poll::Pending;
            };

            let consensus_duration = self
                .state
                .fork_consensus_started_at
                .expect("fork consensus must start with the first fork signal")
                .elapsed();
            crate::metrics::record_ingest_fork_consensus_duration(decision, consensus_duration);

            let chain = self.extract_fork();
            info!(
                decision = decision,
                consensus_duration_seconds = consensus_duration.as_secs_f64(),
                forked_endpoints = forks,
                active_endpoints = active,
                total_endpoints = self.endpoints.len(),
                hint_count = chain.len(),
                oldest_hint =? chain.first().map(|b| b.number),
                newest_hint =? chain.last().map(|b| b.number),
                "fork consensus reached"
            );
            return Poll::Ready(DataEvent::Fork(chain));
        } else {
            self.state.reset_fork_consensus()
        }

        Poll::Pending
    }

    fn fork_consensus_timeout(&mut self, cx: &mut std::task::Context<'_>) -> bool {
        let mut timeout = self
            .state
            .fork_consensus_timeout
            .take()
            .unwrap_or_else(|| Box::pin(tokio::time::sleep(Duration::from_secs(2))));

        if timeout.poll_unpin(cx) == Poll::Pending {
            self.state.fork_consensus_timeout = Some(timeout);
            false
        } else {
            true
        }
    }

    fn extract_fork(&mut self) -> Vec<BlockRef> {
        self.state.reset_fork_consensus();
        let mut chain = Vec::new();
        for ep in self.endpoints.iter_mut() {
            match std::mem::replace(&mut ep.state, EndpointState::Ready) {
                EndpointState::Fork { prev_blocks, .. } => {
                    if prev_blocks.len() > chain.len() {
                        chain = prev_blocks
                    }
                }
                _ => {}
            }
        }
        assert!(!chain.is_empty());
        chain
    }
}

impl<B, C, F> Stream for StandardDataSource<C, F>
where
    B: Block,
    C: DataClient,
    F: Fn(C::Block) -> anyhow::Result<B> + Unpin
{
    type Item = DataEvent<B>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().poll_next_event(cx).map(Some)
    }
}

impl<B, C, F> DataSource for StandardDataSource<C, F>
where
    B: Block,
    C: DataClient,
    F: Fn(C::Block) -> anyhow::Result<B> + Unpin
{
    type Block = B;

    fn set_position(&mut self, next_block: BlockNumber, parent_block_hash: Option<&str>) {
        self.state.position.first_block = next_block;
        self.state.position.set_parent_block_hash(parent_block_hash);
        self.state.position_is_canonical = false;
        self.state.finalized_head = None;
        self.state.reset_fork_consensus();
        for ep in self.endpoints.iter_mut() {
            ep.state = EndpointState::Ready;
            ep.last_committed_block = None;
            ep.reject_counter = 0;
        }
    }

    fn get_next_block(&self) -> BlockNumber {
        self.state.position.first_block
    }

    fn get_parent_block_hash(&self) -> Option<&str> {
        self.state.position.parent_block_hash.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        time::Duration
    };

    use futures::{stream, task::noop_waker, FutureExt, StreamExt};
    use sqd_data_client::{BlockStreamRequest, BlockStreamResponse, DataClient};
    use sqd_primitives::{Block, BlockNumber, BlockRef};

    use super::{backoff_ms, Endpoint, EndpointState, StandardDataSource};
    use crate::{
        metrics::INGEST_LINKAGE_REJECTS,
        types::{DataEvent, DataSource}
    };

    #[derive(Debug, Clone)]
    struct TestBlock {
        number: BlockNumber,
        hash: String,
        parent_hash: String
    }

    impl TestBlock {
        fn new(number: BlockNumber, hash: &str, parent_hash: &str) -> Self {
            Self {
                number,
                hash: hash.to_string(),
                parent_hash: parent_hash.to_string()
            }
        }
    }

    impl Block for TestBlock {
        fn number(&self) -> BlockNumber {
            self.number
        }
        fn hash(&self) -> &str {
            &self.hash
        }
        fn parent_number(&self) -> BlockNumber {
            self.number.saturating_sub(1)
        }
        fn parent_hash(&self) -> &str {
            &self.parent_hash
        }
    }

    /// A client that replays canned batches. `stream()` advances through the queue and repeats the
    /// last batch once exhausted, so "the source is permanently stuck on a non-linking block" is one
    /// queued batch, while "one bad block then a good one" is two.
    #[derive(Debug)]
    struct ReplayClient {
        source: String,
        batches: Arc<Mutex<VecDeque<Vec<TestBlock>>>>
    }

    impl ReplayClient {
        fn new(source: &str, batches: Vec<Vec<TestBlock>>) -> Self {
            Self {
                source: source.to_string(),
                batches: Arc::new(Mutex::new(batches.into()))
            }
        }
    }

    impl DataClient for ReplayClient {
        type Block = TestBlock;

        fn stream(
            &self,
            _req: BlockStreamRequest
        ) -> futures::future::BoxFuture<'static, anyhow::Result<BlockStreamResponse<TestBlock>>> {
            let mut queue = self.batches.lock().unwrap();
            let batch = if queue.len() > 1 {
                queue.pop_front().unwrap()
            } else {
                queue.front().cloned().unwrap_or_default()
            };
            async move {
                Ok(BlockStreamResponse::Stream {
                    finalized_head: None,
                    blocks: stream::iter(batch.into_iter().map(anyhow::Ok)).boxed()
                })
            }
            .boxed()
        }

        fn get_finalized_head(&self) -> futures::future::BoxFuture<'static, anyhow::Result<Option<BlockRef>>> {
            async { Ok(None) }.boxed()
        }

        fn is_retryable(&self, _err: &anyhow::Error) -> bool {
            true
        }

        fn source_label(&self) -> String {
            self.source.clone()
        }
    }

    fn reject_count(source: &str) -> u64 {
        INGEST_LINKAGE_REJECTS
            .get_or_create(&vec![
                ("source", source.to_string()),
                ("reason", "parent_hash_mismatch".to_string()),
            ])
            .get()
    }

    fn build(
        source: &str,
        batches: Vec<Vec<TestBlock>>
    ) -> StandardDataSource<ReplayClient, fn(TestBlock) -> anyhow::Result<TestBlock>> {
        StandardDataSource::new(
            vec![ReplayClient::new(source, batches)],
            Ok as fn(TestBlock) -> anyhow::Result<TestBlock>
        )
    }

    #[test]
    fn backoff_ladder_is_shared_and_clamped_to_ten_seconds() {
        assert_eq!(
            backoff_ms(0),
            0,
            "the first fault must keep the original zero-latency retry"
        );
        assert_eq!(backoff_ms(1), 100);
        assert_eq!(backoff_ms(2), 200);
        assert_eq!(backoff_ms(7), 10_000);
        // Past the last rung the pause must clamp, never index out of bounds.
        assert_eq!(backoff_ms(8), 10_000);
        assert_eq!(backoff_ms(1_000_000), 10_000);
    }

    // A source permanently streaming a block that does not link at our position used to reset
    // straight back to Ready and re-request the same position with no pause — a hot loop. After the
    // fix, the free first retry is followed by a backoff, so a single poll parks the endpoint at
    // strike 2 instead of spinning, and every rejection is counted. Pre-fix there is no
    // `reject_counter`/`Backoff` on this path at all, so this terminal state is unreachable.
    #[tokio::test(start_paused = true)]
    async fn persistent_linkage_mismatch_backs_off_instead_of_spinning() {
        let source = "test-persistent-reject";
        let before = reject_count(source);
        let non_linking = TestBlock::new(10, "0xB10", "0xWRONG_PARENT");
        let mut ds = build(source, vec![vec![non_linking]]);
        ds.set_position(10, Some("0xEXPECTED_PARENT"));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = ds.poll_next_event(&mut cx);

        assert!(
            matches!(poll, Poll::Pending),
            "the endpoint must park in backoff, not emit a block"
        );
        assert_eq!(
            ds.endpoints[0].reject_counter, 2,
            "free retry then an escalating strike"
        );
        assert!(
            matches!(ds.endpoints[0].state, EndpointState::Backoff(_)),
            "a persistent reject must install a backoff sleep"
        );
        assert_eq!(
            reject_count(source) - before,
            2,
            "every rejection is recorded, backoff or not"
        );
    }

    // A committed block clears the consecutive-reject strike, so a transient one-off mismatch
    // followed by a good block accrues no lasting penalty. The counter is 0 at the end only because
    // the accept path resets it: the intervening rejection did happen (the metric proves +1), so
    // this doubles as the negative control for the `reject_counter = 0` reset line.
    #[tokio::test(start_paused = true)]
    async fn a_committed_block_resets_the_reject_strike() {
        let source = "test-reject-reset";
        let before = reject_count(source);
        let non_linking = TestBlock::new(10, "0xB10bad", "0xWRONG_PARENT");
        let linking = TestBlock::new(10, "0xB10good", "0xEXPECTED_PARENT");
        let mut ds = build(source, vec![vec![non_linking], vec![linking]]);
        ds.set_position(10, Some("0xEXPECTED_PARENT"));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = ds.poll_next_event(&mut cx);

        match poll {
            Poll::Ready(DataEvent::Block { block, .. }) => assert_eq!(block.number(), 10),
            other => panic!(
                "expected the linking block to commit, got {:?}",
                matches!(other, Poll::Pending)
            )
        }
        assert_eq!(ds.endpoints[0].reject_counter, 0, "a committed block resets the strike");
        assert_eq!(
            reject_count(source) - before,
            1,
            "exactly one rejection preceded the accepted block"
        );
    }

    // An external reposition scopes out any strike accrued at the old position, holding the field's
    // "at the current position" contract. Negative control for the `set_position` reset: without it,
    // the old-position strike (and its backoff) would leak into the fresh position.
    #[tokio::test(start_paused = true)]
    async fn a_reposition_clears_the_reject_strike() {
        let source = "test-reposition-clears";
        let non_linking = TestBlock::new(10, "0xB10", "0xWRONG_PARENT");
        let mut ds = build(source, vec![vec![non_linking]]);
        ds.set_position(10, Some("0xEXPECTED_PARENT"));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let _ = ds.poll_next_event(&mut cx);
        assert!(
            ds.endpoints[0].reject_counter > 0,
            "precondition: a strike accrued at the old position"
        );
        assert!(
            matches!(ds.endpoints[0].state, EndpointState::Backoff(_)),
            "precondition: parked in backoff"
        );

        ds.set_position(20, Some("0xNEW_PARENT"));

        assert_eq!(
            ds.endpoints[0].reject_counter, 0,
            "a reposition clears the old-position strike"
        );
        assert!(
            matches!(ds.endpoints[0].state, EndpointState::Ready),
            "a reposition also cancels the backoff"
        );
    }

    // Multi-endpoint regression: a committed block advances the shared position, and a
    // linkage-reject strike is scoped to the position it was accrued at, so the commit must clear
    // every *other* endpoint's now-stale strike and cancel its parked backoff. Endpoint order is
    // broken-first: it rejects at position 10 and parks in backoff, then the healthy endpoint
    // commits block 10 and moves the position to 11. Before the fix the broken endpoint stayed
    // asleep for the old position's full backoff though the position had already moved — a
    // multi-endpoint failover regression; this is the negative control for the reset in
    // `poll_next_event`.
    #[tokio::test(start_paused = true)]
    async fn a_committed_block_clears_the_other_endpoints_stale_strike() {
        let broken = ReplayClient::new("xep-broken", vec![vec![TestBlock::new(10, "0xBAD", "0xWRONG_PARENT")]]);
        let healthy = ReplayClient::new(
            "xep-healthy",
            vec![vec![TestBlock::new(10, "0xGOOD", "0xEXPECTED_PARENT")]]
        );
        let mut ds = StandardDataSource::new(vec![broken, healthy], Ok as fn(TestBlock) -> anyhow::Result<TestBlock>);
        ds.set_position(10, Some("0xEXPECTED_PARENT"));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        match ds.poll_next_event(&mut cx) {
            Poll::Ready(DataEvent::Block { block, .. }) => assert_eq!(block.number(), 10),
            other => panic!(
                "expected the healthy endpoint to commit block 10, pending={}",
                matches!(other, Poll::Pending)
            )
        }

        assert_eq!(
            ds.get_next_block(),
            11,
            "the committed block advanced the shared position"
        );
        assert_eq!(
            ds.endpoints[0].reject_counter, 0,
            "the commit cleared the broken endpoint's stale old-position strike"
        );
        assert!(
            matches!(ds.endpoints[0].state, EndpointState::Ready),
            "and cancelled its parked backoff so it can rejoin at the new position at once"
        );
        assert!(
            matches!(ds.endpoints[1].state, EndpointState::Stream { .. }),
            "the committing endpoint is skipped by the reset and keeps its live stream"
        );
    }

    // The linkage-reject ladder (`reject_counter`) and the transport-error ladder
    // (`error_counter`) are independent: neither fault clears the other's strike. Load-bearing —
    // a source flapping between a transport blip and a linkage reject must still climb toward a
    // backoff rather than perpetually resetting itself. Drives an `Endpoint` directly so the two
    // paths are exercised in isolation.
    #[tokio::test(start_paused = true)]
    async fn the_reject_and_error_ladders_are_independent() {
        let mut ep = Endpoint {
            client: ReplayClient::new("test-ladders", vec![vec![]]),
            state: EndpointState::Ready,
            error_counter: 0,
            reject_counter: 0,
            last_committed_block: None
        };

        ep.on_reject("parent_hash_mismatch");
        ep.on_reject("parent_hash_mismatch");
        assert_eq!(ep.reject_counter, 2, "two rejects climb the reject ladder");

        ep.on_error(anyhow::anyhow!("transport blip"));
        assert_eq!(
            ep.reject_counter, 2,
            "a transport error must not clear the linkage-reject strike"
        );
        assert_eq!(ep.error_counter, 1, "on_error advances only its own ladder");

        ep.on_reject("parent_hash_mismatch");
        assert_eq!(ep.error_counter, 1, "a linkage reject must not clear the error strike");
        assert_eq!(ep.reject_counter, 3);
    }

    // The parked backoff actually resolves and the next reject escalates one rung. `start_paused`
    // freezes the 100 ms sleep until the clock is advanced; after it is, the endpoint re-requests,
    // rejects again, and installs the next (200 ms) rung. Covers the `EndpointState::Backoff`
    // resolution path the other tests only park in.
    #[tokio::test(start_paused = true)]
    async fn a_resolved_backoff_lets_the_next_reject_escalate() {
        let source = "test-escalate";
        let mut ds = build(source, vec![vec![TestBlock::new(10, "0xB10", "0xWRONG_PARENT")]]);
        ds.set_position(10, Some("0xEXPECTED_PARENT"));

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(ds.poll_next_event(&mut cx), Poll::Pending));
        assert_eq!(
            ds.endpoints[0].reject_counter, 2,
            "free retry then the first backoff rung"
        );
        assert!(matches!(ds.endpoints[0].state, EndpointState::Backoff(_)));

        tokio::time::advance(Duration::from_millis(101)).await;
        assert!(matches!(ds.poll_next_event(&mut cx), Poll::Pending));
        assert_eq!(
            ds.endpoints[0].reject_counter, 3,
            "the resolved backoff let the next reject install the next rung"
        );
        assert!(matches!(ds.endpoints[0].state, EndpointState::Backoff(_)));
    }
}
