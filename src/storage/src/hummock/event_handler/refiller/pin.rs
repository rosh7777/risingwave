// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::Entry;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Weak};

use futures::future::try_join_all;
use parking_lot::Mutex;
use risingwave_pb::id::TableId;
use tokio::sync::{Notify, Semaphore, oneshot};
use tokio::task::JoinHandle;

use super::CacheRefillConfig;
use crate::hummock::SstableStoreRef;
use crate::hummock::event_handler::ReadOnlyRwLockRef;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache::PinCache;
use crate::hummock::pin_cache_refill::PinCacheRefillPlan;
use crate::hummock::pin_cache_refill::executor::PinCacheRefillExecutor;
use crate::hummock::table_cache_metadata::TableCacheMetadata;

/// One received event's fixed target and policy-bounded intent, before batch admission.
struct Request {
    target: PinnedVersion,
    plan: PinCacheRefillPlan,
    completion: oneshot::Sender<bool>,
}

/// Batches queued intent independently of version publication. The executor owns all
/// admitted work, including cancellation; this worker only waits and reports endpoints.
pub(super) struct PinCacheRefiller {
    pub(super) executor: Arc<PinCacheRefillExecutor>,
    pending: Arc<Mutex<VecDeque<Request>>>,
    wake: Arc<Notify>,
    worker: JoinHandle<()>,
}

impl Drop for PinCacheRefiller {
    fn drop(&mut self) {
        self.worker.abort();
        self.pending.lock().clear();
    }
}

impl PinCacheRefiller {
    pub fn new(
        config: Arc<CacheRefillConfig>,
        store: SstableStoreRef,
        metadata: ReadOnlyRwLockRef<TableCacheMetadata>,
        concurrency: Arc<Semaphore>,
    ) -> Self {
        let executor = Arc::new(PinCacheRefillExecutor::new(store.clone(), concurrency));
        let cache = store
            .pin_cache()
            .expect("Pin refill requires a local backend")
            .clone();
        let pending = Arc::new(Mutex::new(VecDeque::new()));
        let wake = Arc::new(Notify::new());
        let worker = tokio::spawn(Self::run(
            pending.clone(),
            wake.clone(),
            metadata,
            cache,
            Arc::downgrade(&executor),
            config,
        ));
        Self {
            executor,
            pending,
            wake,
            worker,
        }
    }

    /// Enqueues event-scoped intent. The consumer selects using current CN metadata;
    /// dropping this receiver does not cancel queued or submitted work.
    pub fn submit(
        &self,
        plan: PinCacheRefillPlan,
        target: PinnedVersion,
    ) -> oneshot::Receiver<bool> {
        let (completion, receiver) = oneshot::channel();
        self.pending.lock().push_back(Request {
            target,
            plan,
            completion,
        });
        self.wake.notify_one();
        receiver
    }

    async fn run(
        pending: Arc<Mutex<VecDeque<Request>>>,
        wake: Arc<Notify>,
        metadata: ReadOnlyRwLockRef<TableCacheMetadata>,
        cache: Arc<PinCache>,
        executor: Weak<PinCacheRefillExecutor>,
        config: Arc<CacheRefillConfig>,
    ) {
        loop {
            let selected = {
                let metadata = metadata.read();
                let mut pending = pending.lock();
                if pending.is_empty() {
                    None
                } else {
                    let count = config.pin_cache_max_batch_size.max(1).min(pending.len());
                    let target = pending[count - 1].target.clone();
                    let (plans, completions): (Vec<_>, Vec<_>) = pending
                        .drain(..count)
                        .map(|request| (request.plan, request.completion))
                        .unzip();
                    let plan = merge_pin_refill_plans(plans, &target, metadata.pinned_tables());
                    let Some(executor) = executor.upgrade() else {
                        return;
                    };
                    // Selection, admission and submission share the metadata guard. A policy
                    // or ownership change therefore cancels executor work, with no preparation gap.
                    let downloads = plan
                        .objects
                        .into_values()
                        .filter_map(|mut object| {
                            object
                                .admitted_tables
                                .retain(|table| metadata.ownership().has_vnodes(table));
                            if object.admitted_tables.is_empty()
                                || cache.get(object.source_sst.object_id).is_some()
                            {
                                return None;
                            }
                            cache
                                .prepare_refill(object.source_sst.object_id)
                                .map(|token| executor.submit(&object, token))
                        })
                        .collect::<Vec<_>>();
                    Some((target.id(), completions, downloads))
                }
            };
            let Some((target_id, completions, downloads)) = selected else {
                // notify_one retains a permit if submission races the empty check.
                wake.notified().await;
                continue;
            };
            let refill = try_join_all(
                downloads
                    .into_iter()
                    .map(|download| async move { download.await.then_some(()).ok_or(()) }),
            );
            if tokio::time::timeout(config.timeout, refill).await.is_err() {
                tracing::warn!(version_id = %target_id, "Pin cache refill timed out");
            }
            // Failure, cancellation and timeout retain the selected batch's endpoint.
            let count = completions.len();
            for (index, sender) in completions.into_iter().enumerate() {
                let _ = sender.send(index + 1 == count);
            }
            // Empty commits can finish without an I/O await. Let other work run.
            tokio::task::yield_now().await;
        }
    }

    #[cfg(test)]
    pub(super) fn has_pending_requests(&self) -> bool {
        !self.pending.lock().is_empty()
    }

    /// Called under the metadata write guard, serializing with admission and submission.
    /// Pending ownership is read at selection; Pin withdrawal permanently narrows intent.
    pub fn invalidate_refills(
        &self,
        changed_tables: &HashSet<TableId>,
        pinned_tables: &HashSet<TableId>,
    ) {
        if changed_tables.is_empty() {
            return;
        }
        if changed_tables
            .iter()
            .any(|table| !pinned_tables.contains(table))
        {
            for request in &mut *self.pending.lock() {
                request.plan.retain_tables(pinned_tables);
            }
        }
        // Executor work can outlive a batch timeout, so it is always invalidated directly.
        self.executor.cancel_tables(changed_tables);
    }

    pub fn cancel_objects(
        &self,
        objects: impl IntoIterator<Item = risingwave_hummock_sdk::HummockSstableObjectId>,
    ) {
        self.executor.cancel_objects(objects);
    }
}

// Candidate composition is independent of backend registration and download preparation.
// The caller keeps all selected events unpublished until the final target can be published.
fn merge_pin_refill_plans(
    plans: Vec<PinCacheRefillPlan>,
    target: &PinnedVersion,
    pinned_tables: &HashSet<TableId>,
) -> PinCacheRefillPlan {
    let merged_events = plans.len();
    let mut pin_plan = PinCacheRefillPlan::default();
    // Policy withdrawals already narrowed every pending plan before selection.
    for plan in plans {
        for (object, object_plan) in plan.objects {
            match pin_plan.objects.entry(object) {
                Entry::Occupied(mut entry) => {
                    entry
                        .get_mut()
                        .admitted_tables
                        .extend(object_plan.admitted_tables);
                }
                Entry::Vacant(entry) => {
                    entry.insert(object_plan);
                }
            }
        }
    }
    // A single request was already filtered against this immutable target at receipt.
    // Only merging requests can retire a candidate in a later target version.
    if merged_events > 1 {
        pin_plan.retain_in_version(target, pinned_tables);
    }
    pin_plan
}

#[cfg(test)]
mod tests;
