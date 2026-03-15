//! Module: executor::pipeline::operators::post_access::coordinator
//! Responsibility: plan coordination seam for post-access execution phases.
//! Does not own: terminal phase operator mechanics or executor entrypoint wiring.
//! Boundary: exposes one plan-owned coordinator consumed by post-access wrappers.

mod runtime;
#[cfg(test)]
mod safety;

use crate::db::executor::pipeline::contracts::PostAccessContract;

///
/// PostAccessPlan
///
/// Executor-owned post-access operation wrapper over one plan contract.
///

pub(super) struct PostAccessPlan<'a, K> {
    contract: PostAccessContract<'a, K>,
}

impl<'a, K> PostAccessPlan<'a, K> {
    pub(super) const fn new(contract: PostAccessContract<'a, K>) -> Self {
        Self { contract }
    }
}
