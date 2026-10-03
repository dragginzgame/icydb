//! Module: metrics
//!
//! Responsibility: on-canister execution costs and canonical convergence diagnostics.
//! Does not own: endpoints, query identity, or storage inspection.
//! Boundary: one feature, one span, and one heap report.

#[cfg(feature = "metrics")]
mod state;

#[cfg(not(feature = "metrics"))]
use std::marker::PhantomData;

#[cfg(feature = "metrics")]
pub use state::{
    ConvergenceMetrics, EntityMetrics, InstructionMetrics, MetricsReport, SchemaLifecycleMetrics,
    metrics_report, metrics_reset_all,
};

/// Fixed semantic owners; these are observations, never execution choices.
pub(crate) enum ExecutionMetricsPhase {
    Lowering,
    Publication,
    RuntimeCompilation,
    Cardinality,
    StartupRecovery,
    JournalFold,
}

/// Instruction span for a maintained execution owner, including failed attempts.
pub(crate) struct ExecutionMetricsSpan {
    #[cfg(feature = "metrics")]
    phase: ExecutionMetricsPhase,
    #[cfg(feature = "metrics")]
    start: Option<u64>,
}

impl ExecutionMetricsSpan {
    #[cfg_attr(
        not(feature = "metrics"),
        expect(
            clippy::missing_const_for_fn,
            reason = "feature-on construction reads the IC execution mode and instruction counter"
        )
    )]
    pub(crate) fn new(phase: ExecutionMetricsPhase) -> Self {
        #[cfg(feature = "metrics")]
        {
            Self {
                phase,
                start: metrics_are_durable().then(crate::runtime::local_instruction_counter),
            }
        }
        #[cfg(not(feature = "metrics"))]
        {
            let _ = phase;
            Self {}
        }
    }
}

#[cfg(feature = "metrics")]
impl Drop for ExecutionMetricsSpan {
    fn drop(&mut self) {
        let Some(start) = self.start else {
            return;
        };
        state::record_owner_execution(
            &self.phase,
            crate::runtime::local_instruction_counter().saturating_sub(start),
        );
    }
}

/// Instruction span for work owned by exactly one accepted entity.
pub(crate) struct EntityMetricsSpan<'entity> {
    #[cfg(feature = "metrics")]
    entity_path: Option<&'entity str>,
    #[cfg(feature = "metrics")]
    start: u64,
    #[cfg(not(feature = "metrics"))]
    marker: PhantomData<&'entity str>,
}

impl<'entity> EntityMetricsSpan<'entity> {
    #[must_use]
    #[cfg_attr(
        not(feature = "metrics"),
        expect(
            clippy::missing_const_for_fn,
            reason = "feature-on construction reads the IC execution mode and instruction counter"
        )
    )]
    pub(crate) fn new(entity_path: &'entity str) -> Self {
        #[cfg(feature = "metrics")]
        {
            let observable = metrics_are_durable();
            Self {
                entity_path: observable.then_some(entity_path),
                start: if observable {
                    crate::runtime::local_instruction_counter()
                } else {
                    0
                },
            }
        }

        #[cfg(not(feature = "metrics"))]
        {
            let _ = entity_path;
            Self {
                marker: PhantomData,
            }
        }
    }
}

#[cfg(all(feature = "metrics", target_arch = "wasm32"))]
fn metrics_are_durable() -> bool {
    ic_cdk::api::in_replicated_execution()
}

#[cfg(all(feature = "metrics", not(target_arch = "wasm32")))]
const fn metrics_are_durable() -> bool {
    true
}

#[cfg(feature = "metrics")]
impl Drop for EntityMetricsSpan<'_> {
    fn drop(&mut self) {
        let Some(entity_path) = self.entity_path else {
            return;
        };
        state::record_entity_execution(
            entity_path,
            crate::runtime::local_instruction_counter().saturating_sub(self.start),
        );
    }
}

/// Observe a committed tail-control change; replayed or rejected attempts do not count.
#[cfg_attr(
    not(feature = "metrics"),
    expect(
        clippy::missing_const_for_fn,
        reason = "feature-on observation reads IC execution mode and records heap counters"
    )
)]
pub(crate) fn record_journal_movement(retirement: bool, debt: crate::db::ExactBacklogMeasurement) {
    #[cfg(feature = "metrics")]
    if metrics_are_durable() {
        state::record_journal_movement(retirement, debt);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = (retirement, debt);
}
