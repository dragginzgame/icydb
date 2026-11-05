//! Module: metrics::state
//! Responsibility: on-canister execution counters and bounded reporting.
//! Does not own: endpoint attribution, query identity, or persisted metrics.
//! Boundary: bounded canonical debt plus one heap-only window of owner observations.

use crate::{
    metrics::{ExecutionMetricsPhase, InstructionMetrics},
    runtime::now_millis,
};

use ic_metrics::{MeasurementAggregate, MeasurementCounter, MeasurementWindow};
use serde::Deserialize;
use std::{cell::RefCell, collections::BTreeMap};

/// Five fixed, heap-only schema-owner counters in the existing metrics window.
///
/// Only replicated execution records work. Upgrade clears the window and
/// [`metrics_reset_all`] resets these counters together with entity observations.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct SchemaLifecycleMetrics {
    lowering: InstructionMetrics,
    publication: InstructionMetrics,
    runtime_compilation: InstructionMetrics,
    cardinality: InstructionMetrics,
    startup_recovery: InstructionMetrics,
}

impl SchemaLifecycleMetrics {
    /// Candidate lowering/preflight; excludes facade decode and lineage preparation.
    #[must_use]
    pub const fn lowering(&self) -> &InstructionMetrics {
        &self.lowering
    }

    /// Ordinary application lineage/publication preparation and compound commit.
    #[must_use]
    pub const fn publication(&self) -> &InstructionMetrics {
        &self.publication
    }

    /// Cold accepted database-wide runtime-root compilation; excludes cache hits.
    #[must_use]
    pub const fn runtime_compilation(&self) -> &InstructionMetrics {
        &self.runtime_compilation
    }

    /// Startup cardinality driver pages, including authority checks and quiescence.
    #[must_use]
    pub const fn cardinality(&self) -> &InstructionMetrics {
        &self.cardinality
    }

    /// Shared startup recovery pages, including journal folding and failed attempts.
    /// Excludes subsequent schema handoff, runtime compilation and cardinality work.
    #[must_use]
    pub const fn startup_recovery(&self) -> &InstructionMetrics {
        &self.startup_recovery
    }
}

/// Heap-window journal movements recorded only after control publication.
///
/// Values count engine batches, records and encoded envelopes. Replay retries do
/// not append twice. Reset/restart clears these counters, never persisted debt.
/// Fold instructions may nest inside startup recovery and are not additive costs.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct ConvergenceMetrics {
    appended: crate::db::ExactBacklogMeasurement,
    retired: crate::db::ExactBacklogMeasurement,
    overflowed: bool,
    journal_fold: InstructionMetrics,
}

impl ConvergenceMetrics {
    /// Journal contributions published during this metrics window.
    #[must_use]
    pub const fn appended(&self) -> crate::db::ExactBacklogMeasurement {
        self.appended
    }
    /// Complete batches retired after canonical fold publication in this window.
    #[must_use]
    pub const fn retired(&self) -> crate::db::ExactBacklogMeasurement {
        self.retired
    }
    /// Saturation makes these window movements unsuitable for exact conservation.
    #[must_use]
    pub const fn overflowed(&self) -> bool {
        self.overflowed
    }
    /// Actual batch-fold instruction spans, including failed attempts.
    #[must_use]
    pub const fn journal_fold(&self) -> &InstructionMetrics {
        &self.journal_fold
    }
}

#[derive(Clone, Debug, Default)]
struct ConvergenceMeasurements {
    // Dimensions are batches, records and encoded bytes, in report order.
    appended: [MeasurementCounter; 3],
    retired: [MeasurementCounter; 3],
    journal_fold: MeasurementAggregate,
}

impl ConvergenceMeasurements {
    fn snapshot(&self) -> ConvergenceMetrics {
        // Upstream owns accumulation and overflow; this boundary only narrows
        // its wide counters to the maintained u64 report contract.
        let mut overflowed = false;
        let mut project = |counters: [MeasurementCounter; 3]| {
            let [batches, records, bytes] = counters.map(|counter| {
                overflowed |= counter.total_overflowed();
                u64::try_from(counter.total()).unwrap_or_else(|_| {
                    overflowed = true;
                    u64::MAX
                })
            });
            crate::db::ExactBacklogMeasurement::new(batches, records, bytes)
        };
        let appended = project(self.appended);
        let retired = project(self.retired);
        ConvergenceMetrics {
            appended,
            retired,
            overflowed,
            journal_fold: self.journal_fold.into(),
        }
    }
}

// Recording stays independent of the serialized frontend snapshot.
#[derive(Clone, Debug, Default)]
struct SchemaLifecycleMeasurements {
    lowering: MeasurementAggregate,
    publication: MeasurementAggregate,
    runtime_compilation: MeasurementAggregate,
    cardinality: MeasurementAggregate,
    startup_recovery: MeasurementAggregate,
}

impl SchemaLifecycleMeasurements {
    fn snapshot(&self) -> SchemaLifecycleMetrics {
        SchemaLifecycleMetrics {
            lowering: self.lowering.into(),
            publication: self.publication.into(),
            runtime_compilation: self.runtime_compilation.into(),
            cardinality: self.cardinality.into(),
            startup_recovery: self.startup_recovery.into(),
        }
    }
}

#[derive(Clone, Debug)]
struct MetricsState {
    entities: BTreeMap<String, MeasurementAggregate>,
    schema_lifecycle: SchemaLifecycleMeasurements,
    convergence: ConvergenceMeasurements,
    window_start_ms: u64,
}

impl Default for MetricsState {
    fn default() -> Self {
        Self {
            entities: BTreeMap::new(),
            schema_lifecycle: SchemaLifecycleMeasurements::default(),
            convergence: ConvergenceMeasurements::default(),
            window_start_ms: now_millis(),
        }
    }
}

thread_local! {
    static STATE: RefCell<MeasurementWindow<MetricsState>> = RefCell::new(MeasurementWindow::new(MetricsState::default()));
}

/// Cost attributed to one accepted entity during the active metrics window.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct EntityMetrics {
    path: String,
    instructions: InstructionMetrics,
}

impl EntityMetrics {
    /// Accepted entity path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Instruction observations with canonical sample, maximum and overflow quality.
    #[must_use]
    pub const fn instructions(&self) -> &InstructionMetrics {
        &self.instructions
    }
}

/// Bounded snapshot of a volatile, heap-only metrics window.
///
/// Selects a lexical prefix of entity paths before sorting that subset by cost.
/// This is not a global top-cost report. Counters and window IDs are lost on
/// upgrade/restart; consumers must treat heap replacement as a discontinuity.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
#[cfg_attr(
    all(feature = "wire-schema", not(target_family = "wasm")),
    derive(ic_wire::Wire)
)]
pub struct MetricsReport {
    window_id: Option<u64>,
    window_start_ms: u64,
    window_end_ms: u64,
    total_entities: u64,
    entities: Vec<EntityMetrics>,
    schema_lifecycle: SchemaLifecycleMetrics,
    journal_debt: crate::db::ExactBacklogMeasurement,
    convergence: ConvergenceMetrics,
}

impl MetricsReport {
    /// Maximum number of source entities selected by one report.
    pub const MAX_ENTITIES: usize = 64;

    /// Maximum total UTF-8 path bytes copied into one report.
    pub const MAX_PATH_BYTES: usize = 64 * 1024;

    /// Reset sequence, unique only within the current heap lifetime.
    ///
    /// `None` means identity is unavailable (including sequence exhaustion).
    /// Never use two absent IDs as evidence of a shared window. Even equal
    /// present IDs cannot establish continuity across upgrades or restarts.
    #[must_use]
    pub const fn window_id(&self) -> Option<u64> {
        self.window_id
    }

    /// Number of accumulated entities before source selection, including omitted rows.
    #[must_use]
    pub const fn total_entities(&self) -> u64 {
        self.total_entities
    }

    /// Millisecond timestamp at which this metrics window began.
    #[must_use]
    pub const fn window_start_ms(&self) -> u64 {
        self.window_start_ms
    }

    /// Millisecond timestamp at which this report was read.
    #[must_use]
    pub const fn window_end_ms(&self) -> u64 {
        self.window_end_ms
    }

    /// Selected observations, ordered by descending total cost, descending hits,
    /// then ascending path. Fewer rows than `total_entities()` means partial coverage.
    #[must_use]
    pub const fn entities(&self) -> &[EntityMetrics] {
        self.entities.as_slice()
    }

    /// Exact current journal debt, read fallibly from admission's authoritative controls.
    /// It survives metrics resets and is not an estimate from timers or application calls.
    #[must_use]
    pub const fn journal_debt(&self) -> crate::db::ExactBacklogMeasurement {
        self.journal_debt
    }

    /// Published append/retirement and batch-fold observations in this heap window.
    #[must_use]
    pub const fn convergence(&self) -> &ConvergenceMetrics {
        &self.convergence
    }

    /// Fixed schema-owner observations sharing this report's heap-local window.
    #[must_use]
    pub const fn schema_lifecycle(&self) -> &SchemaLifecycleMetrics {
        &self.schema_lifecycle
    }
}

pub(super) fn record_owner_execution(phase: &ExecutionMetricsPhase, instructions: u64) {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let state = state.state_mut();
        let lifecycle = &mut state.schema_lifecycle;
        let counter = match phase {
            ExecutionMetricsPhase::Lowering => &mut lifecycle.lowering,
            ExecutionMetricsPhase::Publication => &mut lifecycle.publication,
            ExecutionMetricsPhase::RuntimeCompilation => &mut lifecycle.runtime_compilation,
            ExecutionMetricsPhase::Cardinality => &mut lifecycle.cardinality,
            ExecutionMetricsPhase::StartupRecovery => &mut lifecycle.startup_recovery,
            ExecutionMetricsPhase::JournalFold => &mut state.convergence.journal_fold,
        };
        counter.record(instructions);
    });
}

pub(super) fn record_journal_movement(retirement: bool, debt: crate::db::ExactBacklogMeasurement) {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let convergence = &mut state.state_mut().convergence;
        let total = if retirement {
            &mut convergence.retired
        } else {
            &mut convergence.appended
        };
        for (counter, added) in total.iter_mut().zip([
            debt.batch_count(),
            debt.record_count(),
            debt.encoded_batch_bytes(),
        ]) {
            counter.add(u128::from(added));
        }
    });
}

pub(super) fn record_entity_execution(entity_path: &str, instructions: u64) {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let state = state.state_mut();
        // Repeated observations borrow the existing path; only a new owner allocates.
        let counter = if let Some(counter) = state.entities.get_mut(entity_path) {
            counter
        } else {
            state.entities.entry(entity_path.to_owned()).or_default()
        };
        counter.record(instructions);
    });
}

/// Snapshot the current on-canister metrics window.
///
/// Visits at most [`MetricsReport::MAX_ENTITIES`] source entries in ascending
/// path order, stopping before the first path exceeding the remaining byte
/// allowance. Only selected rows are copied and sorted. Does not bound retained
/// accumulator size, scan database rows, reset counters or sample IC instructions.
/// Journal debt reads one control per entry in the bounded persisted allocation registry.
///
/// # Errors
///
/// Returns the canonical control/registry error if journal debt cannot be read.
/// An unavailable or malformed control is never reported as zero debt.
pub fn metrics_report() -> Result<MetricsReport, crate::error::InternalError> {
    let debt = crate::db::diagnostics::journal_debt()?;
    Ok(metrics_snapshot(debt))
}

fn metrics_snapshot(journal_debt: crate::db::ExactBacklogMeasurement) -> MetricsReport {
    STATE.with(|state| {
        let state = state.borrow();
        let window_id = state.sequence();
        let state = state.state();
        let mut remaining_path_bytes = MetricsReport::MAX_PATH_BYTES;
        let mut entities = state
            .entities
            .iter()
            .take(MetricsReport::MAX_ENTITIES)
            .take_while(|(path, _)| {
                if path.len() > remaining_path_bytes {
                    return false;
                }
                remaining_path_bytes -= path.len();
                true
            })
            .map(|(path, counter)| EntityMetrics {
                path: path.clone(),
                instructions: (*counter).into(),
            })
            .collect::<Vec<_>>();
        entities.sort_by(|left, right| {
            right
                .instructions()
                .total()
                .cmp(&left.instructions().total())
                .then_with(|| {
                    right
                        .instructions()
                        .samples()
                        .cmp(&left.instructions().samples())
                })
                .then_with(|| left.path.cmp(&right.path))
        });

        MetricsReport {
            window_id,
            window_start_ms: state.window_start_ms,
            window_end_ms: now_millis(),
            total_entities: state.entities.len() as u64,
            entities,
            schema_lifecycle: state.schema_lifecycle.snapshot(),
            journal_debt,
            convergence: state.convergence.snapshot(),
        }
    })
}

/// Reset the heap-only metrics window.
///
/// Advances the heap-local identity even when timestamps are equal. On sequence
/// exhaustion, identity remains unavailable until heap replacement; it never wraps.
pub fn metrics_reset_all() {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.reset(MetricsState::default());
    });
}

#[cfg(test)]
mod tests;
