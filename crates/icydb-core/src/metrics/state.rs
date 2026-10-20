//! Module: metrics::state
//! Responsibility: on-canister execution counters and bounded reporting.
//! Does not own: endpoint attribution, query identity, or persisted metrics.
//! Boundary: bounded canonical debt plus one heap-only window of owner observations.

use crate::{metrics::ExecutionMetricsPhase, runtime::now_millis};

use ic_metrics::MeasurementAggregate;
use serde::Deserialize;
use std::{cell::RefCell, collections::BTreeMap};

/// Local instruction observations and their tracked quality for one owner.
///
/// Spans include failed attempts and may nest; totals are not exclusive
/// accounting and must not be summed or converted to cycles.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct InstructionMetrics {
    samples: u64,
    instructions_total: u128,
    instructions_max: u64,
    samples_saturated: bool,
    total_saturated: bool,
    instructions_mean: Option<u128>,
}

impl InstructionMetrics {
    /// Number of completed observation spans, including failed attempts.
    #[must_use]
    pub const fn samples(&self) -> u64 {
        self.samples
    }

    /// Wide saturating sum of observed local instructions, encoded as native Wire `u128`.
    #[must_use]
    pub const fn instructions_total(&self) -> u128 {
        self.instructions_total
    }

    /// Largest local instruction interval in this window.
    #[must_use]
    pub const fn instructions_max(&self) -> u64 {
        self.instructions_max
    }

    /// Whether the sample count actually overflowed, rather than reaching its cap.
    #[must_use]
    pub const fn samples_saturated(&self) -> bool {
        self.samples_saturated
    }

    /// Whether the instruction total actually overflowed.
    #[must_use]
    pub const fn total_saturated(&self) -> bool {
        self.total_saturated
    }

    /// Exact floor-rounded mean, unavailable for empty or overflowed observations.
    #[must_use]
    pub const fn instructions_mean(&self) -> Option<u128> {
        self.instructions_mean
    }
}

impl From<MeasurementAggregate> for InstructionMetrics {
    fn from(measurement: MeasurementAggregate) -> Self {
        Self {
            samples: measurement.samples(),
            instructions_total: measurement.total(),
            instructions_max: measurement.maximum().unwrap_or(0),
            samples_saturated: measurement.samples_overflowed(),
            total_saturated: measurement.total_overflowed(),
            instructions_mean: measurement.mean().ok().flatten(),
        }
    }
}

/// Five fixed, heap-only schema-owner counters in the existing metrics window.
///
/// Only replicated execution records work. Upgrade clears the window and
/// [`metrics_reset_all`] resets these counters together with entity observations.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
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
struct SchemaLifecycleMeasurements {
    lowering: MeasurementAggregate,
    publication: MeasurementAggregate,
    runtime_compilation: MeasurementAggregate,
    cardinality: MeasurementAggregate,
    startup_recovery: MeasurementAggregate,
}

impl SchemaLifecycleMeasurements {
    // Project only at the report boundary; the upstream aggregate remains the
    // sole recording and overflow authority for each heap observation window.
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

#[derive(Clone, Debug, Default)]
struct ConvergenceMeasurements {
    appended: crate::db::ExactBacklogMeasurement,
    retired: crate::db::ExactBacklogMeasurement,
    overflowed: bool,
    journal_fold: MeasurementAggregate,
}

impl ConvergenceMeasurements {
    fn snapshot(&self) -> ConvergenceMetrics {
        ConvergenceMetrics {
            appended: self.appended,
            retired: self.retired,
            overflowed: self.overflowed,
            journal_fold: self.journal_fold.into(),
        }
    }
}

#[derive(Clone, Debug)]
struct MetricsState {
    entities: BTreeMap<String, MeasurementAggregate>,
    schema_lifecycle: SchemaLifecycleMeasurements,
    convergence: ConvergenceMeasurements,
    window_start_ms: u64,
    window_id: Option<u64>,
}

impl Default for MetricsState {
    fn default() -> Self {
        Self {
            entities: BTreeMap::new(),
            schema_lifecycle: SchemaLifecycleMeasurements::default(),
            convergence: ConvergenceMeasurements::default(),
            window_start_ms: now_millis(),
            window_id: Some(0),
        }
    }
}

thread_local! {
    static STATE: RefCell<MetricsState> = RefCell::new(MetricsState::default());
}

/// Cost attributed to one accepted entity during the active metrics window.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
pub struct EntityMetrics {
    path: String,
    hits: u64,
    instructions_total: u128,
    instructions_max: u64,
    samples_saturated: bool,
    total_saturated: bool,
    instructions_mean: Option<u128>,
}

impl EntityMetrics {
    /// Accepted entity path.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Number of observed entity execution spans.
    #[must_use]
    pub const fn hits(&self) -> u64 {
        self.hits
    }

    /// Wide saturating sum of local instructions, encoded as native Wire `u128`.
    #[must_use]
    pub const fn instructions_total(&self) -> u128 {
        self.instructions_total
    }

    /// Largest local instruction delta attributed to one entity execution.
    #[must_use]
    pub const fn instructions_max(&self) -> u64 {
        self.instructions_max
    }

    /// Whether the observation count actually overflowed.
    #[must_use]
    pub const fn samples_saturated(&self) -> bool {
        self.samples_saturated
    }

    /// Whether the instruction total actually overflowed.
    #[must_use]
    pub const fn total_saturated(&self) -> bool {
        self.total_saturated
    }

    /// Producer-admitted floor-rounded mean, absent when empty or overflowed.
    #[must_use]
    pub const fn instructions_mean(&self) -> Option<u128> {
        self.instructions_mean
    }
}

/// Bounded snapshot of a volatile, heap-only metrics window.
///
/// Selects a lexical prefix of entity paths before sorting that subset by cost.
/// This is not a global top-cost report. Counters and window IDs are lost on
/// upgrade/restart; consumers must treat heap replacement as a discontinuity.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, serde::Serialize)]
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
        let convergence = &mut state.convergence;
        let total = if retirement {
            &mut convergence.retired
        } else {
            &mut convergence.appended
        };
        for (before, added) in [
            (total.batch_count(), debt.batch_count()),
            (total.record_count(), debt.record_count()),
            (total.encoded_batch_bytes(), debt.encoded_batch_bytes()),
        ] {
            convergence.overflowed |= before.checked_add(added).is_none();
        }
        *total = crate::db::ExactBacklogMeasurement::new(
            total.batch_count().saturating_add(debt.batch_count()),
            total.record_count().saturating_add(debt.record_count()),
            total
                .encoded_batch_bytes()
                .saturating_add(debt.encoded_batch_bytes()),
        );
    });
}

pub(super) fn record_entity_execution(entity_path: &str, instructions: u64) {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
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
                hits: counter.samples(),
                instructions_total: counter.total(),
                instructions_max: counter.maximum().unwrap_or(0),
                samples_saturated: counter.samples_overflowed(),
                total_saturated: counter.total_overflowed(),
                instructions_mean: counter.mean().ok().flatten(),
            })
            .collect::<Vec<_>>();
        entities.sort_by(|left, right| {
            right
                .instructions_total
                .cmp(&left.instructions_total)
                .then_with(|| right.hits.cmp(&left.hits))
                .then_with(|| left.path.cmp(&right.path))
        });

        MetricsReport {
            window_id: state.window_id,
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
        state.window_id = state.window_id.and_then(|id| id.checked_add(1));
        state.entities.clear();
        state.schema_lifecycle = SchemaLifecycleMeasurements::default();
        state.convergence = ConvergenceMeasurements::default();
        state.window_start_ms = now_millis();
    });
}

#[cfg(test)]
mod tests {
    use super::{
        MetricsReport, STATE, SchemaLifecycleMetrics, metrics_reset_all, metrics_snapshot,
        record_entity_execution, record_owner_execution,
    };
    use crate::metrics::{ExecutionMetricsPhase, ExecutionMetricsSpan};

    #[test]
    fn repeated_entity_observations_preserve_distinct_paths_and_reset() {
        metrics_reset_all();
        record_entity_execution("entity::a", 0);
        record_entity_execution("entity::b", 7);
        record_entity_execution("entity::a", u64::MAX);
        record_entity_execution("entity::a", 1);
        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        assert_eq!(report.total_entities(), 2);
        let a = report
            .entities()
            .iter()
            .find(|entry| entry.path() == "entity::a")
            .expect("observed entity");
        assert_eq!(a.hits(), 3);
        assert_eq!(a.instructions_total(), u128::from(u64::MAX) + 1);
        assert_eq!(a.instructions_max(), u64::MAX);
        let b = report
            .entities()
            .iter()
            .find(|entry| entry.path() == "entity::b")
            .expect("distinct entity");
        assert_eq!(b.hits(), 1);
        assert_eq!(b.instructions_total(), 7);
        metrics_reset_all();
        record_entity_execution("entity::a", 2);
        let reset = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        assert_ne!(reset.window_id(), report.window_id());
        assert_eq!(reset.total_entities(), 1);
        assert_eq!(reset.entities()[0].hits(), 1);
        assert_eq!(reset.entities()[0].instructions_total(), 2);
    }

    #[test]
    fn convergence_movements_mark_overflow_and_reset_independently_of_debt() {
        use crate::db::ExactBacklogMeasurement as Debt;
        metrics_reset_all();
        super::record_journal_movement(false, Debt::new(u64::MAX, 2, 3));
        super::record_journal_movement(false, Debt::new(1, 4, 5));
        super::record_journal_movement(true, Debt::new(1, 2, 3));
        record_owner_execution(&ExecutionMetricsPhase::JournalFold, 42);
        let debt = Debt::new(7, 8, 9);
        let report = metrics_snapshot(debt);
        assert!(report.convergence().overflowed());
        assert_eq!(report.convergence().appended(), Debt::new(u64::MAX, 6, 8));
        assert_eq!(report.convergence().retired(), Debt::new(1, 2, 3));
        assert_eq!(report.convergence().journal_fold().instructions_total(), 42);
        let encoded =
            icydb_schema::wire::encode_one(&report).expect("convergence report should encode");
        let decoded: MetricsReport =
            icydb_schema::wire::decode_one(&encoded).expect("report should decode");
        assert_eq!(decoded, report);
        metrics_reset_all();
        let reset = metrics_snapshot(debt);
        assert_eq!(reset.journal_debt(), debt);
        assert_eq!(reset.convergence(), &super::ConvergenceMetrics::default());
        assert_ne!(reset.window_id(), report.window_id());
    }

    #[test]
    fn lifecycle_observations_are_separate_saturating_and_reset_with_the_window() {
        metrics_reset_all();
        for (phase, instructions) in [
            (ExecutionMetricsPhase::Lowering, u64::MAX),
            (ExecutionMetricsPhase::Lowering, 1),
            (ExecutionMetricsPhase::Publication, 12),
            (ExecutionMetricsPhase::RuntimeCompilation, 23),
            (ExecutionMetricsPhase::Cardinality, 34),
            (ExecutionMetricsPhase::StartupRecovery, 45),
        ] {
            record_owner_execution(&phase, instructions);
        }
        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        let lifecycle = report.schema_lifecycle();
        assert_eq!(lifecycle.lowering().samples(), 2);
        assert_eq!(
            lifecycle.lowering().instructions_total(),
            u128::from(u64::MAX) + 1
        );
        assert_eq!(lifecycle.lowering().instructions_max(), u64::MAX);
        assert_eq!(lifecycle.publication().instructions_total(), 12);
        assert_eq!(lifecycle.runtime_compilation().instructions_total(), 23);
        assert_eq!(lifecycle.cardinality().instructions_total(), 34);
        assert_eq!(lifecycle.startup_recovery().instructions_total(), 45);
        assert_eq!(report.total_entities(), 0);
        let encoded =
            icydb_schema::wire::encode_one(&report).expect("encode observed lifecycle report");
        let decoded: MetricsReport =
            icydb_schema::wire::decode_one(&encoded).expect("decode lifecycle report");
        assert_eq!(decoded, report);
        metrics_reset_all();
        assert_eq!(
            metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY).schema_lifecycle(),
            &SchemaLifecycleMetrics::default()
        );
        assert_ne!(
            metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY).window_id(),
            report.window_id()
        );
    }

    #[test]
    fn tracked_quality_and_exact_means_survive_every_report_boundary() {
        use super::{ConvergenceMeasurements, SchemaLifecycleMeasurements};
        use ic_metrics::MeasurementAggregate;

        let exact_count =
            MeasurementAggregate::from_parts(u64::MAX, 0, Some(0), false, false).unwrap();
        let mut overflowed_count = exact_count;
        overflowed_count.record(0);
        let mut exact_total =
            MeasurementAggregate::from_parts(u64::MAX, u128::MAX - 1, Some(u64::MAX), true, false)
                .unwrap();
        exact_total.record(1);
        let mut overflowed_total = exact_total;
        overflowed_total.record(1);
        let mut measured_zero = MeasurementAggregate::EMPTY;
        measured_zero.record(0);
        for (measurement, samples_saturated, total_saturated, mean) in [
            (MeasurementAggregate::EMPTY, false, false, None),
            (measured_zero, false, false, Some(0)),
            (exact_count, false, false, Some(0)),
            (overflowed_count, true, false, None),
            (exact_total, true, false, None),
            (overflowed_total, true, true, None),
        ] {
            metrics_reset_all();
            STATE.with_borrow_mut(|state| {
                state.entities.insert("entity".into(), measurement);
                state.schema_lifecycle = SchemaLifecycleMeasurements {
                    lowering: measurement,
                    publication: measurement,
                    runtime_compilation: measurement,
                    cardinality: measurement,
                    startup_recovery: measurement,
                };
                state.convergence = ConvergenceMeasurements {
                    journal_fold: measurement,
                    ..ConvergenceMeasurements::default()
                };
            });
            let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
            let encoded = icydb_schema::wire::encode_one(&report).unwrap();
            let decoded: MetricsReport = icydb_schema::wire::decode_one(&encoded).unwrap();
            assert_eq!(decoded, report);
            let entity = &decoded.entities()[0];
            assert_eq!(entity.samples_saturated(), samples_saturated);
            assert_eq!(entity.total_saturated(), total_saturated);
            assert_eq!(entity.instructions_mean(), mean);
            let lifecycle = decoded.schema_lifecycle();
            for owner in [
                lifecycle.lowering(),
                lifecycle.publication(),
                lifecycle.runtime_compilation(),
                lifecycle.cardinality(),
                lifecycle.startup_recovery(),
                decoded.convergence().journal_fold(),
            ] {
                assert_eq!(owner.samples(), measurement.samples());
                assert_eq!(owner.instructions_total(), measurement.total());
                assert_eq!(owner.instructions_max(), measurement.maximum().unwrap_or(0));
                assert_eq!(owner.samples_saturated(), samples_saturated);
                assert_eq!(owner.total_saturated(), total_saturated);
                assert_eq!(owner.instructions_mean(), mean);
            }
        }
        metrics_reset_all();
    }

    #[test]
    fn failed_owner_attempts_are_observed_when_the_span_exits() {
        fn failed_attempt() -> Result<(), ()> {
            let _span = ExecutionMetricsSpan::new(ExecutionMetricsPhase::Lowering);
            Err(())
        }
        metrics_reset_all();
        assert!(failed_attempt().is_err());
        assert_eq!(
            metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY)
                .schema_lifecycle()
                .lowering()
                .samples(),
            1
        );
        assert_eq!(
            metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY)
                .schema_lifecycle()
                .publication()
                .samples(),
            0
        );
    }

    #[test]
    fn source_selection_is_a_bounded_lexical_prefix_not_global_top_cost() {
        metrics_reset_all();
        for index in (0..4096).rev() {
            record_entity_execution(&format!("entity::{index:04}"), index);
        }
        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        assert_eq!(report.total_entities(), 4096);
        assert_eq!(report.entities().len(), MetricsReport::MAX_ENTITIES);
        assert_eq!(report.entities()[0].path(), "entity::0063");
        assert_eq!(report.entities()[63].path(), "entity::0000");
        assert_eq!(report, metrics_report_with_end_time(report.window_end_ms()));
    }

    // Ignore wall-clock movement when checking that observing does not mutate counters.
    fn metrics_report_with_end_time(end_ms: u64) -> MetricsReport {
        let mut report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        report.window_end_ms = end_ms;
        report
    }

    #[test]
    fn source_selection_accepts_exact_limits_and_reports_omissions() {
        metrics_reset_all();
        for index in 0..MetricsReport::MAX_ENTITIES {
            record_entity_execution(&format!("entity::{index:04}"), 1);
        }
        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        assert_eq!(report.entities().len() as u64, report.total_entities());
        record_entity_execution("zz-extra", 100);
        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        assert_eq!(report.entities().len(), MetricsReport::MAX_ENTITIES);
        assert_eq!(report.total_entities(), 65);

        metrics_reset_all();
        let exact_path = "a".repeat(MetricsReport::MAX_PATH_BYTES);
        record_entity_execution(&exact_path, 1);
        record_entity_execution("z", 1);
        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        assert_eq!(report.entities().len(), 1);
        assert_eq!(report.entities()[0].path(), exact_path);
        assert_eq!(report.total_entities(), 2);

        metrics_reset_all();
        record_entity_execution(&"a".repeat(MetricsReport::MAX_PATH_BYTES + 1), 1);
        record_entity_execution("z", 1);
        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        assert!(report.entities().is_empty());
        assert_eq!(report.total_entities(), 2);

        metrics_reset_all();
        // Count UTF-8 bytes across paths, not characters or a per-path allowance.
        record_entity_execution(&"a".repeat(MetricsReport::MAX_PATH_BYTES - 2), 1);
        record_entity_execution("é", 1);
        record_entity_execution("ê", 1);
        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        assert_eq!(report.entities().len(), 2);
        assert_eq!(report.total_entities(), 3);
        assert_eq!(
            report
                .entities()
                .iter()
                .map(|row| row.path().len())
                .sum::<usize>(),
            MetricsReport::MAX_PATH_BYTES
        );
    }

    #[test]
    fn reset_identity_advances_without_relying_on_clock_resolution() {
        metrics_reset_all();
        let first = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        record_entity_execution("entity", 1);
        metrics_reset_all();
        let second = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        assert_eq!(second.window_id(), first.window_id().map(|id| id + 1));
        assert_eq!(second.total_entities(), 0);
        assert!(second.entities().is_empty());
    }

    #[test]
    fn exhausted_reset_identity_never_wraps_or_reappears() {
        STATE.with(|state| state.borrow_mut().window_id = Some(u64::MAX));
        for _ in 0..2 {
            record_entity_execution("entity", 1);
            metrics_reset_all();
            assert_eq!(
                metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY).window_id(),
                None
            );
            assert_eq!(
                metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY).total_entities(),
                0
            );
        }
        // Restore this thread's fixture without making production IDs reusable.
        STATE.with(|state| *state.borrow_mut() = super::MetricsState::default());
    }

    #[test]
    fn report_round_trips_current_wire_shape() {
        metrics_reset_all();
        for _ in 0..2 {
            record_entity_execution("entity", u64::MAX);
            record_owner_execution(&ExecutionMetricsPhase::Lowering, u64::MAX);
            record_owner_execution(&ExecutionMetricsPhase::JournalFold, u64::MAX);
        }
        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        let total = u128::from(u64::MAX) * 2;
        assert_eq!(report.entities()[0].instructions_total(), total);
        assert_eq!(
            report.schema_lifecycle().lowering().instructions_total(),
            total
        );
        assert_eq!(
            report.convergence().journal_fold().instructions_total(),
            total
        );
        let encoded = icydb_schema::wire::encode_one(&report).expect("encode current report");
        let decoded: MetricsReport =
            icydb_schema::wire::decode_one(&encoded).expect("decode current report");
        assert_eq!(decoded, report);
    }

    #[test]
    fn report_orders_entities_by_total_cost_then_hits_then_path() {
        metrics_reset_all();
        record_entity_execution("store::beta", 10);
        record_entity_execution("store::alpha", 5);
        record_entity_execution("store::alpha", 5);
        record_entity_execution("store::gamma", 10);

        let report = metrics_snapshot(crate::db::ExactBacklogMeasurement::EMPTY);
        let paths = report
            .entities()
            .iter()
            .map(super::EntityMetrics::path)
            .collect::<Vec<_>>();

        assert_eq!(paths, vec!["store::alpha", "store::beta", "store::gamma"]);
        assert_eq!(report.entities()[0].hits(), 2);
        assert_eq!(report.entities()[0].instructions_total(), 10);
        assert_eq!(report.entities()[0].instructions_max(), 5);
    }
}
