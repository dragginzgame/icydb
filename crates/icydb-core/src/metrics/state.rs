//! Module: metrics::state
//! Responsibility: on-canister execution counters and bounded reporting.
//! Does not own: endpoint attribution, query identity, or persisted metrics.
//! Boundary: one heap-only window of entity paths and fixed schema-owner counters.

use crate::{metrics::SchemaLifecyclePhase, runtime::now_millis};
use candid::CandidType;
use serde::Deserialize;
use std::{cell::RefCell, collections::BTreeMap};

/// Saturating local instruction observations for one schema-lifecycle owner.
///
/// Spans include failed attempts and may nest; totals are not exclusive
/// accounting and must not be summed or converted to cycles.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct InstructionMetrics {
    samples: u64,
    instructions_total: u64,
    instructions_max: u64,
}

impl InstructionMetrics {
    /// Number of completed observation spans, including failed attempts.
    #[must_use]
    pub const fn samples(&self) -> u64 {
        self.samples
    }

    /// Saturating sum of observed local instructions.
    #[must_use]
    pub const fn instructions_total(&self) -> u64 {
        self.instructions_total
    }

    /// Largest local instruction interval in this window.
    #[must_use]
    pub const fn instructions_max(&self) -> u64 {
        self.instructions_max
    }

    fn record(&mut self, instructions: u64) {
        self.samples = self.samples.saturating_add(1);
        self.instructions_total = self.instructions_total.saturating_add(instructions);
        self.instructions_max = self.instructions_max.max(instructions);
    }
}

/// Five fixed, heap-only schema-owner counters in the existing metrics window.
///
/// Only replicated execution records work. Upgrade clears the window and
/// [`metrics_reset_all`] resets these counters together with entity observations.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
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

#[derive(Clone, Debug, Default)]
struct EntityCounter {
    hits: u64,
    instructions_total: u64,
    instructions_max: u64,
}

#[derive(Clone, Debug)]
struct MetricsState {
    entities: BTreeMap<String, EntityCounter>,
    schema_lifecycle: SchemaLifecycleMetrics,
    window_start_ms: u64,
    window_id: Option<u64>,
}

impl Default for MetricsState {
    fn default() -> Self {
        Self {
            entities: BTreeMap::new(),
            schema_lifecycle: SchemaLifecycleMetrics::default(),
            window_start_ms: now_millis(),
            window_id: Some(0),
        }
    }
}

thread_local! {
    static STATE: RefCell<MetricsState> = RefCell::new(MetricsState::default());
}

/// Cost attributed to one accepted entity during the active metrics window.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct EntityMetrics {
    path: String,
    hits: u64,
    instructions_total: u64,
    instructions_max: u64,
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

    /// Saturating sum of local instructions attributed to the entity.
    #[must_use]
    pub const fn instructions_total(&self) -> u64 {
        self.instructions_total
    }

    /// Largest local instruction delta attributed to one entity execution.
    #[must_use]
    pub const fn instructions_max(&self) -> u64 {
        self.instructions_max
    }
}

/// Bounded snapshot of a volatile, heap-only metrics window.
///
/// Selects a lexical prefix of entity paths before sorting that subset by cost.
/// This is not a global top-cost report. Counters and window IDs are lost on
/// upgrade/restart; consumers must treat heap replacement as a discontinuity.
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct MetricsReport {
    window_id: Option<u64>,
    window_start_ms: u64,
    window_end_ms: u64,
    total_entities: u64,
    entities: Vec<EntityMetrics>,
    schema_lifecycle: SchemaLifecycleMetrics,
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

    /// Fixed schema-owner observations sharing this report's heap-local window.
    #[must_use]
    pub const fn schema_lifecycle(&self) -> &SchemaLifecycleMetrics {
        &self.schema_lifecycle
    }
}

pub(super) fn record_schema_lifecycle_execution(phase: &SchemaLifecyclePhase, instructions: u64) {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let lifecycle = &mut state.schema_lifecycle;
        let counter = match phase {
            SchemaLifecyclePhase::Lowering => &mut lifecycle.lowering,
            SchemaLifecyclePhase::Publication => &mut lifecycle.publication,
            SchemaLifecyclePhase::RuntimeCompilation => &mut lifecycle.runtime_compilation,
            SchemaLifecyclePhase::Cardinality => &mut lifecycle.cardinality,
            SchemaLifecyclePhase::StartupRecovery => &mut lifecycle.startup_recovery,
        };
        counter.record(instructions);
    });
}

pub(super) fn record_entity_execution(entity_path: &str, instructions: u64) {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let counter = state.entities.entry(entity_path.to_string()).or_default();
        counter.hits = counter.hits.saturating_add(1);
        counter.instructions_total = counter.instructions_total.saturating_add(instructions);
        counter.instructions_max = counter.instructions_max.max(instructions);
    });
}

/// Snapshot the current on-canister metrics window.
///
/// Visits at most [`MetricsReport::MAX_ENTITIES`] source entries in ascending
/// path order, stopping before the first path exceeding the remaining byte
/// allowance. Only selected rows are copied and sorted. Does not bound retained
/// accumulator size, scan database rows, reset counters or sample IC instructions.
#[must_use]
pub fn metrics_report() -> MetricsReport {
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
                hits: counter.hits,
                instructions_total: counter.instructions_total,
                instructions_max: counter.instructions_max,
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
            schema_lifecycle: state.schema_lifecycle.clone(),
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
        state.schema_lifecycle = SchemaLifecycleMetrics::default();
        state.window_start_ms = now_millis();
    });
}

#[cfg(test)]
mod tests {
    use super::{
        MetricsReport, STATE, SchemaLifecycleMetrics, metrics_report, metrics_reset_all,
        record_entity_execution, record_schema_lifecycle_execution,
    };
    use crate::metrics::{SchemaLifecycleMetricsSpan, SchemaLifecyclePhase};

    #[test]
    fn lifecycle_observations_are_separate_saturating_and_reset_with_the_window() {
        metrics_reset_all();
        for (phase, instructions) in [
            (SchemaLifecyclePhase::Lowering, u64::MAX),
            (SchemaLifecyclePhase::Lowering, 1),
            (SchemaLifecyclePhase::Publication, 12),
            (SchemaLifecyclePhase::RuntimeCompilation, 23),
            (SchemaLifecyclePhase::Cardinality, 34),
            (SchemaLifecyclePhase::StartupRecovery, 45),
        ] {
            record_schema_lifecycle_execution(&phase, instructions);
        }
        let report = metrics_report();
        let lifecycle = report.schema_lifecycle();
        assert_eq!(lifecycle.lowering().samples(), 2);
        assert_eq!(lifecycle.lowering().instructions_total(), u64::MAX);
        assert_eq!(lifecycle.lowering().instructions_max(), u64::MAX);
        assert_eq!(lifecycle.publication().instructions_total(), 12);
        assert_eq!(lifecycle.runtime_compilation().instructions_total(), 23);
        assert_eq!(lifecycle.cardinality().instructions_total(), 34);
        assert_eq!(lifecycle.startup_recovery().instructions_total(), 45);
        assert_eq!(report.total_entities(), 0);
        let encoded = candid::encode_one(&report).expect("encode observed lifecycle report");
        let decoded: MetricsReport = candid::decode_one(&encoded).expect("decode lifecycle report");
        assert_eq!(decoded, report);
        metrics_reset_all();
        assert_eq!(
            metrics_report().schema_lifecycle(),
            &SchemaLifecycleMetrics::default()
        );
        assert_ne!(metrics_report().window_id(), report.window_id());
    }

    #[test]
    fn failed_owner_attempts_are_observed_when_the_span_exits() {
        fn failed_attempt() -> Result<(), ()> {
            let _span = SchemaLifecycleMetricsSpan::new(SchemaLifecyclePhase::Lowering);
            Err(())
        }
        metrics_reset_all();
        assert!(failed_attempt().is_err());
        assert_eq!(metrics_report().schema_lifecycle().lowering().samples(), 1);
        assert_eq!(
            metrics_report().schema_lifecycle().publication().samples(),
            0
        );
    }

    #[test]
    fn source_selection_is_a_bounded_lexical_prefix_not_global_top_cost() {
        metrics_reset_all();
        for index in (0..4096).rev() {
            record_entity_execution(&format!("entity::{index:04}"), index);
        }
        let report = metrics_report();
        assert_eq!(report.total_entities(), 4096);
        assert_eq!(report.entities().len(), MetricsReport::MAX_ENTITIES);
        assert_eq!(report.entities()[0].path(), "entity::0063");
        assert_eq!(report.entities()[63].path(), "entity::0000");
        assert_eq!(report, metrics_report_with_end_time(report.window_end_ms()));
    }

    // Ignore wall-clock movement when checking that observing does not mutate counters.
    fn metrics_report_with_end_time(end_ms: u64) -> MetricsReport {
        let mut report = metrics_report();
        report.window_end_ms = end_ms;
        report
    }

    #[test]
    fn source_selection_accepts_exact_limits_and_reports_omissions() {
        metrics_reset_all();
        for index in 0..MetricsReport::MAX_ENTITIES {
            record_entity_execution(&format!("entity::{index:04}"), 1);
        }
        let report = metrics_report();
        assert_eq!(report.entities().len() as u64, report.total_entities());
        record_entity_execution("zz-extra", 100);
        let report = metrics_report();
        assert_eq!(report.entities().len(), MetricsReport::MAX_ENTITIES);
        assert_eq!(report.total_entities(), 65);

        metrics_reset_all();
        let exact_path = "a".repeat(MetricsReport::MAX_PATH_BYTES);
        record_entity_execution(&exact_path, 1);
        record_entity_execution("z", 1);
        let report = metrics_report();
        assert_eq!(report.entities().len(), 1);
        assert_eq!(report.entities()[0].path(), exact_path);
        assert_eq!(report.total_entities(), 2);

        metrics_reset_all();
        record_entity_execution(&"a".repeat(MetricsReport::MAX_PATH_BYTES + 1), 1);
        record_entity_execution("z", 1);
        let report = metrics_report();
        assert!(report.entities().is_empty());
        assert_eq!(report.total_entities(), 2);

        metrics_reset_all();
        // Count UTF-8 bytes across paths, not characters or a per-path allowance.
        record_entity_execution(&"a".repeat(MetricsReport::MAX_PATH_BYTES - 2), 1);
        record_entity_execution("é", 1);
        record_entity_execution("ê", 1);
        let report = metrics_report();
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
        let first = metrics_report();
        record_entity_execution("entity", 1);
        metrics_reset_all();
        let second = metrics_report();
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
            assert_eq!(metrics_report().window_id(), None);
            assert_eq!(metrics_report().total_entities(), 0);
        }
        // Restore this thread's fixture without making production IDs reusable.
        STATE.with(|state| *state.borrow_mut() = super::MetricsState::default());
    }

    #[test]
    fn report_round_trips_current_candid_shape() {
        metrics_reset_all();
        record_entity_execution("entity", 12);
        let report = metrics_report();
        let encoded = candid::encode_one(&report).expect("encode current report");
        let decoded: MetricsReport = candid::decode_one(&encoded).expect("decode current report");
        assert_eq!(decoded, report);
    }

    #[test]
    fn report_orders_entities_by_total_cost_then_hits_then_path() {
        metrics_reset_all();
        record_entity_execution("store::beta", 10);
        record_entity_execution("store::alpha", 5);
        record_entity_execution("store::alpha", 5);
        record_entity_execution("store::gamma", 10);

        let report = metrics_report();
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
