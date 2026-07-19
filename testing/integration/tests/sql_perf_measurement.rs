//! Module: sql_perf_measurement
//! Responsibility: canonical SQL performance measurement coverage and residual identities.
//! Does not own: sampling, phase reconciliation, thresholds, or baseline verdicts.
//! Boundary: projects measured/unmeasured dimensions and reads retained residual facts.

use crate::{MatrixSample, sql_perf_phase::PhaseReconciliation};

use serde::{Deserialize, Serialize};

///
/// PerformanceMeasurementStatus
///
/// Explicit availability of one required performance dimension.
/// Absence is never interpreted as a zero measurement.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PerformanceMeasurementStatus {
    /// The current artifact retains a typed measurement for this dimension.
    Measured,

    /// No trustworthy typed counter currently exists for this dimension.
    NotMeasured,
}

impl PerformanceMeasurementStatus {
    /// Return the stable report spelling for this availability state.
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Measured => "measured",
            Self::NotMeasured => "not_measured",
        }
    }
}

///
/// PerformanceMeasurementCoverage
///
/// Canonical availability table carried by current performance reports.
/// Owned here so every report projects the same measured and unmeasured dimensions.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PerformanceMeasurementCoverage {
    /// Canister-local instruction totals and phase attribution.
    pub(crate) instruction_attribution: PerformanceMeasurementStatus,

    /// Typed data/index store operation counters.
    pub(crate) storage_operations: PerformanceMeasurementStatus,

    /// Typed result row/count cardinality.
    pub(crate) result_cardinality: PerformanceMeasurementStatus,

    /// Typed projected blob payload byte counters.
    pub(crate) projected_blob_output_bytes: PerformanceMeasurementStatus,

    /// Maximum kernel-row candidates retained concurrently during scan collection.
    pub(crate) peak_retained_candidates: PerformanceMeasurementStatus,

    /// Peak heap usage during one scenario.
    pub(crate) peak_heap_bytes: PerformanceMeasurementStatus,

    /// Allocator traffic during one scenario.
    pub(crate) allocator_traffic_bytes: PerformanceMeasurementStatus,

    /// Stable-memory byte volume read and written by one scenario.
    pub(crate) stable_memory_byte_volume: PerformanceMeasurementStatus,
}

impl PerformanceMeasurementCoverage {
    /// Return every required dimension in stable report order.
    pub(crate) const fn entries(self) -> [(&'static str, PerformanceMeasurementStatus); 8] {
        [
            ("instruction_attribution", self.instruction_attribution),
            ("storage_operations", self.storage_operations),
            ("result_cardinality", self.result_cardinality),
            (
                "projected_blob_output_bytes",
                self.projected_blob_output_bytes,
            ),
            ("peak_retained_candidates", self.peak_retained_candidates),
            ("peak_heap_bytes", self.peak_heap_bytes),
            ("allocator_traffic_bytes", self.allocator_traffic_bytes),
            ("stable_memory_byte_volume", self.stable_memory_byte_volume),
        ]
    }
}

/// Return the one current measurement-availability contract.
pub(crate) const fn current_measurement_coverage() -> PerformanceMeasurementCoverage {
    PerformanceMeasurementCoverage {
        instruction_attribution: PerformanceMeasurementStatus::Measured,
        storage_operations: PerformanceMeasurementStatus::Measured,
        result_cardinality: PerformanceMeasurementStatus::Measured,
        projected_blob_output_bytes: PerformanceMeasurementStatus::Measured,
        peak_retained_candidates: PerformanceMeasurementStatus::Measured,
        peak_heap_bytes: PerformanceMeasurementStatus::NotMeasured,
        allocator_traffic_bytes: PerformanceMeasurementStatus::NotMeasured,
        stable_memory_byte_volume: PerformanceMeasurementStatus::NotMeasured,
    }
}

///
/// PerformancePhase
///
/// Additive phase with a retained reconciliation residual.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PerformancePhase {
    /// Complete query instructions against compile plus execute.
    Total,

    /// SQL compile instructions against compile subphases.
    Compile,

    /// SQL execute instructions against planner, store, executor, and finalization.
    Execute,

    /// Planner instructions against planner subphases.
    Planner,

    /// Executor invocation against store plus executor runtime.
    ExecutorInvocation,
}

///
/// PhaseResidualKind
///
/// Typed residual fact retained from one additive phase reconciliation.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PhaseResidualKind {
    /// Parent instructions not attributed to declared additive children.
    UnaccountedInstructions,

    /// Declared additive children exceeding their parent instruction total.
    OverAttributedInstructions,
}

///
/// PhaseResidualMetric
///
/// One phase and residual-kind pair compared across confirmed P2 medians.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhaseResidualMetric {
    /// Reconciled additive phase.
    pub(crate) phase: PerformancePhase,

    /// Residual value read from that phase.
    pub(crate) kind: PhaseResidualKind,
}

impl PhaseResidualMetric {
    /// Borrow every required phase-residual metric in stable artifact order.
    pub(crate) const fn all() -> &'static [Self] {
        PHASE_RESIDUAL_METRICS
    }

    /// Read this metric from one retained matrix sample.
    pub(crate) const fn value(self, sample: &MatrixSample) -> u64 {
        let reconciliation = match self.phase {
            PerformancePhase::Total => sample.total_phase_reconciliation,
            PerformancePhase::Compile => sample.compile_phase_reconciliation,
            PerformancePhase::Execute => sample.execute_phase_reconciliation,
            PerformancePhase::Planner => sample.planner_phase_reconciliation,
            PerformancePhase::ExecutorInvocation => sample.executor_invocation_phase_reconciliation,
        };

        residual_value(reconciliation, self.kind)
    }
}

const fn residual_value(reconciliation: PhaseReconciliation, kind: PhaseResidualKind) -> u64 {
    match kind {
        PhaseResidualKind::UnaccountedInstructions => reconciliation.unaccounted_local_instructions,
        PhaseResidualKind::OverAttributedInstructions => {
            reconciliation.over_attributed_local_instructions
        }
    }
}

macro_rules! residual_metrics {
    ($($phase:ident),+ $(,)?) => {
        &[
            $(
                PhaseResidualMetric {
                    phase: PerformancePhase::$phase,
                    kind: PhaseResidualKind::UnaccountedInstructions,
                },
                PhaseResidualMetric {
                    phase: PerformancePhase::$phase,
                    kind: PhaseResidualKind::OverAttributedInstructions,
                },
            )+
        ]
    };
}

const PHASE_RESIDUAL_METRICS: &[PhaseResidualMetric] =
    residual_metrics!(Total, Compile, Execute, Planner, ExecutorInvocation,);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_coverage_is_explicit_for_measured_and_unmeasured_dimensions() {
        let coverage = current_measurement_coverage();

        assert_eq!(
            coverage.instruction_attribution,
            PerformanceMeasurementStatus::Measured,
        );
        assert_eq!(
            coverage.storage_operations,
            PerformanceMeasurementStatus::Measured,
        );
        assert_eq!(
            coverage.result_cardinality,
            PerformanceMeasurementStatus::Measured,
        );
        assert_eq!(
            coverage.projected_blob_output_bytes,
            PerformanceMeasurementStatus::Measured,
        );
        assert_eq!(
            coverage.peak_retained_candidates,
            PerformanceMeasurementStatus::Measured,
        );
        assert_eq!(
            coverage.peak_heap_bytes,
            PerformanceMeasurementStatus::NotMeasured,
        );
        assert_eq!(
            coverage.allocator_traffic_bytes,
            PerformanceMeasurementStatus::NotMeasured,
        );
        assert_eq!(
            coverage.stable_memory_byte_volume,
            PerformanceMeasurementStatus::NotMeasured,
        );
    }

    #[test]
    fn residual_metric_set_covers_every_phase_and_kind_once() {
        assert_eq!(PhaseResidualMetric::all().len(), 10);
        assert!(
            PhaseResidualMetric::all()
                .windows(2)
                .all(|pair| pair[0] < pair[1]),
        );
    }
}
