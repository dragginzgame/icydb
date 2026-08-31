//! Canonical Wasm measurement subjects, comparison policy, and budgets.
//!
//! This module owns measurement authority only. It does not select canister
//! features, build actors, transform Wasm, or decide runtime semantics.

use serde::Serialize;

/// Current machine-readable Wasm measurement profile version.
pub const WASM_MEASUREMENT_PROFILE_VERSION: u32 = 1;

/// Stable identity carried by every comparable Wasm report.
pub const WASM_MEASUREMENT_PROFILE_ID: &str = "icydb-wasm-footprint/0.250/v1";

/// Maximum final raw-Wasm growth admitted for each additional generated entity.
pub const MAX_ENTITY_SCALE_RAW_BYTES_PER_ENTITY: u64 = 2 * 1024;

/// Generated entities added by the maintained one-entity to ten-entity pair.
pub const ENTITY_SCALE_ADDED_ENTITIES: u64 = 9;

/// Minimum compiler-to-final raw-Wasm reduction required from the maintained
/// post-link optimizer.
pub const MINIMUM_POST_LINK_RAW_REDUCTION_BASIS_POINTS: u16 = 700;

/// Minimum final code-section reduction required from the prepared-page slice.
pub const MINIMUM_PREPARED_PAGE_CODE_REDUCTION_BYTES: u64 = 32 * 1024;

/// Absolute instruction increase that participates in the material-regression gate.
pub const MATERIAL_INSTRUCTION_REGRESSION: u64 = 250_000;

/// Relative instruction increase that participates in the material-regression gate.
pub const MATERIAL_INSTRUCTION_REGRESSION_BASIS_POINTS: u16 = 300;

/// A finalized raw-Wasm entity-scale measurement exceeded its maintained ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityScaleWasmBudgetExceeded {
    /// Ceiling applied to each additional generated entity.
    pub maximum_raw_bytes_per_entity: u64,
    /// Observed final raw-Wasm growth, rounded up per additional entity.
    pub observed_raw_bytes_per_entity: u64,
}

impl std::fmt::Display for EntityScaleWasmBudgetExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "entity-scale final raw-Wasm growth is {} bytes/entity; maximum is {} bytes/entity",
            self.observed_raw_bytes_per_entity, self.maximum_raw_bytes_per_entity
        )
    }
}

impl std::error::Error for EntityScaleWasmBudgetExceeded {}

/// Return finalized raw-Wasm growth per added entity, rounded up.
#[must_use]
pub fn entity_scale_raw_bytes_per_added_entity(
    baseline_raw_bytes: u64,
    candidate_raw_bytes: u64,
) -> u64 {
    let growth = candidate_raw_bytes.saturating_sub(baseline_raw_bytes);
    let whole = growth / ENTITY_SCALE_ADDED_ENTITIES;
    whole + u64::from(!growth.is_multiple_of(ENTITY_SCALE_ADDED_ENTITIES))
}

/// Enforce the maintained finalized raw-Wasm entity-scale ceiling.
///
/// # Errors
///
/// Returns the observed and admitted per-entity costs when the maintained
/// one-entity to ten-entity pair exceeds the ceiling.
pub fn validate_entity_scale_raw_wasm(
    baseline_raw_bytes: u64,
    candidate_raw_bytes: u64,
) -> Result<u64, EntityScaleWasmBudgetExceeded> {
    let observed = entity_scale_raw_bytes_per_added_entity(baseline_raw_bytes, candidate_raw_bytes);
    if observed > MAX_ENTITY_SCALE_RAW_BYTES_PER_ENTITY {
        return Err(EntityScaleWasmBudgetExceeded {
            maximum_raw_bytes_per_entity: MAX_ENTITY_SCALE_RAW_BYTES_PER_ENTITY,
            observed_raw_bytes_per_entity: observed,
        });
    }
    Ok(observed)
}

/// Exact maintained production subjects in canonical report order.
pub const WASM_MEASUREMENT_SUBJECTS: &[&str] = &[
    "default_empty",
    "default_empty_metrics",
    "one_entity_dynamic_query",
    "one_entity_reachable_operations",
    "one_entity_typed_query",
    "one_entity_sql_query",
    "request_future_scale",
    "ten_entity_typed_query",
    "ten_entity_reachable_operations",
    "sql_perf",
    "sql",
];

/// Whether a subtraction may attribute its delta to one controlled change.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WasmComparisonDisposition {
    /// Both subjects share one source/schema and differ by one declared dimension.
    Attributable,
    /// The pair is useful directionally but has more than one differing owner.
    DirectionalOnly,
}

impl WasmComparisonDisposition {
    /// Stable machine-readable report spelling.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Attributable => "attributable",
            Self::DirectionalOnly => "directional_only",
        }
    }
}

/// One frozen pair considered by the opening footprint audit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct WasmComparison {
    /// Stable comparison identity.
    pub id: &'static str,
    /// Smaller or capability-free subject.
    pub baseline: &'static str,
    /// Larger or capability-bearing subject.
    pub candidate: &'static str,
    /// Whether the subtraction can select a production owner.
    pub disposition: WasmComparisonDisposition,
    /// Exact reason the disposition is safe.
    pub reason: &'static str,
}

/// Frozen opening comparison ledger.
///
/// Most audit actors deliberately remain separate maintained packages. The
/// entity-scale pair is attributable because its endpoint shape is identical
/// and only its generated schema cardinality differs.
pub const WASM_MEASUREMENT_COMPARISONS: &[WasmComparison] = &[
    WasmComparison {
        id: "metrics_surface",
        baseline: "default_empty",
        candidate: "default_empty_metrics",
        disposition: WasmComparisonDisposition::DirectionalOnly,
        reason: "distinct actor sources differ by endpoint declarations and retained metrics wiring",
    },
    WasmComparison {
        id: "typed_ingress",
        baseline: "one_entity_dynamic_query",
        candidate: "one_entity_typed_query",
        disposition: WasmComparisonDisposition::DirectionalOnly,
        reason: "distinct actor sources differ by public method bodies and typed adapter reachability",
    },
    WasmComparison {
        id: "sql_ingress",
        baseline: "one_entity_typed_query",
        candidate: "one_entity_sql_query",
        disposition: WasmComparisonDisposition::DirectionalOnly,
        reason: "distinct actor sources differ by public method bodies and SQL capability reachability",
    },
    WasmComparison {
        id: "schema_entity_scale",
        baseline: "one_entity_typed_query",
        candidate: "ten_entity_typed_query",
        disposition: WasmComparisonDisposition::Attributable,
        reason: "both actors expose one exact-key shape; only entity 01 is reachable and the candidate declares nine additional dead-strippable schemas",
    },
    WasmComparison {
        id: "reachable_entity_scale",
        baseline: "one_entity_reachable_operations",
        candidate: "ten_entity_reachable_operations",
        disposition: WasmComparisonDisposition::Attributable,
        reason: "both actors share one endpoint and operation harness; the candidate makes the same page, exact-key, insert, update, batch and delete paths reachable for nine additional simple entities",
    },
    WasmComparison {
        id: "request_future_scale",
        baseline: "default_empty",
        candidate: "request_future_scale",
        disposition: WasmComparisonDisposition::Attributable,
        reason: "both actors use the same empty schema and feature set; the candidate adds 64 request-scoped async queries",
    },
];

/// Cumulative opening-to-closeout raw-Wasm budget for one maintained subject.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct WasmLineBudget {
    /// Maintained production subject.
    pub subject: &'static str,
    /// Minimum final reduction from the accepted opening baseline, in basis points.
    pub minimum_final_raw_reduction_basis_points: u16,
}

/// Frozen cumulative raw-Wasm budgets.
///
/// These do not reset after individual landing slices. Every final production subject
/// must meet its own opening-to-closeout reduction target.
pub const WASM_LINE_BUDGETS: &[WasmLineBudget] = &[
    WasmLineBudget {
        subject: "default_empty",
        minimum_final_raw_reduction_basis_points: 0,
    },
    WasmLineBudget {
        subject: "default_empty_metrics",
        minimum_final_raw_reduction_basis_points: 500,
    },
    WasmLineBudget {
        subject: "one_entity_dynamic_query",
        minimum_final_raw_reduction_basis_points: 700,
    },
    WasmLineBudget {
        subject: "one_entity_reachable_operations",
        minimum_final_raw_reduction_basis_points: 0,
    },
    WasmLineBudget {
        subject: "one_entity_typed_query",
        minimum_final_raw_reduction_basis_points: 700,
    },
    WasmLineBudget {
        subject: "one_entity_sql_query",
        minimum_final_raw_reduction_basis_points: 700,
    },
    WasmLineBudget {
        subject: "request_future_scale",
        minimum_final_raw_reduction_basis_points: 0,
    },
    WasmLineBudget {
        subject: "ten_entity_typed_query",
        minimum_final_raw_reduction_basis_points: 700,
    },
    WasmLineBudget {
        subject: "ten_entity_reachable_operations",
        minimum_final_raw_reduction_basis_points: 0,
    },
    WasmLineBudget {
        subject: "sql_perf",
        minimum_final_raw_reduction_basis_points: 500,
    },
    WasmLineBudget {
        subject: "sql",
        minimum_final_raw_reduction_basis_points: 500,
    },
];

/// Validate the complete fixed measurement contract.
///
/// # Errors
///
/// Returns a static diagnostic when subject, comparison, or budget authority drifts.
pub fn validate_wasm_measurement_contract() -> Result<(), &'static str> {
    if WASM_MEASUREMENT_PROFILE_VERSION != 1
        || WASM_MEASUREMENT_PROFILE_ID != "icydb-wasm-footprint/0.250/v1"
        || WASM_MEASUREMENT_SUBJECTS.len() != 11
    {
        return Err("Wasm measurement identity or subject count drifted");
    }
    if WASM_MEASUREMENT_SUBJECTS
        .iter()
        .enumerate()
        .any(|(index, subject)| {
            subject.is_empty() || WASM_MEASUREMENT_SUBJECTS[index + 1..].contains(subject)
        })
    {
        return Err("Wasm measurement subjects must be non-empty and unique");
    }
    if WASM_MEASUREMENT_COMPARISONS.iter().any(|comparison| {
        comparison.id.is_empty()
            || comparison.reason.is_empty()
            || !WASM_MEASUREMENT_SUBJECTS.contains(&comparison.baseline)
            || !WASM_MEASUREMENT_SUBJECTS.contains(&comparison.candidate)
            || comparison.baseline == comparison.candidate
    }) {
        return Err("Wasm comparison ledger contains an invalid subject or reason");
    }
    if WASM_MEASUREMENT_COMPARISONS
        .iter()
        .enumerate()
        .any(|(index, comparison)| {
            WASM_MEASUREMENT_COMPARISONS[index + 1..]
                .iter()
                .any(|later| later.id == comparison.id)
        })
    {
        return Err("Wasm comparisons must have unique identities");
    }
    if WASM_LINE_BUDGETS.len() != WASM_MEASUREMENT_SUBJECTS.len()
        || WASM_LINE_BUDGETS
            .iter()
            .zip(WASM_MEASUREMENT_SUBJECTS)
            .any(|(budget, subject)| budget.subject != *subject)
    {
        return Err("Wasm line budgets must cover every maintained subject in canonical order");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_measurement_contract_is_complete_and_fail_closed() {
        validate_wasm_measurement_contract().expect("current measurement contract should validate");
        assert_eq!(MAX_ENTITY_SCALE_RAW_BYTES_PER_ENTITY, 2_048);
        assert_eq!(
            WASM_MEASUREMENT_COMPARISONS
                .iter()
                .find(|comparison| comparison.id == "schema_entity_scale")
                .map(|comparison| comparison.disposition),
            Some(WasmComparisonDisposition::Attributable)
        );
        assert_eq!(MINIMUM_PREPARED_PAGE_CODE_REDUCTION_BYTES, 32_768);
        assert_eq!(MATERIAL_INSTRUCTION_REGRESSION, 250_000);
        assert_eq!(MATERIAL_INSTRUCTION_REGRESSION_BASIS_POINTS, 300);
    }

    #[test]
    fn entity_scale_guard_uses_final_raw_bytes_and_rounds_up() {
        let exact_limit = MAX_ENTITY_SCALE_RAW_BYTES_PER_ENTITY * ENTITY_SCALE_ADDED_ENTITIES;
        assert_eq!(
            validate_entity_scale_raw_wasm(1_000_000, 1_000_000 + exact_limit),
            Ok(MAX_ENTITY_SCALE_RAW_BYTES_PER_ENTITY)
        );
        assert_eq!(entity_scale_raw_bytes_per_added_entity(10_000, 10_001), 1);
        assert_eq!(entity_scale_raw_bytes_per_added_entity(10_001, 10_000), 0);

        let exceeded = validate_entity_scale_raw_wasm(1_000_000, 1_000_000 + exact_limit + 1)
            .expect_err("one byte above the aggregate ceiling should fail");
        assert_eq!(
            exceeded,
            EntityScaleWasmBudgetExceeded {
                maximum_raw_bytes_per_entity: MAX_ENTITY_SCALE_RAW_BYTES_PER_ENTITY,
                observed_raw_bytes_per_entity: MAX_ENTITY_SCALE_RAW_BYTES_PER_ENTITY + 1,
            }
        );
    }
}
