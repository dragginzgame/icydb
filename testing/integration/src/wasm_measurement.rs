//! Canonical 0.220 Wasm measurement subjects, comparison policy, and budgets.
//!
//! This module owns measurement authority only. It does not select canister
//! features, build actors, transform Wasm, or decide runtime semantics.

use serde::Serialize;

/// Current machine-readable Wasm measurement profile version.
pub const WASM_MEASUREMENT_PROFILE_VERSION: u32 = 1;

/// Stable identity carried by every comparable 0.220 Wasm report.
pub const WASM_MEASUREMENT_PROFILE_ID: &str = "icydb-wasm-footprint/0.220/v1";

/// Exact maintained production subjects in canonical report order.
pub const WASM_MEASUREMENT_SUBJECTS: &[&str] = &[
    "default_empty",
    "default_empty_metrics",
    "one_entity_dynamic_query",
    "one_entity_typed_query",
    "one_entity_sql_query",
    "ten_entity_typed_query",
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
/// The current audit actors deliberately remain separate maintained packages.
/// Their subtractions are therefore directional and cannot, by themselves,
/// justify deleting code from one production owner.
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
        id: "entity_scale",
        baseline: "one_entity_typed_query",
        candidate: "ten_entity_typed_query",
        disposition: WasmComparisonDisposition::DirectionalOnly,
        reason: "the intended entity-count change also changes the generated accepted-schema proposal",
    },
];

/// Numeric acceptance policy for one later 0.220 landing patch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct WasmPatchBudget {
    /// Planned patch ordinal.
    pub patch: u8,
    /// Minimum raw-Wasm reduction for the selected affected subject, in basis points.
    pub minimum_selected_raw_reduction_basis_points: u16,
    /// Minimum instruction reduction for the selected affected sentinel, in basis points.
    pub minimum_selected_instruction_reduction_basis_points: u16,
    /// Maximum raw-Wasm regression for any maintained non-selected subject.
    pub maximum_other_raw_regression_bytes: u64,
    /// Maximum instruction regression for any maintained non-selected sentinel.
    pub maximum_other_instruction_regression_basis_points: u16,
}

/// Frozen per-patch budgets established before production optimization begins.
///
/// A rejected candidate lands as an evidence-backed no-production-change
/// disposition. An accepted implementation must satisfy its selected-owner
/// improvement and every non-selected regression ceiling.
pub const WASM_PATCH_BUDGETS: &[WasmPatchBudget] = &[
    WasmPatchBudget {
        patch: 2,
        minimum_selected_raw_reduction_basis_points: 700,
        minimum_selected_instruction_reduction_basis_points: 0,
        maximum_other_raw_regression_bytes: 0,
        maximum_other_instruction_regression_basis_points: 100,
    },
    WasmPatchBudget {
        patch: 3,
        minimum_selected_raw_reduction_basis_points: 0,
        minimum_selected_instruction_reduction_basis_points: 300,
        maximum_other_raw_regression_bytes: 8 * 1024,
        maximum_other_instruction_regression_basis_points: 100,
    },
    WasmPatchBudget {
        patch: 4,
        minimum_selected_raw_reduction_basis_points: 500,
        minimum_selected_instruction_reduction_basis_points: 0,
        maximum_other_raw_regression_bytes: 8 * 1024,
        maximum_other_instruction_regression_basis_points: 100,
    },
    WasmPatchBudget {
        patch: 5,
        minimum_selected_raw_reduction_basis_points: 0,
        minimum_selected_instruction_reduction_basis_points: 0,
        maximum_other_raw_regression_bytes: 8 * 1024,
        maximum_other_instruction_regression_basis_points: 100,
    },
    WasmPatchBudget {
        patch: 6,
        minimum_selected_raw_reduction_basis_points: 0,
        minimum_selected_instruction_reduction_basis_points: 0,
        maximum_other_raw_regression_bytes: 8 * 1024,
        maximum_other_instruction_regression_basis_points: 100,
    },
    WasmPatchBudget {
        patch: 7,
        minimum_selected_raw_reduction_basis_points: 0,
        minimum_selected_instruction_reduction_basis_points: 500,
        maximum_other_raw_regression_bytes: 8 * 1024,
        maximum_other_instruction_regression_basis_points: 100,
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

/// Frozen cumulative 0.220 raw-Wasm budgets.
///
/// These do not reset after individual patches. Every final production subject
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
        subject: "one_entity_typed_query",
        minimum_final_raw_reduction_basis_points: 700,
    },
    WasmLineBudget {
        subject: "one_entity_sql_query",
        minimum_final_raw_reduction_basis_points: 700,
    },
    WasmLineBudget {
        subject: "ten_entity_typed_query",
        minimum_final_raw_reduction_basis_points: 700,
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

/// Validate the complete fixed 0.220 measurement contract.
///
/// # Errors
///
/// Returns a static diagnostic when subject, comparison, or budget authority drifts.
pub fn validate_wasm_measurement_contract() -> Result<(), &'static str> {
    if WASM_MEASUREMENT_PROFILE_VERSION != 1
        || WASM_MEASUREMENT_PROFILE_ID != "icydb-wasm-footprint/0.220/v1"
        || WASM_MEASUREMENT_SUBJECTS.len() != 8
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
    if WASM_PATCH_BUDGETS.len() != 6
        || WASM_PATCH_BUDGETS
            .iter()
            .enumerate()
            .any(|(index, budget)| budget.patch != u8::try_from(index + 2).unwrap_or(u8::MAX))
    {
        return Err("Wasm patch budgets must cover patches 2 through 7 exactly");
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
        assert!(WASM_MEASUREMENT_COMPARISONS.iter().all(|comparison| {
            comparison.disposition == WasmComparisonDisposition::DirectionalOnly
        }));
    }
}
