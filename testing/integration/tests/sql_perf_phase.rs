//! Module: sql_perf_phase
//! Responsibility: versioned performance-phase ownership and additive reconciliation.
//! Does not own: runtime counters, scenario execution, ranking, or threshold policy.
//! Boundary: declares which counters add to one parent and which remain nested observations.

use serde::{Deserialize, Serialize};

/// Current performance phase-ownership schema version.
pub(crate) const PERFORMANCE_PHASE_OWNERSHIP_VERSION: u32 = 1;

/// Relationship between one parent counter and its named child counters.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PhaseRelationship {
    /// Child counters are mutually exclusive buckets that reconcile to the parent.
    Additive,
    /// Child counters are nested observations and must not be added to the parent.
    NestedObservation,
}

impl PhaseRelationship {
    /// Return the stable machine-readable relationship code.
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Additive => "additive",
            Self::NestedObservation => "nested_observation",
        }
    }
}

/// One versioned phase-ownership rule carried by a performance artifact.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhaseOwnershipEntry {
    /// Parent counter name.
    pub(crate) parent: String,
    /// Child counter names governed by this rule.
    pub(crate) children: Vec<String>,
    /// Whether the children are additive buckets or nested observations.
    pub(crate) relationship: PhaseRelationship,
}

/// Complete current phase-ownership table for SQL performance artifacts.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhaseOwnershipTable {
    /// Ownership schema version.
    pub(crate) version: u32,
    /// Exhaustive current additive and nested-observation rules.
    pub(crate) entries: Vec<PhaseOwnershipEntry>,
}

/// Additive reconciliation of one parent instruction counter.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhaseReconciliation {
    /// Sum of the declared additive children.
    pub(crate) attributable_local_instructions: u64,
    /// Parent instructions not explained by additive children.
    pub(crate) unaccounted_local_instructions: u64,
    /// Additive child instructions exceeding the parent.
    pub(crate) over_attributed_local_instructions: u64,
    /// Unaccounted instructions divided by the parent, in basis points.
    pub(crate) unaccounted_basis_points: Option<u64>,
}

/// Return the one current phase-ownership table.
pub(crate) fn current_phase_ownership() -> PhaseOwnershipTable {
    PhaseOwnershipTable {
        version: PERFORMANCE_PHASE_OWNERSHIP_VERSION,
        entries: vec![
            additive(
                "total_local_instructions",
                &["compile_local_instructions", "execute_local_instructions"],
            ),
            additive(
                "execute_local_instructions",
                &[
                    "planner_local_instructions",
                    "store_local_instructions",
                    "executor_local_instructions",
                    "response_finalization_local_instructions",
                ],
            ),
            additive(
                "executor_invocation_local_instructions",
                &["store_local_instructions", "executor_local_instructions"],
            ),
            additive(
                "compile_local_instructions",
                &[
                    "compile_cache_key_local_instructions",
                    "compile_cache_lookup_local_instructions",
                    "compile_parse_local_instructions",
                    "compile_aggregate_lane_check_local_instructions",
                    "compile_prepare_local_instructions",
                    "compile_lower_local_instructions",
                    "compile_bind_local_instructions",
                    "compile_cache_insert_local_instructions",
                ],
            ),
            additive(
                "planner_local_instructions",
                &[
                    "planner_schema_info_local_instructions",
                    "planner_prepare_local_instructions",
                    "planner_cache_key_local_instructions",
                    "planner_cache_lookup_local_instructions",
                    "planner_plan_build_local_instructions",
                    "planner_cache_insert_local_instructions",
                ],
            ),
            nested(
                "executor_local_instructions",
                &[
                    "grouped_stream_local_instructions",
                    "grouped_fold_local_instructions",
                    "grouped_finalize_local_instructions",
                    "scalar_aggregate_base_row_local_instructions",
                    "scalar_aggregate_reducer_fold_local_instructions",
                    "direct_data_row_*",
                    "kernel_row_*",
                    "pure_covering_*",
                    "hybrid_covering_*",
                ],
            ),
        ],
    }
}

/// Reconcile one parent against its declared additive children.
pub(crate) fn reconcile_phase(parent: u64, children: &[u64]) -> PhaseReconciliation {
    let attributable = children.iter().copied().fold(0_u64, u64::saturating_add);
    let unaccounted = parent.saturating_sub(attributable);
    let over_attributed = attributable.saturating_sub(parent);
    let unaccounted_basis_points = (parent != 0).then(|| {
        let scaled = u128::from(unaccounted).saturating_mul(10_000) / u128::from(parent);
        u64::try_from(scaled).unwrap_or(u64::MAX)
    });

    PhaseReconciliation {
        attributable_local_instructions: attributable,
        unaccounted_local_instructions: unaccounted,
        over_attributed_local_instructions: over_attributed,
        unaccounted_basis_points,
    }
}

fn additive(parent: &str, children: &[&str]) -> PhaseOwnershipEntry {
    ownership_entry(parent, children, PhaseRelationship::Additive)
}

fn nested(parent: &str, children: &[&str]) -> PhaseOwnershipEntry {
    ownership_entry(parent, children, PhaseRelationship::NestedObservation)
}

fn ownership_entry(
    parent: &str,
    children: &[&str],
    relationship: PhaseRelationship,
) -> PhaseOwnershipEntry {
    PhaseOwnershipEntry {
        parent: parent.to_string(),
        children: children.iter().map(|child| (*child).to_string()).collect(),
        relationship,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconciliation_distinguishes_unaccounted_and_over_attributed_work() {
        assert_eq!(
            reconcile_phase(100, &[30, 50]),
            PhaseReconciliation {
                attributable_local_instructions: 80,
                unaccounted_local_instructions: 20,
                over_attributed_local_instructions: 0,
                unaccounted_basis_points: Some(2_000),
            },
        );
        assert_eq!(
            reconcile_phase(100, &[70, 50]),
            PhaseReconciliation {
                attributable_local_instructions: 120,
                unaccounted_local_instructions: 0,
                over_attributed_local_instructions: 20,
                unaccounted_basis_points: Some(0),
            },
        );
    }

    #[test]
    fn current_table_separates_additive_and_nested_counters() {
        let table = current_phase_ownership();

        assert_eq!(table.version, PERFORMANCE_PHASE_OWNERSHIP_VERSION);
        assert!(table.entries.iter().any(|entry| {
            entry.parent == "execute_local_instructions"
                && entry.relationship == PhaseRelationship::Additive
        }));
        assert!(table.entries.iter().any(|entry| {
            entry.parent == "executor_local_instructions"
                && entry.relationship == PhaseRelationship::NestedObservation
        }));
    }
}
