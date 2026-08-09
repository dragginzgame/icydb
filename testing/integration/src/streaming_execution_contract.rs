//! Stable 0.222 fixture and materialization-audit authority.
//!
//! This module owns declarations only. It does not select executor routes,
//! execute queries, or turn diagnostic expectations into runtime behavior.

/// Current hard-cut fixture declaration format.
pub const STREAMING_EXECUTION_CONTRACT_VERSION: u32 = 1;

/// Deterministic generator seed returned by the audit canister.
pub const STREAMING_EXECUTION_FIXTURE_SEED: u64 = 3;

/// Rows in each maintained streaming fixture entity.
pub const STREAMING_EXECUTION_FIXTURE_ROWS: u32 = 2_048;

/// Rows required by the final live and exhaustive continuation fixtures.
pub const STREAMING_EXECUTION_CONTINUATION_ROWS: u32 = 10_001;

/// Current accepted branch-set fan-out ceiling exercised by the fixture.
pub const STREAMING_EXECUTION_PREFIX_FANOUT: u16 = 16;

/// Exact wide payload sentinels retained by the fixture.
pub const STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES: &[u32] = &[300 * 1_024, 150 * 1_024, 40 * 1_024];

/// Direction required by one fixed query shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamingFixtureDirection {
    /// Query order is ascending.
    Asc,
    /// Query order is descending.
    Desc,
    /// Direction is not the measured contract.
    NotApplicable,
}

/// Continuation behavior exercised by one fixed query shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamingFixtureContinuation {
    /// One result page is sufficient for this baseline.
    OneShot,
    /// Live keyset continuation is required by final acceptance.
    Live,
    /// Revision-strict exhaustive continuation is required by final acceptance.
    Exhaustive,
}

/// Structural counter that must accompany instruction evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamingFixtureEvidence {
    /// Physical index entries read.
    IndexEntries,
    /// Stored rows fetched.
    StoreGets,
    /// Rows owned by the public result.
    OutputRows,
    /// Maximum simultaneously retained candidate rows.
    PeakRetainedCandidates,
    /// Complete backing bytes kept alive by blocking state.
    PeakRetainedBackingBytes,
    /// Cursor/progress correctness only; no optimization gate yet.
    Progress,
}

/// Review gate attached before implementation measurements exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamingFixtureGate {
    /// Named problem fixture requiring at least this instruction reduction.
    Improvement { minimum_basis_points: u16 },
    /// Unaffected control whose movement requires review at this threshold.
    Review { threshold_basis_points: u16 },
    /// Correctness/authority fixture without an instruction threshold.
    Correctness,
}

/// One immutable SQL and dataset declaration used throughout 0.222.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamingExecutionFixture {
    /// Stable identifier used by reports and patch gates.
    pub id: &'static str,
    /// Exact SQL text; later patches may not substitute another query.
    pub sql: &'static str,
    /// Expected logical row count for the fixed generator.
    pub expected_rows: u32,
    /// Output page limit encoded by the query, or zero for a blocking result.
    pub page_limit: u32,
    /// Required traversal direction.
    pub direction: StreamingFixtureDirection,
    /// Continuation contract.
    pub continuation: StreamingFixtureContinuation,
    /// Physical evidence paired with instructions.
    pub evidence: StreamingFixtureEvidence,
    /// Fixed review gate.
    pub gate: StreamingFixtureGate,
}

// Each argument is one independently audited column in the frozen fixture
// matrix; grouping them would make individual declarations less reviewable.
#[allow(clippy::too_many_arguments)]
const fn fixture(
    id: &'static str,
    sql: &'static str,
    expected_rows: u32,
    page_limit: u32,
    direction: StreamingFixtureDirection,
    continuation: StreamingFixtureContinuation,
    evidence: StreamingFixtureEvidence,
    gate: StreamingFixtureGate,
) -> StreamingExecutionFixture {
    StreamingExecutionFixture {
        id,
        sql,
        expected_rows,
        page_limit,
        direction,
        continuation,
        evidence,
        gate,
    }
}

/// Fixed named fixture matrix. Dataset parameters are module constants above.
pub const STREAMING_EXECUTION_FIXTURES: &[StreamingExecutionFixture] = &[
    fixture(
        "seek_index_sparse_asc",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 ORDER BY id ASC LIMIT 10",
        10,
        10,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::IndexEntries,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "seek_index_sparse_desc",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 ORDER BY id DESC LIMIT 10",
        10,
        10,
        StreamingFixtureDirection::Desc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::IndexEntries,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "index_stream_exact",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_b = 0 ORDER BY id ASC LIMIT 20",
        20,
        20,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::IndexEntries,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "prefix_family_max_fanout",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15) ORDER BY id ASC LIMIT 50",
        50,
        50,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::IndexEntries,
        StreamingFixtureGate::Improvement {
            minimum_basis_points: 2_000,
        },
    ),
    fixture(
        "intersection_empty",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 AND lane_b = 1 ORDER BY id ASC LIMIT 10",
        0,
        10,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::IndexEntries,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "intersection_2_sparse",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 AND lane_b = 0 ORDER BY id ASC LIMIT 10",
        1,
        10,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::IndexEntries,
        StreamingFixtureGate::Improvement {
            minimum_basis_points: 2_000,
        },
    ),
    fixture(
        "intersection_3_sparse_desc",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 AND lane_b = 0 AND group_key = 12 ORDER BY id DESC LIMIT 10",
        1,
        10,
        StreamingFixtureDirection::Desc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::IndexEntries,
        StreamingFixtureGate::Improvement {
            minimum_basis_points: 2_000,
        },
    ),
    fixture(
        "intersection_dense",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a >= 0 AND lane_b >= 0 ORDER BY id ASC LIMIT 50",
        50,
        50,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::IndexEntries,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "compound_prefix_control",
        "SELECT id FROM PerfAuditStreamingCompoundRow WHERE lane_a = 0 AND lane_b = 0 ORDER BY id ASC LIMIT 10",
        1,
        10,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::IndexEntries,
        StreamingFixtureGate::Review {
            threshold_basis_points: 100,
        },
    ),
    fixture(
        "limit1_early_wide",
        "SELECT id FROM PerfAuditStreamingRow ORDER BY id ASC LIMIT 1",
        1,
        1,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::StoreGets,
        StreamingFixtureGate::Review {
            threshold_basis_points: 100,
        },
    ),
    fixture(
        "limit1_late_selective",
        "SELECT id FROM PerfAuditStreamingRow WHERE label = 'late-match' ORDER BY id ASC LIMIT 1",
        1,
        1,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::StoreGets,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "order_compatible_control",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a = 0 ORDER BY id ASC LIMIT 21",
        21,
        21,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::PeakRetainedCandidates,
        StreamingFixtureGate::Review {
            threshold_basis_points: 100,
        },
    ),
    fixture(
        "topn_wide_payload",
        "SELECT id FROM PerfAuditStreamingRow ORDER BY sort_key ASC, id ASC LIMIT 10",
        10,
        10,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::PeakRetainedBackingBytes,
        StreamingFixtureGate::Improvement {
            minimum_basis_points: 2_000,
        },
    ),
    fixture(
        "full_sort_control",
        "SELECT id FROM PerfAuditStreamingRow ORDER BY sort_key ASC, id ASC LIMIT 2048",
        2_048,
        2_048,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::PeakRetainedCandidates,
        StreamingFixtureGate::Review {
            threshold_basis_points: 100,
        },
    ),
    fixture(
        "distinct_nonadjacent_cross_page",
        "SELECT DISTINCT label FROM PerfAuditStreamingRow ORDER BY label ASC LIMIT 10",
        3,
        10,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::Live,
        StreamingFixtureEvidence::PeakRetainedBackingBytes,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "group_ordered_mid_group",
        "SELECT group_key, COUNT(*) FROM PerfAuditStreamingRow GROUP BY group_key ORDER BY group_key ASC LIMIT 10",
        10,
        10,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::Live,
        StreamingFixtureEvidence::PeakRetainedCandidates,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "group_hash_noncontiguous",
        "SELECT label, COUNT(*) FROM PerfAuditStreamingRow GROUP BY label ORDER BY label ASC",
        3,
        0,
        StreamingFixtureDirection::NotApplicable,
        StreamingFixtureContinuation::Live,
        StreamingFixtureEvidence::PeakRetainedBackingBytes,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "continuation_live_10k",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a >= 0 ORDER BY id ASC",
        STREAMING_EXECUTION_CONTINUATION_ROWS,
        0,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::Live,
        StreamingFixtureEvidence::Progress,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "continuation_exhaustive_10k",
        "SELECT id FROM PerfAuditStreamingRow WHERE lane_a >= 0 ORDER BY id ASC",
        STREAMING_EXECUTION_CONTINUATION_ROWS,
        0,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::Exhaustive,
        StreamingFixtureEvidence::Progress,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "hard_budget_typed_headroom",
        "SELECT payload FROM PerfAuditStreamingRow ORDER BY sort_key ASC, id ASC",
        2_048,
        0,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::Progress,
        StreamingFixtureGate::Correctness,
    ),
    fixture(
        "point_get_control",
        "SELECT id FROM PerfAuditStreamingRow WHERE id = 1024",
        1,
        1,
        StreamingFixtureDirection::NotApplicable,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::StoreGets,
        StreamingFixtureGate::Review {
            threshold_basis_points: 100,
        },
    ),
    fixture(
        "ordered_empty_scan_control",
        "SELECT id FROM PerfAuditStreamingRow WHERE id = 0 ORDER BY id ASC LIMIT 1",
        0,
        1,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::IndexEntries,
        StreamingFixtureGate::Review {
            threshold_basis_points: 100,
        },
    ),
    fixture(
        "dense_full_return_control",
        "SELECT id FROM PerfAuditStreamingRow ORDER BY id ASC LIMIT 2048",
        2_048,
        2_048,
        StreamingFixtureDirection::Asc,
        StreamingFixtureContinuation::OneShot,
        StreamingFixtureEvidence::OutputRows,
        StreamingFixtureGate::Review {
            threshold_basis_points: 100,
        },
    ),
];

/// Primary semantic reason for one retained allocation family.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum MaterializationReason {
    /// Public result or cursor ownership boundary.
    OutputBoundary,
    /// State required by ordering, distinct, grouping, or deduplication.
    SemanticBlockingState,
    /// Bounded caller-authored exact input.
    BoundedInputContract,
    /// Query-shape-bounded operator topology or chunk state.
    SmallStructuralState,
    /// Ownership introduced only to connect internal stages.
    AvoidableConvenience,
}

/// Lifetime of one allocation family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationLifetime {
    /// Released before advancing from the current row.
    CurrentRow,
    /// Retained by one physical/logical operator.
    Operator,
    /// Retained until page finalization.
    Page,
}

/// Authority that bounds allocation cardinality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationSizeAuthority {
    /// Fixed or accepted-query-shape bounded.
    QueryShape,
    /// Bounded caller input.
    CallerInput,
    /// Depends on rows or candidates and requires a physical budget.
    RowCount,
}

/// Backing ownership retained by the family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationBacking {
    /// Compact owned keys/values.
    CompactOwned,
    /// Whole raw row payload ownership.
    RawPayload,
    /// Shared raw payload plus borrowed views.
    SharedPayload,
    /// Operator topology or descriptors.
    Structural,
}

/// Resume strategy required by the current allocation family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationResumability {
    /// Rebuilt cheaply from accepted query structure.
    Reconstructible,
    /// Rebuilt by deterministic bounded replay.
    Replay,
    /// Covered by the existing scalar cursor frontier.
    CursorFrontier,
    /// Completes as bounded blocking work before output.
    Blocking,
}

/// One reviewed production materialization owner family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaterializationInventoryEntry {
    /// Stable audit identity.
    pub id: &'static str,
    /// Workspace-relative canonical source owner.
    pub owner_file: &'static str,
    /// Source marker checked by the focused audit test.
    pub owner_symbol: &'static str,
    /// Primary semantic reason.
    pub reason: MaterializationReason,
    /// Retention lifetime.
    pub lifetime: MaterializationLifetime,
    /// Cardinality authority.
    pub size_authority: MaterializationSizeAuthority,
    /// Backing allocation family.
    pub backing: MaterializationBacking,
    /// Resume strategy.
    pub resumability: MaterializationResumability,
    /// Planned patch that owns the reviewed action.
    pub action_patch: u8,
}

/// Complete owner-family inventory for the maintained read executor.
pub const MATERIALIZATION_INVENTORY: &[MaterializationInventoryEntry] = &[
    MaterializationInventoryEntry {
        id: "bounded_exact_key_inputs",
        owner_file: "crates/icydb-core/src/db/executor/stream/access/physical.rs",
        owner_symbol: "let mut data_keys = Vec::with_capacity(keys.len())",
        reason: MaterializationReason::BoundedInputContract,
        lifetime: MaterializationLifetime::Operator,
        size_authority: MaterializationSizeAuthority::CallerInput,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::Reconstructible,
        action_patch: 1,
    },
    MaterializationInventoryEntry {
        id: "physical_scan_chunk_buffers",
        owner_file: "crates/icydb-core/src/db/executor/stream/access/scan.rs",
        owner_symbol: "LIMITED_SCAN_PREALLOC_CAP",
        reason: MaterializationReason::SmallStructuralState,
        lifetime: MaterializationLifetime::Operator,
        size_authority: MaterializationSizeAuthority::QueryShape,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::CursorFrontier,
        action_patch: 3,
    },
    MaterializationInventoryEntry {
        id: "materialized_secondary_index_predicate_keys",
        owner_file: "crates/icydb-core/src/db/executor/stream/access/physical.rs",
        owner_symbol: "fn charge_materialized_secondary_index_keys(",
        reason: MaterializationReason::AvoidableConvenience,
        lifetime: MaterializationLifetime::Operator,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::Blocking,
        action_patch: 3,
    },
    MaterializationInventoryEntry {
        id: "merge_child_topology",
        owner_file: "crates/icydb-core/src/db/executor/stream/key/contracts.rs",
        owner_symbol: "pub(in crate::db::executor) fn intersect_all(",
        reason: MaterializationReason::SmallStructuralState,
        lifetime: MaterializationLifetime::Operator,
        size_authority: MaterializationSizeAuthority::QueryShape,
        backing: MaterializationBacking::Structural,
        resumability: MaterializationResumability::Reconstructible,
        action_patch: 4,
    },
    MaterializationInventoryEntry {
        id: "scalar_page_output_candidates",
        owner_file: "crates/icydb-core/src/db/executor/terminal/page/scan.rs",
        owner_symbol: "fn scan_rows_with<T>",
        reason: MaterializationReason::OutputBoundary,
        lifetime: MaterializationLifetime::Page,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::RawPayload,
        resumability: MaterializationResumability::CursorFrontier,
        action_patch: 5,
    },
    MaterializationInventoryEntry {
        id: "kernel_retained_slots",
        owner_file: "crates/icydb-core/src/db/executor/terminal/page/retained.rs",
        owner_symbol: "enum RetainedSlotRowStorage",
        reason: MaterializationReason::SemanticBlockingState,
        lifetime: MaterializationLifetime::Page,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::CursorFrontier,
        action_patch: 5,
    },
    MaterializationInventoryEntry {
        id: "pure_covering_output_rows",
        owner_file: "crates/icydb-core/src/db/executor/projection/covering/pure.rs",
        owner_symbol: "fn collect_covering_rows_in_index_order",
        reason: MaterializationReason::OutputBoundary,
        lifetime: MaterializationLifetime::Page,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::CursorFrontier,
        action_patch: 5,
    },
    MaterializationInventoryEntry {
        id: "hybrid_covering_components",
        owner_file: "crates/icydb-core/src/db/executor/projection/covering/hybrid.rs",
        owner_symbol: "fn project_hybrid_covering_row",
        reason: MaterializationReason::AvoidableConvenience,
        lifetime: MaterializationLifetime::CurrentRow,
        size_authority: MaterializationSizeAuthority::QueryShape,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::Reconstructible,
        action_patch: 5,
    },
    MaterializationInventoryEntry {
        id: "structural_projection_output_rows",
        owner_file: "crates/icydb-core/src/db/executor/projection/materialize/structural/mod.rs",
        owner_symbol: "pub(in crate::db::executor) const fn from_value_rows",
        reason: MaterializationReason::OutputBoundary,
        lifetime: MaterializationLifetime::Page,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::CursorFrontier,
        action_patch: 5,
    },
    MaterializationInventoryEntry {
        id: "adjacent_distinct_output_window",
        owner_file: "crates/icydb-core/src/db/executor/projection/materialize/distinct.rs",
        owner_symbol: "struct AdjacentDistinctAccumulator",
        reason: MaterializationReason::OutputBoundary,
        lifetime: MaterializationLifetime::Page,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::CursorFrontier,
        action_patch: 7,
    },
    MaterializationInventoryEntry {
        id: "global_distinct_output_window",
        owner_file: "crates/icydb-core/src/db/executor/projection/materialize/distinct.rs",
        owner_symbol: "struct GlobalDistinctAccumulator",
        reason: MaterializationReason::OutputBoundary,
        lifetime: MaterializationLifetime::Page,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::Replay,
        action_patch: 7,
    },
    MaterializationInventoryEntry {
        id: "top_n_projection_candidate_window",
        owner_file: "crates/icydb-core/src/db/executor/terminal/page/scan.rs",
        owner_symbol: "fn scan_kernel_rows_with_bounded_order_window",
        reason: MaterializationReason::SemanticBlockingState,
        lifetime: MaterializationLifetime::Operator,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::Replay,
        action_patch: 6,
    },
    MaterializationInventoryEntry {
        id: "top_n_entity_output_window",
        owner_file: "crates/icydb-core/src/db/executor/order.rs",
        owner_symbol: "pub(in crate::db::executor) struct DataRowOrderWindow",
        reason: MaterializationReason::SemanticBlockingState,
        lifetime: MaterializationLifetime::Operator,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::RawPayload,
        resumability: MaterializationResumability::Replay,
        action_patch: 6,
    },
    MaterializationInventoryEntry {
        id: "projection_distinct_set",
        owner_file: "crates/icydb-core/src/db/executor/projection/materialize/distinct.rs",
        owner_symbol: "struct DistinctProjectionRowSet",
        reason: MaterializationReason::SemanticBlockingState,
        lifetime: MaterializationLifetime::Operator,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::Replay,
        action_patch: 7,
    },
    MaterializationInventoryEntry {
        id: "scalar_aggregate_distinct_values",
        owner_file: "crates/icydb-core/src/db/executor/aggregate/scalar_terminals/reducer.rs",
        owner_symbol: "struct ScalarDistinctValueSet",
        reason: MaterializationReason::SemanticBlockingState,
        lifetime: MaterializationLifetime::Operator,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::Blocking,
        action_patch: 8,
    },
    MaterializationInventoryEntry {
        id: "grouped_hash_bundle",
        owner_file: "crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/bundle.rs",
        owner_symbol: "struct GroupedAggregateBundle",
        reason: MaterializationReason::SemanticBlockingState,
        lifetime: MaterializationLifetime::Operator,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::Blocking,
        action_patch: 8,
    },
    MaterializationInventoryEntry {
        id: "grouped_page_top_n",
        owner_file: "crates/icydb-core/src/db/executor/aggregate/runtime/grouped_fold/generic/page_finalize.rs",
        owner_symbol: "BinaryHeap::<GroupedPageCandidate>::new()",
        reason: MaterializationReason::SemanticBlockingState,
        lifetime: MaterializationLifetime::Page,
        size_authority: MaterializationSizeAuthority::RowCount,
        backing: MaterializationBacking::CompactOwned,
        resumability: MaterializationResumability::Replay,
        action_patch: 8,
    },
];

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs, path::Path};

    use super::*;

    #[test]
    fn fixed_fixture_matrix_is_unique_bounded_and_gate_complete() {
        assert!(STREAMING_EXECUTION_FIXTURES.len() <= 32);
        let mut ids = BTreeSet::new();
        for fixture in STREAMING_EXECUTION_FIXTURES {
            assert!(ids.insert(fixture.id), "duplicate fixture {}", fixture.id);
            assert!(!fixture.sql.is_empty());
            assert!(fixture.sql.len() <= 512);
            if matches!(
                fixture.id,
                "continuation_live_10k" | "continuation_exhaustive_10k"
            ) {
                assert_eq!(fixture.expected_rows, STREAMING_EXECUTION_CONTINUATION_ROWS);
                assert!(fixture.expected_rows > 10_000);
            } else {
                assert!(fixture.expected_rows <= STREAMING_EXECUTION_FIXTURE_ROWS);
            }
            if let StreamingFixtureGate::Improvement {
                minimum_basis_points,
            } = fixture.gate
            {
                assert_eq!(minimum_basis_points, 2_000);
            }
            if let StreamingFixtureGate::Review {
                threshold_basis_points,
            } = fixture.gate
            {
                assert_eq!(threshold_basis_points, 100);
            }
        }
        assert_eq!(STREAMING_EXECUTION_PREFIX_FANOUT, 16);
        assert_eq!(STREAMING_EXECUTION_WIDE_PAYLOAD_BYTES.len(), 3);

        let ordered_group = STREAMING_EXECUTION_FIXTURES
            .iter()
            .find(|fixture| fixture.id == "group_ordered_mid_group")
            .expect("ordered grouped fixture must remain declared");
        assert_eq!(
            ordered_group.sql,
            "SELECT group_key, COUNT(*) FROM PerfAuditStreamingRow GROUP BY group_key ORDER BY group_key ASC LIMIT 10",
        );
        let hash_group = STREAMING_EXECUTION_FIXTURES
            .iter()
            .find(|fixture| fixture.id == "group_hash_noncontiguous")
            .expect("hash grouped fixture must remain declared");
        assert_eq!(
            hash_group.sql,
            "SELECT label, COUNT(*) FROM PerfAuditStreamingRow GROUP BY label ORDER BY label ASC",
        );
        assert_eq!(hash_group.page_limit, 0);
    }

    #[test]
    fn materialization_inventory_is_unique_and_source_anchored() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut ids = BTreeSet::new();
        let mut reasons = BTreeSet::new();
        for entry in MATERIALIZATION_INVENTORY {
            assert!(ids.insert(entry.id), "duplicate inventory id {}", entry.id);
            reasons.insert(entry.reason);
            assert!((1..=8).contains(&entry.action_patch));
            let source = fs::read_to_string(workspace.join(entry.owner_file))
                .unwrap_or_else(|error| panic!("{} should be readable: {error}", entry.owner_file));
            assert!(
                source.contains(entry.owner_symbol),
                "{} must retain source marker {}",
                entry.owner_file,
                entry.owner_symbol,
            );
        }

        assert_eq!(reasons.len(), 5, "every primary reason must be represented");
    }
}
