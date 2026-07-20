//! Module: sql_correctness_support::coverage_manifest
//! Responsibility: machine-readable SQL contract coverage obligations and consistency checks.
//! Does not own: product behavior or the deterministic evidence supplied by cited tests.
//! Boundary: validates manifest entries against `SQL_SUBSET.md` and repository test providers.

use crate::sql_harness::{EligibleProvider, EvidenceClass, EvidenceStrength};
use icydb_testing_integration::sql_performance_contract::{
    SQL_PERFORMANCE_BROAD_CONTRACT_FEATURES, SQL_PERFORMANCE_SCALE_CONTRACT_FEATURES,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use icydb_testing_sql_generator::TIER_C_SQL_COVERAGE_MANIFEST_REVISION;
use icydb_testing_sqlite_reference::required_sqlite_reference_scenarios;

///
/// FeatureKind
///
/// Manifest classification for the kind of SQL contract feature being evidenced.
/// Owned by the correctness coverage manifest and parsed from its static vocabulary.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum FeatureKind {
    Interaction,
    Policy,
    Semantic,
    Syntax,
}

impl FeatureKind {
    /// Parse the closed feature-kind vocabulary used by the contract document.
    fn parse(value: &str) -> Option<Self> {
        match value {
            "interaction" => Some(Self::Interaction),
            "policy" => Some(Self::Policy),
            "semantic" => Some(Self::Semantic),
            "syntax" => Some(Self::Syntax),
            _ => None,
        }
    }

    /// Return the stable machine-readable feature-kind identity.
    const fn code(self) -> &'static str {
        match self {
            Self::Interaction => "interaction",
            Self::Policy => "policy",
            Self::Semantic => "semantic",
            Self::Syntax => "syntax",
        }
    }
}

///
/// FeatureStatus
///
/// Accepted or rejected status declared for one SQL contract feature.
/// Owned by the correctness coverage manifest and checked against contract documentation.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum FeatureStatus {
    Accepted,
    Rejected,
}

impl FeatureStatus {
    /// Parse the closed feature-status vocabulary used by the contract document.
    fn parse(value: &str) -> Option<Self> {
        match value {
            "accepted" => Some(Self::Accepted),
            "rejected" => Some(Self::Rejected),
            _ => None,
        }
    }

    /// Return the stable machine-readable feature-status identity.
    const fn code(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
        }
    }
}

///
/// FeatureCategory
///
/// Semantic category used to organize SQL contract coverage cells.
/// Owned and consumed by the correctness coverage manifest.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FeatureCategory {
    Clause,
    Expression,
    Interaction,
    Policy,
    Statement,
    ValueType,
}

impl FeatureCategory {
    /// Return the stable machine-readable feature-category identity.
    const fn code(self) -> &'static str {
        match self {
            Self::Clause => "clause",
            Self::Expression => "expression",
            Self::Interaction => "interaction",
            Self::Policy => "policy",
            Self::Statement => "statement",
            Self::ValueType => "value_type",
        }
    }
}

///
/// PerformanceObligation
///
/// Performance evidence class required by one SQL contract feature.
/// Owned by the correctness coverage manifest and matched against declared scenarios.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PerformanceObligation {
    BroadScan,
    FocusedHotspot,
    None,
    RegressionSentinel,
    ScaleSentinel,
}

impl PerformanceObligation {
    /// Return the stable machine-readable performance-obligation identity.
    const fn code(self) -> &'static str {
        match self {
            Self::BroadScan => "broad_scan",
            Self::FocusedHotspot => "focused_hotspot",
            Self::None => "none",
            Self::RegressionSentinel => "regression_sentinel",
            Self::ScaleSentinel => "scale_sentinel",
        }
    }
}

///
/// EvidenceRequirement
///
/// Minimum evidence layer and strength required for one coverage cell.
/// Owned by the correctness coverage manifest and satisfied by provider declarations.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EvidenceRequirement {
    class: EvidenceClass,
    minimum_strength: EvidenceStrength,
}

///
/// CoverageCell
///
/// Complete manifest obligation for one documented SQL contract feature.
/// Owned by the correctness coverage manifest and validated by its consistency gate.
///

#[derive(Clone, Copy, Debug)]
struct CoverageCell {
    id: &'static str,
    kind: FeatureKind,
    status: FeatureStatus,
    contract_section: &'static str,
    category: FeatureCategory,
    evidence: &'static [EvidenceRequirement],
    performance: &'static [PerformanceObligation],
    eligible_providers: &'static [EligibleProvider],
    deterministic_providers: &'static [&'static str],
    generated_families: &'static [&'static str],
    reference_exclusion: Option<&'static str>,
}

///
/// ProviderSpec
///
/// Static identity and evidence capabilities of one repository test provider.
/// Owned by the correctness coverage manifest and resolved against repository source.
///

#[derive(Clone, Copy, Debug)]
struct ProviderSpec {
    id: &'static str,
    source_path: &'static str,
    test_symbol: &'static str,
    evidence: &'static [EvidenceClass],
    strength: EvidenceStrength,
}

///
/// GeneratedSelectExclusionAttribution
///
/// Maintained deterministic evidence for one schema or value stratum deliberately
/// excluded from the generated SELECT reference profile.
/// Owned by the correctness coverage manifest and validated against `PROVIDERS`.
///

#[derive(Clone, Copy, Debug)]
struct GeneratedSelectExclusionAttribution {
    /// Stable identity of the excluded generated-SELECT stratum.
    stratum: &'static str,
    /// Deterministic providers that exercise the stratum without claiming generated coverage.
    deterministic_providers: &'static [&'static str],
}

///
/// ContractFeature
///
/// SQL feature declaration parsed from the active contract document.
/// Owned by the correctness coverage manifest and compared with static coverage cells.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct ContractFeature {
    kind: FeatureKind,
    status: FeatureStatus,
    section: String,
    line: usize,
}

const REQ_PARSE: &[EvidenceRequirement] = &[EvidenceRequirement {
    class: EvidenceClass::Parse,
    minimum_strength: EvidenceStrength::ContractAssertion,
}];
const REQ_LOWER: &[EvidenceRequirement] = &[EvidenceRequirement {
    class: EvidenceClass::Lower,
    minimum_strength: EvidenceStrength::ContractAssertion,
}];
const REQ_EXECUTE: &[EvidenceRequirement] = &[EvidenceRequirement {
    class: EvidenceClass::Execute,
    minimum_strength: EvidenceStrength::ContractAssertion,
}];
const REQ_EXECUTE_REFERENCE: &[EvidenceRequirement] = &[
    EvidenceRequirement {
        class: EvidenceClass::Execute,
        minimum_strength: EvidenceStrength::ContractAssertion,
    },
    EvidenceRequirement {
        class: EvidenceClass::ReferenceDifferential,
        minimum_strength: EvidenceStrength::ReferenceOracle,
    },
];
const REQ_METAMORPHIC_EXECUTE: &[EvidenceRequirement] = &[EvidenceRequirement {
    class: EvidenceClass::Execute,
    minimum_strength: EvidenceStrength::MetamorphicInvariant,
}];
const REQ_BOUNDARY: &[EvidenceRequirement] = &[EvidenceRequirement {
    class: EvidenceClass::Boundary,
    minimum_strength: EvidenceStrength::BoundaryAssertion,
}];
const REQ_BOUNDARY_LOWER: &[EvidenceRequirement] = &[
    EvidenceRequirement {
        class: EvidenceClass::Boundary,
        minimum_strength: EvidenceStrength::BoundaryAssertion,
    },
    EvidenceRequirement {
        class: EvidenceClass::Lower,
        minimum_strength: EvidenceStrength::ContractAssertion,
    },
];
const REQ_STATE: &[EvidenceRequirement] = &[EvidenceRequirement {
    class: EvidenceClass::State,
    minimum_strength: EvidenceStrength::ContractAssertion,
}];
const REQ_ROUTE_EXECUTE: &[EvidenceRequirement] = &[
    EvidenceRequirement {
        class: EvidenceClass::Route,
        minimum_strength: EvidenceStrength::ContractAssertion,
    },
    EvidenceRequirement {
        class: EvidenceClass::Execute,
        minimum_strength: EvidenceStrength::ContractAssertion,
    },
];

const EVIDENCE_CLASS_TAXONOMY: &[EvidenceClass] = &[
    EvidenceClass::Parse,
    EvidenceClass::Lower,
    EvidenceClass::Execute,
    EvidenceClass::Route,
    EvidenceClass::Boundary,
    EvidenceClass::State,
    EvidenceClass::ReferenceDifferential,
    EvidenceClass::Regression,
];
const EVIDENCE_STRENGTH_TAXONOMY: &[EvidenceStrength] = &[
    EvidenceStrength::ReferenceOracle,
    EvidenceStrength::MetamorphicInvariant,
    EvidenceStrength::ContractAssertion,
    EvidenceStrength::BoundaryAssertion,
];
const PERFORMANCE_OBLIGATION_TAXONOMY: &[PerformanceObligation] = &[
    PerformanceObligation::None,
    PerformanceObligation::BroadScan,
    PerformanceObligation::ScaleSentinel,
    PerformanceObligation::RegressionSentinel,
    PerformanceObligation::FocusedHotspot,
];

const PERF_NONE: &[PerformanceObligation] = &[PerformanceObligation::None];
const PERF_BROAD: &[PerformanceObligation] = &[PerformanceObligation::BroadScan];
const PERF_SCALE: &[PerformanceObligation] = &[PerformanceObligation::ScaleSentinel];

const ELIGIBLE_SQLITE: &[EligibleProvider] = &[
    EligibleProvider::SqliteReference,
    EligibleProvider::FrontendEquivalent,
];
const ELIGIBLE_STATE: &[EligibleProvider] = &[
    EligibleProvider::StateModelReference,
    EligibleProvider::SqliteReference,
];
const ELIGIBLE_FRONTEND: &[EligibleProvider] = &[EligibleProvider::FrontendEquivalent];
const ELIGIBLE_EXECUTION_MODE: &[EligibleProvider] = &[
    EligibleProvider::ExecutionModeEquivalent,
    EligibleProvider::IcyDbContractOnly,
];
const ELIGIBLE_ICYDB: &[EligibleProvider] = &[EligibleProvider::IcyDbContractOnly];
const ELIGIBLE_REJECTION: &[EligibleProvider] = &[EligibleProvider::RejectionInvariant];

const NO_EXTERNAL_SYNTAX: Option<&str> =
    Some("SQL acceptance is an IcyDB product boundary; an external engine cannot define it.");
const NO_EXTERNAL_POLICY: Option<&str> =
    Some("Admission and generated-entrypoint policy are IcyDB-specific boundary contracts.");
const NO_EXTERNAL_ROUTE: Option<&str> = Some(
    "Planner route and continuation facts are IcyDB execution contracts, not reference-engine semantics.",
);
const NO_EXTERNAL_CATALOG: Option<&str> =
    Some("Accepted-schema catalog, DDL publication, and introspection shapes are IcyDB-specific.");
const NO_EXTERNAL_TEXT: Option<&str> = Some(
    "The current text casefold and expression rules do not have a declared lossless SQLite mapping.",
);
const NO_EXTERNAL_BLOB_LIMIT: Option<&str> = Some(
    "The literal allocation cap and SQL transport boundary are IcyDB resource-policy contracts.",
);
const SQLITE_REFERENCE_PROVIDER_ID: &str = "core.query.sqlite_reference_profile";

macro_rules! provider {
    ($id:literal, $path:literal, $symbol:literal, $strength:ident, [$($evidence:ident),+ $(,)?]) => {
        ProviderSpec {
            id: $id,
            source_path: $path,
            test_symbol: $symbol,
            evidence: &[$(EvidenceClass::$evidence),+],
            strength: EvidenceStrength::$strength,
        }
    };
}

const PROVIDERS: &[ProviderSpec] = &[
    provider!(
        "build.sql.trusted_entrypoints",
        "crates/icydb-build/src/db/sql.rs",
        "generated_readonly_sql_surface_uses_trusted_query_and_admin_ddl",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "build.sql.introspection_guard",
        "crates/icydb-build/src/db/sql.rs",
        "generated_sql_query_surface_can_reject_introspection",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "config.sql.introspection_target_policy",
        "crates/icydb-config/src/tests.rs",
        "sql_introspection_policy_defaults_local_on_ic_off",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "config.sql.update_disabled_default",
        "crates/icydb-config/src/tests.rs",
        "absent_config_defaults_minimal_metrics_on",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "config.sql.update_primary_key_boolean",
        "crates/icydb-config/src/tests.rs",
        "boolean_sql_update_policy_enables_primary_key_default",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "config.sql.update_primary_key_named",
        "crates/icydb-config/src/tests.rs",
        "named_primary_key_sql_update_policy_enables_primary_key_policy",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "config.sql.update_bounded_named",
        "crates/icydb-config/src/tests.rs",
        "named_bounded_sql_update_policy_enables_bounded_policy",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "build.sql.update_disabled_by_default",
        "crates/icydb-build/src/db/sql.rs",
        "generated_sql_surface_exports_only_query_ddl_and_fixture_endpoints",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "build.sql.update_primary_key_policy",
        "crates/icydb-build/src/db/sql.rs",
        "generated_sql_update_surface_requires_explicit_primary_key_policy",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "build.sql.update_bounded_policy",
        "crates/icydb-build/src/db/sql.rs",
        "generated_sql_update_surface_can_select_bounded_policy_without_broad_update",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "core.query.public_read_families",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_admits_supported_single_entity_read_shapes",
        ContractAssertion,
        [Parse, Lower, Execute]
    ),
    provider!(
        "core.query.sqlite_reference_profile",
        "crates/icydb-core/src/db/session/tests/sqlite_reference.rs",
        "required_sqlite_reference_profile_matches_native_icydb",
        ReferenceOracle,
        [ReferenceDifferential]
    ),
    provider!(
        "core.query.unsupported_families",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_rejects_unsupported_sql_families",
        ContractAssertion,
        [Parse, Lower]
    ),
    provider!(
        "core.query.unsupported_expressions",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_rejects_unsupported_expression_sql_families",
        ContractAssertion,
        [Parse, Lower]
    ),
    provider!(
        "core.query.invalid_grouped_projection",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_rejects_invalid_grouped_projection_shapes",
        ContractAssertion,
        [Lower]
    ),
    provider!(
        "core.query.scalar_matrix",
        "crates/icydb-core/src/db/session/tests/sql_scalar.rs",
        "execute_sql_scalar_matrix_queries_match_expected_rows",
        ContractAssertion,
        [Parse, Lower, Execute]
    ),
    provider!(
        "core.query.projection_shape",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_preserves_deterministic_projection_shape",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.distinct_window",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_preserves_scalar_distinct_ordered_window",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.global_aggregate",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_evaluates_basic_global_aggregate_loads",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.grouped_aggregate",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_evaluates_basic_grouped_aggregate_loads",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.aggregate_inputs",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_evaluates_aggregate_input_expressions",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.aggregate_distinct_filter",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_evaluates_aggregate_distinct_and_filter_terminals",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.composition",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_composes_supported_scalar_and_grouped_forms",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.null_ordering",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_defines_order_by_null_ordering",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.exact_key_route",
        "crates/icydb-core/src/db/session/tests/explain_execution.rs",
        "session_explain_execution_external_primary_key_filter_and_by_id_use_same_access_path",
        ContractAssertion,
        [Route]
    ),
    provider!(
        "core.query.exact_key_execute",
        "crates/icydb-core/src/db/session/tests/sql_projection.rs",
        "execute_sql_projection_ulid_string_literal_predicate_matches_single_row",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.parameters_rejected",
        "crates/icydb-core/src/db/sql/lowering/tests/mod.rs",
        "prepare_sql_statement_rejects_parameters_before_lowering",
        ContractAssertion,
        [Lower]
    ),
    provider!(
        "core.query.grouped_cursor",
        "crates/icydb-core/src/db/session/tests/execution_convergence.rs",
        "sql_and_fluent_grouped_execution_match_groups_aggregates_and_cursor",
        MetamorphicInvariant,
        [Execute]
    ),
    provider!(
        "core.query.grouped_execution_mode_parity",
        "crates/icydb-core/src/db/session/tests/sql_grouped.rs",
        "grouped_ordered_and_hash_execution_modes_preserve_current_family_parity",
        MetamorphicInvariant,
        [Route, Execute]
    ),
    provider!(
        "core.query.explain_public",
        "crates/icydb-core/src/db/session/tests/sql_explain.rs",
        "execute_trusted_sql_query_explain_plan_matrix_returns_public_explain_payload",
        ContractAssertion,
        [Parse, Lower, Execute]
    ),
    provider!(
        "core.query.explain_route_facts",
        "crates/icydb-core/src/db/session/tests/sql_explain.rs",
        "execute_trusted_sql_query_explain_execution_separates_index_pushdown_from_residual_predicate",
        ContractAssertion,
        [Route, Execute]
    ),
    provider!(
        "core.query.metadata_public",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "sql_metadata_surfaces_execute_through_public_query_entrypoint",
        ContractAssertion,
        [Parse, Lower, Execute]
    ),
    provider!(
        "core.query.catalog_metadata",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "sql_catalog_surfaces_include_store_metadata",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.qualified_names",
        "crates/icydb-core/src/db/session/tests/sql_projection.rs",
        "execute_sql_projection_qualified_identifier_matrix_executes",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.computed_projection",
        "crates/icydb-core/src/db/session/tests/sql_projection.rs",
        "execute_sql_projection_computed_function_matrix_runs_from_session_boundary",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.searched_case",
        "crates/icydb-core/src/db/session/tests/sql_projection.rs",
        "execute_sql_projection_searched_case_matrix_matches_expected_values",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.searched_case_where",
        "crates/icydb-core/src/db/session/tests/sql_scalar.rs",
        "execute_sql_scalar_searched_case_where_matches_expected_rows",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.searched_case_grouped_having",
        "crates/icydb-core/src/db/session/tests/sql_grouped.rs",
        "grouped_select_allows_searched_case_projection_and_having",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.simple_case_rejected",
        "crates/icydb-core/src/db/sql/parser/tests/mod.rs",
        "parse_select_statement_rejects_simple_case_expressions",
        ContractAssertion,
        [Parse]
    ),
    provider!(
        "core.query.value_selection",
        "crates/icydb-core/src/db/session/tests/sql_scalar.rs",
        "execute_sql_scalar_coalesce_and_nullif_where_matches_expected_rows",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.alias_order",
        "crates/icydb-core/src/db/session/tests/sql_projection.rs",
        "execute_sql_projection_order_by_alias_matrix_matches_canonical_rows",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.field_comparison",
        "crates/icydb-core/src/db/session/tests/sql_scalar.rs",
        "execute_sql_scalar_field_to_field_predicate_matches_expected_rows",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.grouped_where_field_comparison",
        "crates/icydb-core/src/db/session/tests/sql_grouped.rs",
        "grouped_select_helper_executes_field_to_field_predicate",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.membership",
        "crates/icydb-core/src/db/session/tests/sql_scalar.rs",
        "execute_sql_scalar_in_trailing_comma_matches_canonical_rows",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.membership_null_semantics",
        "crates/icydb-core/src/db/session/tests/predicate_convergence.rs",
        "predicate_sql_membership_with_null_preserves_three_valued_semantics",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.range",
        "crates/icydb-core/src/db/session/tests/sql_scalar.rs",
        "execute_sql_scalar_not_between_matches_fluent_runtime_result",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.field_bound_range",
        "crates/icydb-core/src/db/session/tests/sql_scalar.rs",
        "execute_sql_scalar_field_bound_between_and_not_between_match_fluent_results",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.null_predicates",
        "crates/icydb-core/src/db/session/tests/sql_scalar.rs",
        "execute_sql_scalar_nullable_field_distinguishes_null_tests_from_null_compares",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.boolean_truth",
        "crates/icydb-core/src/db/session/tests/sql_scalar.rs",
        "execute_sql_scalar_is_true_false_and_is_not_true_false_match_expected_rows",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.prefix_not_like",
        "crates/icydb-core/src/db/session/tests/direct_starts_with.rs",
        "execute_sql_not_like_prefix_matrix_matches_negated_prefix_rows",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.prefix_ilike",
        "crates/icydb-core/src/db/session/tests/direct_starts_with.rs",
        "execute_sql_ilike_prefix_matrix_matches_casefold_prefix_rows",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.prefix_not_ilike",
        "crates/icydb-core/src/db/session/tests/direct_starts_with.rs",
        "execute_sql_not_ilike_prefix_matrix_matches_negated_casefold_prefix_rows",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.starts_with",
        "crates/icydb-core/src/db/session/tests/direct_starts_with.rs",
        "execute_sql_direct_starts_with_family_matrix_matches_indexed_like_rows",
        MetamorphicInvariant,
        [Execute]
    ),
    provider!(
        "core.query.expression_arguments",
        "crates/icydb-core/src/db/session/tests/sql_scalar.rs",
        "execute_sql_scalar_text_predicate_expression_arguments_where_match_expected_rows",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.having",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_having_terms_are_not_auto_projected",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.global_having",
        "crates/icydb-core/src/db/session/tests/sql_aggregate.rs",
        "global_aggregate_having_returns_single_row_when_predicate_matches",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.query.parenthesized_boolean",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_query_preserves_parenthesized_boolean_predicate_semantics",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.ddl.create_field_path",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_publishes_supported_field_path_index",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.create_multi_field",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_publishes_and_drops_supported_multi_field_path_index",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.index_ascending",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_treats_asc_index_order_as_default_order",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.create_filtered",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_publishes_supported_filtered_field_path_index",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.create_expression",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_publishes_supported_expression_index",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.create_if_not_exists",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_create_index_if_not_exists_reports_no_op_for_existing_index",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.create_unique",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_publishes_supported_unique_field_path_index",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.create_unique_field_path_precommit",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_rejects_duplicate_unique_field_path_values_without_publication",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.create_unique_expression_precommit",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_rejects_duplicate_unique_expression_values_without_publication",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.desc_rejected",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_rejects_desc_index_order_without_publication",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.drop_index",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_drops_supported_ddl_published_index",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.drop_index_if_exists",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_drop_index_if_exists_reports_no_op_for_missing_index",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.add_column",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_publishes_supported_nullable_add_column",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.alter_default",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_publishes_supported_set_default",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.alter_nullability",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_publishes_supported_set_not_null_after_row_scan",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.rename_column",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_publishes_rename_column_for_ddl_owned_field",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.drop_column",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_publishes_drop_column_for_non_trailing_ddl_owned_field",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.generated_owned_rejected",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_rejects_generated_index_drop_with_structured_detail",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "core.ddl.generated_field_drop_rejected",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "sql_ddl_alter_table_drop_column_rejects_generated_fields",
        ContractAssertion,
        [Lower]
    ),
    provider!(
        "core.ddl.generated_field_default_rejected",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "sql_ddl_alter_table_alter_column_default_rejects_generated_fields",
        ContractAssertion,
        [Lower]
    ),
    provider!(
        "core.ddl.generated_field_nullability_rejected",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "sql_ddl_alter_table_alter_column_nullability_rejects_generated_changes",
        ContractAssertion,
        [Lower]
    ),
    provider!(
        "core.ddl.generated_field_rename_rejected",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "sql_ddl_alter_table_rename_column_rejects_generated_fields",
        ContractAssertion,
        [Lower]
    ),
    provider!(
        "core.ddl.drop_column_rollback",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_drop_column_rolls_back_rows_when_publication_rejects",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.drop_index_precommit_atomicity",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_admin_sql_ddl_drop_index_rejects_before_physical_replacement",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.ddl.index_recovery_retry",
        "crates/icydb-core/src/db/commit/tests/mod.rs",
        "recovery_secondary_index_rebuild_clear_failpoint_is_retryable_for_error_and_unwind",
        ContractAssertion,
        [State]
    ),
    provider!(
        "core.mutation.public_families",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "execute_trusted_sql_mutation_admits_supported_single_entity_mutation_shapes",
        ContractAssertion,
        [Parse, Lower, Execute, State]
    ),
    provider!(
        "core.mutation.returning_star",
        "crates/icydb-core/src/db/session/tests/sql_write.rs",
        "execute_trusted_sql_mutation_returning_star_public_entrypoint_projects_rows",
        ContractAssertion,
        [Execute, State]
    ),
    provider!(
        "core.mutation.returning_fields",
        "crates/icydb-core/src/db/session/tests/sql_write.rs",
        "execute_trusted_sql_mutation_returning_field_list_public_entrypoint_projects_rows",
        ContractAssertion,
        [Execute, State]
    ),
    provider!(
        "core.mutation.returning_rejected",
        "crates/icydb-core/src/db/session/tests/sql_write.rs",
        "execute_trusted_sql_mutation_rejects_unsupported_sql_without_mutation",
        ContractAssertion,
        [Lower, State]
    ),
    provider!(
        "core.mutation.lane_ownership",
        "crates/icydb-core/src/db/session/tests/sql_surface.rs",
        "sql_ddl_create_index_is_rejected_by_query_and_update_surfaces",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "core.mutation.trusted_update_window",
        "crates/icydb-core/src/db/session/tests/sql_write.rs",
        "execute_sql_statement_update_with_order_limit_and_offset_updates_one_ordered_window",
        ContractAssertion,
        [Execute, State]
    ),
    provider!(
        "canister.mutation.query_rejects_update",
        "testing/integration/tests/sql_canister.rs",
        "sql_canister_query_endpoint_rejects_update_sql",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "canister.mutation.ddl_rejects_rows",
        "testing/integration/tests/sql_canister.rs",
        "sql_canister_ddl_endpoint_rejects_row_mutation_sql_without_row_mutation",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "canister.mutation.primary_key_policy",
        "testing/integration/tests/sql_canister.rs",
        "sql_canister_update_endpoint_admits_primary_key_update_only",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "canister.mutation.bounded_policy",
        "testing/integration/tests/sql_canister.rs",
        "sql_canister_bounded_update_endpoint_admits_explicit_limited_primary_key_order",
        BoundaryAssertion,
        [Boundary]
    ),
    provider!(
        "core.blob.equality",
        "crates/icydb-core/src/db/session/tests/sql_blob.rs",
        "sql_blob_equality_predicates_compare_bytes",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.blob.insert_hex",
        "crates/icydb-core/src/db/session/tests/sql_blob.rs",
        "sql_insert_values_writes_multiple_large_hex_blob_literals",
        ContractAssertion,
        [Execute, State]
    ),
    provider!(
        "core.blob.update_hex",
        "crates/icydb-core/src/db/session/tests/sql_blob.rs",
        "sql_update_writes_large_hex_blob_literals_to_multiple_rows",
        ContractAssertion,
        [Execute, State]
    ),
    provider!(
        "core.blob.octet_length",
        "crates/icydb-core/src/db/session/tests/sql_blob.rs",
        "sql_octet_length_reports_blob_byte_lengths",
        ContractAssertion,
        [Execute]
    ),
    provider!(
        "core.blob.literal_boundary",
        "crates/icydb-core/src/db/session/tests/sql_blob.rs",
        "sql_blob_literals_fail_closed_through_public_update_entrypoint",
        BoundaryAssertion,
        [Parse, Boundary, State]
    ),
    provider!(
        "core.blob.order_rejected",
        "crates/icydb-core/src/db/session/tests/sql_blob.rs",
        "sql_order_by_blob_field_is_rejected",
        ContractAssertion,
        [Lower]
    ),
];

const GENERATED_SELECT_EXCLUSION_ATTRIBUTIONS: &[GeneratedSelectExclusionAttribution] = &[
    GeneratedSelectExclusionAttribution {
        stratum: "blob_value",
        deterministic_providers: &[
            "core.blob.equality",
            "core.blob.insert_hex",
            "core.blob.literal_boundary",
            "core.blob.octet_length",
            "core.blob.order_rejected",
            "core.blob.update_hex",
        ],
    },
    GeneratedSelectExclusionAttribution {
        stratum: "database_default",
        deterministic_providers: &["core.ddl.alter_default"],
    },
    GeneratedSelectExclusionAttribution {
        stratum: "generated_ulid_exact_key",
        deterministic_providers: &["core.query.exact_key_execute", "core.query.exact_key_route"],
    },
    GeneratedSelectExclusionAttribution {
        stratum: "nested_field_path_index",
        deterministic_providers: &["core.ddl.create_field_path"],
    },
    GeneratedSelectExclusionAttribution {
        stratum: "secondary_index",
        deterministic_providers: &[
            "core.ddl.create_expression",
            "core.ddl.create_multi_field",
            "core.query.explain_route_facts",
            "core.query.starts_with",
        ],
    },
];

macro_rules! deterministic_providers {
    (REQ_EXECUTE_REFERENCE, [$($provider:literal),+ $(,)?]) => {
        &[$($provider),+, SQLITE_REFERENCE_PROVIDER_ID]
    };
    ($evidence:ident, [$($provider:literal),+ $(,)?]) => {
        &[$($provider),+]
    };
}

macro_rules! cell {
    (
        $id:literal,
        $kind:ident,
        $status:ident,
        $section:literal,
        $category:ident,
        $evidence:ident,
        $performance:ident,
        $eligible:ident,
        [$($provider:literal),+ $(,)?],
        $reference_exclusion:expr
    ) => {
        CoverageCell {
            id: $id,
            kind: FeatureKind::$kind,
            status: FeatureStatus::$status,
            contract_section: $section,
            category: FeatureCategory::$category,
            evidence: $evidence,
            performance: $performance,
            eligible_providers: $eligible,
            deterministic_providers: deterministic_providers!(
                $evidence,
                [$($provider),+]
            ),
            generated_families: &[],
            reference_exclusion: $reference_exclusion,
        }
    };
}

const MANIFEST: &[CoverageCell] = &[
    cell!(
        "surface.single_entity",
        Semantic,
        Accepted,
        "Core Rule",
        Policy,
        REQ_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_FRONTEND,
        ["core.query.public_read_families"],
        NO_EXTERNAL_ROUTE
    ),
    cell!(
        "surface.trusted_entrypoints",
        Policy,
        Accepted,
        "Core Rule",
        Policy,
        REQ_BOUNDARY,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["build.sql.trusted_entrypoints"],
        NO_EXTERNAL_POLICY
    ),
    cell!(
        "pagination.scalar_cursor",
        Syntax,
        Rejected,
        "Cursor Pagination",
        Clause,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_families"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "pagination.scalar_limit_offset",
        Semantic,
        Accepted,
        "Cursor Pagination",
        Clause,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.scalar_matrix"],
        None
    ),
    cell!(
        "pagination.grouped_cursor",
        Semantic,
        Accepted,
        "Cursor Pagination",
        Clause,
        REQ_METAMORPHIC_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_EXECUTION_MODE,
        [
            "core.query.grouped_cursor",
            "core.query.grouped_execution_mode_parity"
        ],
        NO_EXTERNAL_ROUTE
    ),
    cell!(
        "operational.transport_controls",
        Syntax,
        Rejected,
        "Operational vs Semantic Features",
        Clause,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_families"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "operational.byte_metrics",
        Syntax,
        Rejected,
        "Operational vs Semantic Features",
        Expression,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_expressions"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "select.scalar_rows",
        Syntax,
        Accepted,
        "`SELECT`",
        Statement,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.public_read_families"],
        None
    ),
    cell!(
        "select.scalar_distinct",
        Syntax,
        Accepted,
        "`SELECT`",
        Statement,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.distinct_window"],
        None
    ),
    cell!(
        "select.global_aggregate",
        Syntax,
        Accepted,
        "`SELECT`",
        Statement,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.global_aggregate"],
        None
    ),
    cell!(
        "select.grouped_aggregate",
        Syntax,
        Accepted,
        "`SELECT`",
        Statement,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        [
            "core.query.grouped_aggregate",
            "core.query.grouped_execution_mode_parity"
        ],
        None
    ),
    cell!(
        "select.aggregate_distinct_filter",
        Semantic,
        Accepted,
        "`SELECT`",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.aggregate_distinct_filter"],
        None
    ),
    cell!(
        "select.computed_projection",
        Semantic,
        Accepted,
        "`SELECT`",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.computed_projection"],
        None
    ),
    cell!(
        "select.scalar_composition",
        Interaction,
        Accepted,
        "`SELECT`",
        Interaction,
        REQ_EXECUTE_REFERENCE,
        PERF_NONE,
        ELIGIBLE_SQLITE,
        ["core.query.composition"],
        None
    ),
    cell!(
        "select.grouped_composition",
        Interaction,
        Accepted,
        "`SELECT`",
        Interaction,
        REQ_EXECUTE_REFERENCE,
        PERF_NONE,
        ELIGIBLE_SQLITE,
        ["core.query.composition"],
        None
    ),
    cell!(
        "ordering.null_values",
        Semantic,
        Accepted,
        "`SELECT`",
        Clause,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.null_ordering"],
        None
    ),
    cell!(
        "select.exact_primary_key",
        Interaction,
        Accepted,
        "Exact Primary-Key Reads",
        Interaction,
        REQ_ROUTE_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_EXECUTION_MODE,
        ["core.query.exact_key_route", "core.query.exact_key_execute"],
        NO_EXTERNAL_ROUTE
    ),
    cell!(
        "select.placeholder_parameters",
        Syntax,
        Rejected,
        "Exact Primary-Key Reads",
        Expression,
        REQ_LOWER,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.parameters_rejected"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "explain.query_delete",
        Syntax,
        Accepted,
        "`EXPLAIN`",
        Statement,
        REQ_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_ICYDB,
        [
            "core.query.explain_public",
            "core.query.explain_route_facts"
        ],
        NO_EXTERNAL_ROUTE
    ),
    cell!(
        "introspection.describe",
        Syntax,
        Accepted,
        "Introspection",
        Statement,
        REQ_EXECUTE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.query.metadata_public"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "introspection.show_indexes",
        Syntax,
        Accepted,
        "Introspection",
        Statement,
        REQ_EXECUTE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.query.metadata_public"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "introspection.show_columns",
        Syntax,
        Accepted,
        "Introspection",
        Statement,
        REQ_EXECUTE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.query.metadata_public"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "introspection.show_entities",
        Syntax,
        Accepted,
        "Introspection",
        Statement,
        REQ_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_ICYDB,
        ["core.query.metadata_public"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "introspection.show_entity",
        Syntax,
        Accepted,
        "Introspection",
        Statement,
        REQ_EXECUTE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.query.catalog_metadata"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "introspection.show_stores",
        Syntax,
        Accepted,
        "Introspection",
        Statement,
        REQ_EXECUTE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.query.metadata_public"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "introspection.show_memory",
        Syntax,
        Accepted,
        "Introspection",
        Statement,
        REQ_EXECUTE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.query.metadata_public"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "introspection.catalog_projection",
        Semantic,
        Accepted,
        "Introspection",
        ValueType,
        REQ_EXECUTE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.query.catalog_metadata"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "introspection.generated_policy",
        Policy,
        Accepted,
        "Introspection",
        Policy,
        REQ_BOUNDARY,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        [
            "build.sql.introspection_guard",
            "config.sql.introspection_target_policy",
        ],
        NO_EXTERNAL_POLICY
    ),
    cell!(
        "introspection.storage_modes",
        Semantic,
        Accepted,
        "Introspection",
        ValueType,
        REQ_EXECUTE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.query.catalog_metadata"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "introspection.index_origin",
        Semantic,
        Accepted,
        "Introspection",
        ValueType,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.create_field_path"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "introspection.unsupported_modifiers",
        Syntax,
        Rejected,
        "Introspection",
        Clause,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_families"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "ddl.create_index_field_path",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.create_field_path"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.create_index_multi_field",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.create_multi_field"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.index_ascending",
        Semantic,
        Accepted,
        "DDL",
        Clause,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.index_ascending"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.create_index_filtered",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.create_filtered"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.create_index_expression",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.create_expression"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.create_index_if_not_exists",
        Semantic,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.create_if_not_exists"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.create_unique_index",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        [
            "core.ddl.create_unique",
            "core.ddl.create_unique_field_path_precommit",
            "core.ddl.create_unique_expression_precommit"
        ],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.drop_index",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.drop_index"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.drop_index_if_exists",
        Semantic,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.drop_index_if_exists"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.alter_add_column",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.add_column"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.alter_column_default",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.alter_default"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.alter_column_nullability",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.alter_nullability"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.rename_column",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.rename_column"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.drop_column",
        Syntax,
        Accepted,
        "DDL",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.ddl.drop_column"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.index_descending",
        Syntax,
        Rejected,
        "DDL",
        Clause,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.ddl.desc_rejected"],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.generated_owned_objects",
        Policy,
        Rejected,
        "DDL",
        Policy,
        REQ_BOUNDARY_LOWER,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        [
            "core.ddl.generated_owned_rejected",
            "core.ddl.generated_field_drop_rejected",
            "core.ddl.generated_field_default_rejected",
            "core.ddl.generated_field_nullability_rejected",
            "core.ddl.generated_field_rename_rejected",
        ],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "ddl.destructive_publication_atomicity",
        Interaction,
        Accepted,
        "DDL",
        Interaction,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        [
            "core.ddl.drop_column_rollback",
            "core.ddl.drop_index_precommit_atomicity",
            "core.ddl.index_recovery_retry"
        ],
        NO_EXTERNAL_CATALOG
    ),
    cell!(
        "mutation.insert",
        Syntax,
        Accepted,
        "Public SQL Mutation Execution",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_STATE,
        ["core.mutation.public_families"],
        None
    ),
    cell!(
        "mutation.update",
        Syntax,
        Accepted,
        "Public SQL Mutation Execution",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_STATE,
        ["core.mutation.public_families"],
        None
    ),
    cell!(
        "mutation.delete",
        Syntax,
        Accepted,
        "Public SQL Mutation Execution",
        Statement,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_STATE,
        ["core.mutation.public_families"],
        None
    ),
    cell!(
        "mutation.returning",
        Syntax,
        Accepted,
        "Public SQL Mutation Execution",
        Clause,
        REQ_EXECUTE,
        PERF_NONE,
        ELIGIBLE_STATE,
        ["core.mutation.returning_star"],
        None
    ),
    cell!(
        "mutation.lane_ownership",
        Policy,
        Accepted,
        "Public SQL Mutation Execution",
        Policy,
        REQ_BOUNDARY,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.mutation.lane_ownership"],
        NO_EXTERNAL_POLICY
    ),
    cell!(
        "mutation.trusted_update",
        Policy,
        Accepted,
        "SQL `UPDATE` Availability By Surface",
        Policy,
        REQ_EXECUTE,
        PERF_NONE,
        ELIGIBLE_STATE,
        ["core.mutation.public_families"],
        None
    ),
    cell!(
        "mutation.generated_query_ddl",
        Policy,
        Rejected,
        "SQL `UPDATE` Availability By Surface",
        Policy,
        REQ_BOUNDARY,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        [
            "canister.mutation.query_rejects_update",
            "canister.mutation.ddl_rejects_rows"
        ],
        NO_EXTERNAL_POLICY
    ),
    cell!(
        "mutation.generated_update_disabled",
        Policy,
        Accepted,
        "SQL `UPDATE` Availability By Surface",
        Policy,
        REQ_BOUNDARY,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        [
            "build.sql.update_disabled_by_default",
            "config.sql.update_disabled_default"
        ],
        NO_EXTERNAL_POLICY
    ),
    cell!(
        "mutation.generated_update_primary_key",
        Policy,
        Accepted,
        "SQL `UPDATE` Availability By Surface",
        Policy,
        REQ_BOUNDARY,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        [
            "build.sql.update_primary_key_policy",
            "canister.mutation.primary_key_policy",
            "config.sql.update_primary_key_boolean",
            "config.sql.update_primary_key_named"
        ],
        NO_EXTERNAL_POLICY
    ),
    cell!(
        "mutation.generated_update_bounded",
        Policy,
        Accepted,
        "SQL `UPDATE` Availability By Surface",
        Policy,
        REQ_BOUNDARY,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        [
            "build.sql.update_bounded_policy",
            "canister.mutation.bounded_policy",
            "config.sql.update_bounded_named"
        ],
        NO_EXTERNAL_POLICY
    ),
    cell!(
        "mutation.trusted_update_window",
        Interaction,
        Accepted,
        "SQL `UPDATE` Availability By Surface",
        Interaction,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_STATE,
        ["core.mutation.trusted_update_window"],
        None
    ),
    cell!(
        "blob.hex_literal",
        Syntax,
        Accepted,
        "Blob Literals and Blob Values",
        ValueType,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_STATE,
        ["core.blob.insert_hex"],
        None
    ),
    cell!(
        "blob.literal_size_limit",
        Policy,
        Accepted,
        "Blob Literals and Blob Values",
        Policy,
        REQ_BOUNDARY,
        PERF_NONE,
        ELIGIBLE_ICYDB,
        ["core.blob.literal_boundary"],
        NO_EXTERNAL_BLOB_LIMIT
    ),
    cell!(
        "blob.read_write_compare",
        Semantic,
        Accepted,
        "Blob Literals and Blob Values",
        ValueType,
        REQ_STATE,
        PERF_SCALE,
        ELIGIBLE_STATE,
        [
            "core.blob.insert_hex",
            "core.blob.update_hex",
            "core.blob.equality",
            "core.blob.octet_length",
        ],
        None
    ),
    cell!(
        "blob.ordering",
        Semantic,
        Rejected,
        "Blob Literals and Blob Values",
        ValueType,
        REQ_LOWER,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.blob.order_rejected"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "naming.single_binding",
        Syntax,
        Accepted,
        "Entity Naming And Aliases",
        Clause,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.qualified_names"],
        None
    ),
    cell!(
        "projection.scalar",
        Semantic,
        Accepted,
        "Projection",
        Clause,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.projection_shape"],
        None
    ),
    cell!(
        "projection.aggregate",
        Semantic,
        Accepted,
        "Projection",
        Clause,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.global_aggregate", "core.query.aggregate_inputs"],
        None
    ),
    cell!(
        "projection.grouped_layout",
        Interaction,
        Accepted,
        "Projection",
        Interaction,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        [
            "core.query.grouped_aggregate",
            "core.query.composition",
            "core.query.grouped_execution_mode_parity"
        ],
        None
    ),
    cell!(
        "projection.invalid_grouped_layout",
        Semantic,
        Rejected,
        "Projection",
        Clause,
        REQ_LOWER,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.invalid_grouped_projection"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "expression.numeric_functions",
        Semantic,
        Accepted,
        "Shared SQL Expression Family",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.computed_projection"],
        None
    ),
    cell!(
        "expression.text_functions",
        Semantic,
        Accepted,
        "Shared SQL Expression Family",
        Expression,
        REQ_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_FRONTEND,
        ["core.query.computed_projection"],
        NO_EXTERNAL_TEXT
    ),
    cell!(
        "expression.value_selection",
        Semantic,
        Accepted,
        "Shared SQL Expression Family",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.value_selection"],
        None
    ),
    cell!(
        "expression.searched_case",
        Semantic,
        Accepted,
        "Shared SQL Expression Family",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        [
            "core.query.searched_case",
            "core.query.searched_case_where",
            "core.query.searched_case_grouped_having",
            "core.query.aggregate_inputs",
        ],
        None
    ),
    cell!(
        "expression.simple_case",
        Syntax,
        Rejected,
        "Shared SQL Expression Family",
        Expression,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.simple_case_rejected"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "projection.aliases",
        Semantic,
        Accepted,
        "Projection Aliases",
        Clause,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.alias_order"],
        None
    ),
    cell!(
        "ordering.projection_alias",
        Interaction,
        Accepted,
        "Projection Aliases",
        Interaction,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.alias_order"],
        None
    ),
    cell!(
        "predicate.boolean_comparison",
        Semantic,
        Accepted,
        "Predicates",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        [
            "core.query.scalar_matrix",
            "core.query.parenthesized_boolean"
        ],
        None
    ),
    cell!(
        "predicate.field_comparison",
        Semantic,
        Accepted,
        "Predicates",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.field_comparison"],
        None
    ),
    cell!(
        "predicate.grouped_where_field_comparison",
        Interaction,
        Accepted,
        "Predicates",
        Interaction,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.grouped_where_field_comparison"],
        None
    ),
    cell!(
        "predicate.membership",
        Semantic,
        Accepted,
        "Predicates",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        [
            "core.query.membership",
            "core.query.membership_null_semantics",
        ],
        None
    ),
    cell!(
        "predicate.range",
        Semantic,
        Accepted,
        "Predicates",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.range"],
        None
    ),
    cell!(
        "predicate.null",
        Semantic,
        Accepted,
        "Predicates",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.null_predicates"],
        None
    ),
    cell!(
        "predicate.boolean_truth",
        Semantic,
        Accepted,
        "Predicates",
        Expression,
        REQ_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.boolean_truth"],
        None
    ),
    cell!(
        "predicate.prefix_pattern",
        Semantic,
        Accepted,
        "Predicates",
        Expression,
        REQ_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_FRONTEND,
        [
            "core.query.prefix_not_like",
            "core.query.prefix_ilike",
            "core.query.prefix_not_ilike",
            "core.query.starts_with",
        ],
        NO_EXTERNAL_TEXT
    ),
    cell!(
        "predicate.starts_with",
        Semantic,
        Accepted,
        "Predicates",
        Expression,
        REQ_METAMORPHIC_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_FRONTEND,
        ["core.query.starts_with"],
        NO_EXTERNAL_TEXT
    ),
    cell!(
        "predicate.casefold_prefix",
        Semantic,
        Accepted,
        "Predicates",
        Expression,
        REQ_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_FRONTEND,
        ["core.query.prefix_ilike", "core.query.prefix_not_ilike"],
        NO_EXTERNAL_TEXT
    ),
    cell!(
        "predicate.field_bound_range",
        Semantic,
        Accepted,
        "Predicates",
        Expression,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.field_bound_range"],
        None
    ),
    cell!(
        "predicate.expression_arguments",
        Interaction,
        Accepted,
        "Predicates",
        Interaction,
        REQ_EXECUTE,
        PERF_BROAD,
        ELIGIBLE_FRONTEND,
        ["core.query.expression_arguments"],
        NO_EXTERNAL_TEXT
    ),
    cell!(
        "predicate.non_prefix_pattern",
        Semantic,
        Rejected,
        "Predicates",
        Expression,
        REQ_LOWER,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_expressions"],
        NO_EXTERNAL_TEXT
    ),
    cell!(
        "having.grouped_aggregate",
        Semantic,
        Accepted,
        "`HAVING`",
        Clause,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        [
            "core.query.having",
            "core.query.grouped_execution_mode_parity"
        ],
        None
    ),
    cell!(
        "having.global_aggregate",
        Semantic,
        Accepted,
        "`HAVING`",
        Clause,
        REQ_EXECUTE_REFERENCE,
        PERF_BROAD,
        ELIGIBLE_SQLITE,
        ["core.query.global_having"],
        None
    ),
    cell!(
        "having.raw_row_escape",
        Semantic,
        Rejected,
        "`HAVING`",
        Expression,
        REQ_LOWER,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_expressions"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "returning.star",
        Semantic,
        Accepted,
        "Public SQL Write `RETURNING`",
        Clause,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_STATE,
        ["core.mutation.returning_star"],
        None
    ),
    cell!(
        "returning.fields",
        Semantic,
        Accepted,
        "Public SQL Write `RETURNING`",
        Clause,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_STATE,
        ["core.mutation.returning_fields"],
        None
    ),
    cell!(
        "returning.computed",
        Semantic,
        Rejected,
        "Public SQL Write `RETURNING`",
        Expression,
        REQ_STATE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.mutation.returning_rejected"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "query.multi_entity",
        Syntax,
        Rejected,
        "Explicitly Rejected SQL Families",
        Statement,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_families"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "query.subquery_cte",
        Syntax,
        Rejected,
        "Explicitly Rejected SQL Families",
        Statement,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_families"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "query.set_operations",
        Syntax,
        Rejected,
        "Explicitly Rejected SQL Families",
        Statement,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_families"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "query.window_functions",
        Syntax,
        Rejected,
        "Explicitly Rejected SQL Families",
        Expression,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_families"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "query.transactions",
        Syntax,
        Rejected,
        "Explicitly Rejected SQL Families",
        Statement,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_families"],
        NO_EXTERNAL_SYNTAX
    ),
    cell!(
        "expression.cast",
        Syntax,
        Rejected,
        "Explicitly Rejected SQL Families",
        Expression,
        REQ_PARSE,
        PERF_NONE,
        ELIGIBLE_REJECTION,
        ["core.query.unsupported_expressions"],
        NO_EXTERNAL_SYNTAX
    ),
];

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn parse_contract_features(source: &str) -> Result<BTreeMap<String, ContractFeature>, String> {
    const PREFIX: &str = "<!-- icydb-sql-feature id=\"";
    const KIND_SEPARATOR: &str = "\" kind=\"";
    const STATUS_SEPARATOR: &str = "\" status=\"";
    const SUFFIX: &str = "\" -->";

    let lines = source.lines().collect::<Vec<_>>();
    let mut section = String::new();
    let mut features = BTreeMap::new();

    for (index, raw_line) in lines.iter().enumerate() {
        let line = raw_line.trim();
        if line.starts_with('#') {
            section = line.trim_start_matches('#').trim().to_string();
        }
        if !line.starts_with("<!-- icydb-sql-feature") {
            continue;
        }

        let body = line
            .strip_prefix(PREFIX)
            .and_then(|value| value.strip_suffix(SUFFIX))
            .ok_or_else(|| {
                format!(
                    "line {} uses a non-canonical SQL feature metadata shape",
                    index + 1
                )
            })?;
        let (id, rest) = body
            .split_once(KIND_SEPARATOR)
            .ok_or_else(|| format!("line {} is missing canonical kind metadata", index + 1))?;
        let (kind, status) = rest
            .split_once(STATUS_SEPARATOR)
            .ok_or_else(|| format!("line {} is missing canonical status metadata", index + 1))?;

        validate_feature_id(id).map_err(|reason| format!("line {}: {reason}", index + 1))?;
        let kind = FeatureKind::parse(kind)
            .ok_or_else(|| format!("line {} has unknown feature kind {kind:?}", index + 1))?;
        let status = FeatureStatus::parse(status)
            .ok_or_else(|| format!("line {} has unknown feature status {status:?}", index + 1))?;
        if section.is_empty() {
            return Err(format!(
                "line {} has no containing contract section",
                index + 1
            ));
        }

        let next_contract_line = lines[index + 1..]
            .iter()
            .map(|candidate| candidate.trim())
            .find(|candidate| {
                !candidate.is_empty() && !candidate.starts_with("<!-- icydb-sql-feature")
            })
            .ok_or_else(|| format!("line {} has no following contract text", index + 1))?;
        if next_contract_line.starts_with('#') {
            return Err(format!(
                "line {} metadata must precede contract text, not another section",
                index + 1
            ));
        }

        let feature = ContractFeature {
            kind,
            status,
            section: section.clone(),
            line: index + 1,
        };
        if let Some(previous) = features.insert(id.to_string(), feature) {
            return Err(format!(
                "duplicate contract feature {id:?} at lines {} and {}",
                previous.line,
                index + 1
            ));
        }
    }

    Ok(features)
}

fn validate_feature_id(id: &str) -> Result<(), String> {
    let segments = id.split('.').collect::<Vec<_>>();
    if segments.len() < 2 {
        return Err(format!(
            "feature identifier {id:?} needs at least two segments"
        ));
    }

    for segment in segments {
        let mut characters = segment.chars();
        if !characters
            .next()
            .is_some_and(|value| value.is_ascii_lowercase())
        {
            return Err(format!(
                "feature identifier {id:?} has an invalid segment {segment:?}"
            ));
        }
        if !characters
            .all(|value| value.is_ascii_lowercase() || value.is_ascii_digit() || value == '_')
        {
            return Err(format!(
                "feature identifier {id:?} has an invalid segment {segment:?}"
            ));
        }
    }

    Ok(())
}

fn validate_source_path(path: &str) -> Result<(), String> {
    let path = Path::new(path);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "provider source path {} is not repository-relative",
            path.display()
        ));
    }
    if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
        return Err(format!(
            "provider source path {} is not Rust source",
            path.display()
        ));
    }

    Ok(())
}

fn source_declares_test(source: &str, test_symbol: &str) -> bool {
    let signature = format!("fn {test_symbol}(");
    source
        .match_indices(&signature)
        .any(|(signature_index, _)| {
            let line_start = source[..signature_index]
                .rfind('\n')
                .map_or(0, |index| index + 1);
            if !source[line_start..signature_index].trim().is_empty() {
                return false;
            }

            let Some(test_attribute_index) = source[..signature_index].rfind("#[test]") else {
                return false;
            };
            !source[test_attribute_index + "#[test]".len()..signature_index].contains("fn ")
        })
}

fn provider_specs() -> Result<BTreeMap<&'static str, &'static ProviderSpec>, String> {
    let mut specs = BTreeMap::new();
    let root = repository_root();

    for provider in PROVIDERS {
        if provider.id.is_empty() || provider.evidence.is_empty() {
            return Err(format!(
                "provider {:?} needs stable identity and evidence classes",
                provider.id
            ));
        }
        validate_source_path(provider.source_path)?;
        let source_path = root.join(provider.source_path);
        let source = fs::read_to_string(&source_path).map_err(|error| {
            format!(
                "provider {:?} cannot read {}: {error}",
                provider.id,
                source_path.display()
            )
        })?;
        if !source.contains(&format!("fn {}(", provider.test_symbol)) {
            return Err(format!(
                "provider {:?} names missing test symbol {:?} in {}",
                provider.id, provider.test_symbol, provider.source_path
            ));
        }
        if !source_declares_test(&source, provider.test_symbol) {
            return Err(format!(
                "provider {:?} symbol {:?} is not a test",
                provider.id, provider.test_symbol
            ));
        }
        if specs.insert(provider.id, provider).is_some() {
            return Err(format!(
                "duplicate deterministic provider {:?}",
                provider.id
            ));
        }
    }

    Ok(specs)
}

fn validate_cell(
    cell: &CoverageCell,
    providers: &BTreeMap<&str, &ProviderSpec>,
    used_providers: &mut BTreeSet<&'static str>,
) -> Result<(), String> {
    validate_feature_id(cell.id)?;
    if cell.contract_section.is_empty()
        || cell.evidence.is_empty()
        || cell.performance.is_empty()
        || cell.eligible_providers.is_empty()
        || cell.deterministic_providers.is_empty()
    {
        return Err(format!(
            "manifest cell {:?} has a missing required field",
            cell.id
        ));
    }
    let _ = cell.category;
    let _ = cell.generated_families;

    let has_none = cell.performance.contains(&PerformanceObligation::None);
    if has_none && cell.performance.len() != 1 {
        return Err(format!(
            "manifest cell {:?} mixes no-performance disposition with obligations",
            cell.id
        ));
    }

    let permits_reference = cell.eligible_providers.iter().any(|provider| {
        matches!(
            provider,
            EligibleProvider::SqliteReference | EligibleProvider::StateModelReference
        )
    });
    match (permits_reference, cell.reference_exclusion) {
        (true, Some(_)) => {
            return Err(format!(
                "manifest cell {:?} permits a reference provider but also declares it unavailable",
                cell.id
            ));
        }
        (false, Some(reason)) if !reason.trim().is_empty() => {}
        (false, _) => {
            return Err(format!(
                "manifest cell {:?} excludes reference oracles without a rationale",
                cell.id
            ));
        }
        (true, None) => {}
    }

    let mut resolved = Vec::new();
    for provider_id in cell.deterministic_providers {
        let provider = providers.get(provider_id).ok_or_else(|| {
            format!(
                "manifest cell {:?} names missing provider {provider_id:?}",
                cell.id
            )
        })?;
        used_providers.insert(*provider_id);
        resolved.push(*provider);
    }

    for requirement in cell.evidence {
        if requirement.class == EvidenceClass::ReferenceDifferential
            && requirement.minimum_strength != EvidenceStrength::ReferenceOracle
        {
            return Err(format!(
                "manifest cell {:?} gives reference-differential evidence a non-reference strength",
                cell.id
            ));
        }
        if !resolved.iter().any(|provider| {
            provider.strength == requirement.minimum_strength
                && provider.evidence.contains(&requirement.class)
        }) {
            return Err(format!(
                "manifest cell {:?} lacks {:?} evidence at {:?} strength",
                cell.id, requirement.class, requirement.minimum_strength
            ));
        }
    }
    validate_reference_differential_eligibility(cell)?;

    if cell.status == FeatureStatus::Rejected
        && !cell
            .eligible_providers
            .contains(&EligibleProvider::RejectionInvariant)
    {
        return Err(format!(
            "rejected manifest cell {:?} lacks a typed rejection provider disposition",
            cell.id
        ));
    }
    if cell.kind == FeatureKind::Interaction
        && cell.status == FeatureStatus::Accepted
        && cell.deterministic_providers.is_empty()
    {
        return Err(format!(
            "accepted interaction {:?} lacks a deterministic composition provider",
            cell.id
        ));
    }

    Ok(())
}

fn validate_reference_differential_eligibility(cell: &CoverageCell) -> Result<(), String> {
    let requires_reference = cell
        .evidence
        .iter()
        .any(|requirement| requirement.class == EvidenceClass::ReferenceDifferential);
    if requires_reference
        && !cell
            .eligible_providers
            .contains(&EligibleProvider::SqliteReference)
    {
        return Err(format!(
            "manifest cell {:?} requires the SQLite differential without SQLite eligibility",
            cell.id
        ));
    }

    Ok(())
}

/// Return the canonical semantic revision of the active SQL coverage manifest.
pub(super) fn sql_coverage_manifest_revision() -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"icydb-sql-coverage-manifest/v1");

    let mut cells = MANIFEST.iter().collect::<Vec<_>>();
    cells.sort_by_key(|cell| cell.id);
    hash_count(&mut hasher, cells.len());
    for cell in cells {
        hash_text(&mut hasher, cell.id);
        hash_text(&mut hasher, cell.kind.code());
        hash_text(&mut hasher, cell.status.code());
        hash_text(&mut hasher, cell.contract_section);
        hash_text(&mut hasher, cell.category.code());
        hash_evidence_requirements(&mut hasher, cell.evidence);
        hash_text_list(
            &mut hasher,
            cell.performance.iter().map(|obligation| obligation.code()),
        );
        hash_text_list(
            &mut hasher,
            cell.eligible_providers
                .iter()
                .map(|provider| provider.code()),
        );
        hash_text_list(&mut hasher, cell.deterministic_providers.iter().copied());
        hash_text_list(&mut hasher, cell.generated_families.iter().copied());
        match cell.reference_exclusion {
            Some(reason) => {
                hasher.update(&[1]);
                hash_text(&mut hasher, reason);
            }
            None => {
                hasher.update(&[0]);
            }
        }
    }

    let mut providers = PROVIDERS.iter().collect::<Vec<_>>();
    providers.sort_by_key(|provider| provider.id);
    hash_count(&mut hasher, providers.len());
    for provider in providers {
        hash_text(&mut hasher, provider.id);
        hash_text(&mut hasher, provider.source_path);
        hash_text(&mut hasher, provider.test_symbol);
        hash_text(&mut hasher, provider.strength.code());
        hash_text_list(
            &mut hasher,
            provider.evidence.iter().map(|evidence| evidence.code()),
        );
    }

    let mut attributions = GENERATED_SELECT_EXCLUSION_ATTRIBUTIONS
        .iter()
        .collect::<Vec<_>>();
    attributions.sort_by_key(|attribution| attribution.stratum);
    hash_count(&mut hasher, attributions.len());
    for attribution in attributions {
        hash_text(&mut hasher, attribution.stratum);
        hash_text_list(
            &mut hasher,
            attribution.deterministic_providers.iter().copied(),
        );
    }

    hasher.finalize().to_hex().to_string()
}

fn hash_evidence_requirements(hasher: &mut blake3::Hasher, requirements: &[EvidenceRequirement]) {
    let mut requirements = requirements
        .iter()
        .map(|requirement| {
            (
                requirement.class.code(),
                requirement.minimum_strength.code(),
            )
        })
        .collect::<Vec<_>>();
    requirements.sort_unstable();
    hash_count(hasher, requirements.len());
    for (class, strength) in requirements {
        hash_text(hasher, class);
        hash_text(hasher, strength);
    }
}

fn hash_text_list<'a>(hasher: &mut blake3::Hasher, values: impl IntoIterator<Item = &'a str>) {
    let mut values = values.into_iter().collect::<Vec<_>>();
    values.sort_unstable();
    hash_count(hasher, values.len());
    for value in values {
        hash_text(hasher, value);
    }
}

fn hash_text(hasher: &mut blake3::Hasher, value: &str) {
    let length =
        u32::try_from(value.len()).expect("static SQL manifest text should fit a u32 length");
    hasher.update(&length.to_be_bytes());
    hasher.update(value.as_bytes());
}

fn hash_count(hasher: &mut blake3::Hasher, count: usize) {
    let count = u32::try_from(count).expect("static SQL manifest count should fit u32");
    hasher.update(&count.to_be_bytes());
}

const INVALID_REFERENCE_REQUIREMENT: &[EvidenceRequirement] = &[EvidenceRequirement {
    class: EvidenceClass::ReferenceDifferential,
    minimum_strength: EvidenceStrength::MetamorphicInvariant,
}];

#[test]
fn sql_coverage_manifest_revision_has_a_fixed_golden_vector() {
    assert_eq!(
        sql_coverage_manifest_revision(),
        TIER_C_SQL_COVERAGE_MANIFEST_REVISION,
    );
}

#[test]
fn sql_contract_metadata_and_coverage_manifest_are_consistent() {
    assert_eq!(EVIDENCE_CLASS_TAXONOMY.len(), 8);
    assert_eq!(EVIDENCE_STRENGTH_TAXONOMY.len(), 4);
    assert_eq!(PERFORMANCE_OBLIGATION_TAXONOMY.len(), 5);

    let contract = include_str!("../../../../docs/contracts/SQL_SUBSET.md");
    let contract_features =
        parse_contract_features(contract).expect("SQL contract feature metadata should be valid");
    let providers = provider_specs().expect("deterministic SQL providers should resolve");
    let mut manifest_features = BTreeMap::new();
    let mut used_providers = BTreeSet::new();

    for cell in MANIFEST {
        validate_cell(cell, &providers, &mut used_providers)
            .unwrap_or_else(|error| panic!("invalid SQL coverage manifest: {error}"));
        assert!(
            manifest_features.insert(cell.id, cell).is_none(),
            "duplicate SQL coverage manifest cell {:?}",
            cell.id
        );
    }

    let contract_ids = contract_features
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let manifest_ids = manifest_features.keys().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        contract_ids, manifest_ids,
        "SQL contract metadata and coverage manifest must form an exact bijection"
    );

    let profile_features = required_sqlite_reference_scenarios()
        .iter()
        .flat_map(|scenario| scenario.contract_features().iter().copied())
        .collect::<BTreeSet<_>>();
    let reference_manifest_features = MANIFEST
        .iter()
        .filter(|cell| {
            cell.evidence
                .iter()
                .any(|requirement| requirement.class == EvidenceClass::ReferenceDifferential)
        })
        .map(|cell| cell.id)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        profile_features, reference_manifest_features,
        "the compact SQLite profile and manifest reference obligations must form an exact bijection",
    );

    let broad_performance_features = MANIFEST
        .iter()
        .filter(|cell| cell.performance.contains(&PerformanceObligation::BroadScan))
        .map(|cell| cell.id)
        .collect::<BTreeSet<_>>();
    let required_broad_performance_features = SQL_PERFORMANCE_BROAD_CONTRACT_FEATURES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        broad_performance_features, required_broad_performance_features,
        "manifest broad-scan dispositions must match the shared query-performance contract",
    );

    let scale_performance_features = MANIFEST
        .iter()
        .filter(|cell| {
            cell.performance
                .contains(&PerformanceObligation::ScaleSentinel)
        })
        .map(|cell| cell.id)
        .collect::<BTreeSet<_>>();
    let required_scale_performance_features = SQL_PERFORMANCE_SCALE_CONTRACT_FEATURES
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        scale_performance_features, required_scale_performance_features,
        "manifest scale dispositions must match the shared query-performance contract",
    );

    for (id, contract_feature) in &contract_features {
        let cell = manifest_features
            .get(id.as_str())
            .expect("bijection check should resolve every contract feature");
        assert_eq!(
            cell.kind, contract_feature.kind,
            "feature kind drift for {id}"
        );
        assert_eq!(
            cell.status, contract_feature.status,
            "feature status drift for {id}"
        );
        assert_eq!(
            cell.contract_section, contract_feature.section,
            "contract location drift for {id}"
        );
    }

    let declared_providers = providers.keys().copied().collect::<BTreeSet<_>>();
    assert_eq!(
        used_providers, declared_providers,
        "deterministic provider registry must not retain unreferenced entries"
    );
}

#[test]
fn generated_select_exclusions_are_explicitly_attributed() {
    let providers = provider_specs().expect("deterministic SQL providers should resolve");
    let mut strata = BTreeSet::new();

    for attribution in GENERATED_SELECT_EXCLUSION_ATTRIBUTIONS {
        assert!(
            strata.insert(attribution.stratum),
            "generated SELECT exclusion strata must be unique: {:?}",
            attribution.stratum
        );
        assert!(
            !attribution.deterministic_providers.is_empty(),
            "generated SELECT exclusion {:?} needs deterministic evidence",
            attribution.stratum
        );
        for provider_id in attribution.deterministic_providers {
            assert!(
                providers.contains_key(provider_id),
                "generated SELECT exclusion {:?} names unknown provider {:?}",
                attribution.stratum,
                provider_id
            );
        }
    }
}

#[test]
fn malformed_contract_feature_metadata_fails_closed() {
    for invalid in [
        "<!-- icydb-sql-feature kind=\"syntax\" id=\"query.valid\" status=\"accepted\" -->\ntext",
        "<!-- icydb-sql-feature id=\"Query.invalid\" kind=\"syntax\" status=\"accepted\" -->\ntext",
        "<!-- icydb-sql-feature id=\"query.invalid\" kind=\"unknown\" status=\"accepted\" -->\ntext",
        "<!-- icydb-sql-feature id=\"query.invalid\" kind=\"syntax\" status=\"unknown\" -->\ntext",
    ] {
        assert!(
            parse_contract_features(&format!("## Test\n{invalid}")).is_err(),
            "invalid contract metadata must fail closed: {invalid}"
        );
    }
}

#[test]
fn manifest_consistency_gate_rejects_missing_or_invalid_evidence() {
    let providers = provider_specs().expect("deterministic SQL providers should resolve");

    let mut missing_evidence = MANIFEST[0];
    missing_evidence.evidence = &[];
    assert!(
        validate_cell(&missing_evidence, &providers, &mut BTreeSet::new()).is_err(),
        "a cell without evidence obligations must fail"
    );

    let mut missing_provider = MANIFEST[0];
    missing_provider.deterministic_providers = &["missing.provider"];
    assert!(
        validate_cell(&missing_provider, &providers, &mut BTreeSet::new()).is_err(),
        "a cell naming an absent provider must fail"
    );

    let mut unexplained_reference_exclusion = MANIFEST[0];
    unexplained_reference_exclusion.reference_exclusion = None;
    assert!(
        validate_cell(
            &unexplained_reference_exclusion,
            &providers,
            &mut BTreeSet::new(),
        )
        .is_err(),
        "a cell without a reference oracle needs an explicit exclusion rationale"
    );

    let mut correlated_reference = MANIFEST[0];
    correlated_reference.evidence = INVALID_REFERENCE_REQUIREMENT;
    assert!(
        validate_cell(&correlated_reference, &providers, &mut BTreeSet::new()).is_err(),
        "metamorphic agreement must not satisfy reference-differential evidence"
    );

    let mut sqlite_requirement_without_eligibility = *MANIFEST
        .iter()
        .find(|cell| {
            cell.evidence
                .iter()
                .any(|requirement| requirement.class == EvidenceClass::ReferenceDifferential)
        })
        .expect("the compact profile should create reference obligations");
    sqlite_requirement_without_eligibility.eligible_providers =
        &[EligibleProvider::StateModelReference];
    assert!(
        validate_reference_differential_eligibility(&sqlite_requirement_without_eligibility)
            .is_err(),
        "a SQLite differential obligation without SQLite eligibility must fail"
    );

    let adjacent_non_test = "#[test]\nfn actual_test() {}\nfn adjacent_helper() {}";
    assert!(source_declares_test(adjacent_non_test, "actual_test"));
    assert!(
        !source_declares_test(adjacent_non_test, "adjacent_helper"),
        "a nearby test attribute must not bless a non-test provider symbol"
    );
}
