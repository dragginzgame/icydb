//! Module: sql_performance_contract
//!
//! Responsibility: contract-feature obligations owned by the comparable SQL query profile.
//! Does not own: correctness providers, scenario construction, execution, or thresholds.
//! Boundary: gives the manifest and performance runners one shared required feature set.

/// SQL contract features that require one stable P1 broad-scan query declaration.
pub const SQL_PERFORMANCE_BROAD_CONTRACT_FEATURES: &[&str] = &[
    "explain.query_delete",
    "expression.numeric_functions",
    "expression.searched_case",
    "expression.text_functions",
    "expression.value_selection",
    "having.global_aggregate",
    "having.grouped_aggregate",
    "introspection.show_entities",
    "naming.single_binding",
    "ordering.null_values",
    "ordering.projection_alias",
    "pagination.grouped_cursor",
    "pagination.scalar_limit_offset",
    "predicate.boolean_comparison",
    "predicate.boolean_truth",
    "predicate.casefold_prefix",
    "predicate.expression_arguments",
    "predicate.field_bound_range",
    "predicate.field_comparison",
    "predicate.grouped_where_field_comparison",
    "predicate.membership",
    "predicate.null",
    "predicate.prefix_pattern",
    "predicate.range",
    "predicate.starts_with",
    "projection.aggregate",
    "projection.aliases",
    "projection.grouped_layout",
    "projection.scalar",
    "select.aggregate_distinct_filter",
    "select.computed_projection",
    "select.exact_primary_key",
    "select.global_aggregate",
    "select.grouped_aggregate",
    "select.scalar_distinct",
    "select.scalar_rows",
    "surface.single_entity",
];

/// SQL contract features whose cost is represented by the scale-sentinel profile.
pub const SQL_PERFORMANCE_SCALE_CONTRACT_FEATURES: &[&str] = &["blob.read_write_compare"];

/// Stable P2 scenario identities for the grouped early-finalization evidence line.
///
/// Ordered/hash pairs execute equivalent grouped results through both physical
/// modes. EXPLAIN companions prove the baseline pair's selected mode without
/// making rendered EXPLAIN text an execution authority. The remaining cases
/// keep the required aggregate, `HAVING`, `DISTINCT`, and public-boundary
/// shapes in repeated cold/warm confirmation.
pub const SQL_GROUPED_EARLY_FINALIZATION_P2_SCENARIOS: &[&str] = &[
    "user.grouped_baseline.hash_sum_age_control",
    "user.grouped_baseline.hash_sum_age_control_explain",
    "user.grouped_baseline.ordered_sum_age",
    "user.grouped_baseline.ordered_sum_age_explain",
    "user.grouped_scale.hash_name_having_sum_window16",
    "user.grouped_scale.hash_name_sum_window16",
    "user.grouped_scale.hash_age_distinct_nat_window16",
    "user.grouped_scale.ordered_name_count_window100",
    "user.grouped_scale.ordered_name_having_sum_window16",
    "user.grouped_scale.ordered_name_sum_window16",
];
