//! Module: sql_correctness_support::coverage_manifest::obligations
//! Responsibility: reviewed 0.215 SQL interaction obligations and their deterministic projection.
//! Does not own: product behavior, generated observations, or scheduled evidence receipts.
//! Boundary: extends the SQL coverage manifest with one finite pre-generation obligation catalog.

use super::{MANIFEST, PROVIDERS, ProviderSpec, provider_specs};

use icydb_testing_sql_generator::{
    TIER_A_MUTATION_BUDGETS, TIER_A_ROOT_SEEDS, TIER_A_SELECT_BUDGETS,
    generate_scheduled_mutation_sequence, generate_scheduled_select_case,
    generated_mutation_tier_c_declaration, generated_select_tier_c_declaration,
    scheduled_mutation_witnesses, scheduled_select_witnesses, structural_obligation_catalog_hash,
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

const CATALOG_FORMAT_VERSION: u32 = 1;
const CATALOG_HASH_DOMAIN: &[u8] = b"icydb-sql-coverage-obligations/v1";
const CATALOG_ARTIFACT: &str = include_str!(
    "../../../../../docs/design/0.215-sql-structural-coverage-and-range-remediation/0.215-coverage-obligations.json"
);

///
/// Disposition
///
/// Total reviewed outcome for one type-valid interaction tuple.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Disposition {
    DeterministicRequired,
    GeneratedRequired,
    Inapplicable,
}

///
/// Axis
///
/// One closed dimension of a reviewed interaction group.
///

#[derive(Clone, Copy, Debug, Serialize)]
struct Axis {
    name: &'static str,
    members: &'static [&'static str],
}

///
/// InteractionGroup
///
/// Closed axis-member vocabulary for one bounded interaction family.
///

#[derive(Clone, Copy, Debug, Serialize)]
struct InteractionGroup {
    id: &'static str,
    axes: &'static [Axis],
}

///
/// AxisValue
///
/// One dimension assignment in a reviewed type-valid interaction tuple.
///

#[derive(Clone, Copy, Debug, Serialize)]
struct AxisValue {
    axis: &'static str,
    value: &'static str,
}

///
/// ExpectedStructuralSignature
///
/// Lossless reviewed expectation consumed by the derived signature owner in Slice 1.
/// These fields describe semantic structure only; seeds and literal payloads are excluded.
///

#[derive(Clone, Copy, Debug, Serialize)]
struct ExpectedStructuralSignature {
    declaration_kind: &'static str,
    schema_profile: &'static str,
    statement_family: &'static str,
    result_shape: &'static str,
    projection_shape: &'static str,
    predicate_shape: &'static str,
    grouping_shape: &'static str,
    having_shape: &'static str,
    order_shape: &'static str,
    window_shape: &'static str,
    field_roles: &'static str,
    semantic_value_class: &'static str,
    fixture_class: &'static str,
    required_access: &'static str,
    required_covering: &'static str,
    expected_violation: &'static str,
}

impl ExpectedStructuralSignature {
    #[expect(
        clippy::too_many_arguments,
        reason = "the reviewed SELECT signature keeps every closed semantic dimension explicit"
    )]
    const fn select(
        profile: &'static str,
        result: &'static str,
        projection: &'static str,
        predicate: &'static str,
        grouping: &'static str,
        having: &'static str,
        order: &'static str,
        window: &'static str,
        field_roles: &'static str,
        value_class: &'static str,
        fixture: &'static str,
        access: &'static str,
        covering: &'static str,
    ) -> Self {
        Self {
            declaration_kind: "accepted",
            schema_profile: profile,
            statement_family: "select",
            result_shape: result,
            projection_shape: projection,
            predicate_shape: predicate,
            grouping_shape: grouping,
            having_shape: having,
            order_shape: order,
            window_shape: window,
            field_roles,
            semantic_value_class: value_class,
            fixture_class: fixture,
            required_access: access,
            required_covering: covering,
            expected_violation: "none",
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the reviewed mutation signature keeps every closed semantic dimension explicit"
    )]
    const fn mutation(
        profile: &'static str,
        statement: &'static str,
        result: &'static str,
        projection: &'static str,
        predicate: &'static str,
        order: &'static str,
        field_roles: &'static str,
        value_class: &'static str,
        fixture: &'static str,
        violation: &'static str,
    ) -> Self {
        Self {
            declaration_kind: if violation.is_empty() {
                "accepted"
            } else {
                "singly_invalid"
            },
            schema_profile: profile,
            statement_family: statement,
            result_shape: result,
            projection_shape: projection,
            predicate_shape: predicate,
            grouping_shape: "none",
            having_shape: "none",
            order_shape: order,
            window_shape: "none",
            field_roles,
            semantic_value_class: value_class,
            fixture_class: fixture,
            required_access: "mutation_selection",
            required_covering: "not_applicable",
            expected_violation: if violation.is_empty() {
                "none"
            } else {
                violation
            },
        }
    }
}

///
/// ProviderTarget
///
/// Exact current or planned provider that owns one required structural witness.
///

#[derive(Clone, Copy, Debug)]
enum ProviderTarget {
    Existing(&'static str),
    Planned(&'static str),
}

impl ProviderTarget {
    const fn id(self) -> &'static str {
        match self {
            Self::Existing(id) | Self::Planned(id) => id,
        }
    }

    const fn state(self) -> &'static str {
        match self {
            Self::Existing(_) => "existing_deterministic_provider",
            Self::Planned(_) => "planned_generated_provider",
        }
    }
}

///
/// StructuralRequirement
///
/// Frozen required signature, evidence, and witness for one or more interactions.
///

#[derive(Clone, Copy, Debug)]
struct StructuralRequirement {
    id: &'static str,
    signature: ExpectedStructuralSignature,
    fixture_properties: &'static [&'static str],
    minimum_evidence: &'static str,
    provider_eligibility: &'static str,
    provider: ProviderTarget,
    route_facts: &'static [&'static str],
    witness_id: &'static str,
}

///
/// InteractionObligation
///
/// One reviewed type-valid tuple and its total coverage disposition.
///

#[derive(Clone, Copy, Debug)]
struct InteractionObligation {
    id: &'static str,
    group: &'static str,
    tuple: &'static [AxisValue],
    contract_features: &'static [&'static str],
    schema_facts: &'static [&'static str],
    fixture_properties: &'static [&'static str],
    minimum_evidence: &'static str,
    provider_eligibility: &'static str,
    route_facts: &'static [&'static str],
    disposition: Disposition,
    reason: &'static str,
    requirement: Option<StructuralRequirement>,
}

macro_rules! axes {
    ($($axis:literal = $value:literal),+ $(,)?) => {
        &[$(AxisValue {
            axis: $axis,
            value: $value,
        }),+]
    };
}

macro_rules! requirement {
    (
        $id:literal,
        $signature:expr,
        fixtures = [$($fixture:literal),* $(,)?],
        evidence = $evidence:literal,
        eligibility = $eligibility:literal,
        provider = $provider:expr,
        routes = [$($route:literal),* $(,)?],
        witness = $witness:literal
    ) => {
        StructuralRequirement {
            id: $id,
            signature: $signature,
            fixture_properties: &[$($fixture),*],
            minimum_evidence: $evidence,
            provider_eligibility: $eligibility,
            provider: $provider,
            route_facts: &[$($route),*],
            witness_id: $witness,
        }
    };
}

macro_rules! interaction {
    (
        $id:literal,
        group = $group:literal,
        tuple = $tuple:expr,
        features = [$($feature:literal),+ $(,)?],
        schema = [$($schema:literal),* $(,)?],
        fixtures = [$($fixture:literal),* $(,)?],
        evidence = $evidence:literal,
        eligibility = $eligibility:literal,
        routes = [$($route:literal),* $(,)?],
        disposition = $disposition:ident,
        reason = $reason:literal,
        requirement = $requirement:expr
    ) => {
        InteractionObligation {
            id: $id,
            group: $group,
            tuple: $tuple,
            contract_features: &[$($feature),+],
            schema_facts: &[$($schema),*],
            fixture_properties: &[$($fixture),*],
            minimum_evidence: $evidence,
            provider_eligibility: $eligibility,
            route_facts: &[$($route),*],
            disposition: Disposition::$disposition,
            reason: $reason,
            requirement: $requirement,
        }
    };
}

const GROUPS: &[InteractionGroup] = &[
    InteractionGroup {
        id: "cache_entry_parity",
        axes: &[
            Axis {
                name: "cache",
                members: &["cold", "warm"],
            },
            Axis {
                name: "entry",
                members: &["compiled_direct", "sql_fluent"],
            },
        ],
    },
    InteractionGroup {
        id: "field_paths",
        axes: &[
            Axis {
                name: "operation",
                members: &["select", "returning"],
            },
            Axis {
                name: "leaf",
                members: &["stored_selectable", "stored_unselectable"],
            },
        ],
    },
    InteractionGroup {
        id: "global_aggregation",
        axes: &[
            Axis {
                name: "input",
                members: &["empty", "nonempty"],
            },
            Axis {
                name: "modifiers",
                members: &["distinct_filter", "multiple_projection"],
            },
        ],
    },
    InteractionGroup {
        id: "grouped_aggregation",
        axes: &[
            Axis {
                name: "mode",
                members: &["hash", "ordered"],
            },
            Axis {
                name: "window",
                members: &["bounded", "continuation"],
            },
        ],
    },
    InteractionGroup {
        id: "indexed_scalar_execution",
        axes: &[
            Axis {
                name: "access",
                members: &[
                    "composite_prefix",
                    "expression_range",
                    "primary_exact",
                    "secondary_range",
                ],
            },
            Axis {
                name: "covering",
                members: &["hybrid", "non_covering", "pure"],
            },
            Axis {
                name: "order",
                members: &["compatible", "incompatible", "none"],
            },
        ],
    },
    InteractionGroup {
        id: "mutation",
        axes: &[
            Axis {
                name: "profile",
                members: &["accepted_default", "authored_scalar"],
            },
            Axis {
                name: "operation",
                members: &[
                    "delete_returning",
                    "insert",
                    "insert_from_query",
                    "no_match",
                    "reject_duplicate",
                    "reject_pk_default",
                    "reject_required",
                    "update",
                    "windowed",
                ],
            },
            Axis {
                name: "intent",
                members: &[
                    "authored",
                    "explicit_default",
                    "mixed_batch",
                    "omitted",
                    "preserve",
                ],
            },
            Axis {
                name: "ingress",
                members: &["sql", "sql_and_typed", "typed"],
            },
        ],
    },
    InteractionGroup {
        id: "null_semantics",
        axes: &[
            Axis {
                name: "source",
                members: &["computed", "stored"],
            },
            Axis {
                name: "context",
                members: &[
                    "aggregate",
                    "comparison",
                    "distinct",
                    "membership",
                    "ordering",
                ],
            },
        ],
    },
    InteractionGroup {
        id: "scalar_composition",
        axes: &[
            Axis {
                name: "profile",
                members: &["indexed_nullable_reference", "reference_scalar"],
            },
            Axis {
                name: "shape",
                members: &[
                    "computed_distinct_window",
                    "full_scalar_window",
                    "invalid_alias_order",
                ],
            },
        ],
    },
];

const PLANNED_PROVIDERS: &[&str] = &[
    "generated.mutation.accepted_default",
    "generated.mutation.authored_scalar",
    "generated.select.indexed_nullable_reference",
    "generated.select.reference_scalar",
];

const INTERACTIONS: &[InteractionObligation] = &[
    interaction!(
        "interaction.cache.cold_compiled_direct",
        group = "cache_entry_parity",
        tuple = axes!("cache" = "cold", "entry" = "compiled_direct"),
        features = ["surface.single_entity", "select.scalar_rows"],
        schema = ["accepted entity resolved before both entry paths"],
        fixtures = ["identical fixture snapshot"],
        evidence = "contract_assertion",
        eligibility = "execution_mode_equivalent",
        routes = ["compiled and direct execution select the same access contract"],
        disposition = DeterministicRequired,
        reason = "Compiled/direct parity already has an exact deterministic product provider.",
        requirement = Some(requirement!(
            "required.cache.cold_compiled_direct",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "scalar_rows",
                "plain_fields",
                "strict_scalar_comparison",
                "none",
                "none",
                "primary_key_ascending",
                "limit",
                "sole_primary_key|stored_scalar",
                "ordinary",
                "small_duplicate_rich",
                "primary_exact",
                "non_covering",
            ),
            fixtures = ["identical cold fixture"],
            evidence = "contract_assertion",
            eligibility = "execution_mode_equivalent",
            provider = ProviderTarget::Existing("core.query.public_read_families"),
            routes = ["same selected access"],
            witness = "existing.core.query.public_read_families"
        ))
    ),
    interaction!(
        "interaction.cache.cold_sql_fluent",
        group = "cache_entry_parity",
        tuple = axes!("cache" = "cold", "entry" = "sql_fluent"),
        features = [
            "surface.single_entity",
            "select.scalar_composition",
            "select.scalar_rows",
        ],
        schema = ["one accepted snapshot feeds SQL and fluent lowering"],
        fixtures = ["identical fixture snapshot"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["SQL and fluent observations retain matching route facts"],
        disposition = GeneratedRequired,
        reason =
            "Slice 1 must schedule the same derived declaration through SQL and fluent adapters.",
        requirement = Some(requirement!(
            "required.cache.cold_sql_fluent",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "scalar_rows",
                "computed_and_plain_fields",
                "strict_scalar_comparison",
                "none",
                "none",
                "projection_alias_then_primary_key",
                "limit_offset",
                "sole_primary_key|stored_scalar",
                "ordinary",
                "small_duplicate_rich",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["identical cold fixture"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.reference_scalar"),
            routes = ["matching SQL/fluent route observation"],
            witness = "tier_c.cache.cold_sql_fluent"
        ))
    ),
    interaction!(
        "interaction.cache.warm_sql_fluent",
        group = "cache_entry_parity",
        tuple = axes!("cache" = "warm", "entry" = "sql_fluent"),
        features = [
            "pagination.scalar_limit_offset",
            "select.scalar_composition",
            "select.scalar_rows",
        ],
        schema = ["one accepted snapshot and compiled-plan identity"],
        fixtures = ["identical fixture snapshot"],
        evidence = "contract_assertion",
        eligibility = "execution_mode_equivalent",
        routes = ["warm reuse cannot change result or selected route"],
        disposition = DeterministicRequired,
        reason = "Warm-cache behavior is an IcyDB execution contract, not SQLite authority.",
        requirement = Some(requirement!(
            "required.cache.warm_sql_fluent",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "scalar_rows",
                "plain_fields",
                "strict_scalar_comparison",
                "none",
                "none",
                "primary_key_ascending",
                "limit",
                "sole_primary_key|stored_scalar",
                "ordinary",
                "small_duplicate_rich",
                "primary_exact",
                "non_covering",
            ),
            fixtures = ["identical warm fixture"],
            evidence = "contract_assertion",
            eligibility = "execution_mode_equivalent",
            provider = ProviderTarget::Existing("core.query.scalar_matrix"),
            routes = ["cold/warm selected access parity"],
            witness = "existing.core.query.scalar_matrix"
        ))
    ),
    interaction!(
        "interaction.field_path.selectable_select",
        group = "field_paths",
        tuple = axes!("operation" = "select", "leaf" = "stored_selectable"),
        features = ["ddl.create_index_field_path", "projection.scalar"],
        schema = ["accepted stored leaf is selectable and queryable"],
        fixtures = ["nested leaf values and duplicates"],
        evidence = "contract_assertion",
        eligibility = "icydb_contract_only",
        routes = ["field-path index route and complete row projection"],
        disposition = DeterministicRequired,
        reason = "Nested accepted values remain outside the generated SQLite profile.",
        requirement = Some(requirement!(
            "required.field_path.selectable_select",
            ExpectedStructuralSignature::select(
                "deterministic_nested_field_path",
                "scalar_rows",
                "stored_leaf",
                "stored_leaf_comparison",
                "none",
                "none",
                "stored_leaf_then_primary_key",
                "limit",
                "stored_leaf|single_secondary_index",
                "ordinary",
                "small_duplicate_rich",
                "secondary_range",
                "hybrid",
            ),
            fixtures = ["nested stored leaves"],
            evidence = "contract_assertion",
            eligibility = "icydb_contract_only",
            provider = ProviderTarget::Existing("core.ddl.create_field_path"),
            routes = ["accepted field-path index"],
            witness = "existing.core.ddl.create_field_path"
        ))
    ),
    interaction!(
        "interaction.field_path.selectable_returning",
        group = "field_paths",
        tuple = axes!("operation" = "returning", "leaf" = "stored_selectable"),
        features = ["mutation.returning", "returning.fields"],
        schema = ["accepted stored leaf can be returned from a complete admitted row"],
        fixtures = ["one matching row"],
        evidence = "contract_assertion",
        eligibility = "frontend_equivalent",
        routes = ["mutation RETURNING projects the accepted post-image or old delete image"],
        disposition = DeterministicRequired,
        reason = "Structured RETURNING remains with its exact deterministic mutation provider.",
        requirement = Some(requirement!(
            "required.field_path.selectable_returning",
            ExpectedStructuralSignature::mutation(
                "deterministic_nested_field_path",
                "update",
                "returned_fields",
                "stored_leaf",
                "primary_key_exact",
                "primary_key_ascending",
                "sole_primary_key|stored_leaf",
                "authored",
                "singleton",
                "",
            ),
            fixtures = ["one matching structured row"],
            evidence = "contract_assertion",
            eligibility = "frontend_equivalent",
            provider = ProviderTarget::Existing("core.mutation.returning_fields"),
            routes = ["exact mutation selection"],
            witness = "existing.core.mutation.returning_fields"
        ))
    ),
    interaction!(
        "interaction.field_path.unselectable_select",
        group = "field_paths",
        tuple = axes!("operation" = "select", "leaf" = "stored_unselectable"),
        features = ["projection.scalar"],
        schema = ["accepted stored leaf is explicitly non-selectable"],
        fixtures = [],
        evidence = "boundary_assertion",
        eligibility = "rejection_invariant",
        routes = [],
        disposition = Inapplicable,
        reason = "A non-selectable accepted leaf cannot form a type-valid SELECT obligation.",
        requirement = None
    ),
    interaction!(
        "interaction.global.empty_distinct_filter",
        group = "global_aggregation",
        tuple = axes!("input" = "empty", "modifiers" = "distinct_filter"),
        features = [
            "projection.aggregate",
            "select.aggregate_distinct_filter",
            "select.global_aggregate",
        ],
        schema = ["reference scalar aggregate inputs"],
        fixtures = ["empty input"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["global aggregate emits one result row"],
        disposition = GeneratedRequired,
        reason =
            "The current fixed slots do not make this complete interaction a required witness.",
        requirement = Some(requirement!(
            "required.global.empty_distinct_filter",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "global_aggregate",
                "aggregate_terminals",
                "aggregate_filter",
                "global",
                "none",
                "none",
                "none",
                "stored_scalar",
                "empty",
                "empty",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["empty input"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.reference_scalar"),
            routes = ["global aggregate"],
            witness = "tier_c.global.empty_distinct_filter"
        ))
    ),
    interaction!(
        "interaction.global.nonempty_distinct_filter",
        group = "global_aggregation",
        tuple = axes!("input" = "nonempty", "modifiers" = "distinct_filter"),
        features = [
            "projection.aggregate",
            "select.aggregate_distinct_filter",
            "select.global_aggregate",
        ],
        schema = ["reference scalar aggregate inputs"],
        fixtures = ["duplicate aggregate inputs"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["global aggregate over admitted predicate"],
        disposition = GeneratedRequired,
        reason = "Slice 1 must compose FILTER and DISTINCT under one typed declaration.",
        requirement = Some(requirement!(
            "required.global.nonempty_distinct_filter",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "global_aggregate",
                "aggregate_distinct_filter",
                "strict_scalar_comparison",
                "global",
                "none",
                "none",
                "none",
                "stored_scalar",
                "duplicate_nonempty",
                "duplicate_rich",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["duplicate aggregate inputs"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.reference_scalar"),
            routes = ["global aggregate"],
            witness = "tier_c.global.nonempty_distinct_filter"
        ))
    ),
    interaction!(
        "interaction.global.nonempty_multiple_projection",
        group = "global_aggregation",
        tuple = axes!("input" = "nonempty", "modifiers" = "multiple_projection"),
        features = [
            "having.global_aggregate",
            "projection.aggregate",
            "select.global_aggregate",
        ],
        schema = ["two compatible reference scalar aggregate inputs"],
        fixtures = ["retained and rejected HAVING cases"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["global HAVING after aggregate finalization"],
        disposition = GeneratedRequired,
        reason = "Slice 1 must require multiple aggregate outputs and global HAVING together.",
        requirement = Some(requirement!(
            "required.global.nonempty_multiple_projection",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "global_aggregate",
                "multiple_aggregate_terminals",
                "strict_scalar_comparison",
                "global",
                "aggregate_comparison",
                "none",
                "none",
                "stored_scalar",
                "ordinary",
                "small_duplicate_rich",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["retained and rejected HAVING cases"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.reference_scalar"),
            routes = ["global aggregate"],
            witness = "tier_c.global.nonempty_multiple_projection"
        ))
    ),
    interaction!(
        "interaction.grouped.hash_bounded",
        group = "grouped_aggregation",
        tuple = axes!("mode" = "hash", "window" = "bounded"),
        features = [
            "having.grouped_aggregate",
            "projection.grouped_layout",
            "select.grouped_composition",
        ],
        schema = ["group key lacks compatible ordered route"],
        fixtures = ["multiple duplicate-rich groups"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["hash grouping", "aggregate alias order", "bounded limit"],
        disposition = GeneratedRequired,
        reason = "Slice 1 must require the complete hash-grouped composition.",
        requirement = Some(requirement!(
            "required.grouped.hash_bounded",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "grouped_rows",
                "group_key_then_multiple_aggregates",
                "strict_scalar_comparison",
                "one_group_key",
                "aggregate_comparison",
                "aggregate_alias_desc_then_group_key",
                "limit",
                "stored_scalar|non_indexed_group_key",
                "ordinary",
                "multiple_groups",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["multiple groups", "HAVING retained and rejected groups"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.reference_scalar"),
            routes = ["hash grouping"],
            witness = "tier_c.grouped.hash_bounded"
        ))
    ),
    interaction!(
        "interaction.grouped.ordered_bounded",
        group = "grouped_aggregation",
        tuple = axes!("mode" = "ordered", "window" = "bounded"),
        features = [
            "having.grouped_aggregate",
            "ordering.projection_alias",
            "projection.grouped_layout",
            "select.grouped_composition",
        ],
        schema = ["group key has compatible secondary index order"],
        fixtures = ["multiple duplicate-rich indexed groups"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["ordered grouping", "aggregate alias order", "bounded limit"],
        disposition = GeneratedRequired,
        reason = "The indexed nullable profile is required to own ordered grouping evidence.",
        requirement = Some(requirement!(
            "required.grouped.ordered_bounded",
            ExpectedStructuralSignature::select(
                "indexed_nullable_reference",
                "grouped_rows",
                "group_key_then_multiple_aggregates",
                "indexed_scalar_comparison",
                "one_indexed_group_key",
                "aggregate_comparison",
                "aggregate_alias_desc_then_group_key",
                "limit",
                "single_secondary_index|stored_scalar",
                "ordinary",
                "multiple_duplicate_rich_indexed_groups",
                "secondary_range",
                "hybrid",
            ),
            fixtures = [
                "multiple indexed groups",
                "HAVING retained and rejected groups"
            ],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.indexed_nullable_reference"),
            routes = ["ordered grouping"],
            witness = "tier_c.grouped.ordered_bounded"
        ))
    ),
    interaction!(
        "interaction.grouped.ordered_continuation",
        group = "grouped_aggregation",
        tuple = axes!("mode" = "ordered", "window" = "continuation"),
        features = [
            "pagination.grouped_cursor",
            "select.grouped_aggregate",
            "select.grouped_composition",
        ],
        schema = ["group key has compatible secondary index order"],
        fixtures = ["more groups than one response page"],
        evidence = "contract_assertion",
        eligibility = "execution_mode_equivalent",
        routes = ["ordered grouped continuation"],
        disposition = DeterministicRequired,
        reason = "Opaque grouped continuation is an IcyDB transport contract.",
        requirement = Some(requirement!(
            "required.grouped.ordered_continuation",
            ExpectedStructuralSignature::select(
                "indexed_nullable_reference",
                "grouped_rows_with_cursor",
                "group_key_then_aggregate",
                "none",
                "one_indexed_group_key",
                "none",
                "group_key_ascending",
                "grouped_continuation",
                "single_secondary_index|stored_scalar",
                "ordinary",
                "more_than_one_group_page",
                "secondary_range",
                "pure",
            ),
            fixtures = ["more than one grouped page"],
            evidence = "contract_assertion",
            eligibility = "execution_mode_equivalent",
            provider = ProviderTarget::Existing("core.query.grouped_cursor"),
            routes = ["ordered grouped continuation"],
            witness = "existing.core.query.grouped_cursor"
        ))
    ),
    interaction!(
        "interaction.indexed.composite_prefix_hybrid",
        group = "indexed_scalar_execution",
        tuple = axes!(
            "access" = "composite_prefix",
            "covering" = "hybrid",
            "order" = "compatible",
        ),
        features = [
            "ddl.create_index_multi_field",
            "predicate.membership",
            "select.scalar_composition",
        ],
        schema = ["compatible composite index with equality prefix and projected row field"],
        fixtures = ["duplicate-rich indexed prefixes"],
        evidence = "contract_assertion",
        eligibility = "icydb_contract_only",
        routes = [
            "composite prefix",
            "hybrid covering",
            "compatible suffix order"
        ],
        disposition = GeneratedRequired,
        reason =
            "The indexed nullable profile must make composite-prefix route evidence structural.",
        requirement = Some(requirement!(
            "required.indexed.composite_prefix_hybrid",
            ExpectedStructuralSignature::select(
                "indexed_nullable_reference",
                "scalar_rows",
                "index_and_row_fields",
                "equality_prefix_and_membership",
                "none",
                "none",
                "compatible_index_suffix_then_primary_key",
                "limit",
                "composite_index_prefix_1|row_backed_projection",
                "membership_duplicate_nonnull",
                "duplicate_rich_indexed",
                "composite_prefix",
                "hybrid",
            ),
            fixtures = ["duplicate-rich index prefixes"],
            evidence = "contract_assertion",
            eligibility = "icydb_contract_only",
            provider = ProviderTarget::Planned("generated.select.indexed_nullable_reference"),
            routes = ["composite prefix", "hybrid covering"],
            witness = "tier_c.indexed.composite_prefix_hybrid"
        ))
    ),
    interaction!(
        "interaction.indexed.expression_range_non_covering",
        group = "indexed_scalar_execution",
        tuple = axes!(
            "access" = "expression_range",
            "covering" = "non_covering",
            "order" = "none",
        ),
        features = [
            "ddl.create_index_expression",
            "predicate.casefold_prefix",
            "predicate.starts_with",
        ],
        schema = ["accepted casefold expression index"],
        fixtures = ["Unicode and casefold prefix values"],
        evidence = "contract_assertion",
        eligibility = "icydb_contract_only",
        routes = ["expression index range with exact residual prefix check"],
        disposition = DeterministicRequired,
        reason = "Expression-index semantics have no declared lossless generated SQLite mapping.",
        requirement = Some(requirement!(
            "required.indexed.expression_range_non_covering",
            ExpectedStructuralSignature::select(
                "deterministic_expression_index",
                "scalar_rows",
                "plain_fields",
                "casefold_prefix",
                "none",
                "none",
                "none",
                "limit",
                "accepted_expression_index|row_backed_projection",
                "ordinary_prefix",
                "unicode_prefix",
                "expression_range",
                "non_covering",
            ),
            fixtures = ["Unicode casefold prefixes"],
            evidence = "contract_assertion",
            eligibility = "icydb_contract_only",
            provider = ProviderTarget::Existing("core.ddl.create_expression"),
            routes = ["expression index range", "residual prefix"],
            witness = "existing.core.ddl.create_expression"
        ))
    ),
    interaction!(
        "interaction.indexed.primary_exact_non_covering",
        group = "indexed_scalar_execution",
        tuple = axes!(
            "access" = "primary_exact",
            "covering" = "non_covering",
            "order" = "none",
        ),
        features = ["select.exact_primary_key", "select.scalar_rows"],
        schema = ["sole scalar primary key"],
        fixtures = ["present and absent exact keys"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["ByKey or Empty"],
        disposition = DeterministicRequired,
        reason = "Current exact-key value and route providers already close this interaction.",
        requirement = Some(requirement!(
            "required.indexed.primary_exact_non_covering",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "scalar_rows",
                "plain_fields",
                "primary_key_exact",
                "none",
                "none",
                "none",
                "none",
                "sole_primary_key|row_backed_projection",
                "point",
                "singleton",
                "primary_exact",
                "non_covering",
            ),
            fixtures = ["present and absent exact keys"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Existing("core.query.exact_key_execute"),
            routes = ["ByKey or Empty"],
            witness = "existing.core.query.exact_key_execute"
        ))
    ),
    interaction!(
        "interaction.indexed.secondary_range_non_covering_incompatible",
        group = "indexed_scalar_execution",
        tuple = axes!(
            "access" = "secondary_range",
            "covering" = "non_covering",
            "order" = "incompatible",
        ),
        features = [
            "ordering.projection_alias",
            "predicate.range",
            "select.scalar_composition",
        ],
        schema = ["single-field secondary index and row-backed projection"],
        fixtures = ["nonempty bounded range with order ties"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = [
            "one bounded secondary range",
            "materialized incompatible order"
        ],
        disposition = GeneratedRequired,
        reason = "The current generated snapshot has no secondary index.",
        requirement = Some(requirement!(
            "required.indexed.secondary_range_non_covering_incompatible",
            ExpectedStructuralSignature::select(
                "indexed_nullable_reference",
                "scalar_rows",
                "computed_and_row_fields",
                "nonempty_bounded_range_and_residual",
                "none",
                "none",
                "computed_alias_then_primary_key",
                "limit_offset",
                "single_secondary_index|row_backed_projection",
                "bounded_nonempty",
                "order_ties",
                "secondary_range",
                "non_covering",
            ),
            fixtures = ["nonempty bounded range", "order ties"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.indexed_nullable_reference"),
            routes = ["one bounded secondary range", "materialized order"],
            witness = "tier_c.indexed.secondary_range_non_covering_incompatible"
        ))
    ),
    interaction!(
        "interaction.indexed.secondary_range_pure_compatible",
        group = "indexed_scalar_execution",
        tuple = axes!(
            "access" = "secondary_range",
            "covering" = "pure",
            "order" = "compatible",
        ),
        features = [
            "ordering.null_values",
            "predicate.range",
            "projection.scalar",
            "select.scalar_composition",
        ],
        schema = ["single-field nullable secondary index with index-only projection"],
        fixtures = ["stored nulls", "duplicate-rich indexed values"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = [
            "one bounded secondary range",
            "compatible index order",
            "pure covering"
        ],
        disposition = GeneratedRequired,
        reason =
            "The indexed nullable profile is required to exercise route and null roles together.",
        requirement = Some(requirement!(
            "required.indexed.secondary_range_pure_compatible",
            ExpectedStructuralSignature::select(
                "indexed_nullable_reference",
                "scalar_rows",
                "index_fields_only",
                "nonempty_bounded_range",
                "none",
                "none",
                "compatible_index_order_then_primary_key",
                "limit",
                "nullable|single_secondary_index|index_projectable",
                "bounded_nonempty",
                "stored_null_duplicate_rich_indexed",
                "secondary_range",
                "pure",
            ),
            fixtures = ["stored nulls", "duplicate-rich indexed values"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.indexed_nullable_reference"),
            routes = ["one bounded secondary range", "pure covering"],
            witness = "tier_c.indexed.secondary_range_pure_compatible"
        ))
    ),
    interaction!(
        "interaction.null.computed_aggregate",
        group = "null_semantics",
        tuple = axes!("source" = "computed", "context" = "aggregate"),
        features = [
            "expression.value_selection",
            "projection.aggregate",
            "select.aggregate_distinct_filter",
        ],
        schema = ["reference scalar nullable result kind"],
        fixtures = ["computed null and non-null aggregate inputs"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["global aggregate"],
        disposition = GeneratedRequired,
        reason =
            "Slice 1 must require computed-null aggregate input rather than incidental generation.",
        requirement = Some(requirement!(
            "required.null.computed_aggregate",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "global_aggregate",
                "aggregate_over_nullif",
                "none",
                "global",
                "none",
                "none",
                "none",
                "stored_scalar",
                "computed_null",
                "computed_null_and_nonnull",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["computed null and non-null inputs"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.reference_scalar"),
            routes = ["global aggregate"],
            witness = "tier_c.null.computed_aggregate"
        ))
    ),
    interaction!(
        "interaction.null.computed_distinct",
        group = "null_semantics",
        tuple = axes!("source" = "computed", "context" = "distinct"),
        features = [
            "expression.value_selection",
            "select.computed_projection",
            "select.scalar_distinct",
        ],
        schema = ["reference scalar nullable result kind"],
        fixtures = ["duplicate computed nulls and values"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["scalar distinct"],
        disposition = GeneratedRequired,
        reason = "The fixed families do not require computed projection and DISTINCT together.",
        requirement = Some(requirement!(
            "required.null.computed_distinct",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "scalar_rows",
                "distinct_nullif",
                "none",
                "none",
                "none",
                "computed_alias_then_primary_key",
                "limit",
                "stored_scalar",
                "computed_null",
                "duplicate_computed_null",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["duplicate computed nulls"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.reference_scalar"),
            routes = ["scalar distinct"],
            witness = "tier_c.null.computed_distinct"
        ))
    ),
    interaction!(
        "interaction.null.stored_comparison_membership",
        group = "null_semantics",
        tuple = axes!("source" = "stored", "context" = "membership"),
        features = ["predicate.membership", "predicate.null"],
        schema = ["nullable stored scalar"],
        fixtures = ["stored nulls", "membership null and duplicate members"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["residual three-valued membership"],
        disposition = GeneratedRequired,
        reason = "The indexed nullable profile must bind null membership to accepted schema roles.",
        requirement = Some(requirement!(
            "required.null.stored_comparison_membership",
            ExpectedStructuralSignature::select(
                "indexed_nullable_reference",
                "scalar_rows",
                "plain_fields",
                "nullable_membership_and_null_test",
                "none",
                "none",
                "primary_key_ascending",
                "limit",
                "nullable|stored_scalar",
                "membership_duplicate_with_null",
                "stored_null_duplicate_rich",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["stored nulls", "membership nulls and duplicates"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.indexed_nullable_reference"),
            routes = ["three-valued residual membership"],
            witness = "tier_c.null.stored_comparison_membership"
        ))
    ),
    interaction!(
        "interaction.null.stored_ordering",
        group = "null_semantics",
        tuple = axes!("source" = "stored", "context" = "ordering"),
        features = ["ordering.null_values", "projection.scalar"],
        schema = ["nullable stored scalar"],
        fixtures = ["stored nulls and tied present values"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["canonical null-sensitive order"],
        disposition = GeneratedRequired,
        reason = "Slice 1 must require null ordering under the accepted nullable profile.",
        requirement = Some(requirement!(
            "required.null.stored_ordering",
            ExpectedStructuralSignature::select(
                "indexed_nullable_reference",
                "scalar_rows",
                "nullable_and_primary_key",
                "none",
                "none",
                "none",
                "nullable_ascending_then_primary_key",
                "limit_offset",
                "nullable|stored_scalar",
                "stored_null",
                "stored_null_order_ties",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["stored nulls", "order ties"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.indexed_nullable_reference"),
            routes = ["canonical null-sensitive order"],
            witness = "tier_c.null.stored_ordering"
        ))
    ),
    interaction!(
        "interaction.null.computed_ordering",
        group = "null_semantics",
        tuple = axes!("source" = "computed", "context" = "ordering"),
        features = [
            "expression.value_selection",
            "ordering.null_values",
            "ordering.projection_alias",
        ],
        schema = ["reference scalar nullable computed result"],
        fixtures = ["computed nulls and order ties"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["materialized computed order"],
        disposition = GeneratedRequired,
        reason = "Computed-null ordering is a reviewed composition gap.",
        requirement = Some(requirement!(
            "required.null.computed_ordering",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "scalar_rows",
                "nullif_alias",
                "none",
                "none",
                "none",
                "computed_nullable_alias_then_primary_key",
                "limit_offset",
                "stored_scalar",
                "computed_null",
                "computed_null_order_ties",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["computed nulls", "order ties"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.reference_scalar"),
            routes = ["materialized computed order"],
            witness = "tier_c.null.computed_ordering"
        ))
    ),
    interaction!(
        "interaction.scalar.reference_full_window",
        group = "scalar_composition",
        tuple = axes!(
            "profile" = "reference_scalar",
            "shape" = "full_scalar_window"
        ),
        features = [
            "pagination.scalar_limit_offset",
            "projection.aliases",
            "select.computed_projection",
            "select.scalar_composition",
        ],
        schema = ["reference scalar fields"],
        fixtures = ["order ties and more rows than the window"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["predicate before materialized order and window"],
        disposition = GeneratedRequired,
        reason = "The old window slots do not compose every required scalar axis in one case.",
        requirement = Some(requirement!(
            "required.scalar.reference_full_window",
            ExpectedStructuralSignature::select(
                "reference_scalar",
                "scalar_rows",
                "plain_and_computed_aliases",
                "boolean_tree_with_comparison",
                "none",
                "none",
                "projection_alias_then_primary_key",
                "limit_offset",
                "stored_scalar",
                "ordinary",
                "order_ties_more_than_window",
                "full_scan",
                "non_covering",
            ),
            fixtures = ["order ties", "more rows than window"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.reference_scalar"),
            routes = ["predicate", "materialized order", "window"],
            witness = "tier_c.scalar.reference_full_window"
        ))
    ),
    interaction!(
        "interaction.scalar.indexed_computed_distinct_window",
        group = "scalar_composition",
        tuple = axes!(
            "profile" = "indexed_nullable_reference",
            "shape" = "computed_distinct_window",
        ),
        features = [
            "ordering.projection_alias",
            "select.computed_projection",
            "select.scalar_composition",
            "select.scalar_distinct",
        ],
        schema = ["indexed nullable reference fields"],
        fixtures = ["duplicate computed values", "stored nulls", "order ties"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = ["indexed predicate with materialized computed DISTINCT order"],
        disposition = GeneratedRequired,
        reason = "This is a primary 0.215 missing interaction and requires the new profile.",
        requirement = Some(requirement!(
            "required.scalar.indexed_computed_distinct_window",
            ExpectedStructuralSignature::select(
                "indexed_nullable_reference",
                "scalar_rows",
                "distinct_computed_and_plain_aliases",
                "indexed_comparison_and_residual",
                "none",
                "none",
                "computed_alias_then_primary_key",
                "limit_offset",
                "nullable|single_secondary_index|row_backed_projection",
                "bounded_nonempty",
                "duplicate_computed_stored_null",
                "secondary_range",
                "non_covering",
            ),
            fixtures = ["duplicate computed values", "stored nulls", "order ties"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Planned("generated.select.indexed_nullable_reference"),
            routes = ["secondary range", "materialized DISTINCT order"],
            witness = "tier_c.scalar.indexed_computed_distinct_window"
        ))
    ),
    interaction!(
        "interaction.scalar.reference_invalid_alias_order",
        group = "scalar_composition",
        tuple = axes!(
            "profile" = "reference_scalar",
            "shape" = "invalid_alias_order"
        ),
        features = ["ordering.projection_alias", "projection.aliases"],
        schema = ["reference scalar fields"],
        fixtures = ["valid base declaration"],
        evidence = "boundary_assertion",
        eligibility = "rejection_invariant",
        routes = [],
        disposition = GeneratedRequired,
        reason = "Slice 1 must add a singly-invalid alias-binding violation with a typed cause.",
        requirement = Some(requirement!(
            "required.scalar.reference_invalid_alias_order",
            ExpectedStructuralSignature {
                declaration_kind: "singly_invalid",
                schema_profile: "reference_scalar",
                statement_family: "select",
                result_shape: "scalar_rows",
                projection_shape: "ambiguous_aliases",
                predicate_shape: "none",
                grouping_shape: "none",
                having_shape: "none",
                order_shape: "ambiguous_projection_alias",
                window_shape: "limit",
                field_roles: "stored_scalar",
                semantic_value_class: "ordinary",
                fixture_class: "valid_base",
                required_access: "not_applicable",
                required_covering: "not_applicable",
                expected_violation: "ambiguous_alias_binding",
            },
            fixtures = ["valid base declaration"],
            evidence = "boundary_assertion",
            eligibility = "rejection_invariant",
            provider = ProviderTarget::Planned("generated.select.reference_scalar"),
            routes = [],
            witness = "tier_c.scalar.reference_invalid_alias_order"
        ))
    ),
    interaction!(
        "interaction.mutation.authored_insert",
        group = "mutation",
        tuple = axes!(
            "profile" = "authored_scalar",
            "operation" = "insert",
            "intent" = "authored",
            "ingress" = "sql_and_typed",
        ),
        features = ["mutation.insert", "mutation.lane_ownership"],
        schema = ["caller-authored Nat64 primary key, text, and Nat64 value"],
        fixtures = ["single and multi-row inserts"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["SQL and typed converge before admission"],
        disposition = GeneratedRequired,
        reason = "Slice 2 replaces the fixed mutation vector with an obligation-owned sequence.",
        requirement = Some(requirement!(
            "required.mutation.authored_insert",
            ExpectedStructuralSignature::mutation(
                "authored_scalar",
                "insert",
                "affected_count_and_optional_rows",
                "none",
                "none",
                "none",
                "sole_primary_key|authored_fields",
                "authored_single_and_multi",
                "empty_then_nonempty_state",
                "",
            ),
            fixtures = ["single row", "multi row"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.authored_scalar"),
            routes = ["shared structural admission"],
            witness = "tier_c.mutation.authored_insert"
        ))
    ),
    interaction!(
        "interaction.mutation.authored_insert_from_query",
        group = "mutation",
        tuple = axes!(
            "profile" = "authored_scalar",
            "operation" = "insert_from_query",
            "intent" = "authored",
            "ingress" = "sql",
        ),
        features = ["mutation.insert", "mutation.lane_ownership"],
        schema = ["caller-authored scalar destination and maintained key/number source"],
        fixtures = ["bounded source rows"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["SQL INSERT FROM QUERY into shared admission"],
        disposition = GeneratedRequired,
        reason = "INSERT FROM QUERY is SQL-only but belongs in the independent state model.",
        requirement = Some(requirement!(
            "required.mutation.authored_insert_from_query",
            ExpectedStructuralSignature::mutation(
                "authored_scalar",
                "insert_from_query",
                "affected_count",
                "none",
                "source_query",
                "source_primary_key_ascending",
                "sole_primary_key|authored_fields",
                "authored_from_query",
                "bounded_source",
                "",
            ),
            fixtures = ["bounded source rows"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.authored_scalar"),
            routes = ["shared structural admission"],
            witness = "tier_c.mutation.authored_insert_from_query"
        ))
    ),
    interaction!(
        "interaction.mutation.authored_windowed",
        group = "mutation",
        tuple = axes!(
            "profile" = "authored_scalar",
            "operation" = "windowed",
            "intent" = "authored",
            "ingress" = "sql_and_typed",
        ),
        features = [
            "mutation.delete",
            "mutation.returning",
            "mutation.trusted_update",
            "mutation.trusted_update_window",
            "mutation.update",
        ],
        schema = ["caller-authored scalar row"],
        fixtures = ["exact, compound, and bounded matching sets"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["exact and bounded canonical primary-key selection"],
        disposition = GeneratedRequired,
        reason = "Slice 2 must derive update/delete windows from bounded typed operations.",
        requirement = Some(requirement!(
            "required.mutation.authored_windowed",
            ExpectedStructuralSignature::mutation(
                "authored_scalar",
                "update_delete_window",
                "affected_count_and_returning",
                "plain_fields",
                "exact_compound_bounded",
                "primary_key_ascending",
                "sole_primary_key|authored_fields",
                "authored_patch",
                "multiple_matching_rows",
                "",
            ),
            fixtures = ["exact", "compound", "bounded matches"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.authored_scalar"),
            routes = ["canonical bounded selection"],
            witness = "tier_c.mutation.authored_windowed"
        ))
    ),
    interaction!(
        "interaction.mutation.default_insert_authored",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "insert",
            "intent" = "authored",
            "ingress" = "sql_and_typed",
        ),
        features = ["mutation.insert", "mutation.lane_ownership"],
        schema = ["accepted default profile with indexed tier"],
        fixtures = ["all five fields authored"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["shared structural admission and tier index delta"],
        disposition = GeneratedRequired,
        reason = "Slice 2 introduces the exact accepted-default profile.",
        requirement = Some(requirement!(
            "required.mutation.default_insert_authored",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "insert",
                "affected_count_and_complete_row",
                "all_fields",
                "none",
                "none",
                "sole_primary_key|default_fields|single_secondary_index",
                "all_authored",
                "empty_state",
                "",
            ),
            fixtures = ["all fields authored"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["shared structural admission", "tier index delta"],
            witness = "tier_c.mutation.default_insert_authored"
        ))
    ),
    interaction!(
        "interaction.mutation.default_insert_omitted",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "insert",
            "intent" = "omitted",
            "ingress" = "sql_and_typed",
        ),
        features = ["mutation.insert", "mutation.lane_ownership"],
        schema = ["tier and score constant defaults; nullable note omission"],
        fixtures = ["id and name authored only"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["accepted omission materialization and tier index delta"],
        disposition = GeneratedRequired,
        reason = "Omission provenance must be explicit in SQL and canonical typed create.",
        requirement = Some(requirement!(
            "required.mutation.default_insert_omitted",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "insert",
                "affected_count_and_complete_row",
                "all_fields",
                "none",
                "none",
                "sole_primary_key|default_fields|single_secondary_index",
                "omitted_defaults",
                "empty_state",
                "",
            ),
            fixtures = ["id and name authored only"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["accepted default resolution", "tier index delta"],
            witness = "tier_c.mutation.default_insert_omitted"
        ))
    ),
    interaction!(
        "interaction.mutation.default_insert_explicit",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "insert",
            "intent" = "explicit_default",
            "ingress" = "sql",
        ),
        features = ["mutation.insert", "mutation.lane_ownership"],
        schema = ["tier and score constant defaults; nullable note default"],
        fixtures = ["explicit DEFAULT for tier, score, and note"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["SQL intent lowering into accepted default materialization"],
        disposition = GeneratedRequired,
        reason = "Explicit DEFAULT is a SQL-only provenance form with state-model authority.",
        requirement = Some(requirement!(
            "required.mutation.default_insert_explicit",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "insert",
                "affected_count_and_complete_row",
                "all_fields",
                "none",
                "none",
                "sole_primary_key|default_fields|single_secondary_index",
                "explicit_defaults",
                "empty_state",
                "",
            ),
            fixtures = ["explicit defaults"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["accepted default resolution", "tier index delta"],
            witness = "tier_c.mutation.default_insert_explicit"
        ))
    ),
    interaction!(
        "interaction.mutation.default_insert_explicit_typed",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "insert",
            "intent" = "explicit_default",
            "ingress" = "typed",
        ),
        features = ["mutation.insert", "mutation.lane_ownership"],
        schema = ["accepted default profile"],
        fixtures = [],
        evidence = "boundary_assertion",
        eligibility = "frontend_equivalent",
        routes = [],
        disposition = Inapplicable,
        reason = "The canonical typed create surface expresses omission, not SQL DEFAULT tokens.",
        requirement = None
    ),
    interaction!(
        "interaction.mutation.default_insert_mixed_batch",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "insert",
            "intent" = "mixed_batch",
            "ingress" = "sql",
        ),
        features = ["mutation.insert", "mutation.lane_ownership"],
        schema = ["accepted default profile with indexed tier"],
        fixtures = ["authored, omitted, and explicit-default rows"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["one atomic SQL batch and complete tier index delta"],
        disposition = GeneratedRequired,
        reason = "Mixed provenance must remain row-specific inside one atomic batch.",
        requirement = Some(requirement!(
            "required.mutation.default_insert_mixed_batch",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "insert",
                "affected_count_and_complete_rows",
                "all_fields",
                "none",
                "none",
                "sole_primary_key|default_fields|single_secondary_index",
                "mixed_authored_omitted_explicit_default",
                "empty_state",
                "",
            ),
            fixtures = ["three-row mixed provenance batch"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["atomic batch", "complete tier index delta"],
            witness = "tier_c.mutation.default_insert_mixed_batch"
        ))
    ),
    interaction!(
        "interaction.mutation.default_update_authored",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "update",
            "intent" = "authored",
            "ingress" = "sql_and_typed",
        ),
        features = ["mutation.returning", "mutation.update", "returning.star"],
        schema = ["accepted default profile with indexed tier"],
        fixtures = ["one primary-key match"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["exact primary-key mutation and tier index replacement"],
        disposition = GeneratedRequired,
        reason = "Authored default-profile updates require SQL/typed parity.",
        requirement = Some(requirement!(
            "required.mutation.default_update_authored",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "update",
                "affected_count_and_complete_row",
                "returning_star",
                "primary_key_exact",
                "primary_key_ascending",
                "sole_primary_key|default_fields|single_secondary_index",
                "authored_patch",
                "one_matching_row",
                "",
            ),
            fixtures = ["one matching row"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["exact mutation selection", "tier index replacement"],
            witness = "tier_c.mutation.default_update_authored"
        ))
    ),
    interaction!(
        "interaction.mutation.default_update_default",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "update",
            "intent" = "explicit_default",
            "ingress" = "sql",
        ),
        features = ["mutation.returning", "mutation.update", "returning.fields"],
        schema = ["accepted default profile with indexed tier"],
        fixtures = ["one row holding non-default values"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["exact update and accepted default resolution"],
        disposition = GeneratedRequired,
        reason = "SET field = DEFAULT is a SQL-only state transition.",
        requirement = Some(requirement!(
            "required.mutation.default_update_default",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "update",
                "affected_count_and_returned_fields",
                "tier_score_note",
                "primary_key_exact",
                "primary_key_ascending",
                "sole_primary_key|default_fields|single_secondary_index",
                "explicit_update_defaults",
                "one_nondefault_row",
                "",
            ),
            fixtures = ["one non-default row"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["accepted default resolution", "tier index replacement"],
            witness = "tier_c.mutation.default_update_default"
        ))
    ),
    interaction!(
        "interaction.mutation.default_update_default_typed",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "update",
            "intent" = "explicit_default",
            "ingress" = "typed",
        ),
        features = ["mutation.update"],
        schema = ["accepted default profile"],
        fixtures = [],
        evidence = "boundary_assertion",
        eligibility = "frontend_equivalent",
        routes = [],
        disposition = Inapplicable,
        reason = "The typed update patch does not expose a SQL DEFAULT token.",
        requirement = None
    ),
    interaction!(
        "interaction.mutation.default_update_preserve",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "update",
            "intent" = "preserve",
            "ingress" = "sql_and_typed",
        ),
        features = ["mutation.lane_ownership", "mutation.update"],
        schema = ["accepted default profile with indexed tier"],
        fixtures = ["one row holding non-default values"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["absent assignment preserves accepted values and index state"],
        disposition = GeneratedRequired,
        reason = "Preservation must remain distinct from default application.",
        requirement = Some(requirement!(
            "required.mutation.default_update_preserve",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "update",
                "affected_count_and_complete_row",
                "all_fields",
                "primary_key_exact",
                "primary_key_ascending",
                "sole_primary_key|default_fields|single_secondary_index",
                "absent_assignments_preserve",
                "one_nondefault_row",
                "",
            ),
            fixtures = ["one non-default row"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["preserved tier index state"],
            witness = "tier_c.mutation.default_update_preserve"
        ))
    ),
    interaction!(
        "interaction.mutation.default_no_match",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "no_match",
            "intent" = "authored",
            "ingress" = "sql_and_typed",
        ),
        features = ["mutation.delete", "mutation.update"],
        schema = ["accepted default profile"],
        fixtures = ["absent primary key"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["exact empty mutation selection"],
        disposition = GeneratedRequired,
        reason = "No-match update/delete must preserve complete state.",
        requirement = Some(requirement!(
            "required.mutation.default_no_match",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "update_delete_no_match",
                "zero_affected",
                "none",
                "primary_key_exact_absent",
                "primary_key_ascending",
                "sole_primary_key",
                "authored",
                "absent_key",
                "",
            ),
            fixtures = ["absent primary key"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["exact empty mutation selection"],
            witness = "tier_c.mutation.default_no_match"
        ))
    ),
    interaction!(
        "interaction.mutation.default_delete_returning",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "delete_returning",
            "intent" = "authored",
            "ingress" = "sql_and_typed",
        ),
        features = ["mutation.delete", "mutation.returning", "returning.star"],
        schema = ["accepted default profile with indexed tier"],
        fixtures = ["one matching row"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["delete returns complete old logical row and removes tier index entry"],
        disposition = GeneratedRequired,
        reason = "Delete RETURNING must agree with the independent complete-state oracle.",
        requirement = Some(requirement!(
            "required.mutation.default_delete_returning",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "delete",
                "affected_count_and_old_complete_row",
                "returning_star",
                "primary_key_exact",
                "primary_key_ascending",
                "sole_primary_key|default_fields|single_secondary_index",
                "authored",
                "one_matching_row",
                "",
            ),
            fixtures = ["one matching row"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["exact delete", "tier index removal"],
            witness = "tier_c.mutation.default_delete_returning"
        ))
    ),
    interaction!(
        "interaction.mutation.default_reject_required",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "reject_required",
            "intent" = "omitted",
            "ingress" = "sql",
        ),
        features = ["mutation.insert", "mutation.lane_ownership"],
        schema = ["required name has no default"],
        fixtures = ["unchanged pre-state"],
        evidence = "boundary_assertion",
        eligibility = "rejection_invariant",
        routes = ["rejection before marker publication"],
        disposition = GeneratedRequired,
        reason = "Required-field omission needs one typed singly-invalid mutation witness.",
        requirement = Some(requirement!(
            "required.mutation.default_reject_required",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "insert",
                "typed_error",
                "none",
                "none",
                "none",
                "required_without_default",
                "omitted_required",
                "unchanged_pre_state",
                "missing_required_field",
            ),
            fixtures = ["unchanged pre-state"],
            evidence = "boundary_assertion",
            eligibility = "rejection_invariant",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["no marker publication"],
            witness = "tier_c.mutation.default_reject_required"
        ))
    ),
    interaction!(
        "interaction.mutation.default_reject_pk_default",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "reject_pk_default",
            "intent" = "explicit_default",
            "ingress" = "sql",
        ),
        features = ["mutation.insert", "mutation.lane_ownership"],
        schema = ["policy-free required caller-authored primary key"],
        fixtures = ["unchanged pre-state"],
        evidence = "boundary_assertion",
        eligibility = "rejection_invariant",
        routes = ["rejection before marker publication"],
        disposition = GeneratedRequired,
        reason = "DEFAULT on a policy-free primary key must reject without state change.",
        requirement = Some(requirement!(
            "required.mutation.default_reject_pk_default",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "insert",
                "typed_error",
                "none",
                "none",
                "none",
                "sole_primary_key|required_without_default",
                "explicit_default",
                "unchanged_pre_state",
                "default_unavailable",
            ),
            fixtures = ["unchanged pre-state"],
            evidence = "boundary_assertion",
            eligibility = "rejection_invariant",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["no marker publication"],
            witness = "tier_c.mutation.default_reject_pk_default"
        ))
    ),
    interaction!(
        "interaction.mutation.default_reject_pk_default_typed",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "reject_pk_default",
            "intent" = "explicit_default",
            "ingress" = "typed",
        ),
        features = ["mutation.insert"],
        schema = ["policy-free required caller-authored primary key"],
        fixtures = [],
        evidence = "boundary_assertion",
        eligibility = "frontend_equivalent",
        routes = [],
        disposition = Inapplicable,
        reason =
            "The typed create surface requires primary-key authorship and has no DEFAULT token.",
        requirement = None
    ),
    interaction!(
        "interaction.mutation.default_reject_duplicate",
        group = "mutation",
        tuple = axes!(
            "profile" = "accepted_default",
            "operation" = "reject_duplicate",
            "intent" = "mixed_batch",
            "ingress" = "sql_and_typed",
        ),
        features = ["mutation.insert", "mutation.lane_ownership"],
        schema = ["sole Nat64 primary key and indexed tier"],
        fixtures = ["duplicate-primary-key multi-row batch"],
        evidence = "state_model_reference",
        eligibility = "state_model_reference",
        routes = ["atomic rejection with unchanged row and index state"],
        disposition = GeneratedRequired,
        reason = "Duplicate rejection must prove batch atomicity under both ingresses.",
        requirement = Some(requirement!(
            "required.mutation.default_reject_duplicate",
            ExpectedStructuralSignature::mutation(
                "accepted_default",
                "insert",
                "typed_error",
                "none",
                "none",
                "none",
                "sole_primary_key|single_secondary_index",
                "duplicate_primary_key_batch",
                "unchanged_pre_state",
                "duplicate_primary_key",
            ),
            fixtures = ["duplicate primary-key batch"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Planned("generated.mutation.accepted_default"),
            routes = ["no marker publication", "unchanged tier index"],
            witness = "tier_c.mutation.default_reject_duplicate"
        ))
    ),
];

const NO_INTERACTION_OPERATIONAL_OR_POLICY: &[&str] = &[
    "introspection.catalog_projection",
    "introspection.generated_policy",
    "introspection.index_origin",
    "introspection.storage_modes",
    "mutation.generated_query_ddl",
    "mutation.generated_update_bounded",
    "mutation.generated_update_disabled",
    "mutation.generated_update_primary_key",
    "mutation.trusted_resumable_update",
    "operational.byte_metrics",
    "operational.transport_controls",
    "surface.trusted_entrypoints",
];

const NO_INTERACTION_CATALOG_OR_DDL: &[&str] = &[
    "ddl.alter_add_check_constraint",
    "ddl.alter_add_check_not_valid",
    "ddl.alter_add_column",
    "ddl.alter_column_default",
    "ddl.alter_column_nullability",
    "ddl.alter_drop_constraint",
    "ddl.alter_validate_constraint",
    "ddl.create_index_filtered",
    "ddl.create_index_if_not_exists",
    "ddl.create_unique_index",
    "ddl.destructive_publication_atomicity",
    "ddl.drop_column",
    "ddl.drop_index",
    "ddl.drop_index_if_exists",
    "ddl.generated_owned_objects",
    "ddl.index_ascending",
    "ddl.index_descending",
    "ddl.rename_column",
    "introspection.describe",
    "introspection.show_columns",
    "introspection.show_constraints",
    "introspection.show_entities",
    "introspection.show_entity",
    "introspection.show_indexes",
    "introspection.show_memory",
    "introspection.show_stores",
    "introspection.unsupported_modifiers",
];

const NO_INTERACTION_DETERMINISTIC_VALUE_OR_EXPRESSION: &[&str] = &[
    "blob.hex_literal",
    "blob.literal_size_limit",
    "blob.ordering",
    "blob.read_write_compare",
    "expression.numeric_functions",
    "expression.searched_case",
    "expression.simple_case",
    "expression.text_functions",
    "having.raw_row_escape",
    "predicate.boolean_comparison",
    "predicate.boolean_truth",
    "predicate.expression_arguments",
    "predicate.field_bound_range",
    "predicate.field_comparison",
    "predicate.grouped_where_field_comparison",
    "predicate.non_prefix_pattern",
    "predicate.prefix_pattern",
    "projection.invalid_grouped_layout",
    "returning.computed",
];

const NO_INTERACTION_SYNTAX_OR_ENTRY_BOUNDARY: &[&str] = &[
    "explain.query_delete",
    "expression.cast",
    "naming.single_binding",
    "pagination.scalar_cursor",
    "query.multi_entity",
    "query.set_operations",
    "query.subquery_cte",
    "query.transactions",
    "query.window_functions",
    "select.placeholder_parameters",
];

fn no_interaction_reason(feature_id: &str) -> Option<&'static str> {
    if NO_INTERACTION_OPERATIONAL_OR_POLICY.contains(&feature_id) {
        return Some(
            "Operational exposure or policy has direct boundary evidence and does not create a generated structural interaction.",
        );
    }
    if NO_INTERACTION_CATALOG_OR_DDL.contains(&feature_id) {
        return Some(
            "Catalog mutation or introspection remains with deterministic accepted-schema evidence outside the generated query/mutation profiles.",
        );
    }
    if NO_INTERACTION_DETERMINISTIC_VALUE_OR_EXPRESSION.contains(&feature_id) {
        return Some(
            "The feature retains exact deterministic evidence and adds no separately reviewed cross-feature tuple in the frozen 0.215 universe.",
        );
    }
    if NO_INTERACTION_SYNTAX_OR_ENTRY_BOUNDARY.contains(&feature_id) {
        return Some(
            "Parser, unsupported-syntax, naming, or entry-boundary behavior is closed by direct typed evidence rather than a generated structural interaction.",
        );
    }

    None
}

#[derive(Clone, Copy, Debug, Serialize)]
struct OpeningBaseline {
    release: &'static str,
    commit: &'static str,
    manifest_features: usize,
    deterministic_providers: usize,
    raw_sql_perf_wasm_bytes: usize,
    raw_sql_perf_wasm_sha256: &'static str,
}

const OPENING_BASELINE: OpeningBaseline = OpeningBaseline {
    release: "v0.214.1",
    commit: "420aeec1160780a4b11e6c111e07384acbc43ab7",
    manifest_features: 106,
    deterministic_providers: 102,
    raw_sql_perf_wasm_bytes: 5_272_813,
    raw_sql_perf_wasm_sha256: "80c9371ea616a9c0f37f7b0a77f7f54b506cba6584d6d5ce777248843092f381",
};

#[derive(Clone, Copy, Debug, Serialize)]
struct OpeningInventory {
    tier_c_top_level_declarations: usize,
    valid_generated_select_declarations: usize,
    invalid_generated_select_declarations: usize,
    generated_mutation_sequence_declarations: usize,
    deterministic_sqlite_declarations: usize,
    regression_corpus_declarations: usize,
    current_select_profiles: &'static [&'static str],
    current_mutation_profiles: &'static [&'static str],
    current_valid_select_template_slots: usize,
    current_invalid_select_templates: usize,
    current_mutation_structural_variants: usize,
    current_structural_identities: usize,
    p1_scenarios: usize,
    scale_scenarios: usize,
    accepted_p2_confirmations: usize,
    focused_hotspot_scenarios: usize,
    regression_sentinels: usize,
}

const OPENING_INVENTORY: OpeningInventory = OpeningInventory {
    tier_c_top_level_declarations: 2_505,
    valid_generated_select_declarations: 2_048,
    invalid_generated_select_declarations: 320,
    generated_mutation_sequence_declarations: 128,
    deterministic_sqlite_declarations: 8,
    regression_corpus_declarations: 1,
    current_select_profiles: &["reference_scalar"],
    current_mutation_profiles: &["authored_scalar"],
    current_valid_select_template_slots: 64,
    current_invalid_select_templates: 5,
    current_mutation_structural_variants: 4,
    current_structural_identities: 73,
    p1_scenarios: 1_787,
    scale_scenarios: 72,
    accepted_p2_confirmations: 424,
    focused_hotspot_scenarios: 15,
    regression_sentinels: 351,
};

#[derive(Clone, Copy, Debug, Serialize)]
struct OwnerMapEntry {
    concern: &'static str,
    current_owner: &'static str,
    current_entrypoint: &'static str,
    slice_0_disposition: &'static str,
}

const OWNER_MAP: &[OwnerMapEntry] = &[
    OwnerMapEntry {
        concern: "compound_range_bound_intersection",
        current_owner: "crates/icydb-core/src/db/query/plan/planner/range/bounds.rs",
        current_entrypoint: "merge_range_constraint",
        slice_0_disposition: "current owner already merges lower and upper bounds and rejects empty or crossed intervals",
    },
    OwnerMapEntry {
        concern: "compound_range_candidate",
        current_owner: "crates/icydb-core/src/db/query/plan/planner/range/extract.rs",
        current_entrypoint: "index_range_from_and",
        slice_0_disposition: "current normalized AND path emits one SemanticIndexRangeSpec and leaves unrelated compares residual",
    },
    OwnerMapEntry {
        concern: "compound_range_execution",
        current_owner: "crates/icydb-core/src/db/executor/stream/access/traversal.rs",
        current_entrypoint: "access visitor index_range path",
        slice_0_disposition: "consumes one lowered range envelope; current instructions still require the Slice 3 cohort",
    },
    OwnerMapEntry {
        concern: "compound_range_lowering",
        current_owner: "crates/icydb-core/src/db/access/lowering.rs",
        current_entrypoint: "lower_access_with_schema_info",
        slice_0_disposition: "materializes one raw lower/upper envelope from SemanticIndexRangeSpec",
    },
    OwnerMapEntry {
        concern: "coverage_manifest",
        current_owner: "testing/integration/tests/sql_correctness_support/coverage_manifest.rs",
        current_entrypoint: "MANIFEST and PROVIDERS",
        slice_0_disposition: "extended by this module; remains the code authority projected into the checked-in JSON",
    },
    OwnerMapEntry {
        concern: "generated_select_fixture",
        current_owner: "testing/sql-generator/src/generator.rs",
        current_entrypoint: "generate_fixture",
        slice_0_disposition: "one reference-scalar fixture owner; indexed nullable profile is a Slice 1 gap",
    },
    OwnerMapEntry {
        concern: "generated_select_structure",
        current_owner: "testing/sql-generator/src/generator.rs",
        current_entrypoint: "generate_query and eight family builders",
        slice_0_disposition: "64 family/slot identities; replacement belongs to Slice 1",
    },
    OwnerMapEntry {
        concern: "membership_canonicalization",
        current_owner: "crates/icydb-core/src/db/predicate/membership.rs",
        current_entrypoint: "canonical_membership_value_list",
        slice_0_disposition: "canonicalizes a value set; no eliminated-work counter exists",
    },
    OwnerMapEntry {
        concern: "membership_sql_binding",
        current_owner: "crates/icydb-core/src/db/sql/lowering/select/binding.rs",
        current_entrypoint: "canonicalize_sql_in_list_expr_for_schema",
        slice_0_disposition: "binds and canonicalizes SQL list members; finding 206-011 remains measurement-only",
    },
    OwnerMapEntry {
        concern: "mutation_generation",
        current_owner: "testing/sql-generator/src/mutation/generator.rs",
        current_entrypoint: "sequence_statements",
        slice_0_disposition: "four complete authored-scalar variants; replacement belongs to Slice 2",
    },
    OwnerMapEntry {
        concern: "performance_scenario_authority",
        current_owner: "testing/integration/tests/sql_perf_matrix_audit.rs",
        current_entrypoint: "sql_perf_scenarios and scale declarations",
        slice_0_disposition: "current P1/P2/scale owner; hard-cut profile replacement belongs to Slice 3",
    },
    OwnerMapEntry {
        concern: "prefix_branch_construction",
        current_owner: "crates/icydb-core/src/db/query/plan/planner/prefix.rs",
        current_entrypoint: "index_branch_set_from_and",
        slice_0_disposition: "owns exclusion pruning, branch-cap admission, and wide branch construction for findings 206-012 through 206-014",
    },
    OwnerMapEntry {
        concern: "prefix_multi_lookup",
        current_owner: "crates/icydb-core/src/db/query/plan/planner/prefix.rs",
        current_entrypoint: "index_multi_lookup_for_in",
        slice_0_disposition: "owns sparse membership access construction for finding 206-010; no eliminated-work counter exists",
    },
    OwnerMapEntry {
        concern: "route_and_materialization_facts",
        current_owner: "crates/icydb-core/src/db/query/plan and crates/icydb-core/src/db/executor",
        current_entrypoint: "access choice, covering plan, grouped strategy, and execution diagnostics",
        slice_0_disposition: "observed facts must feed the Slice 1 structural signature without SQL-text inference",
    },
    OwnerMapEntry {
        concern: "scheduled_correctness",
        current_owner: "testing/sql-generator/src/scheduled.rs",
        current_entrypoint: "Tier C declarations, shards, receipts, and merge",
        slice_0_disposition: "receipts do not yet carry the 0.215 catalog hash or observed signatures",
    },
];

#[derive(Clone, Copy, Debug, Serialize)]
struct GapLedgerEntry {
    id: &'static str,
    finding: &'static str,
    evidence: &'static str,
    owning_slice: &'static str,
    exclusion: &'static str,
}

const GAP_LEDGER: &[GapLedgerEntry] = &[
    GapLedgerEntry {
        id: "215-000",
        finding: "The provisional design baseline says 98 features and 93 providers; v0.214.1 has 106 and 102.",
        evidence: "Exact SQL_SUBSET metadata/manifest bijection and provider registry counts.",
        owning_slice: "design_review_before_slice_1",
        exclusion: "Do not delete post-design 0.213/0.214 obligations to recover provisional counts.",
    },
    GapLedgerEntry {
        id: "215-001",
        finding: "Current generated SELECT evidence has 64 family/slot identities but no derived lossless structural signature.",
        evidence: "Eight SelectGeneratorFamily variants multiplied by eight handwritten case slots.",
        owning_slice: "slice_1",
        exclusion: "Slice 0 does not change generator or replay formats.",
    },
    GapLedgerEntry {
        id: "215-002",
        finding: "The indexed nullable reference SELECT profile and its required generated witnesses are absent.",
        evidence: "Current SelectSnapshot has one reference-scalar profile and no secondary index.",
        owning_slice: "slice_1",
        exclusion: "Do not add a third generated SELECT profile.",
    },
    GapLedgerEntry {
        id: "215-003",
        finding: "Five fixed invalid SELECT templates do not cover the reviewed alias/grouping/leaf interaction rejections.",
        evidence: "ALL_SELECT_VIOLATIONS contains exactly five variants.",
        owning_slice: "slice_1",
        exclusion: "Keep singly-invalid typed proposals; do not generate free-form malformed SQL.",
    },
    GapLedgerEntry {
        id: "215-004",
        finding: "Mutation generation has four authored-scalar vectors and no accepted-default profile or intent-provenance oracle.",
        evidence: "MUTATION_STRUCTURAL_VARIANT_COUNT is four and sequence_statements authors fixed rows.",
        owning_slice: "slice_2",
        exclusion: "Do not generalize the model to relations, managed fields, Identity, or structured values.",
    },
    GapLedgerEntry {
        id: "215-005",
        finding: "The current P1/P2/scale evidence predates the post-0.214 source and Wasm identity.",
        evidence: "Opening v0.214.1 raw Wasm is recorded; no comparable current instruction cohort exists.",
        owning_slice: "slice_3",
        exclusion: "Historical 0.206 instructions are context, not a comparable 0.215 baseline.",
    },
    GapLedgerEntry {
        id: "215-006",
        finding: "The current planner already constructs one bounded SemanticIndexRangeSpec for a normalized AND range.",
        evidence: "index_range_from_and plus merge_range_constraint lower both bounds into one range envelope.",
        owning_slice: "slice_3_then_slice_4_decision",
        exclusion: "Do not assume the historical two-traversal symptom still exists; require current attributed evidence before production change.",
    },
    GapLedgerEntry {
        id: "215-007",
        finding: "Findings 206-010 through 206-014 still lack member/pass/branch eliminated-work counters.",
        evidence: "Current membership, SQL binding, and prefix planner owners expose instructions but no typed work counters.",
        owning_slice: "slice_3",
        exclusion: "Measurement does not authorize a second production optimization in 0.215.",
    },
    GapLedgerEntry {
        id: "215-008",
        finding: "Tier C receipts do not carry the frozen catalog hash, scheduled witness ID, or observed structural signature.",
        evidence: "Current scheduled evidence format remains version 1 with the 0.204 manifest revision only.",
        owning_slice: "slice_1",
        exclusion: "No compatibility decoder or dual current receipt format.",
    },
];

#[derive(Debug, Serialize)]
struct CurrentStructuralIdentity {
    id: String,
    kind: &'static str,
    family: &'static str,
    slot: u32,
}

fn current_structural_identities() -> Vec<CurrentStructuralIdentity> {
    const SELECT_FAMILIES: &[&str] = &[
        "distinct",
        "expression",
        "global_aggregate",
        "grouped_aggregate",
        "having",
        "predicate",
        "scalar_projection",
        "window",
    ];
    const SELECT_VIOLATIONS: &[&str] = &[
        "clause_order",
        "function_signature",
        "limit_overflow",
        "operator_type",
        "unknown_field",
    ];

    let mut identities = Vec::with_capacity(OPENING_INVENTORY.current_structural_identities);
    for family in SELECT_FAMILIES {
        for slot in 0..8 {
            identities.push(CurrentStructuralIdentity {
                id: format!("current.select.{family}.slot_{slot}"),
                kind: "valid_select_template_slot",
                family,
                slot,
            });
        }
    }
    for (slot, violation) in SELECT_VIOLATIONS.iter().enumerate() {
        identities.push(CurrentStructuralIdentity {
            id: format!("current.invalid_select.{violation}"),
            kind: "invalid_select_template",
            family: violation,
            slot: u32::try_from(slot).expect("five static violation slots fit u32"),
        });
    }
    for slot in 0..4 {
        identities.push(CurrentStructuralIdentity {
            id: format!("current.mutation.authored_scalar.variant_{slot}"),
            kind: "mutation_structural_variant",
            family: "authored_scalar",
            slot,
        });
    }
    identities.sort_by(|left, right| left.id.cmp(&right.id));
    identities
}

#[derive(Debug, Serialize)]
struct GroupProjection {
    id: &'static str,
    axes: &'static [Axis],
    interaction_obligations: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct ManifestParticipationProjection {
    feature_id: &'static str,
    interaction_obligations: Vec<&'static str>,
    no_structural_interaction_required: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct InteractionProjection {
    id: &'static str,
    group: &'static str,
    tuple: &'static [AxisValue],
    contract_features: &'static [&'static str],
    schema_facts: &'static [&'static str],
    fixture_properties: &'static [&'static str],
    minimum_evidence: &'static str,
    provider_eligibility: &'static str,
    route_facts: &'static [&'static str],
    disposition: Disposition,
    reason: &'static str,
    required_structural_obligations: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct RequirementProjection {
    id: &'static str,
    interaction_obligations: Vec<&'static str>,
    expected_structural_signature: ExpectedStructuralSignature,
    fixture_properties: &'static [&'static str],
    minimum_evidence: &'static str,
    provider_eligibility: &'static str,
    provider_id: &'static str,
    provider_state: &'static str,
    route_facts: &'static [&'static str],
    witness_id: &'static str,
    opening_evidence_state: &'static str,
}

#[derive(Debug, Serialize)]
struct CatalogBody {
    design_line: &'static str,
    opening_baseline: OpeningBaseline,
    opening_inventory: OpeningInventory,
    current_structural_identities: Vec<CurrentStructuralIdentity>,
    interaction_groups: Vec<GroupProjection>,
    manifest_participation: Vec<ManifestParticipationProjection>,
    interaction_obligations: Vec<InteractionProjection>,
    required_structural_obligations: Vec<RequirementProjection>,
    gap_ledger: &'static [GapLedgerEntry],
    owner_map: &'static [OwnerMapEntry],
}

#[derive(Debug, Serialize)]
struct CatalogArtifact {
    format_version: u32,
    catalog_hash: String,
    catalog: CatalogBody,
}

fn validate_group_tuple(interaction: &InteractionObligation) -> Result<(), String> {
    let group = GROUPS
        .iter()
        .find(|group| group.id == interaction.group)
        .ok_or_else(|| {
            format!(
                "interaction {:?} names unknown group {:?}",
                interaction.id, interaction.group
            )
        })?;
    let mut assigned_axes = BTreeSet::new();
    for assignment in interaction.tuple {
        let axis = group
            .axes
            .iter()
            .find(|axis| axis.name == assignment.axis)
            .ok_or_else(|| {
                format!(
                    "interaction {:?} names unknown {:?} axis {:?}",
                    interaction.id, interaction.group, assignment.axis
                )
            })?;
        if !axis.members.contains(&assignment.value) {
            return Err(format!(
                "interaction {:?} uses unknown {:?} value {:?} for axis {:?}",
                interaction.id, interaction.group, assignment.value, assignment.axis
            ));
        }
        if !assigned_axes.insert(assignment.axis) {
            return Err(format!(
                "interaction {:?} assigns axis {:?} more than once",
                interaction.id, assignment.axis
            ));
        }
    }
    let required_axes = group
        .axes
        .iter()
        .map(|axis| axis.name)
        .collect::<BTreeSet<_>>();
    if assigned_axes != required_axes {
        return Err(format!(
            "interaction {:?} does not assign the exact {:?} axes",
            interaction.id, interaction.group
        ));
    }

    Ok(())
}

fn validate_provider_target(
    interaction: &InteractionObligation,
    requirement: &StructuralRequirement,
    providers: &BTreeMap<&str, &ProviderSpec>,
) -> Result<(), String> {
    match requirement.provider {
        ProviderTarget::Existing(provider_id) => {
            let provider = providers.get(provider_id).ok_or_else(|| {
                format!(
                    "requirement {:?} names absent existing provider {provider_id:?}",
                    requirement.id
                )
            })?;
            let provider_is_referenced = interaction.contract_features.iter().any(|feature_id| {
                MANIFEST.iter().any(|cell| {
                    cell.id == *feature_id && cell.deterministic_providers.contains(&provider.id)
                })
            });
            if !provider_is_referenced {
                return Err(format!(
                    "requirement {:?} provider {provider_id:?} is not owned by a referenced manifest feature",
                    requirement.id
                ));
            }
        }
        ProviderTarget::Planned(provider_id) => {
            if !PLANNED_PROVIDERS.contains(&provider_id) {
                return Err(format!(
                    "requirement {:?} names undeclared planned provider {provider_id:?}",
                    requirement.id
                ));
            }
        }
    }
    if requirement.minimum_evidence.is_empty()
        || requirement.provider_eligibility.is_empty()
        || requirement.witness_id.is_empty()
    {
        return Err(format!(
            "requirement {:?} has an incomplete provider/evidence contract",
            requirement.id
        ));
    }

    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "one audit gate validates the complete finite obligation catalog before projection"
)]
fn validate_catalog() -> Result<(), String> {
    if MANIFEST.len() != OPENING_BASELINE.manifest_features {
        return Err(format!(
            "post-0.214 manifest drift: expected {}, observed {}",
            OPENING_BASELINE.manifest_features,
            MANIFEST.len()
        ));
    }
    if PROVIDERS.len() != OPENING_BASELINE.deterministic_providers {
        return Err(format!(
            "post-0.214 provider drift: expected {}, observed {}",
            OPENING_BASELINE.deterministic_providers,
            PROVIDERS.len()
        ));
    }
    let providers = provider_specs()?;
    let manifest_ids = MANIFEST.iter().map(|cell| cell.id).collect::<BTreeSet<_>>();
    let mut interaction_ids = BTreeSet::new();
    let mut requirement_ids = BTreeSet::new();
    let mut tuple_identities = BTreeSet::new();
    let mut referenced_features = BTreeSet::new();
    let mut used_groups = BTreeSet::new();

    for interaction in INTERACTIONS {
        if !interaction_ids.insert(interaction.id) {
            return Err(format!(
                "duplicate interaction obligation {:?}",
                interaction.id
            ));
        }
        validate_group_tuple(interaction)?;
        used_groups.insert(interaction.group);

        let tuple_identity = (
            interaction.group,
            interaction
                .tuple
                .iter()
                .map(|assignment| (assignment.axis, assignment.value))
                .collect::<Vec<_>>(),
        );
        if !tuple_identities.insert(tuple_identity) {
            return Err(format!(
                "interaction group {:?} contains a duplicate type-valid tuple",
                interaction.group
            ));
        }

        for feature_id in interaction.contract_features {
            if !manifest_ids.contains(feature_id) {
                return Err(format!(
                    "interaction {:?} names absent manifest feature {feature_id:?}",
                    interaction.id
                ));
            }
            referenced_features.insert(*feature_id);
        }
        if interaction.reason.trim().is_empty()
            || interaction.minimum_evidence.is_empty()
            || interaction.provider_eligibility.is_empty()
        {
            return Err(format!(
                "interaction {:?} has an incomplete disposition",
                interaction.id
            ));
        }
        match (interaction.disposition, interaction.requirement) {
            (Disposition::Inapplicable, None) => {}
            (
                Disposition::GeneratedRequired | Disposition::DeterministicRequired,
                Some(requirement),
            ) => {
                if !requirement_ids.insert(requirement.id) {
                    return Err(format!(
                        "duplicate required structural obligation {:?}",
                        requirement.id
                    ));
                }
                if requirement.minimum_evidence != interaction.minimum_evidence
                    || requirement.provider_eligibility != interaction.provider_eligibility
                {
                    return Err(format!(
                        "interaction {:?} and requirement {:?} drifted on evidence authority",
                        interaction.id, requirement.id
                    ));
                }
                validate_provider_target(interaction, &requirement, &providers)?;
            }
            _ => {
                return Err(format!(
                    "interaction {:?} disposition and structural requirement disagree",
                    interaction.id
                ));
            }
        }
    }

    let declared_groups = GROUPS.iter().map(|group| group.id).collect::<BTreeSet<_>>();
    if used_groups != declared_groups {
        return Err("every frozen interaction group must contain a reviewed tuple".to_string());
    }

    for feature_id in &manifest_ids {
        match (
            referenced_features.contains(feature_id),
            no_interaction_reason(feature_id),
        ) {
            (true, None) | (false, Some(_)) => {}
            (true, Some(_)) => {
                return Err(format!(
                    "manifest feature {feature_id:?} both participates and declares no interaction"
                ));
            }
            (false, None) => {
                return Err(format!(
                    "manifest feature {feature_id:?} has no total interaction-participation decision"
                ));
            }
        }
    }

    let structural = current_structural_identities();
    if structural.len() != OPENING_INVENTORY.current_structural_identities {
        return Err(format!(
            "current structural identity count drifted: expected {}, observed {}",
            OPENING_INVENTORY.current_structural_identities,
            structural.len()
        ));
    }
    let structural_ids = structural
        .iter()
        .map(|identity| identity.id.as_str())
        .collect::<BTreeSet<_>>();
    if structural_ids.len() != structural.len() {
        return Err("current structural identity map contains duplicates".to_string());
    }

    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "one deterministic projection keeps catalog ordering and derivation visible together"
)]
fn catalog_body() -> CatalogBody {
    let mut interactions = INTERACTIONS.iter().collect::<Vec<_>>();
    interactions.sort_by_key(|interaction| interaction.id);

    let mut groups = GROUPS
        .iter()
        .map(|group| {
            let mut obligation_ids = interactions
                .iter()
                .filter(|interaction| interaction.group == group.id)
                .map(|interaction| interaction.id)
                .collect::<Vec<_>>();
            obligation_ids.sort_unstable();
            GroupProjection {
                id: group.id,
                axes: group.axes,
                interaction_obligations: obligation_ids,
            }
        })
        .collect::<Vec<_>>();
    groups.sort_by_key(|group| group.id);

    let mut manifest = MANIFEST.iter().collect::<Vec<_>>();
    manifest.sort_by_key(|cell| cell.id);
    let manifest_participation = manifest
        .into_iter()
        .map(|cell| {
            let mut obligation_ids = interactions
                .iter()
                .filter(|interaction| interaction.contract_features.contains(&cell.id))
                .map(|interaction| interaction.id)
                .collect::<Vec<_>>();
            obligation_ids.sort_unstable();
            ManifestParticipationProjection {
                feature_id: cell.id,
                no_structural_interaction_required: obligation_ids.is_empty().then(|| {
                    no_interaction_reason(cell.id)
                        .expect("validated non-participant must have an exact reason")
                }),
                interaction_obligations: obligation_ids,
            }
        })
        .collect();

    let interaction_obligations = interactions
        .iter()
        .map(|interaction| InteractionProjection {
            id: interaction.id,
            group: interaction.group,
            tuple: interaction.tuple,
            contract_features: interaction.contract_features,
            schema_facts: interaction.schema_facts,
            fixture_properties: interaction.fixture_properties,
            minimum_evidence: interaction.minimum_evidence,
            provider_eligibility: interaction.provider_eligibility,
            route_facts: interaction.route_facts,
            disposition: interaction.disposition,
            reason: interaction.reason,
            required_structural_obligations: interaction
                .requirement
                .map(|requirement| vec![requirement.id])
                .unwrap_or_default(),
        })
        .collect();

    let mut requirements = interactions
        .iter()
        .filter_map(|interaction| {
            interaction
                .requirement
                .map(|requirement| (interaction.id, requirement))
        })
        .collect::<Vec<_>>();
    requirements.sort_by_key(|(_, requirement)| requirement.id);
    let required_structural_obligations = requirements
        .into_iter()
        .map(|(interaction_id, requirement)| RequirementProjection {
            id: requirement.id,
            interaction_obligations: vec![interaction_id],
            expected_structural_signature: requirement.signature,
            fixture_properties: requirement.fixture_properties,
            minimum_evidence: requirement.minimum_evidence,
            provider_eligibility: requirement.provider_eligibility,
            provider_id: requirement.provider.id(),
            provider_state: requirement.provider.state(),
            route_facts: requirement.route_facts,
            witness_id: requirement.witness_id,
            opening_evidence_state: match requirement.provider {
                ProviderTarget::Existing(_) => {
                    "existing_evidence_has_no_observed_structural_signature_receipt"
                }
                ProviderTarget::Planned(_) => "missing_scheduled_witness_and_receipt",
            },
        })
        .collect();

    CatalogBody {
        design_line: "0.215",
        opening_baseline: OPENING_BASELINE,
        opening_inventory: OPENING_INVENTORY,
        current_structural_identities: current_structural_identities(),
        interaction_groups: groups,
        manifest_participation,
        interaction_obligations,
        required_structural_obligations,
        gap_ledger: GAP_LEDGER,
        owner_map: OWNER_MAP,
    }
}

fn catalog_artifact() -> Result<(String, String), String> {
    validate_catalog()?;
    let catalog = catalog_body();
    let catalog_bytes = serde_json::to_vec(&catalog)
        .map_err(|error| format!("coverage obligation catalog serialization failed: {error}"))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(CATALOG_HASH_DOMAIN);
    hasher.update(&catalog_bytes);
    let catalog_hash = hasher.finalize().to_hex().to_string();
    let artifact = CatalogArtifact {
        format_version: CATALOG_FORMAT_VERSION,
        catalog_hash: catalog_hash.clone(),
        catalog,
    };
    let mut rendered = serde_json::to_string_pretty(&artifact)
        .map_err(|error| format!("coverage obligation artifact rendering failed: {error}"))?;
    rendered.push('\n');

    Ok((rendered, catalog_hash))
}

#[test]
fn sql_coverage_obligation_catalog_is_complete_and_matches_checked_in_projection() {
    let (rendered, catalog_hash) =
        catalog_artifact().expect("0.215 coverage obligation catalog should be valid");
    assert_eq!(
        CATALOG_ARTIFACT, rendered,
        "checked-in 0.215 coverage obligations drifted; reviewed code authority hash is {catalog_hash}"
    );
}

#[test]
fn generated_select_schedule_closes_the_frozen_requirements_exactly() {
    let witnesses =
        scheduled_select_witnesses().expect("frozen generated SELECT schedule should decode");
    let observed = witnesses
        .iter()
        .map(|witness| {
            (
                witness.witness_id(),
                (
                    witness.requirement_id(),
                    witness.provider_id(),
                    serde_json::to_value(witness.signature())
                        .expect("observed structural signature should serialize"),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let expected = INTERACTIONS
        .iter()
        .filter_map(|interaction| interaction.requirement)
        .filter(|requirement| {
            matches!(
                requirement.provider,
                ProviderTarget::Planned(provider)
                    if provider.starts_with("generated.select.")
            )
        })
        .map(|requirement| {
            (
                requirement.witness_id,
                (
                    requirement.id,
                    requirement.provider.id(),
                    serde_json::to_value(requirement.signature)
                        .expect("expected structural signature should serialize"),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(observed, expected);
    assert_eq!(observed.len(), 17);
    assert_eq!(
        structural_obligation_catalog_hash()
            .expect("generator should expose the frozen obligation catalog hash"),
        "b4f839c170e09a2691dd8cbc6a5b14ad8c3794af0d2ea796df47bec4968e4b9f",
    );

    let manifest_features = MANIFEST.iter().map(|cell| cell.id).collect::<BTreeSet<_>>();
    for witness in witnesses {
        let generated = generate_scheduled_select_case(
            &witness,
            TIER_A_ROOT_SEEDS[0],
            0,
            TIER_A_SELECT_BUDGETS,
        )
        .expect("frozen generated SELECT witness should construct");
        let declaration =
            generated_select_tier_c_declaration(generated.identity().id(), &generated)
                .expect("typed generated declaration should derive");
        assert!(
            declaration
                .contract_features()
                .iter()
                .all(|feature| manifest_features.contains(feature.as_str())),
            "witness {:?} derived an unknown contract feature",
            witness.witness_id(),
        );
    }
}

#[test]
fn generated_mutation_schedule_closes_the_frozen_matrix_exactly() {
    let witnesses =
        scheduled_mutation_witnesses().expect("frozen generated mutation schedule should decode");
    let observed = witnesses
        .iter()
        .map(|witness| {
            (
                witness.witness_id(),
                (
                    witness.requirement_id(),
                    witness.provider_id(),
                    serde_json::to_value(witness.signature())
                        .expect("observed mutation signature should serialize"),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let expected = INTERACTIONS
        .iter()
        .filter_map(|interaction| interaction.requirement)
        .filter(|requirement| {
            matches!(
                requirement.provider,
                ProviderTarget::Planned(provider)
                    if provider.starts_with("generated.mutation.")
            )
        })
        .map(|requirement| {
            (
                requirement.witness_id,
                (
                    requirement.id,
                    requirement.provider.id(),
                    serde_json::to_value(requirement.signature)
                        .expect("expected mutation signature should serialize"),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(observed, expected);
    assert_eq!(observed.len(), 15);

    let manifest_features = MANIFEST.iter().map(|cell| cell.id).collect::<BTreeSet<_>>();
    for witness in witnesses {
        let interaction = INTERACTIONS
            .iter()
            .find(|interaction| {
                interaction
                    .requirement
                    .is_some_and(|requirement| requirement.witness_id == witness.witness_id())
            })
            .expect("scheduled mutation witness should retain one interaction tuple");
        let generated = generate_scheduled_mutation_sequence(
            &witness,
            TIER_A_ROOT_SEEDS[0],
            0,
            TIER_A_MUTATION_BUDGETS,
        )
        .expect("frozen generated mutation witness should construct");
        assert_eq!(
            generated.snapshot().profile().id(),
            interaction_axis(interaction, "profile"),
        );
        assert_eq!(
            generated.intent_class().id(),
            interaction_axis(interaction, "intent"),
        );
        assert_eq!(
            generated.ingress().id(),
            interaction_axis(interaction, "ingress"),
        );
        let declaration =
            generated_mutation_tier_c_declaration(generated.identity().id(), &generated)
                .expect("typed generated mutation declaration should derive");
        assert!(
            declaration
                .contract_features()
                .iter()
                .all(|feature| manifest_features.contains(feature.as_str())),
            "mutation witness {:?} derived an unknown contract feature",
            witness.witness_id(),
        );
    }
}

fn interaction_axis(interaction: &InteractionObligation, axis: &str) -> &'static str {
    interaction
        .tuple
        .iter()
        .find(|assignment| assignment.axis == axis)
        .map(|assignment| assignment.value)
        .expect("generated interaction should assign every frozen mutation axis")
}

#[test]
#[ignore = "writes the reviewed deterministic catalog projection; run explicitly when freezing a revised catalog"]
fn write_sql_coverage_obligation_catalog_projection() {
    let (rendered, _) =
        catalog_artifact().expect("0.215 coverage obligation catalog should be valid");
    let path = super::repository_root()
        .join("docs/design/0.215-sql-structural-coverage-and-range-remediation")
        .join("0.215-coverage-obligations.json");
    fs::write(&path, rendered)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", path.display()));
}
