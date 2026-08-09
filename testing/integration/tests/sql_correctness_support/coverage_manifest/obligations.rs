//! Module: sql_correctness_support::coverage_manifest::obligations
//! Responsibility: current SQL interaction obligations and their deterministic projection.
//! Does not own: product behavior, generated observations, or scheduled evidence receipts.
//! Boundary: extends the SQL coverage manifest with one finite pre-generation obligation catalog.

use super::{MANIFEST, PROVIDERS, ProviderSpec, provider_specs};

use icydb_testing_sql_generator::{
    ExecutionAccess, ExecutionCovering, RequiredExecutionFacts, StructuralSignature,
    TIER_A_MUTATION_BUDGETS, TIER_A_ROOT_SEEDS, TIER_A_SELECT_BUDGETS,
    generate_scheduled_mutation_sequence, generate_scheduled_select_case,
    generated_mutation_tier_c_declaration, generated_select_tier_c_declaration,
    scheduled_mutation_witnesses, scheduled_select_witnesses,
    structural_signature_for_scheduled_mutation_witness,
    structural_signature_for_scheduled_select_witness, structural_witness_schedule_hash,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

const MAX_CODE_OWNED_CATALOG_BYTES: usize = 262_144;

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

const fn required_execution_facts(
    access: ExecutionAccess,
    covering: ExecutionCovering,
) -> RequiredExecutionFacts {
    RequiredExecutionFacts::new(access, covering)
}

///
/// ProviderTarget
///
/// Exact deterministic or generated provider that owns one required structural witness.
///

#[derive(Clone, Copy, Debug)]
enum ProviderTarget {
    Existing(&'static str),
    Generated(&'static str),
}

impl ProviderTarget {
    const fn id(self) -> &'static str {
        match self {
            Self::Existing(id) | Self::Generated(id) => id,
        }
    }

    const fn state(self) -> &'static str {
        match self {
            Self::Existing(_) => "existing_deterministic_provider",
            Self::Generated(_) => "generated_provider",
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
    required_execution_facts: RequiredExecutionFacts,
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
        $execution:expr,
        fixtures = [$($fixture:literal),* $(,)?],
        evidence = $evidence:literal,
        eligibility = $eligibility:literal,
        provider = $provider:expr,
        routes = [$($route:literal),* $(,)?],
        witness = $witness:literal
    ) => {
        StructuralRequirement {
            id: $id,
            required_execution_facts: $execution,
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
                members: &["filter", "multiple_projection"],
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
                    "unknown_alias_order",
                ],
            },
        ],
    },
];

const GENERATED_PROVIDERS: &[&str] = &[
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
            required_execution_facts(
                ExecutionAccess::PrimaryExact,
                ExecutionCovering::NonCovering
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
        reason = "The same derived declaration must run through SQL and fluent adapters.",
        requirement = Some(requirement!(
            "required.cache.cold_sql_fluent",
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["identical cold fixture"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.reference_scalar"),
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
            required_execution_facts(
                ExecutionAccess::PrimaryExact,
                ExecutionCovering::NonCovering
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
            required_execution_facts(ExecutionAccess::SecondaryRange, ExecutionCovering::Hybrid),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
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
        "interaction.global.empty_filter",
        group = "global_aggregation",
        tuple = axes!("input" = "empty", "modifiers" = "filter"),
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
        reason = "The empty aggregate-filter case requires an explicit witness.",
        requirement = Some(requirement!(
            "required.global.empty_filter",
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["empty input"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.reference_scalar"),
            routes = ["global aggregate"],
            witness = "tier_c.global.empty_filter"
        ))
    ),
    interaction!(
        "interaction.global.nonempty_filter",
        group = "global_aggregation",
        tuple = axes!("input" = "nonempty", "modifiers" = "filter"),
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
        reason = "Aggregate FILTER over non-empty input requires an explicit witness.",
        requirement = Some(requirement!(
            "required.global.nonempty_filter",
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["duplicate aggregate inputs"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.reference_scalar"),
            routes = ["global aggregate"],
            witness = "tier_c.global.nonempty_filter"
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
        reason = "Multiple aggregate outputs and global HAVING require a combined witness.",
        requirement = Some(requirement!(
            "required.global.nonempty_multiple_projection",
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["retained and rejected HAVING cases"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.reference_scalar"),
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
        reason = "The complete hash-grouped composition requires an explicit witness.",
        requirement = Some(requirement!(
            "required.grouped.hash_bounded",
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["multiple groups", "HAVING retained and rejected groups"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.reference_scalar"),
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
            required_execution_facts(
                ExecutionAccess::SecondaryRange,
                ExecutionCovering::NonCovering,
            ),
            fixtures = [
                "multiple indexed groups",
                "HAVING retained and rejected groups"
            ],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.indexed_nullable_reference"),
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
            required_execution_facts(ExecutionAccess::SecondaryRange, ExecutionCovering::Pure),
            fixtures = ["more than one grouped page"],
            evidence = "contract_assertion",
            eligibility = "execution_mode_equivalent",
            provider = ProviderTarget::Existing("core.query.grouped_cursor"),
            routes = ["ordered grouped continuation"],
            witness = "existing.core.query.grouped_cursor"
        ))
    ),
    interaction!(
        "interaction.indexed.composite_prefix_non_covering",
        group = "indexed_scalar_execution",
        tuple = axes!(
            "access" = "composite_prefix",
            "covering" = "non_covering",
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
            "row-backed projection",
            "compatible suffix order"
        ],
        disposition = GeneratedRequired,
        reason =
            "The indexed nullable profile must make composite-prefix route evidence structural.",
        requirement = Some(requirement!(
            "required.indexed.composite_prefix_non_covering",
            required_execution_facts(
                ExecutionAccess::CompositePrefix,
                ExecutionCovering::NonCovering,
            ),
            fixtures = ["duplicate-rich index prefixes"],
            evidence = "contract_assertion",
            eligibility = "icydb_contract_only",
            provider = ProviderTarget::Generated("generated.select.indexed_nullable_reference"),
            routes = ["composite prefix", "row-backed projection"],
            witness = "tier_c.indexed.composite_prefix_non_covering"
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
            required_execution_facts(
                ExecutionAccess::ExpressionRange,
                ExecutionCovering::NonCovering
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
            required_execution_facts(
                ExecutionAccess::PrimaryExact,
                ExecutionCovering::NonCovering
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
            required_execution_facts(
                ExecutionAccess::SecondaryRange,
                ExecutionCovering::NonCovering
            ),
            fixtures = ["nonempty bounded range", "order ties"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.indexed_nullable_reference"),
            routes = ["one bounded secondary range", "materialized order"],
            witness = "tier_c.indexed.secondary_range_non_covering_incompatible"
        ))
    ),
    interaction!(
        "interaction.indexed.secondary_range_direct_compatible",
        group = "indexed_scalar_execution",
        tuple = axes!(
            "access" = "secondary_range",
            "covering" = "non_covering",
            "order" = "compatible",
        ),
        features = [
            "ordering.null_values",
            "predicate.range",
            "projection.scalar",
            "select.scalar_composition",
        ],
        schema = ["single-field nullable secondary index with direct projection"],
        fixtures = ["stored nulls", "duplicate-rich indexed values"],
        evidence = "reference_oracle",
        eligibility = "sqlite_reference",
        routes = [
            "one bounded secondary range",
            "compatible index order",
            "direct row-backed projection"
        ],
        disposition = GeneratedRequired,
        reason =
            "The indexed nullable profile is required to exercise route and null roles together.",
        requirement = Some(requirement!(
            "required.indexed.secondary_range_direct_compatible",
            required_execution_facts(
                ExecutionAccess::SecondaryRange,
                ExecutionCovering::NonCovering,
            ),
            fixtures = ["stored nulls", "duplicate-rich indexed values"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.indexed_nullable_reference"),
            routes = [
                "one bounded secondary range",
                "direct row-backed projection"
            ],
            witness = "tier_c.indexed.secondary_range_direct_compatible"
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
        reason = "Computed-null aggregate input requires an explicit generated witness.",
        requirement = Some(requirement!(
            "required.null.computed_aggregate",
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["computed null and non-null inputs"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.reference_scalar"),
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
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["duplicate computed nulls"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.reference_scalar"),
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
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["stored nulls", "membership nulls and duplicates"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.indexed_nullable_reference"),
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
        reason = "Null ordering requires an explicit accepted nullable-profile witness.",
        requirement = Some(requirement!(
            "required.null.stored_ordering",
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["stored nulls", "order ties"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.indexed_nullable_reference"),
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
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["computed nulls", "order ties"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.reference_scalar"),
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
            required_execution_facts(ExecutionAccess::FullScan, ExecutionCovering::NonCovering),
            fixtures = ["order ties", "more rows than window"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.reference_scalar"),
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
        reason = "The indexed nullable profile must cover the complete scalar composition.",
        requirement = Some(requirement!(
            "required.scalar.indexed_computed_distinct_window",
            required_execution_facts(
                ExecutionAccess::SecondaryRange,
                ExecutionCovering::NonCovering
            ),
            fixtures = ["duplicate computed values", "stored nulls", "order ties"],
            evidence = "reference_oracle",
            eligibility = "sqlite_reference",
            provider = ProviderTarget::Generated("generated.select.indexed_nullable_reference"),
            routes = ["secondary range", "materialized DISTINCT order"],
            witness = "tier_c.scalar.indexed_computed_distinct_window"
        ))
    ),
    interaction!(
        "interaction.scalar.reference_unknown_alias_order",
        group = "scalar_composition",
        tuple = axes!(
            "profile" = "reference_scalar",
            "shape" = "unknown_alias_order"
        ),
        features = ["ordering.projection_alias", "projection.aliases"],
        schema = ["reference scalar fields"],
        fixtures = ["valid base declaration"],
        evidence = "boundary_assertion",
        eligibility = "rejection_invariant",
        routes = [],
        disposition = GeneratedRequired,
        reason = "Unknown-alias ordering requires one singly-invalid target with a typed cause.",
        requirement = Some(requirement!(
            "required.scalar.reference_unknown_alias_order",
            required_execution_facts(
                ExecutionAccess::NotApplicable,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["valid base declaration"],
            evidence = "boundary_assertion",
            eligibility = "rejection_invariant",
            provider = ProviderTarget::Generated("generated.select.reference_scalar"),
            routes = [],
            witness = "tier_c.scalar.reference_unknown_alias_order"
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
        reason = "The obligation-owned sequence must cover authored mutation intent.",
        requirement = Some(requirement!(
            "required.mutation.authored_insert",
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["single row", "multi row"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.authored_scalar"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["bounded source rows"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.authored_scalar"),
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
        reason = "Update and delete windows must derive from bounded typed operations.",
        requirement = Some(requirement!(
            "required.mutation.authored_windowed",
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["exact", "compound", "bounded matches"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.authored_scalar"),
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
        reason = "The accepted-default profile requires an exact authored-insert witness.",
        requirement = Some(requirement!(
            "required.mutation.default_insert_authored",
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["all fields authored"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["id and name authored only"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["explicit defaults"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["three-row mixed provenance batch"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["one matching row"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["one non-default row"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["one non-default row"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["absent primary key"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["one matching row"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["unchanged pre-state"],
            evidence = "boundary_assertion",
            eligibility = "rejection_invariant",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["unchanged pre-state"],
            evidence = "boundary_assertion",
            eligibility = "rejection_invariant",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            required_execution_facts(
                ExecutionAccess::MutationSelection,
                ExecutionCovering::NotApplicable,
            ),
            fixtures = ["duplicate primary-key batch"],
            evidence = "state_model_reference",
            eligibility = "state_model_reference",
            provider = ProviderTarget::Generated("generated.mutation.accepted_default"),
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
            "The feature retains exact deterministic evidence and adds no separately reviewed cross-feature tuple.",
        );
    }
    if NO_INTERACTION_SYNTAX_OR_ENTRY_BOUNDARY.contains(&feature_id) {
        return Some(
            "Parser, unsupported-syntax, naming, or entry-boundary behavior is closed by direct typed evidence rather than a generated structural interaction.",
        );
    }

    None
}

const EXPECTED_MANIFEST_FEATURE_COUNT: usize = 106;
const EXPECTED_DETERMINISTIC_PROVIDER_COUNT: usize = 98;

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
    expected_structural_signature: StructuralSignature,
    required_execution_facts: RequiredExecutionFacts,
    fixture_properties: &'static [&'static str],
    minimum_evidence: &'static str,
    provider_eligibility: &'static str,
    provider_id: &'static str,
    provider_state: &'static str,
    route_facts: &'static [&'static str],
    witness_id: &'static str,
}

#[derive(Debug, Serialize)]
struct CatalogBody {
    interaction_groups: Vec<GroupProjection>,
    manifest_participation: Vec<ManifestParticipationProjection>,
    interaction_obligations: Vec<InteractionProjection>,
    required_structural_obligations: Vec<RequirementProjection>,
}

#[derive(Clone, Copy)]
struct DeterministicSignatureSpec {
    profile: &'static str,
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
}

fn expected_structural_signature(
    requirement: &StructuralRequirement,
) -> Result<StructuralSignature, String> {
    match requirement.provider {
        ProviderTarget::Generated(provider) if provider.starts_with("generated.select.") => {
            structural_signature_for_scheduled_select_witness(requirement.witness_id)
                .map_err(|error| error.to_string())
        }
        ProviderTarget::Generated(provider) if provider.starts_with("generated.mutation.") => {
            structural_signature_for_scheduled_mutation_witness(requirement.witness_id)
                .map_err(|error| error.to_string())
        }
        ProviderTarget::Existing(_) => {
            deterministic_structural_signature(requirement.id, requirement.witness_id)
        }
        ProviderTarget::Generated(provider) => Err(format!(
            "requirement {:?} has no structural signature derivation for provider {provider:?}",
            requirement.id,
        )),
    }
}

fn deterministic_structural_signature(
    requirement_id: &str,
    witness_id: &str,
) -> Result<StructuralSignature, String> {
    let spec = deterministic_signature_spec(requirement_id)?;
    let canonical_structure = serde_json::to_string(&BTreeMap::from([
        ("field_roles", spec.field_roles),
        ("grouping_shape", spec.grouping_shape),
        ("having_shape", spec.having_shape),
        ("order_shape", spec.order_shape),
        ("predicate_shape", spec.predicate_shape),
        ("projection_shape", spec.projection_shape),
        ("provider_witness", witness_id),
        ("result_shape", spec.result_shape),
        ("semantic_value_class", spec.semantic_value_class),
        ("window_shape", spec.window_shape),
    ]))
    .map_err(|error| format!("deterministic structural signature failed to encode: {error}"))?;
    StructuralSignature::try_new_deterministic_requirement(
        spec.profile,
        spec.statement_family,
        canonical_structure,
    )
    .map_err(|error| error.to_string())
}

fn deterministic_signature_spec(
    requirement_id: &str,
) -> Result<DeterministicSignatureSpec, String> {
    match requirement_id {
        "required.cache.cold_compiled_direct" | "required.cache.warm_sql_fluent" => {
            Ok(DeterministicSignatureSpec {
                profile: "reference_scalar",
                statement_family: "select",
                result_shape: "scalar_rows",
                projection_shape: "plain_fields",
                predicate_shape: "strict_scalar_comparison",
                grouping_shape: "none",
                having_shape: "none",
                order_shape: "primary_key_ascending",
                window_shape: "limit",
                field_roles: "sole_primary_key|stored_scalar",
                semantic_value_class: "ordinary",
            })
        }
        "required.field_path.selectable_returning" => Ok(DeterministicSignatureSpec {
            profile: "deterministic_nested_field_path",
            statement_family: "update",
            result_shape: "returned_fields",
            projection_shape: "stored_leaf",
            predicate_shape: "primary_key_exact",
            grouping_shape: "none",
            having_shape: "none",
            order_shape: "primary_key_ascending",
            window_shape: "none",
            field_roles: "sole_primary_key|stored_leaf",
            semantic_value_class: "authored",
        }),
        "required.field_path.selectable_select" => Ok(DeterministicSignatureSpec {
            profile: "deterministic_nested_field_path",
            statement_family: "select",
            result_shape: "scalar_rows",
            projection_shape: "stored_leaf",
            predicate_shape: "stored_leaf_comparison",
            grouping_shape: "none",
            having_shape: "none",
            order_shape: "stored_leaf_then_primary_key",
            window_shape: "limit",
            field_roles: "stored_leaf|single_secondary_index",
            semantic_value_class: "ordinary",
        }),
        "required.grouped.ordered_continuation" => Ok(DeterministicSignatureSpec {
            profile: "indexed_nullable_reference",
            statement_family: "select",
            result_shape: "grouped_rows_with_cursor",
            projection_shape: "group_key_then_aggregate",
            predicate_shape: "none",
            grouping_shape: "one_indexed_group_key",
            having_shape: "none",
            order_shape: "group_key_ascending",
            window_shape: "grouped_continuation",
            field_roles: "single_secondary_index|stored_scalar",
            semantic_value_class: "ordinary",
        }),
        "required.indexed.expression_range_non_covering" => Ok(DeterministicSignatureSpec {
            profile: "deterministic_expression_index",
            statement_family: "select",
            result_shape: "scalar_rows",
            projection_shape: "plain_fields",
            predicate_shape: "casefold_prefix",
            grouping_shape: "none",
            having_shape: "none",
            order_shape: "none",
            window_shape: "limit",
            field_roles: "accepted_expression_index|row_backed_projection",
            semantic_value_class: "ordinary_prefix",
        }),
        "required.indexed.primary_exact_non_covering" => Ok(DeterministicSignatureSpec {
            profile: "reference_scalar",
            statement_family: "select",
            result_shape: "scalar_rows",
            projection_shape: "plain_fields",
            predicate_shape: "primary_key_exact",
            grouping_shape: "none",
            having_shape: "none",
            order_shape: "none",
            window_shape: "none",
            field_roles: "sole_primary_key|row_backed_projection",
            semantic_value_class: "point",
        }),
        _ => Err(format!(
            "deterministic requirement {requirement_id:?} has no canonical structural construction",
        )),
    }
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
        ProviderTarget::Generated(provider_id) => {
            if !GENERATED_PROVIDERS.contains(&provider_id) {
                return Err(format!(
                    "requirement {:?} names undeclared generated provider {provider_id:?}",
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
    if MANIFEST.len() != EXPECTED_MANIFEST_FEATURE_COUNT {
        return Err(format!(
            "manifest drift: expected {}, observed {}",
            EXPECTED_MANIFEST_FEATURE_COUNT,
            MANIFEST.len()
        ));
    }
    if PROVIDERS.len() != EXPECTED_DETERMINISTIC_PROVIDER_COUNT {
        return Err(format!(
            "current provider drift: expected {}, observed {}",
            EXPECTED_DETERMINISTIC_PROVIDER_COUNT,
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

    Ok(())
}

fn catalog_body() -> Result<CatalogBody, String> {
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
        .map(|(interaction_id, requirement)| {
            Ok(RequirementProjection {
                id: requirement.id,
                interaction_obligations: vec![interaction_id],
                expected_structural_signature: expected_structural_signature(&requirement)?,
                required_execution_facts: requirement.required_execution_facts,
                fixture_properties: requirement.fixture_properties,
                minimum_evidence: requirement.minimum_evidence,
                provider_eligibility: requirement.provider_eligibility,
                provider_id: requirement.provider.id(),
                provider_state: requirement.provider.state(),
                route_facts: requirement.route_facts,
                witness_id: requirement.witness_id,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(CatalogBody {
        interaction_groups: groups,
        manifest_participation,
        interaction_obligations,
        required_structural_obligations,
    })
}

#[test]
fn sql_coverage_obligation_catalog_is_complete() {
    validate_catalog().expect("SQL coverage obligation catalog should be valid");
    let catalog = catalog_body().expect("code-owned SQL obligation catalog should project");
    let bytes = serde_json::to_vec(&catalog)
        .expect("code-owned SQL obligation catalog should serialize deterministically");
    assert!(
        bytes.len() <= MAX_CODE_OWNED_CATALOG_BYTES,
        "code-owned SQL obligation catalog exceeded its bounded inspection projection",
    );
}

#[test]
fn generated_select_schedule_closes_the_frozen_requirements_exactly() {
    let witnesses =
        scheduled_select_witnesses().expect("code-owned generated SELECT schedule should derive");
    let observed = witnesses
        .iter()
        .map(|witness| {
            (
                witness.witness_id(),
                (
                    witness.requirement_id(),
                    witness.provider_id(),
                    witness.signature().clone(),
                    witness.required_execution_facts(),
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
                ProviderTarget::Generated(provider)
                    if provider.starts_with("generated.select.")
            )
        })
        .map(|requirement| {
            (
                requirement.witness_id,
                (
                    requirement.id,
                    requirement.provider.id(),
                    expected_structural_signature(&requirement)
                        .expect("code-owned SELECT signature should derive"),
                    requirement.required_execution_facts,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(observed, expected);
    assert_eq!(observed.len(), 17);
    assert_eq!(
        structural_witness_schedule_hash()
            .expect("generator should expose the code-owned witness schedule hash"),
        "787d66c237f92a2269ec0053fafee092dfe398952ac3fe1cbc726c8774beb246",
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
    let witnesses = scheduled_mutation_witnesses()
        .expect("code-owned generated mutation schedule should derive");
    let observed = witnesses
        .iter()
        .map(|witness| {
            (
                witness.witness_id(),
                (
                    witness.requirement_id(),
                    witness.provider_id(),
                    witness.signature().clone(),
                    witness.required_execution_facts(),
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
                ProviderTarget::Generated(provider)
                    if provider.starts_with("generated.mutation.")
            )
        })
        .map(|requirement| {
            (
                requirement.witness_id,
                (
                    requirement.id,
                    requirement.provider.id(),
                    expected_structural_signature(&requirement)
                        .expect("code-owned mutation signature should derive"),
                    requirement.required_execution_facts,
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
