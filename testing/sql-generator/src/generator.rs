//! Module: sql_generator::generator
//! Responsibility: obligation-driven SELECT composition, fixtures, feature facts, and rendering.
//! Does not own: parser acceptance, reference execution, or mismatch shrinking policy.
//! Boundary: derives one independent stream per stable witness/repetition and validates before emission.

use crate::{
    ScheduledSelectWitness, StructuralSignature,
    error::{SqlGeneratorError, SqlGeneratorErrorKind},
    fixture::{
        GeneratedFieldValue, GeneratedFixture, GeneratedFixtureRow, GeneratedValue,
        REVIEWED_INTEGER_MAX_BOUNDARY, REVIEWED_INTEGER_MIN_BOUNDARY,
    },
    model::{
        GeneratedSelectCase, GeneratedSelectIdentity, SelectArithmeticOperator, SelectBudgets,
        SelectComparisonOperator, SelectExpectedOutcome, SelectExpression, SelectFeature,
        SelectField, SelectFieldKind, SelectFunction, SelectIndex, SelectOrderDirection,
        SelectOrderTarget, SelectOrderTerm, SelectPredicate, SelectProjection, SelectProvider,
        SelectQuery, SelectSchemaProfile, SelectSnapshot, SelectViolation,
    },
    rng::{SELECT_GENERATOR_VERSION, SplitMix64, derive_select_witness_sub_seed},
    scheduled_select_witnesses,
};
use std::{collections::BTreeSet, fmt::Write as _};

const INTEGER_FIXTURE_VALUES: &[i64] = &[
    REVIEWED_INTEGER_MIN_BOUNDARY,
    -1,
    0,
    1,
    24,
    31,
    43,
    REVIEWED_INTEGER_MAX_BOUNDARY,
];

/// Required pull-request root seeds for deterministic value repetition.
pub const TIER_A_ROOT_SEEDS: &[u64] = &[0x1cdb_0204_0000_0001, 0x1cdb_0204_0000_0002];

/// Required pull-request repetitions per reviewed SELECT witness and root.
pub const TIER_A_SELECT_REPETITIONS: u64 = 1;

/// Required pull-request repetitions per typed invalid proposal and root.
pub const TIER_A_INVALID_REPETITIONS: u64 = 1;

/// Required scheduled root seeds from the 0.204 design.
pub const TIER_C_ROOT_SEEDS: &[u64] = &[
    0x1cdb_0204_0000_0011,
    0x1cdb_0204_0000_0012,
    0x1cdb_0204_0000_0013,
    0x1cdb_0204_0000_0014,
    0x1cdb_0204_0000_0015,
    0x1cdb_0204_0000_0016,
    0x1cdb_0204_0000_0017,
    0x1cdb_0204_0000_0018,
];

/// Required scheduled repetitions per reviewed SELECT witness and root.
pub const TIER_C_SELECT_REPETITIONS: u64 = 2;

/// Required closeout repetitions per typed invalid proposal and root.
pub const TIER_C_INVALID_REPETITIONS: u64 = 2;

/// Generate one reviewed current-contract SELECT witness.
///
/// # Errors
///
/// Returns a typed generator error when the witness is stale, its accepted
/// profile or typed composition is unsupported, or any bounded fact disagrees.
pub fn generate_scheduled_select_case(
    witness: &ScheduledSelectWitness,
    root_seed: u64,
    repetition: u64,
    budgets: SelectBudgets,
) -> Result<GeneratedSelectCase, SqlGeneratorError> {
    let recipe = SelectRecipe::from_witness_id(witness.witness_id())?;
    let profile = recipe.profile();
    let structural_signature = recipe.structural_signature();
    if witness.signature() != &structural_signature {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "scheduled SELECT witness disagrees with its typed structural recipe",
        ));
    }
    let snapshot = select_snapshot(profile)?;
    let sub_seed = derive_select_witness_sub_seed(
        SELECT_GENERATOR_VERSION,
        root_seed,
        witness.witness_id(),
        repetition,
    )?;
    let mut rng = SplitMix64::new(sub_seed);
    let fixture_recipe = FixtureRecipe::from_signature(&structural_signature, budgets)?;
    let fixture = generate_fixture(&snapshot, repetition, budgets, fixture_recipe, &mut rng)?;
    let query = query_for_recipe(&snapshot, recipe, budgets, &mut rng)?;
    let violation = recipe.violation();
    let rendered_sql = render_generated_select_case(&snapshot, &query, violation, budgets)?;
    let features = collect_select_features(&query);
    let identity = generated_identity(
        &snapshot,
        witness.witness_id(),
        root_seed,
        sub_seed,
        repetition,
        if violation.is_some() {
            SelectProvider::RejectionInvariant
        } else {
            SelectProvider::SqliteReference
        },
    );
    let expected = violation.map_or(SelectExpectedOutcome::Accepted, |violation| {
        SelectExpectedOutcome::Rejected(violation.expected_rejection())
    });
    let provider = if violation.is_some() {
        SelectProvider::RejectionInvariant
    } else {
        SelectProvider::SqliteReference
    };
    let generated = GeneratedSelectCase::new(
        identity,
        structural_signature,
        violation,
        snapshot,
        fixture,
        query,
        rendered_sql,
        expected,
        provider,
        features,
        budgets,
    );
    generated.validate()?;

    Ok(generated)
}

/// Generate one valid base query with exactly one classified invalid mutation.
///
/// # Errors
///
/// Returns a typed generator error when snapshot facts, deterministic choices,
/// fixture values, invalid rendering, or budgets are inconsistent.
pub fn generate_invalid_select_case(
    profile: SelectSchemaProfile,
    root_seed: u64,
    violation: SelectViolation,
    repetition: u64,
    budgets: SelectBudgets,
) -> Result<GeneratedSelectCase, SqlGeneratorError> {
    let snapshot = select_snapshot(profile)?;
    let sub_seed = derive_select_witness_sub_seed(
        SELECT_GENERATOR_VERSION,
        root_seed,
        violation.id(),
        repetition,
    )?;
    let mut rng = SplitMix64::new(sub_seed);
    let fixture = generate_fixture(
        &snapshot,
        repetition,
        budgets,
        FixtureRecipe::valid_base(budgets)?,
        &mut rng,
    )?;
    let query = invalid_base_query(&snapshot, repetition, &mut rng)?;
    let rendered_sql = render_generated_select_case(&snapshot, &query, Some(violation), budgets)?;
    let features = collect_select_features(&query);
    let identity = generated_identity(
        &snapshot,
        violation.id(),
        root_seed,
        sub_seed,
        repetition,
        SelectProvider::RejectionInvariant,
    );
    let generated = GeneratedSelectCase::new(
        identity,
        StructuralSignature::invalid_select(profile.id(), violation.code()),
        Some(violation),
        snapshot,
        fixture,
        query,
        rendered_sql,
        SelectExpectedOutcome::Rejected(violation.expected_rejection()),
        SelectProvider::RejectionInvariant,
        features,
        budgets,
    );
    generated.validate()?;

    Ok(generated)
}

#[expect(
    clippy::too_many_lines,
    reason = "one validation boundary cross-checks the complete generated-case authority"
)]
pub(crate) fn validate_generated_select_case(
    generated: &GeneratedSelectCase,
) -> Result<(), SqlGeneratorError> {
    if generated.identity().generator_version() != SELECT_GENERATOR_VERSION {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            format!(
                "generated case uses version {}, expected {SELECT_GENERATOR_VERSION}",
                generated.identity().generator_version()
            ),
        ));
    }
    generated.structural_signature().validate()?;
    if generated.structural_signature().schema_profile() != generated.snapshot().fixture_family()
        || generated.structural_signature().is_singly_invalid() != generated.violation().is_some()
        || generated.violation().is_some_and(|violation| {
            generated.structural_signature().expected_violation() != violation.code()
        })
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated case structural signature disagrees with its accepted profile or violation",
        ));
    }
    let witness_id = generated.identity().witness_id();
    if generated
        .violation()
        .is_some_and(|violation| witness_id != violation.id())
        && !generated.structural_signature().is_singly_invalid()
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated case witness identity disagrees with its typed violation",
        ));
    }
    let derived = derive_select_witness_sub_seed(
        SELECT_GENERATOR_VERSION,
        generated.identity().root_seed(),
        witness_id,
        generated.identity().repetition(),
    )?;
    if generated.identity().sub_seed() != derived {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated case sub-seed does not match its BLAKE3 identity",
        ));
    }
    let expected_provider = if generated.violation().is_some() {
        SelectProvider::RejectionInvariant
    } else {
        SelectProvider::SqliteReference
    };
    if generated.provider() != expected_provider {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated case provider disagrees with its validity class",
        ));
    }
    let expected_outcome = generated
        .violation()
        .map_or(SelectExpectedOutcome::Accepted, |violation| {
            SelectExpectedOutcome::Rejected(violation.expected_rejection())
        });
    if generated.expected() != expected_outcome {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated case expected outcome disagrees with its classified violation",
        ));
    }
    generated
        .fixture()
        .validate(generated.snapshot(), generated.budgets().max_fixture_rows())?;
    validate_fixture_class(
        generated.structural_signature(),
        generated.snapshot(),
        generated.fixture(),
        generated.query(),
    )?;
    generated
        .query()
        .validate(generated.snapshot(), generated.budgets())?;
    validate_structural_query(generated)?;
    validate_witness_construction(generated)?;
    let rendered = render_generated_select_case(
        generated.snapshot(),
        generated.query(),
        generated.violation(),
        generated.budgets(),
    )?;
    if rendered != generated.rendered_sql() {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::Rendering,
            "generated case SQL does not match current-contract rendering",
        ));
    }
    if collect_select_features(generated.query()) != *generated.features() {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated case feature facts do not match its typed AST",
        ));
    }
    let expected_identity = generated_identity(
        generated.snapshot(),
        witness_id,
        generated.identity().root_seed(),
        derived,
        generated.identity().repetition(),
        expected_provider,
    );
    if expected_identity.id() != generated.identity().id() {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated case stable identity drifted",
        ));
    }

    Ok(())
}

fn validate_structural_query(generated: &GeneratedSelectCase) -> Result<(), SqlGeneratorError> {
    let signature = generated.structural_signature();
    let query = generated.query();
    let result_matches = signature.result_shape() == "typed_error"
        || match query.shape() {
            crate::SelectQueryShape::Scalar => signature.result_shape().starts_with("scalar_"),
            crate::SelectQueryShape::GlobalAggregate => {
                signature.result_shape() == "global_aggregate"
            }
            crate::SelectQueryShape::GroupedAggregate => {
                signature.result_shape().starts_with("grouped_")
            }
        };
    let grouping_matches = match query.shape() {
        crate::SelectQueryShape::Scalar => signature.grouping_shape() == "none",
        crate::SelectQueryShape::GlobalAggregate => signature.grouping_shape() == "global",
        crate::SelectQueryShape::GroupedAggregate => signature.grouping_shape().starts_with("one_"),
    };
    let having_matches =
        (signature.having_shape() == "none") == generated.query().having().is_none();
    let window_matches = match signature.window_shape() {
        "none" => query.limit().is_none() && query.offset().is_none(),
        "limit" => query.limit().is_some() && query.offset().is_none(),
        "limit_offset" => query.limit().is_some() && query.offset().is_some(),
        _ => false,
    };
    let indexed_profile = signature.schema_profile() == "indexed_nullable_reference";
    let profile_matches = if indexed_profile {
        generated.snapshot().indexes().len() == 2
            && generated
                .snapshot()
                .fields()
                .iter()
                .filter(|field| field.nullable())
                .count()
                == 2
    } else {
        generated.snapshot().indexes().is_empty()
            && generated
                .snapshot()
                .fields()
                .iter()
                .all(|field| !field.nullable())
    };
    if !result_matches
        || !grouping_matches
        || !having_matches
        || !window_matches
        || !profile_matches
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            format!(
                "derived structural signature for {:?} disagrees with typed query/profile facts: result={result_matches}, grouping={grouping_matches}, having={having_matches}, window={window_matches}, profile={profile_matches}",
                generated.identity().witness_id(),
            ),
        ));
    }
    Ok(())
}

fn validate_fixture_class(
    signature: &StructuralSignature,
    snapshot: &SelectSnapshot,
    fixture: &GeneratedFixture,
    query: &SelectQuery,
) -> Result<(), SqlGeneratorError> {
    let fixture_class = signature.fixture_class();
    let cardinality_matches = match fixture_class {
        "empty" => fixture.is_empty(),
        "singleton" | "valid_base" => fixture.len() == 1,
        "more_than_one_group_page" => fixture.len() > 16,
        _ => !fixture.is_empty(),
    };
    let duplicate_matches =
        !fixture_class.contains("duplicate") || fixture.has_duplicate_non_null_field_value();
    let stored_null_matches = !fixture_class.contains("stored_null") || fixture.has_stored_null();
    let computed_null_matches = !fixture_class.contains("computed_null")
        || fixture_has_computed_null_and_nonnull(snapshot, fixture);
    let window_matches = !fixture_class.contains("more_than_window")
        || query
            .limit()
            .and_then(|limit| limit.checked_add(query.offset().unwrap_or_default()))
            .is_some_and(|window| fixture.len() > usize::try_from(window).unwrap_or(usize::MAX));
    if !cardinality_matches
        || !duplicate_matches
        || !stored_null_matches
        || !computed_null_matches
        || !window_matches
    {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            format!("generated fixture does not satisfy structural class {fixture_class:?}"),
        ));
    }
    Ok(())
}

fn fixture_has_computed_null_and_nonnull(
    snapshot: &SelectSnapshot,
    fixture: &GeneratedFixture,
) -> bool {
    let Some(first) = snapshot.first_query_field(SelectFieldKind::Integer) else {
        return false;
    };
    let integers = snapshot.query_fields(SelectFieldKind::Integer);
    let Some(second) = integers.get(1) else {
        return false;
    };
    let mut equal = false;
    let mut different = false;
    for row in fixture.rows() {
        match (
            row.value_by_field_id(first.id()),
            row.value_by_field_id(second.id()),
        ) {
            (Some(left), Some(right)) if left == right => equal = true,
            (Some(GeneratedValue::Integer(_)), Some(GeneratedValue::Integer(_))) => {
                different = true;
            }
            _ => {}
        }
    }
    equal && different
}

fn validate_witness_construction(generated: &GeneratedSelectCase) -> Result<(), SqlGeneratorError> {
    let witnesses = scheduled_select_witnesses()?;
    if let Some(witness) = witnesses
        .iter()
        .find(|witness| witness.witness_id() == generated.identity().witness_id())
    {
        let recipe = SelectRecipe::from_witness_id(witness.witness_id())?;
        let derived_signature = recipe.structural_signature();
        if witness.signature() != &derived_signature
            || generated.structural_signature() != &derived_signature
            || generated.snapshot().fixture_family() != recipe.profile().id()
        {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "scheduled SELECT case signature drifted from its typed recipe or frozen catalog",
            ));
        }
        let mut rng = SplitMix64::new(generated.identity().sub_seed());
        let fixture_recipe =
            FixtureRecipe::from_signature(&derived_signature, generated.budgets())?;
        let _ = generate_fixture(
            generated.snapshot(),
            generated.identity().repetition(),
            generated.budgets(),
            fixture_recipe,
            &mut rng,
        )?;
        let query = query_for_recipe(generated.snapshot(), recipe, generated.budgets(), &mut rng)?;
        if &query != generated.query() {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "scheduled SELECT query drifted from its typed witness construction",
            ));
        }
        return Ok(());
    }

    let Some(violation) = generated.violation() else {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated SELECT case has no frozen witness or typed violation owner",
        ));
    };
    if generated.identity().witness_id() != violation.id() {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "typed invalid proposal identity drifted",
        ));
    }
    Ok(())
}

fn generated_identity(
    snapshot: &SelectSnapshot,
    witness_id: &str,
    root_seed: u64,
    sub_seed: u64,
    repetition: u64,
    provider: SelectProvider,
) -> GeneratedSelectIdentity {
    let provider_id = match provider {
        SelectProvider::RejectionInvariant => "rejection_invariant",
        SelectProvider::SqliteReference => "sqlite_reference",
    };
    let id = format!(
        "sql-select/v{SELECT_GENERATOR_VERSION}/{}/{witness_id}/{root_seed:016x}/{repetition:016x}/{provider_id}",
        snapshot.fixture_family(),
    );

    GeneratedSelectIdentity::new(
        id,
        SELECT_GENERATOR_VERSION,
        witness_id.to_string(),
        root_seed,
        sub_seed,
        repetition,
    )
}

#[derive(Clone, Copy)]
enum SelectRecipe {
    ColdSqlFluent,
    GlobalEmptyDistinctFilter,
    GlobalNonemptyDistinctFilter,
    GlobalNonemptyMultipleProjection,
    GroupedHashBounded,
    GroupedOrderedBounded,
    IndexedCompositePrefixHybrid,
    IndexedSecondaryRangeNonCovering,
    IndexedSecondaryRangePure,
    NullComputedAggregate,
    NullComputedDistinct,
    NullComputedOrdering,
    NullStoredComparisonMembership,
    NullStoredOrdering,
    ScalarIndexedComputedDistinctWindow,
    ScalarReferenceFullWindow,
    ScalarReferenceInvalidAliasOrder,
}

impl SelectRecipe {
    fn from_witness_id(witness_id: &str) -> Result<Self, SqlGeneratorError> {
        match witness_id {
            "tier_c.cache.cold_sql_fluent" => Ok(Self::ColdSqlFluent),
            "tier_c.global.empty_distinct_filter" => Ok(Self::GlobalEmptyDistinctFilter),
            "tier_c.global.nonempty_distinct_filter" => Ok(Self::GlobalNonemptyDistinctFilter),
            "tier_c.global.nonempty_multiple_projection" => {
                Ok(Self::GlobalNonemptyMultipleProjection)
            }
            "tier_c.grouped.hash_bounded" => Ok(Self::GroupedHashBounded),
            "tier_c.grouped.ordered_bounded" => Ok(Self::GroupedOrderedBounded),
            "tier_c.indexed.composite_prefix_hybrid" => Ok(Self::IndexedCompositePrefixHybrid),
            "tier_c.indexed.secondary_range_non_covering_incompatible" => {
                Ok(Self::IndexedSecondaryRangeNonCovering)
            }
            "tier_c.indexed.secondary_range_pure_compatible" => Ok(Self::IndexedSecondaryRangePure),
            "tier_c.null.computed_aggregate" => Ok(Self::NullComputedAggregate),
            "tier_c.null.computed_distinct" => Ok(Self::NullComputedDistinct),
            "tier_c.null.computed_ordering" => Ok(Self::NullComputedOrdering),
            "tier_c.null.stored_comparison_membership" => Ok(Self::NullStoredComparisonMembership),
            "tier_c.null.stored_ordering" => Ok(Self::NullStoredOrdering),
            "tier_c.scalar.indexed_computed_distinct_window" => {
                Ok(Self::ScalarIndexedComputedDistinctWindow)
            }
            "tier_c.scalar.reference_full_window" => Ok(Self::ScalarReferenceFullWindow),
            "tier_c.scalar.reference_invalid_alias_order" => {
                Ok(Self::ScalarReferenceInvalidAliasOrder)
            }
            _ => Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                format!("unsupported scheduled SELECT witness {witness_id:?}"),
            )),
        }
    }

    const fn violation(self) -> Option<SelectViolation> {
        match self {
            Self::ScalarReferenceInvalidAliasOrder => Some(SelectViolation::AmbiguousAlias),
            _ => None,
        }
    }

    const fn profile(self) -> SelectSchemaProfile {
        match self {
            Self::GroupedOrderedBounded
            | Self::IndexedCompositePrefixHybrid
            | Self::IndexedSecondaryRangeNonCovering
            | Self::IndexedSecondaryRangePure
            | Self::NullStoredComparisonMembership
            | Self::NullStoredOrdering
            | Self::ScalarIndexedComputedDistinctWindow => {
                SelectSchemaProfile::IndexedNullableReference
            }
            Self::ColdSqlFluent
            | Self::GlobalEmptyDistinctFilter
            | Self::GlobalNonemptyDistinctFilter
            | Self::GlobalNonemptyMultipleProjection
            | Self::GroupedHashBounded
            | Self::NullComputedAggregate
            | Self::NullComputedDistinct
            | Self::NullComputedOrdering
            | Self::ScalarReferenceFullWindow
            | Self::ScalarReferenceInvalidAliasOrder => SelectSchemaProfile::ReferenceScalar,
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive typed recipe derives every lossless structural signature"
    )]
    fn structural_signature(self) -> StructuralSignature {
        let profile = self.profile();
        match self {
            Self::ColdSqlFluent => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::GlobalEmptyDistinctFilter => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::GlobalNonemptyDistinctFilter => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::GlobalNonemptyMultipleProjection => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::GroupedHashBounded => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::GroupedOrderedBounded => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::IndexedCompositePrefixHybrid => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::IndexedSecondaryRangeNonCovering => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::IndexedSecondaryRangePure => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::NullComputedAggregate => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::NullComputedDistinct => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::NullComputedOrdering => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::NullStoredComparisonMembership => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::NullStoredOrdering => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::ScalarIndexedComputedDistinctWindow => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::ScalarReferenceFullWindow => StructuralSignature::select(
                "accepted",
                profile,
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
                "none",
            ),
            Self::ScalarReferenceInvalidAliasOrder => StructuralSignature::select(
                "singly_invalid",
                profile,
                "scalar_rows",
                "ambiguous_aliases",
                "none",
                "none",
                "none",
                "ambiguous_projection_alias",
                "limit",
                "stored_scalar",
                "ordinary",
                "valid_base",
                "not_applicable",
                "not_applicable",
                "ambiguous_alias_binding",
            ),
        }
    }
}

#[derive(Clone, Copy)]
struct FixtureRecipe {
    row_count: u32,
    computed_null_values: bool,
    repeated_values: bool,
    ordered_values: bool,
    text_domain: GeneratedTextDomain,
    variant: u64,
}

impl FixtureRecipe {
    fn from_signature(
        signature: &StructuralSignature,
        budgets: SelectBudgets,
    ) -> Result<Self, SqlGeneratorError> {
        let fixture = signature.fixture_class();
        let row_count = match fixture {
            "empty" => 0,
            "singleton" | "valid_base" => 1,
            "more_than_one_group_page" => 32,
            _ => 10,
        };
        if row_count > budgets.max_fixture_rows() {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                format!(
                    "fixture package {fixture:?} requires {row_count} rows, exceeding its {}-row budget",
                    budgets.max_fixture_rows(),
                ),
            ));
        }
        let repeated_values = fixture.contains("duplicate")
            || fixture.contains("multiple_group")
            || fixture.contains("order_ties");
        let ordered_values = fixture.contains("order_ties") || fixture.contains("window");
        let text_domain = if fixture.contains("unicode") {
            GeneratedTextDomain::Unicode
        } else {
            GeneratedTextDomain::Ascii
        };
        let variant = u64::from(
            fixture
                .as_bytes()
                .iter()
                .fold(0_u16, |sum, byte| sum.wrapping_add(u16::from(*byte))),
        );
        Ok(Self {
            row_count,
            computed_null_values: fixture.contains("computed_null"),
            repeated_values,
            ordered_values,
            text_domain,
            variant,
        })
    }

    fn valid_base(budgets: SelectBudgets) -> Result<Self, SqlGeneratorError> {
        Self::from_signature(
            &StructuralSignature::invalid_select("reference_scalar", "valid_base"),
            budgets,
        )
    }
}

fn select_snapshot(profile: SelectSchemaProfile) -> Result<SelectSnapshot, SqlGeneratorError> {
    let indexed = profile == SelectSchemaProfile::IndexedNullableReference;
    SelectSnapshot::try_new(
        profile.id(),
        format!("sql_generator::{}", profile.id()),
        match profile {
            SelectSchemaProfile::ReferenceScalar => "GeneratedReferenceScalar",
            SelectSchemaProfile::IndexedNullableReference => "GeneratedIndexedNullable",
        },
        1,
        vec![
            SelectField::new(1, "id", SelectFieldKind::Ulid, false, true, true),
            SelectField::new(2, "name", SelectFieldKind::Text, false, false, false),
            SelectField::new(3, "age", SelectFieldKind::Integer, false, false, false),
            SelectField::new(4, "score", SelectFieldKind::Integer, indexed, false, false),
            SelectField::new(5, "active", SelectFieldKind::Boolean, false, false, false),
            SelectField::new(6, "note", SelectFieldKind::Text, indexed, false, false),
        ],
        if indexed {
            vec![
                SelectIndex::new(1, "by_score", vec![4]),
                SelectIndex::new(2, "by_name_score", vec![2, 4]),
            ]
        } else {
            Vec::new()
        },
    )
}

fn query_for_recipe(
    snapshot: &SelectSnapshot,
    recipe: SelectRecipe,
    budgets: SelectBudgets,
    rng: &mut SplitMix64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let query = match recipe {
        SelectRecipe::ColdSqlFluent | SelectRecipe::ScalarReferenceFullWindow => {
            full_composition_query(snapshot)?
        }
        SelectRecipe::GlobalEmptyDistinctFilter => global_empty_filter_query(snapshot)?,
        SelectRecipe::GlobalNonemptyDistinctFilter => global_distinct_filter_query(snapshot)?,
        SelectRecipe::GlobalNonemptyMultipleProjection => {
            global_multiple_projection_query(snapshot)?
        }
        SelectRecipe::GroupedHashBounded => grouped_bounded_query(snapshot, false)?,
        SelectRecipe::GroupedOrderedBounded => grouped_bounded_query(snapshot, true)?,
        SelectRecipe::IndexedCompositePrefixHybrid => composite_prefix_query(snapshot)?,
        SelectRecipe::IndexedSecondaryRangeNonCovering => secondary_range_query(snapshot, false)?,
        SelectRecipe::IndexedSecondaryRangePure => secondary_range_query(snapshot, true)?,
        SelectRecipe::NullComputedAggregate => computed_null_aggregate_query(snapshot)?,
        SelectRecipe::NullComputedDistinct => computed_null_distinct_query(snapshot)?,
        SelectRecipe::NullComputedOrdering => computed_null_ordering_query(snapshot)?,
        SelectRecipe::NullStoredComparisonMembership => stored_null_membership_query(snapshot)?,
        SelectRecipe::NullStoredOrdering => stored_null_ordering_query(snapshot)?,
        SelectRecipe::ScalarIndexedComputedDistinctWindow => {
            indexed_computed_distinct_window_query(snapshot)?
        }
        SelectRecipe::ScalarReferenceInvalidAliasOrder => invalid_base_query(snapshot, 0, rng)?,
    };
    query.validate(snapshot, budgets)?;
    Ok(query)
}

fn generate_fixture(
    snapshot: &SelectSnapshot,
    repetition: u64,
    budgets: SelectBudgets,
    recipe: FixtureRecipe,
    rng: &mut SplitMix64,
) -> Result<GeneratedFixture, SqlGeneratorError> {
    let row_count = u64::from(recipe.row_count);
    if row_count > u64::from(budgets.max_fixture_rows()) {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::Budget,
            "generated fixture row choice exceeds its configured budget",
        ));
    }

    let mut rows = Vec::with_capacity(usize::try_from(row_count).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::Budget,
            "generated fixture row count does not fit usize",
        )
    })?);
    for row_index in 0..row_count {
        let mut values = Vec::new();
        let mut integer_ordinal = 0_u64;
        for field in snapshot.fields() {
            if field.primary_key() || field.generated() || !field.kind().is_generated_scalar() {
                continue;
            }
            let window_integer_ordinal =
                if recipe.ordered_values && field.kind() == SelectFieldKind::Integer {
                    integer_ordinal = integer_ordinal.saturating_add(1);
                    Some(integer_ordinal)
                } else {
                    None
                };
            let value = generated_field_value(
                field,
                recipe.variant.wrapping_add(repetition),
                row_index,
                recipe.text_domain,
                window_integer_ordinal,
                recipe.computed_null_values,
                recipe.repeated_values,
                rng,
            )?;
            values.push(GeneratedFieldValue::new(field.id(), value));
        }
        rows.push(GeneratedFixtureRow::new(values));
    }
    let fixture = GeneratedFixture::new(rows);
    fixture.validate(snapshot, budgets.max_fixture_rows())?;

    Ok(fixture)
}

#[expect(
    clippy::too_many_arguments,
    reason = "fixture value generation keeps every closed structural choice explicit"
)]
fn generated_field_value(
    field: &SelectField,
    fixture_variant: u64,
    row_index: u64,
    text_domain: GeneratedTextDomain,
    window_integer_ordinal: Option<u64>,
    computed_null_values: bool,
    repeated_values: bool,
    rng: &mut SplitMix64,
) -> Result<GeneratedValue, SqlGeneratorError> {
    if field.nullable()
        && fixture_variant
            .wrapping_add(row_index)
            .wrapping_add(u64::from(field.id()))
            % 4
            == 0
    {
        return Ok(GeneratedValue::Null(field.kind().value_kind().ok_or_else(
            || {
                SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidSnapshot,
                    "nullable generated fixture field has no scalar value kind",
                )
            },
        )?));
    }

    let row_selector = if repeated_values {
        row_index % 3
    } else {
        row_index
    };
    let random_selector = if repeated_values { 0 } else { rng.bounded(7)? };
    let selector = fixture_variant
        .wrapping_add(row_selector)
        .wrapping_add(u64::from(field.id()))
        .wrapping_add(random_selector);
    match field.kind() {
        SelectFieldKind::Boolean => Ok(GeneratedValue::Boolean(selector % 2 == 0)),
        SelectFieldKind::Integer => {
            if computed_null_values && matches!(field.id(), 3 | 4) {
                let value = i64::from(!(row_index == 0 || field.id() == 3));
                return Ok(GeneratedValue::Integer(value));
            }
            if fixture_variant >= 24 && row_index == 0 {
                match fixture_variant % 8 {
                    2 => return Ok(GeneratedValue::Integer(REVIEWED_INTEGER_MIN_BOUNDARY)),
                    3 => return Ok(GeneratedValue::Integer(REVIEWED_INTEGER_MAX_BOUNDARY)),
                    _ => {}
                }
            }
            if let Some(ordinal) = window_integer_ordinal {
                let order_row = if fixture_variant >= 24 && fixture_variant % 8 == 6 && ordinal == 2
                {
                    row_index % 3
                } else {
                    row_index
                };
                let value = order_row.checked_mul(ordinal).ok_or_else(|| {
                    SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "generated window fixture integer overflowed",
                    )
                })?;
                return i64::try_from(value)
                    .map(GeneratedValue::Integer)
                    .map_err(|_| {
                        SqlGeneratorError::new(
                            SqlGeneratorErrorKind::InvalidCase,
                            "generated window fixture integer does not fit i64",
                        )
                    });
            }
            let index =
                usize::try_from(selector % INTEGER_FIXTURE_VALUES.len() as u64).map_err(|_| {
                    SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        "generated integer fixture selector does not fit usize",
                    )
                })?;
            Ok(GeneratedValue::Integer(INTEGER_FIXTURE_VALUES[index]))
        }
        SelectFieldKind::Text => {
            const ASCII_VALUES: &[&str] = &["", "Alpha", "alpha", "alphabet", "beta"];
            const UNICODE_VALUES: &[&str] =
                &["", "Alpha", "alpha", "alphabet", "beta", "éclair", "βeta"];
            let values = match text_domain {
                GeneratedTextDomain::Ascii => ASCII_VALUES,
                GeneratedTextDomain::Unicode => UNICODE_VALUES,
            };
            let index = usize::try_from(selector % values.len() as u64).map_err(|_| {
                SqlGeneratorError::new(
                    SqlGeneratorErrorKind::InvalidCase,
                    "generated text fixture selector does not fit usize",
                )
            })?;
            Ok(GeneratedValue::Text(values[index].to_string()))
        }
        SelectFieldKind::Blob | SelectFieldKind::Ulid => Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidSnapshot,
            "fixture generation reached an excluded accepted field kind",
        )),
    }
}

#[derive(Clone, Copy)]
enum GeneratedTextDomain {
    Ascii,
    Unicode,
}

fn global_empty_filter_query(snapshot: &SelectSnapshot) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    Ok(SelectQuery::global_aggregate(
        vec![projection(
            filtered_count_all(comparison(
                field(fields.boolean),
                SelectComparisonOperator::Equal,
                SelectExpression::literal(GeneratedValue::Boolean(true)),
            )),
            Some("active_count"),
        )],
        None,
        None,
    ))
}

fn global_multiple_projection_query(
    snapshot: &SelectSnapshot,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let count = count_all();
    Ok(SelectQuery::global_aggregate(
        vec![
            projection(count.clone(), Some("row_count")),
            projection(
                count_value(field(fields.text), true),
                Some("distinct_names"),
            ),
        ],
        None,
        Some(comparison(
            count,
            SelectComparisonOperator::GreaterOrEqual,
            SelectExpression::literal(GeneratedValue::Integer(1)),
        )),
    ))
}

fn full_composition_query(snapshot: &SelectSnapshot) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    Ok(SelectQuery::new(
        vec![
            projection(field(fields.text), Some("display_name")),
            projection(
                function(SelectFunction::Abs, vec![field(fields.first_integer)]),
                Some("sort_value"),
            ),
        ],
        Some(comparison(
            field(fields.second_integer),
            SelectComparisonOperator::GreaterOrEqual,
            SelectExpression::literal(GeneratedValue::Integer(0)),
        )),
        vec![
            SelectOrderTerm::alias("sort_value", SelectOrderDirection::Descending),
            SelectOrderTerm::alias("display_name", SelectOrderDirection::Ascending),
        ],
        Some(4),
        Some(2),
    ))
}

fn global_distinct_filter_query(
    snapshot: &SelectSnapshot,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let aggregate = SelectExpression::Count {
        argument: Some(Box::new(field(fields.first_integer))),
        distinct: true,
        filter: Some(Box::new(comparison(
            field(fields.boolean),
            SelectComparisonOperator::Equal,
            SelectExpression::literal(GeneratedValue::Boolean(true)),
        ))),
    };
    Ok(SelectQuery::global_aggregate(
        vec![projection(aggregate, Some("distinct_active_values"))],
        Some(comparison(
            field(fields.first_integer),
            SelectComparisonOperator::GreaterOrEqual,
            SelectExpression::literal(GeneratedValue::Integer(-1)),
        )),
        None,
    ))
}

fn grouped_bounded_query(
    snapshot: &SelectSnapshot,
    indexed: bool,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let group = if indexed {
        field(fields.second_integer)
    } else {
        field(fields.text)
    };
    let count = count_all();
    let predicate = comparison(
        if indexed {
            field(fields.second_integer)
        } else {
            field(fields.first_integer)
        },
        SelectComparisonOperator::GreaterOrEqual,
        SelectExpression::literal(GeneratedValue::Integer(-1)),
    );
    Ok(SelectQuery::grouped_aggregate(
        vec![
            projection(group.clone(), Some("group_value")),
            projection(count.clone(), Some("row_count")),
            projection(
                count_value(field(fields.text), true),
                Some("distinct_names"),
            ),
        ],
        Some(predicate),
        vec![group.clone()],
        Some(comparison(
            count,
            SelectComparisonOperator::Greater,
            SelectExpression::literal(GeneratedValue::Integer(0)),
        )),
        vec![
            SelectOrderTerm::alias("row_count", SelectOrderDirection::Descending),
            order_expression(group, SelectOrderDirection::Ascending),
        ],
        16,
    ))
}

fn composite_prefix_query(snapshot: &SelectSnapshot) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    Ok(SelectQuery::new(
        vec![
            projection(field(fields.text), None),
            projection(field(fields.second_integer), None),
            projection(field(fields.nullable_text), None),
        ],
        Some(SelectPredicate::And {
            left: Box::new(comparison(
                field(fields.text),
                SelectComparisonOperator::Equal,
                SelectExpression::literal(GeneratedValue::Text("alpha".to_string())),
            )),
            right: Box::new(SelectPredicate::InList {
                expression: field(fields.second_integer),
                members: vec![
                    SelectExpression::literal(GeneratedValue::Integer(-1)),
                    SelectExpression::literal(GeneratedValue::Integer(0)),
                    SelectExpression::literal(GeneratedValue::Integer(0)),
                ],
                negated: false,
            }),
        }),
        vec![
            order_expression(
                field(fields.second_integer),
                SelectOrderDirection::Ascending,
            ),
            order_expression(field(fields.text), SelectOrderDirection::Ascending),
        ],
        Some(8),
        None,
    ))
}

fn secondary_range_query(
    snapshot: &SelectSnapshot,
    covering: bool,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let range = SelectPredicate::Between {
        expression: field(fields.second_integer),
        lower: SelectExpression::literal(GeneratedValue::Integer(-1)),
        upper: SelectExpression::literal(GeneratedValue::Integer(43)),
        negated: false,
    };
    if covering {
        return Ok(SelectQuery::new(
            vec![projection(field(fields.second_integer), None)],
            Some(range),
            vec![
                order_expression(
                    field(fields.second_integer),
                    SelectOrderDirection::Ascending,
                ),
                order_expression(field(fields.text), SelectOrderDirection::Ascending),
            ],
            Some(8),
            None,
        ));
    }
    Ok(SelectQuery::new(
        vec![
            projection(
                function(SelectFunction::Abs, vec![field(fields.first_integer)]),
                Some("computed_value"),
            ),
            projection(field(fields.nullable_text), None),
        ],
        Some(SelectPredicate::And {
            left: Box::new(range),
            right: Box::new(comparison(
                field(fields.boolean),
                SelectComparisonOperator::Equal,
                SelectExpression::literal(GeneratedValue::Boolean(true)),
            )),
        }),
        vec![
            SelectOrderTerm::alias("computed_value", SelectOrderDirection::Ascending),
            order_expression(field(fields.text), SelectOrderDirection::Ascending),
        ],
        Some(5),
        Some(1),
    ))
}

fn computed_null_expression(fields: &RequiredFields<'_>) -> SelectExpression {
    function(
        SelectFunction::NullIf,
        vec![field(fields.first_integer), field(fields.second_integer)],
    )
}

fn computed_null_aggregate_query(
    snapshot: &SelectSnapshot,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    Ok(SelectQuery::global_aggregate(
        vec![projection(
            count_value(computed_null_expression(&fields), false),
            Some("nonnull_count"),
        )],
        None,
        None,
    ))
}

fn computed_null_distinct_query(
    snapshot: &SelectSnapshot,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    Ok(SelectQuery::distinct(
        vec![projection(
            computed_null_expression(&fields),
            Some("nullable_value"),
        )],
        None,
        vec![SelectOrderTerm::alias(
            "nullable_value",
            SelectOrderDirection::Ascending,
        )],
        Some(8),
    ))
}

fn computed_null_ordering_query(
    snapshot: &SelectSnapshot,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    Ok(SelectQuery::new(
        vec![
            projection(computed_null_expression(&fields), Some("nullable_value")),
            projection(field(fields.text), None),
        ],
        None,
        vec![
            SelectOrderTerm::alias("nullable_value", SelectOrderDirection::Ascending),
            order_expression(field(fields.text), SelectOrderDirection::Ascending),
        ],
        Some(5),
        Some(1),
    ))
}

fn stored_null_membership_query(
    snapshot: &SelectSnapshot,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let nullable = field(fields.nullable_text);
    Ok(SelectQuery::new(
        vec![
            projection(nullable.clone(), None),
            projection(field(fields.text), None),
        ],
        Some(SelectPredicate::Or {
            left: Box::new(SelectPredicate::InList {
                expression: nullable.clone(),
                members: vec![
                    SelectExpression::literal(GeneratedValue::Text("alpha".to_string())),
                    SelectExpression::literal(GeneratedValue::Text("alpha".to_string())),
                ],
                negated: false,
            }),
            right: Box::new(SelectPredicate::IsNull {
                expression: nullable,
                negated: false,
            }),
        }),
        vec![order_expression(
            field(fields.text),
            SelectOrderDirection::Ascending,
        )],
        Some(8),
        None,
    ))
}

fn stored_null_ordering_query(snapshot: &SelectSnapshot) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    Ok(SelectQuery::new(
        vec![
            projection(field(fields.nullable_text), Some("nullable_value")),
            projection(field(fields.text), None),
        ],
        None,
        vec![
            SelectOrderTerm::alias("nullable_value", SelectOrderDirection::Ascending),
            order_expression(field(fields.text), SelectOrderDirection::Ascending),
        ],
        Some(5),
        Some(1),
    ))
}

fn indexed_computed_distinct_window_query(
    snapshot: &SelectSnapshot,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    Ok(SelectQuery::distinct_window(
        vec![
            projection(
                function(
                    SelectFunction::Coalesce,
                    vec![
                        field(fields.nullable_text),
                        SelectExpression::literal(GeneratedValue::Text("missing".to_string())),
                    ],
                ),
                Some("computed_value"),
            ),
            projection(field(fields.text), Some("name_value")),
        ],
        Some(SelectPredicate::And {
            left: Box::new(SelectPredicate::Between {
                expression: field(fields.second_integer),
                lower: SelectExpression::literal(GeneratedValue::Integer(-1)),
                upper: SelectExpression::literal(GeneratedValue::Integer(43)),
                negated: false,
            }),
            right: Box::new(comparison(
                field(fields.boolean),
                SelectComparisonOperator::Equal,
                SelectExpression::literal(GeneratedValue::Boolean(true)),
            )),
        }),
        vec![
            SelectOrderTerm::alias("computed_value", SelectOrderDirection::Ascending),
            SelectOrderTerm::alias("name_value", SelectOrderDirection::Ascending),
        ],
        5,
        1,
    ))
}

fn invalid_base_query(
    snapshot: &SelectSnapshot,
    case_index: u64,
    rng: &mut SplitMix64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let limit = u32::try_from(2_u64.saturating_add(rng.bounded(3)?)).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "invalid base-query limit does not fit u32",
        )
    })?;
    let alias = if case_index.is_multiple_of(2) {
        Some("base_value")
    } else {
        None
    };

    Ok(SelectQuery::new(
        vec![projection(field(fields.text), alias)],
        None,
        vec![order_expression(
            field(fields.text),
            SelectOrderDirection::Ascending,
        )],
        Some(limit),
        None,
    ))
}

pub(crate) fn render_generated_select_case(
    snapshot: &SelectSnapshot,
    query: &SelectQuery,
    violation: Option<SelectViolation>,
    budgets: SelectBudgets,
) -> Result<String, SqlGeneratorError> {
    query.validate(snapshot, budgets)?;
    if let Some(violation) = violation {
        return render_invalid_query(snapshot, query, violation);
    }

    let projections = query
        .projections()
        .iter()
        .map(|projection| {
            let mut rendered = render_expression(snapshot, projection.expression())?;
            if let Some(alias) = projection.alias() {
                rendered.push_str(" AS ");
                rendered.push_str(alias);
            }
            Ok(rendered)
        })
        .collect::<Result<Vec<_>, SqlGeneratorError>>()?;
    let distinct = if query.is_distinct() { "DISTINCT " } else { "" };
    let mut sql = format!(
        "SELECT {distinct}{} FROM {}",
        projections.join(", "),
        snapshot.entity_name()
    );
    if let Some(predicate) = query.predicate() {
        sql.push_str(" WHERE ");
        sql.push_str(&render_predicate(snapshot, predicate)?);
    }
    if !query.group_by().is_empty() {
        let group_by = query
            .group_by()
            .iter()
            .map(|expression| render_expression(snapshot, expression))
            .collect::<Result<Vec<_>, _>>()?;
        sql.push_str(" GROUP BY ");
        sql.push_str(&group_by.join(", "));
    }
    if let Some(having) = query.having() {
        sql.push_str(" HAVING ");
        sql.push_str(&render_predicate(snapshot, having)?);
    }
    if !query.order().is_empty() {
        let order = query
            .order()
            .iter()
            .map(|term| {
                let target = match term.target() {
                    SelectOrderTarget::Alias(alias) => alias.clone(),
                    SelectOrderTarget::Expression(expression) => {
                        render_expression(snapshot, expression)?
                    }
                };
                let direction = match term.direction() {
                    SelectOrderDirection::Ascending => "ASC",
                    SelectOrderDirection::Descending => "DESC",
                };
                Ok(format!("{target} {direction}"))
            })
            .collect::<Result<Vec<_>, SqlGeneratorError>>()?;
        sql.push_str(" ORDER BY ");
        sql.push_str(&order.join(", "));
    }
    if let Some(limit) = query.limit() {
        write!(sql, " LIMIT {limit}").map_err(|_| {
            SqlGeneratorError::new(
                SqlGeneratorErrorKind::Rendering,
                "generated LIMIT rendering failed",
            )
        })?;
    }
    if let Some(offset) = query.offset() {
        write!(sql, " OFFSET {offset}").map_err(|_| {
            SqlGeneratorError::new(
                SqlGeneratorErrorKind::Rendering,
                "generated OFFSET rendering failed",
            )
        })?;
    }

    Ok(sql)
}

fn render_invalid_query(
    snapshot: &SelectSnapshot,
    query: &SelectQuery,
    violation: SelectViolation,
) -> Result<String, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let entity = snapshot.entity_name();
    let text = fields.text.name();
    let integer = fields.first_integer.name();
    let sql = match violation {
        SelectViolation::AmbiguousAlias => format!(
            "SELECT {text} AS duplicate_alias, {integer} AS duplicate_alias FROM {entity} ORDER BY duplicate_alias ASC LIMIT 1"
        ),
        SelectViolation::InvalidAggregateScope => {
            format!("SELECT {text}, COUNT(*) FROM {entity}")
        }
        SelectViolation::InvalidClauseOrder => format!(
            "SELECT {text} FROM {entity} OFFSET {} LIMIT {}",
            query.offset().unwrap_or(1),
            query.limit().unwrap_or(1),
        ),
        SelectViolation::InvalidGrouping => {
            format!("SELECT {text}, {integer}, COUNT(*) FROM {entity} GROUP BY {text}")
        }
        SelectViolation::InvalidOrderTarget => {
            format!("SELECT {text} FROM {entity} ORDER BY icydb_missing_alias ASC LIMIT 1")
        }
        SelectViolation::LimitOverflow => {
            format!("SELECT {text} FROM {entity} LIMIT 4294967296")
        }
        SelectViolation::UnknownField => {
            format!("SELECT icydb_missing_field FROM {entity} ORDER BY {text} ASC LIMIT 1")
        }
        SelectViolation::UnsupportedFunctionSignature => {
            format!("SELECT LOWER({integer}) FROM {entity} ORDER BY {text} ASC LIMIT 1")
        }
        SelectViolation::WrongOperatorType => {
            format!("SELECT ({text} + 1) FROM {entity} ORDER BY {text} ASC LIMIT 1")
        }
    };

    Ok(sql)
}

fn render_expression(
    snapshot: &SelectSnapshot,
    expression: &SelectExpression,
) -> Result<String, SqlGeneratorError> {
    expression.validate(snapshot)?;
    match expression {
        SelectExpression::Field { field_id } => snapshot
            .field_by_id(*field_id)
            .map(|field| field.name().to_string())
            .ok_or_else(|| {
                SqlGeneratorError::new(
                    SqlGeneratorErrorKind::Rendering,
                    format!("cannot render missing accepted field {field_id}"),
                )
            }),
        SelectExpression::Literal { value } => Ok(render_literal(value)),
        SelectExpression::Count {
            argument,
            distinct,
            filter,
        } => {
            let argument = match argument {
                Some(argument) => render_expression(snapshot, argument)?,
                None => "*".to_string(),
            };
            let distinct = if *distinct { "DISTINCT " } else { "" };
            let filter = match filter {
                Some(filter) => format!(" FILTER (WHERE {})", render_predicate(snapshot, filter)?),
                None => String::new(),
            };
            Ok(format!("COUNT({distinct}{argument}){filter}"))
        }
        SelectExpression::Arithmetic {
            operator,
            left,
            right,
        } => {
            let operator = match operator {
                SelectArithmeticOperator::Add => "+",
                SelectArithmeticOperator::Subtract => "-",
            };
            Ok(format!(
                "({} {operator} {})",
                render_expression(snapshot, left)?,
                render_expression(snapshot, right)?,
            ))
        }
        SelectExpression::Function {
            function,
            arguments,
        } => {
            let name = match function {
                SelectFunction::Abs => "ABS",
                SelectFunction::Coalesce => "COALESCE",
                SelectFunction::Length => "LENGTH",
                SelectFunction::Lower => "LOWER",
                SelectFunction::NullIf => "NULLIF",
                SelectFunction::Upper => "UPPER",
            };
            let arguments = arguments
                .iter()
                .map(|argument| render_expression(snapshot, argument))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("{name}({})", arguments.join(", ")))
        }
        SelectExpression::Case {
            condition,
            then_expression,
            else_expression,
        } => Ok(format!(
            "CASE WHEN {} THEN {} ELSE {} END",
            render_predicate(snapshot, condition)?,
            render_expression(snapshot, then_expression)?,
            render_expression(snapshot, else_expression)?,
        )),
    }
}

fn render_predicate(
    snapshot: &SelectSnapshot,
    predicate: &SelectPredicate,
) -> Result<String, SqlGeneratorError> {
    predicate.validate(snapshot)?;
    match predicate {
        SelectPredicate::And { left, right } => Ok(format!(
            "({} AND {})",
            render_predicate(snapshot, left)?,
            render_predicate(snapshot, right)?,
        )),
        SelectPredicate::Or { left, right } => Ok(format!(
            "({} OR {})",
            render_predicate(snapshot, left)?,
            render_predicate(snapshot, right)?,
        )),
        SelectPredicate::Not { predicate } => {
            Ok(format!("NOT ({})", render_predicate(snapshot, predicate)?))
        }
        SelectPredicate::Comparison {
            operator,
            left,
            right,
        } => {
            let operator = match operator {
                SelectComparisonOperator::Equal => "=",
                SelectComparisonOperator::Greater => ">",
                SelectComparisonOperator::GreaterOrEqual => ">=",
                SelectComparisonOperator::Less => "<",
                SelectComparisonOperator::LessOrEqual => "<=",
                SelectComparisonOperator::NotEqual => "!=",
            };
            Ok(format!(
                "{} {operator} {}",
                render_expression(snapshot, left)?,
                render_expression(snapshot, right)?,
            ))
        }
        SelectPredicate::Between {
            expression,
            lower,
            upper,
            negated,
        } => Ok(format!(
            "{} {}BETWEEN {} AND {}",
            render_expression(snapshot, expression)?,
            if *negated { "NOT " } else { "" },
            render_expression(snapshot, lower)?,
            render_expression(snapshot, upper)?,
        )),
        SelectPredicate::InList {
            expression,
            members,
            negated,
        } => {
            let members = members
                .iter()
                .map(|member| render_expression(snapshot, member))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            Ok(format!(
                "{} {}IN ({members})",
                render_expression(snapshot, expression)?,
                if *negated { "NOT " } else { "" },
            ))
        }
        SelectPredicate::IsNull {
            expression,
            negated,
        } => Ok(format!(
            "{} IS {}NULL",
            render_expression(snapshot, expression)?,
            if *negated { "NOT " } else { "" },
        )),
        SelectPredicate::IsTruth {
            expression,
            expected,
            negated,
        } => Ok(format!(
            "{} IS {}{}",
            render_expression(snapshot, expression)?,
            if *negated { "NOT " } else { "" },
            if *expected { "TRUE" } else { "FALSE" },
        )),
        SelectPredicate::PrefixLike {
            expression,
            prefix,
            case_insensitive,
            negated,
        } => Ok(format!(
            "{} {}{} '{}%'",
            render_expression(snapshot, expression)?,
            if *negated { "NOT " } else { "" },
            if *case_insensitive { "ILIKE" } else { "LIKE" },
            escape_sql_text(prefix),
        )),
        SelectPredicate::StartsWith { value, prefix } => Ok(format!(
            "STARTS_WITH({}, {})",
            render_expression(snapshot, value)?,
            render_expression(snapshot, prefix)?,
        )),
    }
}

fn render_literal(value: &GeneratedValue) -> String {
    match value {
        GeneratedValue::Boolean(value) => if *value { "TRUE" } else { "FALSE" }.to_string(),
        GeneratedValue::Integer(value) => value.to_string(),
        GeneratedValue::Null(_) => "NULL".to_string(),
        GeneratedValue::Text(value) => format!("'{}'", escape_sql_text(value)),
    }
}

fn escape_sql_text(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn collect_select_features(query: &SelectQuery) -> BTreeSet<SelectFeature> {
    let mut features = BTreeSet::from([SelectFeature::Projection]);
    if query.is_distinct() {
        features.insert(SelectFeature::Distinct);
    }
    for projection in query.projections() {
        if projection.alias().is_some() {
            features.insert(SelectFeature::Alias);
        }
        collect_expression_features(projection.expression(), &mut features);
    }
    if let Some(predicate) = query.predicate() {
        features.insert(SelectFeature::Predicate);
        collect_predicate_features(predicate, &mut features);
    }
    for expression in query.group_by() {
        features.insert(SelectFeature::Grouping);
        collect_expression_features(expression, &mut features);
    }
    if let Some(having) = query.having() {
        features.insert(SelectFeature::Having);
        collect_predicate_features(having, &mut features);
    }
    for term in query.order() {
        features.insert(SelectFeature::Ordering);
        match term.target() {
            SelectOrderTarget::Alias(_) => {
                features.insert(SelectFeature::Alias);
            }
            SelectOrderTarget::Expression(expression) => {
                collect_expression_features(expression, &mut features);
            }
        }
    }
    if query.limit().is_some() {
        features.insert(SelectFeature::Limit);
    }
    if query.offset().is_some() {
        features.insert(SelectFeature::Offset);
    }

    features
}

fn collect_expression_features(
    expression: &SelectExpression,
    features: &mut BTreeSet<SelectFeature>,
) {
    match expression {
        SelectExpression::Arithmetic { left, right, .. } => {
            features.insert(SelectFeature::Arithmetic);
            collect_expression_features(left, features);
            collect_expression_features(right, features);
        }
        SelectExpression::Case {
            condition,
            then_expression,
            else_expression,
        } => {
            features.insert(SelectFeature::SearchedCase);
            collect_predicate_features(condition, features);
            collect_expression_features(then_expression, features);
            collect_expression_features(else_expression, features);
        }
        SelectExpression::Count {
            argument,
            distinct,
            filter,
        } => {
            features.insert(SelectFeature::Aggregate);
            if *distinct {
                features.insert(SelectFeature::AggregateDistinct);
            }
            if let Some(filter) = filter {
                features.insert(SelectFeature::AggregateFilter);
                collect_predicate_features(filter, features);
            }
            if let Some(argument) = argument {
                collect_expression_features(argument, features);
            }
        }
        SelectExpression::Field { .. } | SelectExpression::Literal { .. } => {}
        SelectExpression::Function {
            function,
            arguments,
        } => {
            features.insert(SelectFeature::Function);
            if matches!(function, SelectFunction::Abs) {
                features.insert(SelectFeature::NumericFunction);
            }
            if matches!(function, SelectFunction::Coalesce | SelectFunction::NullIf) {
                features.insert(SelectFeature::Null);
            }
            if matches!(
                function,
                SelectFunction::Length | SelectFunction::Lower | SelectFunction::Upper
            ) {
                features.insert(SelectFeature::Text);
            }
            for argument in arguments {
                collect_expression_features(argument, features);
            }
        }
    }
}

fn collect_predicate_features(predicate: &SelectPredicate, features: &mut BTreeSet<SelectFeature>) {
    match predicate {
        SelectPredicate::And { left, right } | SelectPredicate::Or { left, right } => {
            features.insert(SelectFeature::Boolean);
            collect_predicate_features(left, features);
            collect_predicate_features(right, features);
        }
        SelectPredicate::Between {
            expression,
            lower,
            upper,
            ..
        } => {
            features.insert(SelectFeature::Range);
            collect_expression_features(expression, features);
            collect_expression_features(lower, features);
            collect_expression_features(upper, features);
        }
        SelectPredicate::Not { predicate } => {
            features.insert(SelectFeature::Boolean);
            collect_predicate_features(predicate, features);
        }
        SelectPredicate::Comparison { left, right, .. } => {
            features.insert(SelectFeature::Comparison);
            collect_expression_features(left, features);
            collect_expression_features(right, features);
        }
        SelectPredicate::IsNull { expression, .. } => {
            features.insert(SelectFeature::Null);
            collect_expression_features(expression, features);
        }
        SelectPredicate::InList {
            expression,
            members,
            ..
        } => {
            features.insert(SelectFeature::Membership);
            collect_expression_features(expression, features);
            for member in members {
                collect_expression_features(member, features);
            }
        }
        SelectPredicate::IsTruth { expression, .. } => {
            features.insert(SelectFeature::Boolean);
            collect_expression_features(expression, features);
        }
        SelectPredicate::PrefixLike { expression, .. } => {
            features.insert(SelectFeature::Text);
            collect_expression_features(expression, features);
        }
        SelectPredicate::StartsWith { value, prefix } => {
            features.insert(SelectFeature::Text);
            collect_expression_features(value, features);
            collect_expression_features(prefix, features);
        }
    }
}

struct RequiredFields<'a> {
    text: &'a SelectField,
    nullable_text: &'a SelectField,
    first_integer: &'a SelectField,
    second_integer: &'a SelectField,
    boolean: &'a SelectField,
}

fn required_fields(snapshot: &SelectSnapshot) -> Result<RequiredFields<'_>, SqlGeneratorError> {
    let text = snapshot
        .first_query_field(SelectFieldKind::Text)
        .ok_or_else(|| missing_field_kind(SelectFieldKind::Text))?;
    let nullable_text = snapshot
        .query_fields(SelectFieldKind::Text)
        .into_iter()
        .find(|field| field.nullable())
        .unwrap_or(text);
    let integer_fields = snapshot.query_fields(SelectFieldKind::Integer);
    let first_integer = integer_fields
        .first()
        .copied()
        .ok_or_else(|| missing_field_kind(SelectFieldKind::Integer))?;
    let second_integer = integer_fields
        .get(1)
        .copied()
        .ok_or_else(|| missing_field_kind(SelectFieldKind::Integer))?;
    let boolean = snapshot
        .first_query_field(SelectFieldKind::Boolean)
        .ok_or_else(|| missing_field_kind(SelectFieldKind::Boolean))?;

    Ok(RequiredFields {
        text,
        nullable_text,
        first_integer,
        second_integer,
        boolean,
    })
}

fn missing_field_kind(kind: SelectFieldKind) -> SqlGeneratorError {
    SqlGeneratorError::new(
        SqlGeneratorErrorKind::InvalidSnapshot,
        format!("SELECT generation requires accepted {kind:?} field facts"),
    )
}

const fn field(field: &SelectField) -> SelectExpression {
    SelectExpression::field(field.id())
}

fn projection(expression: SelectExpression, alias: Option<&str>) -> SelectProjection {
    SelectProjection::new(expression, alias)
}

const fn count_all() -> SelectExpression {
    SelectExpression::Count {
        argument: None,
        distinct: false,
        filter: None,
    }
}

fn count_value(expression: SelectExpression, distinct: bool) -> SelectExpression {
    SelectExpression::Count {
        argument: Some(Box::new(expression)),
        distinct,
        filter: None,
    }
}

fn filtered_count_all(filter: SelectPredicate) -> SelectExpression {
    SelectExpression::Count {
        argument: None,
        distinct: false,
        filter: Some(Box::new(filter)),
    }
}

const fn function(function: SelectFunction, arguments: Vec<SelectExpression>) -> SelectExpression {
    SelectExpression::Function {
        function,
        arguments,
    }
}

const fn comparison(
    left: SelectExpression,
    operator: SelectComparisonOperator,
    right: SelectExpression,
) -> SelectPredicate {
    SelectPredicate::Comparison {
        operator,
        left,
        right,
    }
}

const fn order_expression(
    expression: SelectExpression,
    direction: SelectOrderDirection,
) -> SelectOrderTerm {
    SelectOrderTerm::expression(expression, direction)
}
