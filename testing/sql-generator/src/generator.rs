//! Module: sql_generator::generator
//! Responsibility: fixed SELECT families, fixtures, feature facts, and SQL rendering.
//! Does not own: parser acceptance, reference execution, or mismatch shrinking policy.
//! Boundary: derives one independent stream per family/case and validates before emission.

use crate::{
    error::{SqlGeneratorError, SqlGeneratorErrorKind},
    fixture::{GeneratedFieldValue, GeneratedFixture, GeneratedFixtureRow, GeneratedValue},
    model::{
        GeneratedSelectCase, GeneratedSelectIdentity, SelectArithmeticOperator, SelectBudgets,
        SelectComparisonOperator, SelectExpectedOutcome, SelectExpression, SelectFeature,
        SelectField, SelectFieldKind, SelectFunction, SelectGeneratorFamily, SelectOrderDirection,
        SelectOrderTarget, SelectOrderTerm, SelectPredicate, SelectProjection, SelectProvider,
        SelectQuery, SelectSnapshot, SelectViolation,
    },
    rng::{SELECT_GENERATOR_VERSION, SplitMix64, derive_sql_sub_seed},
};
use std::{collections::BTreeSet, fmt::Write as _};

const INTEGER_FIXTURE_VALUES: &[i64] = &[-2_147_483_648, -1, 0, 1, 24, 31, 43, 2_147_483_647];

/// Required pull-request root seeds from the 0.204 design.
pub const TIER_A_ROOT_SEEDS: &[u64] = &[0x1cdb_0204_0000_0001, 0x1cdb_0204_0000_0002];

/// Required valid cases generated per SELECT family and root.
pub const TIER_A_VALID_CASES_PER_FAMILY: u64 = 8;

/// Required invalid cases generated per violation family and root.
pub const TIER_A_INVALID_CASES_PER_VIOLATION: u64 = 4;

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

/// Required scheduled valid cases generated per SELECT family and root.
pub const TIER_C_VALID_CASES_PER_FAMILY: u64 = 32;

/// Required scheduled invalid cases generated per violation family and root.
pub const TIER_C_INVALID_CASES_PER_VIOLATION: u64 = 8;

/// Generate one valid current-contract SELECT case.
///
/// # Errors
///
/// Returns a typed generator error when snapshot facts, deterministic choices,
/// fixture values, query shape, rendering, or budgets are inconsistent.
pub fn generate_valid_select_case(
    snapshot: &SelectSnapshot,
    root_seed: u64,
    family: SelectGeneratorFamily,
    case_index: u64,
    budgets: SelectBudgets,
) -> Result<GeneratedSelectCase, SqlGeneratorError> {
    let sub_seed =
        derive_sql_sub_seed(SELECT_GENERATOR_VERSION, root_seed, family.id(), case_index)?;
    let mut rng = SplitMix64::new(sub_seed);
    let fixture = generate_fixture(
        snapshot,
        case_index,
        budgets,
        Some(family),
        generated_text_domain(family, case_index),
        &mut rng,
    )?;
    let query = generate_query(snapshot, family, case_index, budgets, &mut rng)?;
    let rendered_sql = render_generated_select_case(snapshot, &query, None, budgets)?;
    let features = collect_select_features(&query);
    let identity = generated_identity(
        snapshot,
        family.id(),
        root_seed,
        sub_seed,
        case_index,
        SelectProvider::SqliteReference,
    );
    let generated = GeneratedSelectCase::new(
        identity,
        family,
        None,
        snapshot.clone(),
        fixture,
        query,
        rendered_sql,
        SelectExpectedOutcome::Accepted,
        SelectProvider::SqliteReference,
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
    snapshot: &SelectSnapshot,
    root_seed: u64,
    violation: SelectViolation,
    case_index: u64,
    budgets: SelectBudgets,
) -> Result<GeneratedSelectCase, SqlGeneratorError> {
    let sub_seed = derive_sql_sub_seed(
        SELECT_GENERATOR_VERSION,
        root_seed,
        violation.id(),
        case_index,
    )?;
    let mut rng = SplitMix64::new(sub_seed);
    let fixture = generate_fixture(
        snapshot,
        case_index,
        budgets,
        None,
        GeneratedTextDomain::Unicode,
        &mut rng,
    )?;
    let query = invalid_base_query(snapshot, case_index, &mut rng)?;
    let rendered_sql = render_generated_select_case(snapshot, &query, Some(violation), budgets)?;
    let features = collect_select_features(&query);
    let identity = generated_identity(
        snapshot,
        violation.id(),
        root_seed,
        sub_seed,
        case_index,
        SelectProvider::RejectionInvariant,
    );
    let generated = GeneratedSelectCase::new(
        identity,
        SelectGeneratorFamily::ScalarProjection,
        Some(violation),
        snapshot.clone(),
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
    let family_id = generated
        .violation()
        .map_or_else(|| generated.family().id(), SelectViolation::id);
    if generated.identity().family_id() != family_id {
        return Err(SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated case family identity disagrees with its typed family",
        ));
    }
    let derived = derive_sql_sub_seed(
        SELECT_GENERATOR_VERSION,
        generated.identity().root_seed(),
        family_id,
        generated.identity().case_index(),
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
    generated
        .query()
        .validate(generated.snapshot(), generated.budgets())?;
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
        family_id,
        generated.identity().root_seed(),
        derived,
        generated.identity().case_index(),
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

fn generated_identity(
    snapshot: &SelectSnapshot,
    family_id: &str,
    root_seed: u64,
    sub_seed: u64,
    case_index: u64,
    provider: SelectProvider,
) -> GeneratedSelectIdentity {
    let provider_id = match provider {
        SelectProvider::RejectionInvariant => "rejection_invariant",
        SelectProvider::SqliteReference => "sqlite_reference",
    };
    let id = format!(
        "sql-select/v{SELECT_GENERATOR_VERSION}/{}/{family_id}/{root_seed:016x}/{case_index:016x}/{provider_id}",
        snapshot.fixture_family(),
    );

    GeneratedSelectIdentity::new(
        id,
        SELECT_GENERATOR_VERSION,
        family_id.to_string(),
        root_seed,
        sub_seed,
        case_index,
    )
}

fn generate_fixture(
    snapshot: &SelectSnapshot,
    case_index: u64,
    budgets: SelectBudgets,
    family: Option<SelectGeneratorFamily>,
    text_domain: GeneratedTextDomain,
    rng: &mut SplitMix64,
) -> Result<GeneratedFixture, SqlGeneratorError> {
    let family_case = case_index % TIER_A_VALID_CASES_PER_FAMILY;
    let row_count = match (case_index, family_case) {
        (24.., 6) if budgets.max_fixture_rows() >= 32 => 32,
        (24.., 7) if budgets.max_fixture_rows() >= 64 => 64,
        (_, 0 | 4) => 0,
        (_, 1 | 5) => 1,
        _ => 6_u64.saturating_add(rng.bounded(5)?),
    };
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
            let window_integer_ordinal = if family == Some(SelectGeneratorFamily::Window)
                && field.kind() == SelectFieldKind::Integer
            {
                integer_ordinal = integer_ordinal.saturating_add(1);
                Some(integer_ordinal)
            } else {
                None
            };
            let value = generated_field_value(
                field,
                case_index,
                row_index,
                text_domain,
                window_integer_ordinal,
                family.is_some_and(family_uses_repeated_fixture_values),
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

fn generated_field_value(
    field: &SelectField,
    case_index: u64,
    row_index: u64,
    text_domain: GeneratedTextDomain,
    window_integer_ordinal: Option<u64>,
    repeated_values: bool,
    rng: &mut SplitMix64,
) -> Result<GeneratedValue, SqlGeneratorError> {
    if field.nullable()
        && case_index
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
    let selector = case_index
        .wrapping_add(row_selector)
        .wrapping_add(u64::from(field.id()))
        .wrapping_add(random_selector);
    match field.kind() {
        SelectFieldKind::Boolean => Ok(GeneratedValue::Boolean(selector % 2 == 0)),
        SelectFieldKind::Integer => {
            if let Some(ordinal) = window_integer_ordinal {
                let value = row_index.checked_mul(ordinal).ok_or_else(|| {
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

const fn generated_text_domain(
    family: SelectGeneratorFamily,
    case_index: u64,
) -> GeneratedTextDomain {
    match (family, case_index % TIER_A_VALID_CASES_PER_FAMILY) {
        (SelectGeneratorFamily::Expression, 2 | 7)
        | (SelectGeneratorFamily::Predicate, 3)
        | (SelectGeneratorFamily::Window, 5) => GeneratedTextDomain::Ascii,
        _ => GeneratedTextDomain::Unicode,
    }
}

const fn family_uses_repeated_fixture_values(family: SelectGeneratorFamily) -> bool {
    matches!(
        family,
        SelectGeneratorFamily::Distinct
            | SelectGeneratorFamily::GlobalAggregate
            | SelectGeneratorFamily::GroupedAggregate
            | SelectGeneratorFamily::Having
    )
}

fn generate_query(
    snapshot: &SelectSnapshot,
    family: SelectGeneratorFamily,
    case_index: u64,
    budgets: SelectBudgets,
    rng: &mut SplitMix64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let query = match family {
        SelectGeneratorFamily::Distinct => distinct_query(snapshot, case_index)?,
        SelectGeneratorFamily::Expression => expression_query(snapshot, case_index, rng)?,
        SelectGeneratorFamily::GlobalAggregate => global_aggregate_query(snapshot, case_index)?,
        SelectGeneratorFamily::GroupedAggregate => grouped_aggregate_query(snapshot, case_index)?,
        SelectGeneratorFamily::Having => having_query(snapshot, case_index)?,
        SelectGeneratorFamily::Predicate => predicate_query(snapshot, case_index, rng)?,
        SelectGeneratorFamily::ScalarProjection => scalar_projection_query(snapshot, case_index)?,
        SelectGeneratorFamily::Window => window_query(snapshot, case_index, rng)?,
    };
    query.validate(snapshot, budgets)?;

    Ok(query)
}

fn distinct_query(
    snapshot: &SelectSnapshot,
    case_index: u64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let (projections, predicate, order, limit) = match case_index % TIER_A_VALID_CASES_PER_FAMILY {
        0 => (
            vec![projection(field(fields.text), None)],
            None,
            Vec::new(),
            None,
        ),
        1 => (
            vec![projection(field(fields.first_integer), None)],
            None,
            Vec::new(),
            None,
        ),
        2 => (
            vec![projection(field(fields.boolean), None)],
            None,
            Vec::new(),
            None,
        ),
        3 => (
            vec![
                projection(field(fields.text), None),
                projection(field(fields.first_integer), None),
            ],
            None,
            Vec::new(),
            None,
        ),
        4 => (
            vec![projection(field(fields.first_integer), None)],
            Some(comparison(
                field(fields.first_integer),
                SelectComparisonOperator::GreaterOrEqual,
                SelectExpression::literal(GeneratedValue::Integer(0)),
            )),
            Vec::new(),
            None,
        ),
        5 => (
            vec![projection(field(fields.text), Some("distinct_name"))],
            None,
            vec![SelectOrderTerm::alias(
                "distinct_name",
                SelectOrderDirection::Ascending,
            )],
            Some(5),
        ),
        6 => (
            vec![projection(
                function(SelectFunction::Lower, vec![field(fields.text)]),
                Some("normalized_name"),
            )],
            None,
            vec![SelectOrderTerm::alias(
                "normalized_name",
                SelectOrderDirection::Ascending,
            )],
            None,
        ),
        _ => (
            vec![
                projection(field(fields.first_integer), None),
                projection(field(fields.second_integer), None),
            ],
            None,
            Vec::new(),
            None,
        ),
    };

    Ok(SelectQuery::distinct(projections, predicate, order, limit))
}

fn global_aggregate_query(
    snapshot: &SelectSnapshot,
    case_index: u64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let (projections, predicate) = match case_index % TIER_A_VALID_CASES_PER_FAMILY {
        0 => (vec![projection(count_all(), Some("row_count"))], None),
        1 => (
            vec![projection(
                count_value(field(fields.first_integer), false),
                Some("value_count"),
            )],
            None,
        ),
        2 => (
            vec![projection(
                count_value(field(fields.first_integer), true),
                Some("distinct_value_count"),
            )],
            None,
        ),
        3 => (
            vec![projection(
                count_value(field(fields.text), false),
                Some("name_count"),
            )],
            None,
        ),
        4 => (
            vec![projection(count_value(field(fields.boolean), false), None)],
            None,
        ),
        5 => (
            vec![
                projection(count_all(), Some("row_count")),
                projection(
                    count_value(field(fields.text), true),
                    Some("distinct_names"),
                ),
            ],
            None,
        ),
        6 => (
            vec![projection(count_all(), Some("non_negative_count"))],
            Some(comparison(
                field(fields.first_integer),
                SelectComparisonOperator::GreaterOrEqual,
                SelectExpression::literal(GeneratedValue::Integer(0)),
            )),
        ),
        _ => (
            vec![
                projection(count_all(), Some("active_rows")),
                projection(
                    count_value(field(fields.first_integer), false),
                    Some("ages"),
                ),
            ],
            Some(comparison(
                field(fields.boolean),
                SelectComparisonOperator::Equal,
                SelectExpression::literal(GeneratedValue::Boolean(true)),
            )),
        ),
    };

    Ok(SelectQuery::global_aggregate(projections, predicate, None))
}

fn grouped_aggregate_query(
    snapshot: &SelectSnapshot,
    case_index: u64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let (group_key, aggregate_projections, predicate, group_alias) =
        match case_index % TIER_A_VALID_CASES_PER_FAMILY {
            0 => (
                field(fields.text),
                vec![projection(count_all(), Some("row_count"))],
                None,
                None,
            ),
            1 => (
                field(fields.first_integer),
                vec![projection(count_all(), Some("row_count"))],
                None,
                None,
            ),
            2 => (
                field(fields.boolean),
                vec![projection(count_all(), Some("row_count"))],
                None,
                None,
            ),
            3 => (
                field(fields.second_integer),
                vec![projection(count_all(), Some("row_count"))],
                None,
                None,
            ),
            4 => (
                field(fields.text),
                vec![projection(
                    count_value(field(fields.first_integer), false),
                    Some("age_count"),
                )],
                Some(comparison(
                    field(fields.first_integer),
                    SelectComparisonOperator::GreaterOrEqual,
                    SelectExpression::literal(GeneratedValue::Integer(0)),
                )),
                None,
            ),
            5 => (
                field(fields.first_integer),
                vec![projection(count_all(), Some("row_count"))],
                None,
                Some("group_value"),
            ),
            6 => (
                field(fields.text),
                vec![projection(
                    count_value(field(fields.first_integer), true),
                    Some("distinct_ages"),
                )],
                None,
                None,
            ),
            _ => (
                field(fields.first_integer),
                vec![
                    projection(count_all(), Some("row_count")),
                    projection(
                        count_value(field(fields.text), true),
                        Some("distinct_names"),
                    ),
                ],
                None,
                None,
            ),
        };
    Ok(grouped_count_query(
        group_key,
        aggregate_projections,
        predicate,
        None,
        group_alias,
    ))
}

fn having_query(
    snapshot: &SelectSnapshot,
    case_index: u64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let family_case = case_index % TIER_A_VALID_CASES_PER_FAMILY;
    let threshold = SelectExpression::literal(GeneratedValue::Integer(match family_case % 4 {
        0 => 0,
        2 => 2,
        _ => 1,
    }));
    let operator = if case_index.is_multiple_of(2) {
        SelectComparisonOperator::Greater
    } else {
        SelectComparisonOperator::GreaterOrEqual
    };

    if family_case < 4 {
        let counted = if family_case == 2 {
            count_value(field(fields.first_integer), true)
        } else {
            count_all()
        };
        let projections = if family_case == 3 {
            vec![
                projection(count_all(), Some("row_count")),
                projection(
                    count_value(field(fields.text), true),
                    Some("distinct_names"),
                ),
            ]
        } else {
            vec![projection(counted.clone(), Some("aggregate_value"))]
        };
        return Ok(SelectQuery::global_aggregate(
            projections,
            None,
            Some(comparison(counted, operator, threshold)),
        ));
    }

    let (group_key, aggregate) = match family_case {
        4 => (field(fields.text), count_all()),
        5 => (field(fields.first_integer), count_all()),
        6 => (field(fields.boolean), count_all()),
        _ => (
            field(fields.text),
            count_value(field(fields.first_integer), true),
        ),
    };
    Ok(grouped_count_query(
        group_key,
        vec![projection(aggregate.clone(), Some("aggregate_value"))],
        None,
        Some(comparison(aggregate, operator, threshold)),
        None,
    ))
}

fn grouped_count_query(
    group_key: SelectExpression,
    mut aggregate_projections: Vec<SelectProjection>,
    predicate: Option<SelectPredicate>,
    having: Option<SelectPredicate>,
    group_alias: Option<&str>,
) -> SelectQuery {
    let mut projections = Vec::with_capacity(1 + aggregate_projections.len());
    projections.push(projection(group_key.clone(), group_alias));
    projections.append(&mut aggregate_projections);
    let order = group_alias.map_or_else(
        || {
            vec![order_expression(
                group_key.clone(),
                SelectOrderDirection::Ascending,
            )]
        },
        |alias| {
            vec![SelectOrderTerm::alias(
                alias,
                SelectOrderDirection::Ascending,
            )]
        },
    );

    SelectQuery::grouped_aggregate(projections, predicate, vec![group_key], having, order, 16)
}

fn scalar_projection_query(
    snapshot: &SelectSnapshot,
    case_index: u64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let projections = match case_index % TIER_A_VALID_CASES_PER_FAMILY {
        0 => vec![projection(field(fields.text), None)],
        1 => vec![projection(field(fields.first_integer), Some("value_alias"))],
        2 => vec![projection(field(fields.boolean), None)],
        3 => vec![
            projection(field(fields.text), None),
            projection(field(fields.first_integer), None),
        ],
        4 => vec![
            projection(field(fields.second_integer), Some("rank_alias")),
            projection(field(fields.text), None),
        ],
        5 => vec![projection(field(fields.first_integer), None)],
        6 => vec![
            projection(field(fields.text), Some("label")),
            projection(field(fields.first_integer), None),
            projection(field(fields.boolean), None),
        ],
        _ => vec![projection(field(fields.text), Some("display_name"))],
    };

    Ok(SelectQuery::new(projections, None, Vec::new(), None, None))
}

fn predicate_query(
    snapshot: &SelectSnapshot,
    case_index: u64,
    rng: &mut SplitMix64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let literal = i64::try_from(rng.bounded(17)?).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated predicate literal does not fit i64",
        )
    })? - 8;
    let integer_literal = || SelectExpression::literal(GeneratedValue::Integer(literal));
    let boolean_literal = || SelectExpression::literal(GeneratedValue::Boolean(true));
    let predicate = match case_index % TIER_A_VALID_CASES_PER_FAMILY {
        0 => comparison(
            field(fields.first_integer),
            SelectComparisonOperator::GreaterOrEqual,
            integer_literal(),
        ),
        1 => SelectPredicate::And {
            left: Box::new(comparison(
                field(fields.first_integer),
                SelectComparisonOperator::GreaterOrEqual,
                integer_literal(),
            )),
            right: Box::new(comparison(
                field(fields.boolean),
                SelectComparisonOperator::Equal,
                boolean_literal(),
            )),
        },
        2 => SelectPredicate::PrefixLike {
            expression: field(fields.text),
            prefix: "a".to_string(),
            case_insensitive: false,
            negated: false,
        },
        3 => SelectPredicate::StartsWith {
            value: function(SelectFunction::Lower, vec![field(fields.text)]),
            prefix: SelectExpression::literal(GeneratedValue::Text("a".to_string())),
        },
        4 => comparison(
            field(fields.first_integer),
            SelectComparisonOperator::Greater,
            field(fields.second_integer),
        ),
        5 => SelectPredicate::IsNull {
            expression: function(
                SelectFunction::NullIf,
                vec![field(fields.first_integer), integer_literal()],
            ),
            negated: false,
        },
        6 => SelectPredicate::IsTruth {
            expression: field(fields.boolean),
            expected: true,
            negated: false,
        },
        _ => SelectPredicate::Or {
            left: Box::new(comparison(
                field(fields.first_integer),
                SelectComparisonOperator::Less,
                integer_literal(),
            )),
            right: Box::new(comparison(
                field(fields.boolean),
                SelectComparisonOperator::Equal,
                boolean_literal(),
            )),
        },
    };

    Ok(SelectQuery::new(
        vec![
            projection(field(fields.text), None),
            projection(field(fields.first_integer), None),
        ],
        Some(predicate),
        Vec::new(),
        None,
        None,
    ))
}

fn expression_query(
    snapshot: &SelectSnapshot,
    case_index: u64,
    rng: &mut SplitMix64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let small = i64::try_from(rng.bounded(5)?).map_err(|_| {
        SqlGeneratorError::new(
            SqlGeneratorErrorKind::InvalidCase,
            "generated expression literal does not fit i64",
        )
    })?;
    let family_case = case_index % TIER_A_VALID_CASES_PER_FAMILY;
    let expression = match family_case {
        0 => arithmetic(
            SelectArithmeticOperator::Add,
            field(fields.first_integer),
            SelectExpression::literal(GeneratedValue::Integer(small)),
        ),
        1 => arithmetic(
            SelectArithmeticOperator::Subtract,
            field(fields.first_integer),
            field(fields.second_integer),
        ),
        2 => function(SelectFunction::Lower, vec![field(fields.text)]),
        3 => function(SelectFunction::Length, vec![field(fields.text)]),
        4 => function(
            SelectFunction::NullIf,
            vec![field(fields.first_integer), field(fields.second_integer)],
        ),
        5 => function(
            SelectFunction::Coalesce,
            vec![
                function(
                    SelectFunction::NullIf,
                    vec![field(fields.text), field(fields.text)],
                ),
                SelectExpression::literal(GeneratedValue::Text("missing".to_string())),
            ],
        ),
        6 => SelectExpression::Case {
            condition: Box::new(comparison(
                field(fields.boolean),
                SelectComparisonOperator::Equal,
                SelectExpression::literal(GeneratedValue::Boolean(true)),
            )),
            then_expression: Box::new(field(fields.first_integer)),
            else_expression: Box::new(field(fields.second_integer)),
        },
        _ => SelectExpression::Case {
            condition: Box::new(comparison(
                field(fields.first_integer),
                SelectComparisonOperator::GreaterOrEqual,
                field(fields.second_integer),
            )),
            then_expression: Box::new(function(SelectFunction::Upper, vec![field(fields.text)])),
            else_expression: Box::new(function(SelectFunction::Lower, vec![field(fields.text)])),
        },
    };
    let expression = if case_index >= TIER_A_VALID_CASES_PER_FAMILY && family_case == 5 {
        function(SelectFunction::Upper, vec![expression])
    } else {
        expression
    };

    Ok(SelectQuery::new(
        vec![projection(expression, Some("generated_value"))],
        None,
        Vec::new(),
        None,
        None,
    ))
}

fn window_query(
    snapshot: &SelectSnapshot,
    case_index: u64,
    rng: &mut SplitMix64,
) -> Result<SelectQuery, SqlGeneratorError> {
    let fields = required_fields(snapshot)?;
    let ascending = if rng.weighted_index(&[1, 1])? == 0 {
        SelectOrderDirection::Ascending
    } else {
        SelectOrderDirection::Descending
    };
    let projections = vec![
        projection(field(fields.text), Some("display_name")),
        projection(field(fields.first_integer), Some("sort_value")),
    ];
    let (order, limit, offset) = match case_index % TIER_A_VALID_CASES_PER_FAMILY {
        0 => (
            vec![
                order_expression(field(fields.first_integer), SelectOrderDirection::Ascending),
                order_expression(field(fields.text), SelectOrderDirection::Ascending),
            ],
            Some(3),
            None,
        ),
        1 => (
            vec![
                order_expression(
                    field(fields.first_integer),
                    SelectOrderDirection::Descending,
                ),
                order_expression(field(fields.text), SelectOrderDirection::Ascending),
            ],
            Some(4),
            None,
        ),
        2 => (
            vec![SelectOrderTerm::alias(
                "sort_value",
                SelectOrderDirection::Ascending,
            )],
            Some(5),
            Some(1),
        ),
        3 => (
            vec![
                order_expression(field(fields.first_integer), SelectOrderDirection::Ascending),
                order_expression(field(fields.text), SelectOrderDirection::Ascending),
            ],
            Some(6),
            None,
        ),
        4 => (
            vec![
                order_expression(field(fields.text), ascending),
                order_expression(field(fields.first_integer), SelectOrderDirection::Ascending),
            ],
            None,
            None,
        ),
        5 => (
            vec![
                order_expression(
                    function(SelectFunction::Lower, vec![field(fields.text)]),
                    SelectOrderDirection::Ascending,
                ),
                order_expression(field(fields.text), SelectOrderDirection::Ascending),
                order_expression(field(fields.first_integer), SelectOrderDirection::Ascending),
            ],
            Some(7),
            None,
        ),
        6 => (
            vec![
                order_expression(
                    field(fields.second_integer),
                    SelectOrderDirection::Descending,
                ),
                order_expression(field(fields.text), SelectOrderDirection::Ascending),
            ],
            Some(8),
            None,
        ),
        _ => (
            vec![
                order_expression(field(fields.boolean), SelectOrderDirection::Ascending),
                order_expression(field(fields.text), SelectOrderDirection::Ascending),
                order_expression(field(fields.first_integer), SelectOrderDirection::Ascending),
            ],
            Some(4),
            Some(2),
        ),
    };

    Ok(SelectQuery::new(projections, None, order, limit, offset))
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
        Some(1),
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
        SelectViolation::InvalidClauseOrder => format!(
            "SELECT {text} FROM {entity} OFFSET {} LIMIT {}",
            query.offset().unwrap_or(1),
            query.limit().unwrap_or(1),
        ),
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
        SelectExpression::Count { argument, distinct } => {
            let argument = match argument {
                Some(argument) => render_expression(snapshot, argument)?,
                None => "*".to_string(),
            };
            let distinct = if *distinct { "DISTINCT " } else { "" };
            Ok(format!("COUNT({distinct}{argument})"))
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
        SelectExpression::Count { argument, distinct } => {
            features.insert(SelectFeature::Aggregate);
            if *distinct {
                features.insert(SelectFeature::AggregateDistinct);
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
    first_integer: &'a SelectField,
    second_integer: &'a SelectField,
    boolean: &'a SelectField,
}

fn required_fields(snapshot: &SelectSnapshot) -> Result<RequiredFields<'_>, SqlGeneratorError> {
    let text = snapshot
        .first_query_field(SelectFieldKind::Text)
        .ok_or_else(|| missing_field_kind(SelectFieldKind::Text))?;
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
    }
}

fn count_value(expression: SelectExpression, distinct: bool) -> SelectExpression {
    SelectExpression::Count {
        argument: Some(Box::new(expression)),
        distinct,
    }
}

fn arithmetic(
    operator: SelectArithmeticOperator,
    left: SelectExpression,
    right: SelectExpression,
) -> SelectExpression {
    SelectExpression::Arithmetic {
        operator,
        left: Box::new(left),
        right: Box::new(right),
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
