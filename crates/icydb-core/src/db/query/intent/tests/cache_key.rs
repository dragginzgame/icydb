//! Module: db::query::intent::tests::cache_key
//! Covers structural cache-key normalization across equivalent fluent query
//! shapes.
//! Does not own: shared query-intent test fixtures outside this focused cache
//! identity surface.
//! Boundary: exercises query-intent cache identity from the owner `tests/`
//! boundary rather than from the leaf implementation file.

use crate::{
    db::{
        CoercionId, CompareOp, ComparePredicate, MissingRowPolicy, Predicate,
        query::{
            intent::{Query, StructuralQuery, StructuralQueryCacheKey, model::QueryModel},
            plan::expr::{
                BinaryOp, CaseWhenArm, Expr, FieldId, Function,
                canonicalize_grouped_having_bool_expr,
            },
        },
    },
    model::{entity::EntityModel, field::FieldKind},
    testing::PLAN_ENTITY_TAG,
    traits::{EntitySchema, Path, RuntimeValueEncode},
    types::Ulid,
    value::Value,
};
use icydb_derive::FieldProjection;
use serde::Deserialize;

#[derive(Clone, Debug, Default, Deserialize, FieldProjection, PartialEq)]
struct CacheKeyEntity {
    id: Ulid,
    name: String,
}

struct CacheKeyCanister;

impl Path for CacheKeyCanister {
    const PATH: &'static str = concat!(module_path!(), "::CacheKeyCanister");
}

impl crate::traits::CanisterKind for CacheKeyCanister {
    const COMMIT_MEMORY_ID: u8 = crate::testing::test_commit_memory_id();
}

struct CacheKeyStore;

impl Path for CacheKeyStore {
    const PATH: &'static str = concat!(module_path!(), "::CacheKeyStore");
}

impl crate::traits::StoreKind for CacheKeyStore {
    type Canister = CacheKeyCanister;
}

crate::test_entity_schema! {
    ident = CacheKeyEntity,
    id = Ulid,
    id_field = id,
    entity_name = "CacheKeyEntity",
    entity_tag = PLAN_ENTITY_TAG,
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("name", FieldKind::Text { max_len: None }),
    ],
    indexes = [],
    store = CacheKeyStore,
    canister = CacheKeyCanister,
}

fn basic_model() -> &'static EntityModel {
    <CacheKeyEntity as EntitySchema>::MODEL
}

#[test]
fn structural_query_cache_key_matches_for_identical_scalar_queries() {
    let left = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(crate::db::Predicate::eq(
            "name".to_string(),
            Value::Text("Ada".to_string()),
        ))
        .order_term(crate::db::asc("name"))
        .limit(2);
    let right = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("name"))
        .filter_predicate(crate::db::Predicate::eq(
            "name".to_string(),
            Value::Text("Ada".to_string()),
        ))
        .limit(2);

    assert_eq!(
        left.structural().structural_cache_key(),
        right.structural().structural_cache_key(),
        "equivalent scalar fluent queries must normalize onto one shared cache key",
    );
}

#[test]
fn structural_query_cache_key_distinguishes_order_direction() {
    let asc = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("name"));
    let desc = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .order_term(crate::db::desc("name"));

    assert_ne!(
        asc.structural_cache_key(),
        desc.structural_cache_key(),
        "order direction must remain part of shared query cache identity",
    );
}

#[test]
fn structural_query_cache_key_distinguishes_expression_owned_filter_expr() {
    let left = QueryModel::<Ulid>::new(basic_model(), MissingRowPolicy::Ignore).filter_expr(
        Expr::FunctionCall {
            function: Function::StartsWith,
            args: vec![
                Expr::FunctionCall {
                    function: Function::Replace,
                    args: vec![
                        Expr::Field(FieldId::new("name")),
                        Expr::Literal(Value::Text("a".to_string())),
                        Expr::Literal(Value::Text("A".to_string())),
                    ],
                },
                Expr::Literal(Value::Text("A".to_string())),
            ],
        },
    );
    let right = QueryModel::<Ulid>::new(basic_model(), MissingRowPolicy::Ignore).filter_expr(
        Expr::FunctionCall {
            function: Function::StartsWith,
            args: vec![
                Expr::FunctionCall {
                    function: Function::Replace,
                    args: vec![
                        Expr::Field(FieldId::new("name")),
                        Expr::Literal(Value::Text("a".to_string())),
                        Expr::Literal(Value::Text("A".to_string())),
                    ],
                },
                Expr::Literal(Value::Text("B".to_string())),
            ],
        },
    );

    assert_ne!(
        StructuralQueryCacheKey::from_query_model(&left),
        StructuralQueryCacheKey::from_query_model(&right),
        "expression-owned scalar filter expressions must remain part of shared query cache identity",
    );
}

#[test]
fn structural_query_cache_key_treats_equivalent_expression_owned_boolean_shapes_as_identical() {
    let left = QueryModel::<Ulid>::new(basic_model(), MissingRowPolicy::Ignore).filter_expr(
        Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("name"))),
                right: Box::new(Expr::Literal(Value::Text("Ada".to_string()))),
            }),
            right: Box::new(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("id"))),
                right: Box::new(Expr::Literal(Ulid::default().to_value())),
            }),
        },
    );
    let right = QueryModel::<Ulid>::new(basic_model(), MissingRowPolicy::Ignore).filter_expr(
        Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("id"))),
                right: Box::new(Expr::Literal(Ulid::default().to_value())),
            }),
            right: Box::new(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("name"))),
                right: Box::new(Expr::Literal(Value::Text("Ada".to_string()))),
            }),
        },
    );

    assert_eq!(
        StructuralQueryCacheKey::from_query_model(&left),
        StructuralQueryCacheKey::from_query_model(&right),
        "equivalent normalized expression-owned boolean filters must share one structural query cache key",
    );
}

#[test]
fn structural_query_cache_key_distinguishes_unary_boolean_filter_expr() {
    let positive = QueryModel::<Ulid>::new(basic_model(), MissingRowPolicy::Ignore).filter_expr(
        Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Eq,
            left: Box::new(Expr::Field(FieldId::new("name"))),
            right: Box::new(Expr::Literal(Value::Text("Ada".to_string()))),
        },
    );
    let negated =
        QueryModel::<Ulid>::new(basic_model(), MissingRowPolicy::Ignore).filter_expr(Expr::Unary {
            op: crate::db::query::plan::expr::UnaryOp::Not,
            expr: Box::new(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("name"))),
                right: Box::new(Expr::Literal(Value::Text("Ada".to_string()))),
            }),
        });

    assert_ne!(
        StructuralQueryCacheKey::from_query_model(&positive),
        StructuralQueryCacheKey::from_query_model(&negated),
        "unary boolean operators must remain part of structural query cache identity",
    );
}

#[test]
fn structural_query_cache_key_ignores_predicate_fingerprint_when_filter_expr_exists() {
    let model = QueryModel::<Ulid>::new(basic_model(), MissingRowPolicy::Ignore).filter_expr(
        Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("name"))),
                right: Box::new(Expr::Literal(Value::Text("Ada".to_string()))),
            }),
            right: Box::new(Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("id"))),
                right: Box::new(Expr::Literal(Ulid::default().to_value())),
            }),
        },
    );

    assert_eq!(
        model.structural_cache_key_with_normalized_predicate_fingerprint(Some([0x11; 32])),
        model.structural_cache_key_with_normalized_predicate_fingerprint(Some([0x22; 32])),
        "canonical scalar filter expressions must be the sole structural filter identity owner when present",
    );
}

#[test]
fn structural_query_cache_key_keeps_predicate_identity_when_filter_expr_is_absent() {
    let model = QueryModel::<Ulid>::new(basic_model(), MissingRowPolicy::Ignore);

    assert_ne!(
        model.structural_cache_key_with_normalized_predicate_fingerprint(Some([0x11; 32])),
        model.structural_cache_key_with_normalized_predicate_fingerprint(Some([0x22; 32])),
        "predicate-only queries must still key shared structural identity by predicate when no semantic filter expression exists",
    );
}

#[test]
fn structural_query_cache_key_treats_extrema_distinct_as_semantic_noop() {
    let plain_min = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .aggregate(crate::db::min_by("name"));
    let distinct_min = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .aggregate(crate::db::min_by("name").distinct());

    assert_eq!(
        plain_min.structural_cache_key(),
        distinct_min.structural_cache_key(),
        "shared structural query cache identity must follow aggregate semantics: MIN(DISTINCT field) and MIN(field) are equivalent",
    );
}

#[test]
fn structural_query_cache_key_keeps_count_distinct_semantically_distinct() {
    let plain_count = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name"));
    let distinct_count = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .aggregate(crate::db::count_by("name").distinct());

    assert_ne!(
        plain_count.structural_cache_key(),
        distinct_count.structural_cache_key(),
        "shared structural query cache identity must keep COUNT(DISTINCT field) distinct from COUNT(field)",
    );
}

#[test]
fn structural_query_cache_key_keeps_sum_distinct_semantically_distinct() {
    let plain_sum = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("name"));
    let distinct_sum = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("name").distinct());

    assert_ne!(
        plain_sum.structural_cache_key(),
        distinct_sum.structural_cache_key(),
        "shared structural query cache identity must keep SUM(DISTINCT field) distinct from SUM(field)",
    );
}

#[test]
fn structural_query_cache_key_keeps_aggregate_filter_expr_distinct() {
    let active_filter = Expr::Binary {
        op: crate::db::query::plan::expr::BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("name"))),
        right: Box::new(Expr::Literal(Value::Text("Ada".to_string()))),
    };
    let archived_filter = Expr::Binary {
        op: crate::db::query::plan::expr::BinaryOp::Eq,
        left: Box::new(Expr::Field(FieldId::new("name"))),
        right: Box::new(Expr::Literal(Value::Text("Grace".to_string()))),
    };
    let active_sum = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("name").with_filter_expr(active_filter));
    let archived_sum = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .aggregate(crate::db::sum("name").with_filter_expr(archived_filter));

    assert_ne!(
        active_sum.structural_cache_key(),
        archived_sum.structural_cache_key(),
        "shared structural query cache identity must keep aggregate-local FILTER expressions distinct",
    );
}

#[test]
fn structural_query_cache_key_distinguishes_grouped_having_expr() {
    let left = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_expr(crate::db::query::plan::expr::Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Gt,
            left: Box::new(crate::db::query::plan::expr::Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Add,
                left: Box::new(crate::db::query::plan::expr::Expr::Aggregate(
                    crate::db::count(),
                )),
                right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Nat(1))),
            }),
            right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Nat(5))),
        })
        .expect("widened grouped having should append");
    let right = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_expr(crate::db::query::plan::expr::Expr::Binary {
            op: crate::db::query::plan::expr::BinaryOp::Gt,
            left: Box::new(crate::db::query::plan::expr::Expr::Binary {
                op: crate::db::query::plan::expr::BinaryOp::Add,
                left: Box::new(crate::db::query::plan::expr::Expr::Aggregate(
                    crate::db::count(),
                )),
                right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Nat(2))),
            }),
            right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Nat(5))),
        })
        .expect("widened grouped having should append");

    assert_ne!(
        left.structural_cache_key(),
        right.structural_cache_key(),
        "grouped having expressions must remain part of shared grouped cache identity",
    );
}

#[test]
fn structural_query_cache_key_treats_equivalent_grouped_having_boolean_shapes_as_identical() {
    let left = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_group("name", CompareOp::Eq, Value::Text("Ada".to_string()))
        .expect("grouped HAVING group-field compare should append")
        .having_aggregate(0, CompareOp::Gt, Value::Nat(0))
        .expect("grouped HAVING aggregate compare should append");
    let right = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_aggregate(0, CompareOp::Gt, Value::Nat(0))
        .expect("grouped HAVING aggregate compare should append")
        .having_group("name", CompareOp::Eq, Value::Text("Ada".to_string()))
        .expect("grouped HAVING group-field compare should append");

    assert_eq!(
        left.structural_cache_key(),
        right.structural_cache_key(),
        "canonical-equivalent grouped HAVING boolean shapes must share one structural cache key",
    );
}

#[test]
fn structural_query_cache_key_treats_explicit_else_grouped_case_as_canonical_equivalent() {
    let case = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_expr(Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Binary {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Aggregate(crate::db::count())),
                    right: Box::new(Expr::Literal(Value::Nat(1))),
                },
                Expr::Literal(Value::Bool(true)),
            )],
            else_expr: Box::new(Expr::Literal(Value::Bool(false))),
        })
        .expect("grouped searched CASE HAVING should append");
    let canonical = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_expr(Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(Expr::FunctionCall {
                function: Function::Coalesce,
                args: vec![
                    Expr::Binary {
                        op: BinaryOp::Gt,
                        left: Box::new(Expr::Aggregate(crate::db::count())),
                        right: Box::new(Expr::Literal(Value::Nat(1))),
                    },
                    Expr::Literal(Value::Bool(false)),
                ],
            }),
            right: Box::new(Expr::Literal(Value::Bool(false))),
        })
        .expect("canonical grouped boolean HAVING should append");

    assert_eq!(
        case.structural_cache_key(),
        canonical.structural_cache_key(),
        "explicit-ELSE grouped searched CASE HAVING must share one structural cache key with its shipped canonical grouped boolean form",
    );
}

#[test]
fn structural_query_cache_key_treats_omitted_else_grouped_case_as_explicit_null_equivalent() {
    let omitted_else = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_expr(Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Binary {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Aggregate(crate::db::count())),
                    right: Box::new(Expr::Literal(Value::Nat(1))),
                },
                Expr::Literal(Value::Bool(true)),
            )],
            else_expr: Box::new(Expr::Literal(Value::Null)),
        })
        .expect("grouped searched CASE HAVING without ELSE should append");
    let explicit_null_canonical = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_expr(canonicalize_grouped_having_bool_expr(Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Binary {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Aggregate(crate::db::count())),
                    right: Box::new(Expr::Literal(Value::Nat(1))),
                },
                Expr::Literal(Value::Bool(true)),
            )],
            else_expr: Box::new(Expr::Literal(Value::Null)),
        }))
        .expect("canonical grouped boolean HAVING with explicit ELSE NULL should append");

    assert_eq!(
        omitted_else.structural_cache_key(),
        explicit_null_canonical.structural_cache_key(),
        "omitted-ELSE grouped searched CASE HAVING must share one structural cache key with the explicit ELSE NULL grouped boolean family once canonicalization proof succeeds",
    );
}

#[test]
fn structural_query_cache_key_keeps_omitted_else_grouped_case_distinct_from_false_family() {
    let omitted_else = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_expr(Expr::Case {
            when_then_arms: vec![CaseWhenArm::new(
                Expr::Binary {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Aggregate(crate::db::count())),
                    right: Box::new(Expr::Literal(Value::Nat(1))),
                },
                Expr::Literal(Value::Bool(true)),
            )],
            else_expr: Box::new(Expr::Literal(Value::Null)),
        })
        .expect("grouped searched CASE HAVING without ELSE should append");
    let canonical_false = StructuralQuery::new(basic_model(), MissingRowPolicy::Ignore)
        .group_by("name")
        .expect("grouped query should accept grouped field")
        .aggregate(crate::db::count())
        .having_expr(Expr::Binary {
            op: BinaryOp::Or,
            left: Box::new(Expr::FunctionCall {
                function: Function::Coalesce,
                args: vec![
                    Expr::Binary {
                        op: BinaryOp::Gt,
                        left: Box::new(Expr::Aggregate(crate::db::count())),
                        right: Box::new(Expr::Literal(Value::Nat(1))),
                    },
                    Expr::Literal(Value::Bool(false)),
                ],
            }),
            right: Box::new(Expr::Literal(Value::Bool(false))),
        })
        .expect("canonical grouped boolean HAVING should append");

    assert_ne!(
        omitted_else.structural_cache_key(),
        canonical_false.structural_cache_key(),
        "omitted-ELSE grouped searched CASE HAVING must stay distinct from the explicit-ELSE FALSE grouped boolean family",
    );
}

#[test]
fn structural_query_cache_key_treats_equivalent_in_list_permutations_as_identical() {
    let left = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::in_(
            "name".to_string(),
            vec![
                Value::Text("Ada".to_string()),
                Value::Text("Bob".to_string()),
                Value::Text("Cara".to_string()),
            ],
        )),
    );
    let right = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::in_(
            "name".to_string(),
            vec![
                Value::Text("Cara".to_string()),
                Value::Text("Ada".to_string()),
                Value::Text("Bob".to_string()),
            ],
        )),
    );

    assert_eq!(
        left.structural().structural_cache_key(),
        right.structural().structural_cache_key(),
        "equivalent IN-list permutations must collapse onto one shared structural query cache key",
    );
}

#[test]
fn structural_query_cache_key_treats_duplicate_in_list_literals_as_identical() {
    let left = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::in_(
            "name".to_string(),
            vec![
                Value::Text("Ada".to_string()),
                Value::Text("Bob".to_string()),
                Value::Text("Ada".to_string()),
            ],
        )),
    );
    let right = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::in_(
            "name".to_string(),
            vec![
                Value::Text("Bob".to_string()),
                Value::Text("Ada".to_string()),
            ],
        )),
    );

    assert_eq!(
        left.structural().structural_cache_key(),
        right.structural().structural_cache_key(),
        "duplicate literals in one canonical IN-list must not grow distinct shared cache keys",
    );
}

#[test]
fn structural_query_cache_key_treats_same_field_or_eq_and_in_as_identical() {
    let left = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Eq,
                Value::Text("Ada".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Eq,
                Value::Text("Bob".to_string()),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "name",
                CompareOp::Eq,
                Value::Text("Ada".to_string()),
                CoercionId::Strict,
            )),
        ]),
    );
    let right = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::In,
            Value::List(vec![
                Value::Text("Bob".to_string()),
                Value::Text("Ada".to_string()),
            ]),
            CoercionId::Strict,
        )),
    );

    assert_eq!(
        left.structural().structural_cache_key(),
        right.structural().structural_cache_key(),
        "same-field OR-of-EQ and IN forms must collapse onto one shared structural query cache key",
    );
}

#[test]
fn structural_query_cache_key_distinguishes_in_and_not_in() {
    let in_list = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::in_(
            "name".to_string(),
            vec![
                Value::Text("Ada".to_string()),
                Value::Text("Bob".to_string()),
            ],
        )),
    );
    let not_in_list = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::not_in(
            "name".to_string(),
            vec![
                Value::Text("Ada".to_string()),
                Value::Text("Bob".to_string()),
            ],
        )),
    );

    assert_ne!(
        in_list.structural().structural_cache_key(),
        not_in_list.structural().structural_cache_key(),
        "shared structural query cache identity must keep IN and NOT IN semantically distinct",
    );
}

#[test]
fn structural_query_cache_key_treats_duplicate_and_children_as_identical() {
    let left = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq(
                "name".to_string(),
                Value::Text("Ada".to_string()),
            )),
            Predicate::Compare(ComparePredicate::eq(
                "name".to_string(),
                Value::Text("Ada".to_string()),
            )),
        ]),
    );
    let right = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::eq(
            "name".to_string(),
            Value::Text("Ada".to_string()),
        )),
    );

    assert_eq!(
        left.structural().structural_cache_key(),
        right.structural().structural_cache_key(),
        "duplicate AND children must collapse onto one shared structural query cache key",
    );
}

#[test]
fn structural_query_cache_key_treats_duplicate_or_children_as_identical() {
    let left = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Or(vec![
            Predicate::Compare(ComparePredicate::eq(
                "name".to_string(),
                Value::Text("Ada".to_string()),
            )),
            Predicate::Compare(ComparePredicate::eq(
                "name".to_string(),
                Value::Text("Ada".to_string()),
            )),
        ]),
    );
    let right = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::eq(
            "name".to_string(),
            Value::Text("Ada".to_string()),
        )),
    );

    assert_eq!(
        left.structural().structural_cache_key(),
        right.structural().structural_cache_key(),
        "duplicate OR children must collapse onto one shared structural query cache key",
    );
}

#[test]
fn structural_query_cache_key_treats_equal_bounds_as_eq() {
    let left = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate::gte(
                "name".to_string(),
                Value::Text("Ada".to_string()),
            )),
            Predicate::Compare(ComparePredicate::lte(
                "name".to_string(),
                Value::Text("Ada".to_string()),
            )),
        ]),
    );
    let right = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::eq(
            "name".to_string(),
            Value::Text("Ada".to_string()),
        )),
    );

    assert_eq!(
        left.structural().structural_cache_key(),
        right.structural().structural_cache_key(),
        "equal lower and upper bounds must collapse onto one shared structural query cache key",
    );
}

#[test]
fn structural_query_cache_key_treats_conflicting_equalities_as_false() {
    let left = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::And(vec![
            Predicate::Compare(ComparePredicate::eq(
                "name".to_string(),
                Value::Text("Ada".to_string()),
            )),
            Predicate::Compare(ComparePredicate::eq(
                "name".to_string(),
                Value::Text("Bob".to_string()),
            )),
        ]),
    );
    let right =
        Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(Predicate::False);

    assert_eq!(
        left.structural().structural_cache_key(),
        right.structural().structural_cache_key(),
        "conflicting equalities must collapse onto the same shared structural query cache key as FALSE",
    );
}

#[test]
fn structural_query_cache_key_treats_text_casefold_case_variants_as_identical() {
    let lower = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("ada".to_string()),
            CoercionId::TextCasefold,
        )),
    );
    let upper = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("ADA".to_string()),
            CoercionId::TextCasefold,
        )),
    );

    assert_eq!(
        lower.structural().structural_cache_key(),
        upper.structural().structural_cache_key(),
        "text-casefold case-only literal variants must collapse onto one shared structural query cache key",
    );
}

#[test]
fn structural_query_cache_key_distinguishes_strict_from_text_casefold_coercion() {
    let strict = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("ada".to_string()),
            CoercionId::Strict,
        )),
    );
    let casefold = Query::<CacheKeyEntity>::new(MissingRowPolicy::Ignore).filter_predicate(
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::Eq,
            Value::Text("ada".to_string()),
            CoercionId::TextCasefold,
        )),
    );

    assert_ne!(
        strict.structural().structural_cache_key(),
        casefold.structural().structural_cache_key(),
        "shared structural query cache identity must keep strict and text-casefold coercion distinct",
    );
}
