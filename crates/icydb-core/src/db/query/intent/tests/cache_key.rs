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
            plan::expr::{Expr, FieldId, Function},
        },
    },
    model::{entity::EntityModel, field::FieldKind},
    testing::PLAN_ENTITY_TAG,
    traits::{EntitySchema, FieldValue, Path},
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
        ("name", FieldKind::Text),
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
                right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Uint(1))),
            }),
            right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Uint(5))),
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
                right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Uint(2))),
            }),
            right: Box::new(crate::db::query::plan::expr::Expr::Literal(Value::Uint(5))),
        })
        .expect("widened grouped having should append");

    assert_ne!(
        left.structural_cache_key(),
        right.structural_cache_key(),
        "grouped having expressions must remain part of shared grouped cache identity",
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
