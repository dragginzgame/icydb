//! Module: db::query::plan::tests::order
//! Covers planner-owned scalar order-expression rendering and canonical
//! index-order contracts.
//! Does not own: grouped order policy or executor slot materialization.
//! Boundary: keeps shared planner order-expression helper coverage under the
//! planner `tests/` boundary instead of one leaf rendering helper.

use crate::{
    db::query::plan::{
        DeterministicSecondaryIndexOrderMatch, GroupedIndexOrderMatch, OrderDirection, OrderSpec,
        deterministic_secondary_index_order_compatibility,
        deterministic_secondary_index_order_satisfied,
        expr::{
            BinaryOp, Expr, FieldId, Function, render_supported_order_expr,
            supported_order_expr_field, supported_order_expr_is_plain_field,
        },
        grouped_index_order_match, index_order_terms,
    },
    model::index::{IndexExpression, IndexKeyItem, IndexModel},
};

const EXPRESSION_INDEX_FIELDS: [&str; 1] = ["name"];
const EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("name"))];
const EXPRESSION_INDEX_MODEL: IndexModel = IndexModel::generated_with_key_items(
    "order_term_tests::idx_name_lower",
    "order_term_tests::Store",
    &EXPRESSION_INDEX_FIELDS,
    &EXPRESSION_INDEX_KEY_ITEMS,
    false,
);
const GROUPED_INDEX_FIELDS: [&str; 2] = ["group", "rank"];
const GROUPED_INDEX_MODEL: IndexModel = IndexModel::generated(
    "order_term_tests::idx_group_rank",
    "order_term_tests::Store",
    &GROUPED_INDEX_FIELDS,
    false,
);
const SCALAR_ORDER_INDEX_FIELDS: [&str; 2] = ["rank", "name"];
const SCALAR_ORDER_INDEX_MODEL: IndexModel = IndexModel::generated(
    "order_term_tests::idx_rank_name",
    "order_term_tests::Store",
    &SCALAR_ORDER_INDEX_FIELDS,
    false,
);

fn field(name: &str) -> Expr {
    Expr::Field(FieldId::new(name))
}

fn int(value: i64) -> Expr {
    Expr::Literal(crate::value::Value::Int(value))
}

fn text(value: &str) -> Expr {
    Expr::Literal(crate::value::Value::Text(value.to_string()))
}

fn function(function: Function, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall { function, args }
}

fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

#[test]
fn supported_order_expr_helpers_round_trip_supported_scalar_text_terms() {
    let lower = function(Function::Lower, vec![field("name")]);
    assert_eq!(
        supported_order_expr_field(&lower)
            .expect("lower(name) should preserve one field leaf")
            .as_str(),
        "name"
    );
    assert_eq!(
        render_supported_order_expr(&lower),
        Some("LOWER(name)".to_string())
    );

    let upper = function(Function::Upper, vec![field("email")]);
    assert_eq!(
        render_supported_order_expr(&upper),
        Some("UPPER(email)".to_string())
    );

    let trim = function(Function::Trim, vec![field("name")]);
    assert_eq!(
        render_supported_order_expr(&trim),
        Some("TRIM(name)".to_string())
    );

    let ltrim = function(Function::Ltrim, vec![field("name")]);
    assert_eq!(
        render_supported_order_expr(&ltrim),
        Some("LTRIM(name)".to_string())
    );

    let rtrim = function(Function::Rtrim, vec![field("name")]);
    assert_eq!(
        render_supported_order_expr(&rtrim),
        Some("RTRIM(name)".to_string())
    );

    let length = function(Function::Length, vec![field("name")]);
    assert_eq!(
        render_supported_order_expr(&length),
        Some("LENGTH(name)".to_string())
    );

    let left = function(Function::Left, vec![field("name"), int(2)]);
    assert_eq!(
        render_supported_order_expr(&left),
        Some("LEFT(name, 2)".to_string())
    );

    let position = function(Function::Position, vec![text("a"), field("name")]);
    assert_eq!(
        render_supported_order_expr(&position),
        Some("POSITION('a', name)".to_string())
    );
}

#[test]
fn supported_order_expr_helpers_round_trip_bounded_numeric_terms() {
    let arithmetic = binary(BinaryOp::Add, field("age"), field("rank"));
    assert_eq!(
        render_supported_order_expr(&arithmetic),
        Some("age + rank".to_string())
    );

    let rounded = function(
        Function::Round,
        vec![binary(BinaryOp::Add, field("age"), field("rank")), int(2)],
    );
    assert_eq!(
        render_supported_order_expr(&rounded),
        Some("ROUND(age + rank, 2)".to_string())
    );

    let nested = function(
        Function::Round,
        vec![
            binary(
                BinaryOp::Div,
                binary(BinaryOp::Add, field("age"), field("rank")),
                binary(BinaryOp::Add, field("age"), int(1)),
            ),
            int(2),
        ],
    );
    assert_eq!(
        render_supported_order_expr(&nested),
        Some("ROUND((age + rank) / (age + 1), 2)".to_string())
    );
}

#[test]
fn supported_order_expr_helpers_round_trip_nested_scalar_wrappers() {
    let abs = function(
        Function::Abs,
        vec![binary(BinaryOp::Sub, field("age"), int(30))],
    );
    assert_eq!(
        render_supported_order_expr(&abs),
        Some("ABS(age - 30)".to_string())
    );

    let coalesce = function(
        Function::Coalesce,
        vec![
            function(Function::NullIf, vec![field("age"), int(20)]),
            int(99),
        ],
    );
    assert_eq!(
        render_supported_order_expr(&coalesce),
        Some("COALESCE(NULLIF(age, 20), 99)".to_string())
    );

    let nested = function(
        Function::Lower,
        vec![function(
            Function::Coalesce,
            vec![field("name"), text("fallback")],
        )],
    );
    assert_eq!(
        render_supported_order_expr(&nested),
        Some("LOWER(COALESCE(name, 'fallback'))".to_string())
    );
}

#[test]
fn supported_order_expr_helpers_distinguish_plain_fields_from_computed_terms() {
    let direct_field = field("id");
    assert!(
        supported_order_expr_is_plain_field(&direct_field),
        "plain field order terms must stay on the schema-field path",
    );

    let lower = function(Function::Lower, vec![field("name")]);
    assert!(
        !supported_order_expr_is_plain_field(&lower),
        "computed order terms must stay on the expression path",
    );
}

#[test]
fn index_order_terms_use_canonical_key_item_text() {
    assert_eq!(
        index_order_terms(&EXPRESSION_INDEX_MODEL),
        vec!["LOWER(name)".to_string()]
    );
}

#[test]
fn grouped_index_order_contract_classifies_full_and_suffix_matches() {
    let full_order = OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("group", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
        ],
    };
    let full_contract = full_order
        .grouped_index_order_contract()
        .expect("uniform-direction grouped ORDER BY should build one grouped index-order contract");
    let suffix_order = OrderSpec {
        fields: vec![crate::db::query::plan::OrderTerm::field(
            "rank",
            OrderDirection::Asc,
        )],
    };
    let suffix_contract = suffix_order.grouped_index_order_contract().expect(
        "uniform-direction grouped ORDER BY suffix should build one grouped index-order contract",
    );
    let index_terms = index_order_terms(&GROUPED_INDEX_MODEL);

    assert_eq!(
        full_contract.classify_index_match(&index_terms, 0),
        GroupedIndexOrderMatch::Full,
    );
    assert_eq!(
        grouped_index_order_match(&suffix_contract, &GROUPED_INDEX_MODEL, 1),
        GroupedIndexOrderMatch::Suffix,
    );
}

#[test]
fn deterministic_secondary_order_compatibility_classifies_suffix_and_none() {
    let full_order = OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("rank", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("name", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    };
    let full_contract = full_order
        .deterministic_secondary_order_contract("id")
        .expect("deterministic secondary order should require terminal primary-key tie-break");
    let full_compatibility = deterministic_secondary_index_order_compatibility(
        &full_contract,
        &SCALAR_ORDER_INDEX_MODEL,
        0,
    );

    assert_eq!(
        full_compatibility.match_kind(),
        DeterministicSecondaryIndexOrderMatch::Suffix,
    );
    assert!(full_compatibility.is_satisfied());

    let suffix_order = OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("name", OrderDirection::Desc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
        ],
    };
    let suffix_contract = suffix_order
        .deterministic_secondary_order_contract("id")
        .expect("DESC deterministic secondary order should still classify by index terms");

    assert!(deterministic_secondary_index_order_satisfied(
        &suffix_contract,
        &SCALAR_ORDER_INDEX_MODEL,
        1,
    ));

    let mismatch_order = OrderSpec {
        fields: vec![
            crate::db::query::plan::OrderTerm::field("group", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
    };
    let mismatch_contract = mismatch_order
        .deterministic_secondary_order_contract("id")
        .expect("mismatch order still has a deterministic secondary shape");
    let mismatch_compatibility = deterministic_secondary_index_order_compatibility(
        &mismatch_contract,
        &SCALAR_ORDER_INDEX_MODEL,
        0,
    );

    assert_eq!(
        mismatch_compatibility.match_kind(),
        DeterministicSecondaryIndexOrderMatch::None,
    );
    assert!(!mismatch_compatibility.is_satisfied());
}
