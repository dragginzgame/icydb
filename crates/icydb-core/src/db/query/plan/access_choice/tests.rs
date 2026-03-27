use super::{CandidateEvaluation, CandidateScore};
use crate::{
    db::{
        predicate::CoercionId,
        query::plan::access_choice::{
            evaluate_multi_lookup_candidate, evaluate_prefix_compare_candidate,
            evaluate_range_candidate,
        },
        schema::SchemaInfo,
    },
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
        index::{IndexExpression, IndexKeyItem, IndexModel},
    },
    testing::entity_model_from_static,
    value::Value,
};

static ACCESS_CHOICE_FIELDS: [FieldModel; 2] = [
    FieldModel::new("id", FieldKind::Ulid),
    FieldModel::new("email", FieldKind::Text),
];
static ACCESS_CHOICE_RAW_INDEX_FIELDS: [&str; 1] = ["email"];
static ACCESS_CHOICE_RAW_INDEXES: [IndexModel; 1] = [IndexModel::new(
    "access_choice::email_raw",
    "access_choice::store",
    &ACCESS_CHOICE_RAW_INDEX_FIELDS,
    false,
)];
static ACCESS_CHOICE_EXPRESSION_INDEX_FIELDS: [&str; 1] = ["email"];
static ACCESS_CHOICE_EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
static ACCESS_CHOICE_EXPRESSION_INDEXES: [IndexModel; 1] = [IndexModel::new_with_key_items(
    "access_choice::email_lower",
    "access_choice::store",
    &ACCESS_CHOICE_EXPRESSION_INDEX_FIELDS,
    &ACCESS_CHOICE_EXPRESSION_INDEX_KEY_ITEMS,
    false,
)];
static ACCESS_CHOICE_UPPER_EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Upper("email"))];
static ACCESS_CHOICE_UPPER_EXPRESSION_INDEXES: [IndexModel; 1] = [IndexModel::new_with_key_items(
    "access_choice::email_upper",
    "access_choice::store",
    &ACCESS_CHOICE_EXPRESSION_INDEX_FIELDS,
    &ACCESS_CHOICE_UPPER_EXPRESSION_INDEX_KEY_ITEMS,
    false,
)];
static ACCESS_CHOICE_UNSUPPORTED_EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::LowerTrim(
        "email",
    ))];
static ACCESS_CHOICE_UNSUPPORTED_EXPRESSION_INDEXES: [IndexModel; 1] =
    [IndexModel::new_with_key_items(
        "access_choice::email_lower_trim",
        "access_choice::store",
        &ACCESS_CHOICE_EXPRESSION_INDEX_FIELDS,
        &ACCESS_CHOICE_UNSUPPORTED_EXPRESSION_INDEX_KEY_ITEMS,
        false,
    )];
static ACCESS_CHOICE_INDEX_REFS: [&IndexModel; 2] = [
    &ACCESS_CHOICE_RAW_INDEXES[0],
    &ACCESS_CHOICE_EXPRESSION_INDEXES[0],
];
static ACCESS_CHOICE_MODEL: EntityModel = entity_model_from_static(
    "access_choice::entity",
    "AccessChoiceEntity",
    &ACCESS_CHOICE_FIELDS[0],
    &ACCESS_CHOICE_FIELDS,
    &ACCESS_CHOICE_INDEX_REFS,
);
static ACCESS_CHOICE_RANGE_FIELDS: [FieldModel; 3] = [
    FieldModel::new("id", FieldKind::Ulid),
    FieldModel::new("city", FieldKind::Text),
    FieldModel::new("email", FieldKind::Text),
];
static ACCESS_CHOICE_RANGE_INDEX_FIELDS: [&str; 2] = ["city", "email"];
static ACCESS_CHOICE_RANGE_INDEXES: [IndexModel; 1] = [IndexModel::new(
    "access_choice::city_email",
    "access_choice::store",
    &ACCESS_CHOICE_RANGE_INDEX_FIELDS,
    false,
)];
static ACCESS_CHOICE_RANGE_INDEX_REFS: [&IndexModel; 1] = [&ACCESS_CHOICE_RANGE_INDEXES[0]];
static ACCESS_CHOICE_RANGE_MODEL: EntityModel = entity_model_from_static(
    "access_choice::range_entity",
    "AccessChoiceRangeEntity",
    &ACCESS_CHOICE_RANGE_FIELDS[0],
    &ACCESS_CHOICE_RANGE_FIELDS,
    &ACCESS_CHOICE_RANGE_INDEX_REFS,
);

fn schema() -> SchemaInfo {
    SchemaInfo::from_entity_model(&ACCESS_CHOICE_MODEL)
        .expect("access_choice test model should produce schema info")
}

fn range_schema() -> SchemaInfo {
    SchemaInfo::from_entity_model(&ACCESS_CHOICE_RANGE_MODEL)
        .expect("access_choice range test model should produce schema info")
}

#[test]
fn evaluate_prefix_compare_candidate_accepts_text_casefold_expression_index() {
    let cmp = crate::db::predicate::ComparePredicate::with_coercion(
        "email",
        crate::db::predicate::CompareOp::Eq,
        Value::Text("ALICE@Example.Com".to_string()),
        CoercionId::TextCasefold,
    );

    let evaluation =
        evaluate_prefix_compare_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], &schema(), &cmp);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: true,
        }),
    );
}

#[test]
fn evaluate_prefix_compare_candidate_accepts_text_casefold_upper_expression_index() {
    let cmp = crate::db::predicate::ComparePredicate::with_coercion(
        "email",
        crate::db::predicate::CompareOp::Eq,
        Value::Text("ALICE@Example.Com".to_string()),
        CoercionId::TextCasefold,
    );

    let evaluation = evaluate_prefix_compare_candidate(
        &ACCESS_CHOICE_UPPER_EXPRESSION_INDEXES[0],
        &schema(),
        &cmp,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: true,
        }),
    );
}

#[test]
fn evaluate_prefix_compare_candidate_rejects_text_casefold_on_raw_field_index() {
    let cmp = crate::db::predicate::ComparePredicate::with_coercion(
        "email",
        crate::db::predicate::CompareOp::Eq,
        Value::Text("ALICE@Example.Com".to_string()),
        CoercionId::TextCasefold,
    );

    let evaluation =
        evaluate_prefix_compare_candidate(&ACCESS_CHOICE_RAW_INDEXES[0], &schema(), &cmp);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(super::AccessChoiceRejectedReason::LeadingFieldMismatch),
    );
}

#[test]
fn evaluate_prefix_compare_candidate_rejects_text_casefold_for_unsupported_expression_kind() {
    let cmp = crate::db::predicate::ComparePredicate::with_coercion(
        "email",
        crate::db::predicate::CompareOp::Eq,
        Value::Text("ALICE@Example.Com".to_string()),
        CoercionId::TextCasefold,
    );

    let evaluation = evaluate_prefix_compare_candidate(
        &ACCESS_CHOICE_UNSUPPORTED_EXPRESSION_INDEXES[0],
        &schema(),
        &cmp,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(super::AccessChoiceRejectedReason::LeadingFieldMismatch),
    );
}

#[test]
fn evaluate_multi_lookup_candidate_accepts_text_casefold_expression_index() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::In,
            Value::List(vec![
                Value::Text("ALICE@example.com".to_string()),
                Value::Text("bob@EXAMPLE.com".to_string()),
            ]),
            CoercionId::TextCasefold,
        ),
    );

    let evaluation = evaluate_multi_lookup_candidate(
        &ACCESS_CHOICE_EXPRESSION_INDEXES[0],
        &schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: true,
        }),
    );
}

#[test]
fn evaluate_multi_lookup_candidate_accepts_text_casefold_upper_expression_index() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::In,
            Value::List(vec![
                Value::Text("ALICE@example.com".to_string()),
                Value::Text("bob@EXAMPLE.com".to_string()),
            ]),
            CoercionId::TextCasefold,
        ),
    );

    let evaluation = evaluate_multi_lookup_candidate(
        &ACCESS_CHOICE_UPPER_EXPRESSION_INDEXES[0],
        &schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: true,
        }),
    );
}

#[test]
fn evaluate_multi_lookup_candidate_rejects_mixed_literal_set_for_expression_index() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::In,
            Value::List(vec![
                Value::Text("ALICE@example.com".to_string()),
                Value::Uint(7),
            ]),
            CoercionId::TextCasefold,
        ),
    );

    let evaluation = evaluate_multi_lookup_candidate(
        &ACCESS_CHOICE_EXPRESSION_INDEXES[0],
        &schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(super::AccessChoiceRejectedReason::InLiteralIncompatible),
    );
}

#[test]
fn evaluate_multi_lookup_candidate_rejects_text_casefold_for_unsupported_expression_kind() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::In,
            Value::List(vec![
                Value::Text("ALICE@example.com".to_string()),
                Value::Text("bob@EXAMPLE.com".to_string()),
            ]),
            CoercionId::TextCasefold,
        ),
    );

    let evaluation = evaluate_multi_lookup_candidate(
        &ACCESS_CHOICE_UNSUPPORTED_EXPRESSION_INDEXES[0],
        &schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(super::AccessChoiceRejectedReason::LeadingFieldMismatch),
    );
}

#[test]
fn evaluate_range_candidate_rejects_gt_for_expression_index() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::Gt,
            Value::Text("alice@example.com".to_string()),
            CoercionId::Strict,
        ),
    );

    let evaluation =
        evaluate_range_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], &schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(super::AccessChoiceRejectedReason::OperatorNotRangeSupported),
    );
}

#[test]
fn evaluate_range_candidate_rejects_starts_with_for_expression_index() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::StartsWith,
            Value::Text("alice".to_string()),
            CoercionId::Strict,
        ),
    );

    let evaluation =
        evaluate_range_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], &schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(super::AccessChoiceRejectedReason::OperatorNotRangeSupported),
    );
}

#[test]
fn evaluate_range_candidate_accepts_text_casefold_starts_with_for_expression_index() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::StartsWith,
            Value::Text("ALICE".to_string()),
            CoercionId::TextCasefold,
        ),
    );

    let evaluation =
        evaluate_range_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], &schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 0,
            exact: true,
        }),
    );
}

#[test]
fn evaluate_range_candidate_accepts_text_casefold_starts_with_for_upper_expression_index() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::StartsWith,
            Value::Text("ALICE".to_string()),
            CoercionId::TextCasefold,
        ),
    );

    let evaluation = evaluate_range_candidate(
        &ACCESS_CHOICE_UPPER_EXPRESSION_INDEXES[0],
        &schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 0,
            exact: true,
        }),
    );
}

#[test]
fn evaluate_range_candidate_rejects_text_casefold_starts_with_for_unsupported_expression_kind() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::StartsWith,
            Value::Text("ALICE".to_string()),
            CoercionId::TextCasefold,
        ),
    );

    let evaluation = evaluate_range_candidate(
        &ACCESS_CHOICE_UNSUPPORTED_EXPRESSION_INDEXES[0],
        &schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(super::AccessChoiceRejectedReason::LeadingFieldMismatch),
    );
}

#[test]
fn evaluate_range_candidate_rejects_empty_text_casefold_starts_with_prefix() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::StartsWith,
            Value::Text(String::new()),
            CoercionId::TextCasefold,
        ),
    );

    let evaluation =
        evaluate_range_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], &schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(super::AccessChoiceRejectedReason::StartsWithPrefixInvalid),
    );
}

#[test]
fn evaluate_range_candidate_accepts_contiguous_eq_prefix_then_range_field() {
    let predicate = crate::db::predicate::Predicate::And(vec![
        crate::db::predicate::Predicate::Compare(
            crate::db::predicate::ComparePredicate::with_coercion(
                "city",
                crate::db::predicate::CompareOp::Eq,
                Value::Text("paris".to_string()),
                CoercionId::Strict,
            ),
        ),
        crate::db::predicate::Predicate::Compare(
            crate::db::predicate::ComparePredicate::with_coercion(
                "email",
                crate::db::predicate::CompareOp::Gt,
                Value::Text("alice@example.com".to_string()),
                CoercionId::Strict,
            ),
        ),
    ]);

    let evaluation =
        evaluate_range_candidate(&ACCESS_CHOICE_RANGE_INDEXES[0], &range_schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: false,
        }),
    );
}

#[test]
fn evaluate_range_candidate_rejects_eq_range_conflict_on_same_field() {
    let predicate = crate::db::predicate::Predicate::And(vec![
        crate::db::predicate::Predicate::Compare(
            crate::db::predicate::ComparePredicate::with_coercion(
                "city",
                crate::db::predicate::CompareOp::Eq,
                Value::Text("paris".to_string()),
                CoercionId::Strict,
            ),
        ),
        crate::db::predicate::Predicate::Compare(
            crate::db::predicate::ComparePredicate::with_coercion(
                "email",
                crate::db::predicate::CompareOp::Eq,
                Value::Text("alice@example.com".to_string()),
                CoercionId::Strict,
            ),
        ),
        crate::db::predicate::Predicate::Compare(
            crate::db::predicate::ComparePredicate::with_coercion(
                "email",
                crate::db::predicate::CompareOp::Gt,
                Value::Text("anna@example.com".to_string()),
                CoercionId::Strict,
            ),
        ),
    ]);

    let evaluation =
        evaluate_range_candidate(&ACCESS_CHOICE_RANGE_INDEXES[0], &range_schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(super::AccessChoiceRejectedReason::EqRangeConflict),
    );
}
