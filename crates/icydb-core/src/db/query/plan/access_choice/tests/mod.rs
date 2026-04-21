//! Module: db::query::plan::access_choice::tests
//! Covers access-choice classification and route-selection heuristics.
//! Does not own: production access-choice behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use super::{
    evaluator::{
        chosen_selection_reason, evaluate_multi_lookup_candidate,
        evaluate_prefix_compare_candidate, evaluate_range_candidate,
    },
    model::{
        AccessChoiceRankingReason, AccessChoiceRejectedReason, AccessChoiceSelectedReason,
        CandidateEvaluation, CandidateScore,
    },
};
use crate::{
    db::{predicate::CoercionId, schema::SchemaInfo},
    model::{
        entity::EntityModel,
        field::{FieldKind, FieldModel},
        index::{IndexExpression, IndexKeyItem, IndexModel},
    },
    testing::entity_model_from_static,
    value::Value,
};

static ACCESS_CHOICE_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("email", FieldKind::Text),
];
static ACCESS_CHOICE_RAW_INDEX_FIELDS: [&str; 1] = ["email"];
static ACCESS_CHOICE_RAW_INDEXES: [IndexModel; 1] = [IndexModel::generated(
    "access_choice::email_raw",
    "access_choice::store",
    &ACCESS_CHOICE_RAW_INDEX_FIELDS,
    false,
)];
static ACCESS_CHOICE_EXPRESSION_INDEX_FIELDS: [&str; 1] = ["email"];
static ACCESS_CHOICE_EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
static ACCESS_CHOICE_EXPRESSION_INDEXES: [IndexModel; 1] = [IndexModel::generated_with_key_items(
    "access_choice::email_lower",
    "access_choice::store",
    &ACCESS_CHOICE_EXPRESSION_INDEX_FIELDS,
    &ACCESS_CHOICE_EXPRESSION_INDEX_KEY_ITEMS,
    false,
)];
static ACCESS_CHOICE_UPPER_EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Upper("email"))];
static ACCESS_CHOICE_UPPER_EXPRESSION_INDEXES: [IndexModel; 1] =
    [IndexModel::generated_with_key_items(
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
    [IndexModel::generated_with_key_items(
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
    0,
    &ACCESS_CHOICE_FIELDS,
    &ACCESS_CHOICE_INDEX_REFS,
);
static ACCESS_CHOICE_RANGE_FIELDS: [FieldModel; 3] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated("city", FieldKind::Text),
    FieldModel::generated("email", FieldKind::Text),
];
static ACCESS_CHOICE_RANGE_INDEX_FIELDS: [&str; 2] = ["city", "email"];
static ACCESS_CHOICE_RANGE_INDEXES: [IndexModel; 1] = [IndexModel::generated(
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
    0,
    &ACCESS_CHOICE_RANGE_FIELDS,
    &ACCESS_CHOICE_RANGE_INDEX_REFS,
);

fn schema() -> &'static SchemaInfo {
    SchemaInfo::cached_for_entity_model(&ACCESS_CHOICE_MODEL)
}

fn range_schema() -> &'static SchemaInfo {
    SchemaInfo::cached_for_entity_model(&ACCESS_CHOICE_RANGE_MODEL)
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
        evaluate_prefix_compare_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], schema(), &cmp);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: true,
            filtered: false,
            range_bound_count: 0,
            order_compatible: false,
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
        schema(),
        &cmp,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: true,
            filtered: false,
            range_bound_count: 0,
            order_compatible: false,
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
        evaluate_prefix_compare_candidate(&ACCESS_CHOICE_RAW_INDEXES[0], schema(), &cmp);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch),
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
        schema(),
        &cmp,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch),
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

    let evaluation =
        evaluate_multi_lookup_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: true,
            filtered: false,
            range_bound_count: 0,
            order_compatible: false,
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
        schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: true,
            filtered: false,
            range_bound_count: 0,
            order_compatible: false,
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

    let evaluation =
        evaluate_multi_lookup_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(AccessChoiceRejectedReason::InLiteralIncompatible),
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
        schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch),
    );
}

#[test]
fn evaluate_range_candidate_rejects_strict_gt_for_expression_index() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::Gt,
            Value::Text("alice@example.com".to_string()),
            CoercionId::Strict,
        ),
    );

    let evaluation =
        evaluate_range_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(AccessChoiceRejectedReason::OperatorNotRangeSupported),
    );
}

#[test]
fn evaluate_range_candidate_accepts_text_casefold_gt_for_expression_index() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::Gt,
            Value::Text("ALICE@example.com".to_string()),
            CoercionId::TextCasefold,
        ),
    );

    let evaluation =
        evaluate_range_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 0,
            exact: true,
            filtered: false,
            range_bound_count: 1,
            order_compatible: false,
        }),
    );
}

#[test]
fn evaluate_range_candidate_accepts_text_casefold_lt_for_upper_expression_index() {
    let predicate = crate::db::predicate::Predicate::Compare(
        crate::db::predicate::ComparePredicate::with_coercion(
            "email",
            crate::db::predicate::CompareOp::Lt,
            Value::Text("ALICE@example.com".to_string()),
            CoercionId::TextCasefold,
        ),
    );

    let evaluation = evaluate_range_candidate(
        &ACCESS_CHOICE_UPPER_EXPRESSION_INDEXES[0],
        schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 0,
            exact: true,
            filtered: false,
            range_bound_count: 1,
            order_compatible: false,
        }),
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
        evaluate_range_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(AccessChoiceRejectedReason::OperatorNotRangeSupported),
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
        evaluate_range_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 0,
            exact: true,
            filtered: false,
            range_bound_count: 1,
            order_compatible: false,
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
        schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 0,
            exact: true,
            filtered: false,
            range_bound_count: 1,
            order_compatible: false,
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
        schema(),
        &predicate,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(AccessChoiceRejectedReason::LeadingFieldMismatch),
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
        evaluate_range_candidate(&ACCESS_CHOICE_EXPRESSION_INDEXES[0], schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(AccessChoiceRejectedReason::StartsWithPrefixInvalid),
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
        evaluate_range_candidate(&ACCESS_CHOICE_RANGE_INDEXES[0], range_schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: false,
            filtered: false,
            range_bound_count: 1,
            order_compatible: false,
        }),
    );
}

#[test]
fn evaluate_range_candidate_tracks_two_sided_bounds_for_same_range_field() {
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
        crate::db::predicate::Predicate::Compare(
            crate::db::predicate::ComparePredicate::with_coercion(
                "email",
                crate::db::predicate::CompareOp::Lt,
                Value::Text("morgan@example.com".to_string()),
                CoercionId::Strict,
            ),
        ),
    ]);

    let evaluation =
        evaluate_range_candidate(&ACCESS_CHOICE_RANGE_INDEXES[0], range_schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: false,
            filtered: false,
            range_bound_count: 2,
            order_compatible: false,
        }),
        "range scoring should record both bounded sides when one canonical range slot has lower and upper predicates",
    );
}

#[test]
fn chosen_selection_reason_prefers_filtered_candidate_before_order_compatibility() {
    let chosen = CandidateScore {
        prefix_len: 1,
        exact: false,
        filtered: true,
        range_bound_count: 0,
        order_compatible: false,
    };
    let competing = [CandidateScore {
        prefix_len: 1,
        exact: false,
        filtered: false,
        range_bound_count: 0,
        order_compatible: true,
    }];

    assert_eq!(
        chosen_selection_reason(
            super::model::AccessChoiceFamily::Prefix,
            chosen,
            &competing,
            false,
        ),
        AccessChoiceSelectedReason::Ranked(AccessChoiceRankingReason::FilteredPredicatePreferred),
        "filtered candidate preference should surface explicitly in access-choice reason codes",
    );
}

#[test]
fn chosen_selection_reason_prefers_stronger_range_bounds_before_order_compatibility() {
    let chosen = CandidateScore {
        prefix_len: 1,
        exact: false,
        filtered: false,
        range_bound_count: 2,
        order_compatible: false,
    };
    let competing = [CandidateScore {
        prefix_len: 1,
        exact: false,
        filtered: false,
        range_bound_count: 1,
        order_compatible: true,
    }];

    assert_eq!(
        chosen_selection_reason(
            super::model::AccessChoiceFamily::Range,
            chosen,
            &competing,
            false,
        ),
        AccessChoiceSelectedReason::Ranked(AccessChoiceRankingReason::StrongerRangeBoundsPreferred,),
        "range-bound strength should surface before downstream order compatibility in access-choice reason codes",
    );
}

#[test]
fn chosen_selection_reason_prefers_lower_residual_burden_before_order_compatibility() {
    let chosen = CandidateScore {
        prefix_len: 1,
        exact: false,
        filtered: true,
        range_bound_count: 0,
        order_compatible: false,
    };
    let competing = [CandidateScore {
        prefix_len: 1,
        exact: false,
        filtered: true,
        range_bound_count: 0,
        order_compatible: true,
    }];

    assert_eq!(
        chosen_selection_reason(
            super::model::AccessChoiceFamily::Prefix,
            chosen,
            &competing,
            true,
        ),
        AccessChoiceSelectedReason::Ranked(AccessChoiceRankingReason::ResidualBurdenPreferred),
        "residual-burden preference should surface before downstream order compatibility when structural scores already tie",
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
        evaluate_range_candidate(&ACCESS_CHOICE_RANGE_INDEXES[0], range_schema(), &predicate);

    assert_eq!(
        evaluation,
        CandidateEvaluation::Rejected(AccessChoiceRejectedReason::EqRangeConflict),
    );
}
