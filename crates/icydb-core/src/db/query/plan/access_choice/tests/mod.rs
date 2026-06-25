//! Module: db::query::plan::access_choice::tests
//! Covers access-choice classification and route-selection heuristics.
//! Does not own: production access-choice behavior outside this test module.
//! Boundary: verifies this module API while keeping fixture details internal.

use super::{
    evaluator::{
        chosen_selection_reason, evaluate_index_candidate, evaluate_multi_lookup_candidate,
        evaluate_prefix_compare_candidate, evaluate_range_candidate,
    },
    model::{
        AccessChoiceFamily, AccessChoiceRankingReason, AccessChoiceRejectedReason,
        AccessChoiceSelectedReason, CandidateEvaluation, CandidateScore,
    },
};
use crate::{
    db::{
        access::{AccessPlan, SemanticIndexAccessContract},
        predicate::{CompareOp, ComparePredicate, MissingRowPolicy, Predicate},
        query::plan::{
            AccessPlannedQuery, LoadSpec, LogicalPlanningInputs, OrderDirection, OrderSpec,
            QueryMode, build_logical_plan, expr::ProjectionSelection,
            logical_query_from_logical_inputs,
        },
    },
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
    FieldModel::generated("email", FieldKind::Text { max_len: None }),
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
    FieldModel::generated("city", FieldKind::Text { max_len: None }),
    FieldModel::generated("email", FieldKind::Text { max_len: None }),
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
    SchemaInfo::cached_for_generated_entity_model(&ACCESS_CHOICE_MODEL)
}

fn range_schema() -> &'static SchemaInfo {
    SchemaInfo::cached_for_generated_entity_model(&ACCESS_CHOICE_RANGE_MODEL)
}

fn canonical_order(fields: &[(&str, OrderDirection)]) -> OrderSpec {
    OrderSpec {
        fields: fields
            .iter()
            .map(|(field, direction)| crate::db::query::plan::OrderTerm::field(*field, *direction))
            .collect(),
    }
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
fn evaluate_multi_lookup_candidate_projects_primary_key_suffix_order_compatibility() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "city",
        CompareOp::In,
        Value::List(vec![
            Value::Text("Paris".to_string()),
            Value::Text("Berlin".to_string()),
        ]),
        CoercionId::Strict,
    ));
    let order = canonical_order(&[("email", OrderDirection::Asc), ("id", OrderDirection::Asc)]);
    let index = SemanticIndexAccessContract::model_only_from_generated_index(
        ACCESS_CHOICE_RANGE_INDEXES[0],
    );

    let evaluation = evaluate_index_candidate(
        AccessChoiceFamily::MultiLookup,
        index,
        range_schema(),
        Some(&predicate),
        Some(&order),
        false,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: false,
            filtered: false,
            range_bound_count: 0,
            order_compatible: true,
        }),
        "multi-lookup should report ordered when the consumed IN slot leaves the requested suffix",
    );
}

#[test]
fn evaluate_multi_lookup_candidate_rejects_order_compatibility_when_suffix_is_blocked() {
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "city",
        CompareOp::In,
        Value::List(vec![
            Value::Text("Paris".to_string()),
            Value::Text("Berlin".to_string()),
        ]),
        CoercionId::Strict,
    ));
    let order = canonical_order(&[("id", OrderDirection::Asc)]);
    let index = SemanticIndexAccessContract::model_only_from_generated_index(
        ACCESS_CHOICE_RANGE_INDEXES[0],
    );

    let evaluation = evaluate_index_candidate(
        AccessChoiceFamily::MultiLookup,
        index,
        range_schema(),
        Some(&predicate),
        Some(&order),
        false,
    );

    assert_eq!(
        evaluation,
        CandidateEvaluation::Eligible(CandidateScore {
            prefix_len: 1,
            exact: false,
            filtered: false,
            range_bound_count: 0,
            order_compatible: false,
        }),
        "multi-lookup must not claim ORDER BY id when an unconstrained index slot blocks the primary-key suffix",
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
                Value::Nat64(7),
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
fn residual_burden_profile_uses_residual_filter_shape_authority() {
    use crate::db::query::plan::ResidualFilterShape;

    assert_eq!(
        super::ResidualBurdenProfile::kind_rank_for_residual_shape(ResidualFilterShape::Absent),
        0,
    );
    assert_eq!(
        super::ResidualBurdenProfile::kind_rank_for_residual_shape(ResidualFilterShape::Predicate),
        1,
    );
    assert_eq!(
        super::ResidualBurdenProfile::kind_rank_for_residual_shape(ResidualFilterShape::Expression),
        2,
    );
    assert_eq!(
        super::ResidualBurdenProfile::kind_rank_for_residual_shape(
            ResidualFilterShape::ExpressionAndPredicate,
        ),
        2,
    );
}

#[test]
fn chosen_selection_reason_prefers_order_compatible_multi_lookup_candidate() {
    let chosen = CandidateScore {
        prefix_len: 1,
        exact: false,
        filtered: false,
        range_bound_count: 0,
        order_compatible: true,
    };
    let competing = [CandidateScore {
        prefix_len: 1,
        exact: false,
        filtered: false,
        range_bound_count: 0,
        order_compatible: false,
    }];

    assert_eq!(
        chosen_selection_reason(AccessChoiceFamily::MultiLookup, chosen, &competing, false,),
        AccessChoiceSelectedReason::Ranked(AccessChoiceRankingReason::OrderCompatiblePreferred),
        "multi-lookup should share the same order-compatible tie-break policy as other index routes",
    );
}

#[test]
fn residual_free_rerank_skips_same_score_candidate_scan() {
    let values = vec![
        Value::Text("alice@example.com".to_string()),
        Value::Text("bob@example.com".to_string()),
        Value::Text("carol@example.com".to_string()),
    ];
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::In,
        Value::List(values.clone()),
        CoercionId::Strict,
    ));
    let schema = schema();
    let logical_inputs = LogicalPlanningInputs::new(
        QueryMode::Load(LoadSpec::new()),
        None,
        false,
        None,
        false,
        None,
        None,
    );
    let logical = build_logical_plan(
        schema,
        logical_query_from_logical_inputs(
            logical_inputs,
            Some(predicate),
            MissingRowPolicy::Ignore,
        ),
    );
    let index =
        SemanticIndexAccessContract::model_only_from_generated_index(ACCESS_CHOICE_RAW_INDEXES[0]);
    let access: AccessPlan<Value> = AccessPlan::index_multi_lookup_from_contract(index, values);
    let plan = AccessPlannedQuery::from_logical_access_and_projection(
        logical,
        access,
        ProjectionSelection::All,
    );
    let visible_indexes = ACCESS_CHOICE_INDEX_REFS
        .iter()
        .copied()
        .map(|index| SemanticIndexAccessContract::model_only_from_generated_index(*index))
        .collect::<Vec<_>>();

    assert!(
        super::residual_burden_for_plan(&plan).is_empty(),
        "the selected multi-lookup route proves the full IN predicate",
    );

    super::reset_same_score_competing_candidate_scan_count_for_tests();
    let reranked = super::rerank_access_plan_by_residual_burden_from_authority(
        &ACCESS_CHOICE_MODEL,
        visible_indexes.as_slice(),
        schema,
        &plan,
    );

    assert!(reranked.is_none());
    assert_eq!(
        super::same_score_competing_candidate_scan_count_for_tests(),
        0,
        "residual-free routes cannot improve on residual burden and should not rescan candidates",
    );
}

#[test]
fn chosen_residual_burden_preference_scans_same_score_candidates_once() {
    let values = vec![
        Value::Text("alice@example.com".to_string()),
        Value::Text("bob@example.com".to_string()),
    ];
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "email",
        CompareOp::In,
        Value::List(values.clone()),
        CoercionId::TextCasefold,
    ));
    let schema = schema();
    let logical_inputs = LogicalPlanningInputs::new(
        QueryMode::Load(LoadSpec::new()),
        None,
        false,
        None,
        false,
        None,
        None,
    );
    let logical = build_logical_plan(
        schema,
        logical_query_from_logical_inputs(
            logical_inputs,
            Some(predicate),
            MissingRowPolicy::Ignore,
        ),
    );
    let chosen_index = SemanticIndexAccessContract::model_only_from_generated_index(
        ACCESS_CHOICE_EXPRESSION_INDEXES[0],
    );
    let access: AccessPlan<Value> =
        AccessPlan::index_multi_lookup_from_contract(chosen_index, values);
    let plan = AccessPlannedQuery::from_logical_access_and_projection(
        logical,
        access,
        ProjectionSelection::All,
    );
    let visible_indexes = [
        SemanticIndexAccessContract::model_only_from_generated_index(
            ACCESS_CHOICE_EXPRESSION_INDEXES[0],
        ),
        SemanticIndexAccessContract::model_only_from_generated_index(
            ACCESS_CHOICE_UPPER_EXPRESSION_INDEXES[0],
        ),
    ];

    super::reset_same_score_competing_candidate_scan_count_for_tests();
    let preferred = super::chosen_access_prefers_lower_residual_burden(
        &ACCESS_CHOICE_MODEL,
        visible_indexes.as_slice(),
        schema,
        &plan,
    );

    assert!(
        !preferred,
        "equivalent expression indexes should not produce a residual-burden preference",
    );
    assert_eq!(
        super::same_score_competing_candidate_scan_count_for_tests(),
        1,
        "chosen residual-burden preference should inspect same-score candidates once",
    );

    super::reset_same_score_competing_candidate_scan_count_for_tests();
    let _snapshot = super::project_access_choice_explain_snapshot_from_authority(
        &ACCESS_CHOICE_MODEL,
        visible_indexes.as_slice(),
        schema,
        &plan,
    );

    assert_eq!(
        super::same_score_competing_candidate_scan_count_for_tests(),
        0,
        "access-choice EXPLAIN should derive residual facts from its main candidate loop",
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
