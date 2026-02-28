use super::*;
use crate::db::access::{AccessPath, AccessPlan};
use crate::db::contracts::{MissingRowPolicy, Predicate};
use crate::db::query::builder::field::FieldRef;
use crate::db::query::intent::{KeyAccess, LoadSpec, QueryMode, access_plan_from_keys_value};
use crate::db::query::plan::{AccessPlannedQuery, LogicalPlan, OrderDirection, OrderSpec};
use crate::model::{field::FieldKind, index::IndexModel};
use crate::traits::EntitySchema;
use crate::types::Ulid;
use crate::value::Value;

const PUSHDOWN_INDEX_FIELDS: [&str; 1] = ["tag"];
const PUSHDOWN_INDEX: IndexModel = IndexModel::new(
    "explain::pushdown_tag",
    "explain::pushdown_store",
    &PUSHDOWN_INDEX_FIELDS,
    false,
);

crate::test_entity! {
ident = ExplainPushdownEntity,
    id = Ulid,
    entity_name = "PushdownEntity",
    primary_key = "id",
    pk_index = 0,
    fields = [
        ("id", FieldKind::Ulid),
        ("tag", FieldKind::Text),
        ("rank", FieldKind::Int),
    ],
    indexes = [&PUSHDOWN_INDEX],
}

#[test]
fn explain_is_deterministic_for_same_query() {
    let predicate = FieldRef::new("id").eq(Ulid::default());
    let mut plan: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan.predicate = Some(predicate);

    assert_eq!(plan.explain(), plan.explain());
}

#[test]
fn explain_is_deterministic_for_equivalent_predicates() {
    let id = Ulid::default();

    let predicate_a = Predicate::And(vec![
        FieldRef::new("id").eq(id),
        FieldRef::new("other").eq(Value::Text("x".to_string())),
    ]);
    let predicate_b = Predicate::And(vec![
        FieldRef::new("other").eq(Value::Text("x".to_string())),
        FieldRef::new("id").eq(id),
    ]);

    let mut plan_a: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_a.predicate = Some(predicate_a);

    let mut plan_b: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
    plan_b.predicate = Some(predicate_b);

    assert_eq!(plan_a.explain(), plan_b.explain());
}

#[test]
fn explain_is_deterministic_for_by_keys() {
    let a = Ulid::from_u128(1);
    let b = Ulid::from_u128(2);

    let access_a = access_plan_from_keys_value(&KeyAccess::Many(vec![a, b, a]));
    let access_b = access_plan_from_keys_value(&KeyAccess::Many(vec![b, a]));

    let plan_a: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_a,
    };
    let plan_b: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: access_b,
    };

    assert_eq!(plan_a.explain(), plan_b.explain());
}

#[test]
fn explain_reports_deterministic_index_choice() {
    const INDEX_FIELDS: [&str; 1] = ["idx_a"];
    const INDEX_A: IndexModel =
        IndexModel::new("explain::idx_a", "explain::store", &INDEX_FIELDS, false);
    const INDEX_B: IndexModel =
        IndexModel::new("explain::idx_a_alt", "explain::store", &INDEX_FIELDS, false);

    let mut indexes = [INDEX_B, INDEX_A];
    indexes.sort_by(|left, right| left.name.cmp(right.name));
    let chosen = indexes[0];

    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: chosen,
            values: vec![Value::Text("alpha".to_string())],
        },
        crate::db::contracts::MissingRowPolicy::Ignore,
    );

    let explain = plan.explain();
    match explain.access {
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            ..
        } => {
            assert_eq!(name, "explain::idx_a");
            assert_eq!(fields, vec!["idx_a"]);
            assert_eq!(prefix_len, 1);
        }
        _ => panic!("expected index prefix"),
    }
}

#[test]
fn explain_differs_for_semantic_changes() {
    let plan_a: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
        AccessPath::ByKey(Value::Ulid(Ulid::from_u128(1))),
        MissingRowPolicy::Ignore,
    );
    let plan_b: AccessPlannedQuery<Value> =
        AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);

    assert_ne!(plan_a.explain(), plan_b.explain());
}

#[test]
fn explain_with_model_reports_eligible_order_pushdown() {
    let model = <ExplainPushdownEntity as EntitySchema>::MODEL;
    let mut plan: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    assert_eq!(
        plan.explain_with_model(model).order_pushdown,
        ExplainOrderPushdown::EligibleSecondaryIndex {
            index: PUSHDOWN_INDEX.name,
            prefix_len: 1,
        }
    );
}

#[test]
fn explain_with_model_reports_descending_pushdown_eligibility() {
    let model = <ExplainPushdownEntity as EntitySchema>::MODEL;
    let mut plan: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Desc)],
    });

    assert_eq!(
        plan.explain_with_model(model).order_pushdown,
        ExplainOrderPushdown::EligibleSecondaryIndex {
            index: PUSHDOWN_INDEX.name,
            prefix_len: 1,
        }
    );
}

#[test]
fn explain_with_model_reports_composite_index_range_pushdown_rejection_reason() {
    let model = <ExplainPushdownEntity as EntitySchema>::MODEL;
    let plan: AccessPlannedQuery<Value> = AccessPlannedQuery {
        logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: MissingRowPolicy::Ignore,
        }),
        access: AccessPlan::Union(vec![
            AccessPlan::path(AccessPath::index_range(
                PUSHDOWN_INDEX,
                vec![],
                Bound::Included(Value::Text("alpha".to_string())),
                Bound::Excluded(Value::Text("omega".to_string())),
            )),
            AccessPlan::path(AccessPath::FullScan),
        ]),
    };

    assert_eq!(
        plan.explain_with_model(model).order_pushdown,
        ExplainOrderPushdown::Rejected(
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                index: PUSHDOWN_INDEX.name,
                prefix_len: 0,
            }
        )
    );
}

#[test]
fn explain_without_model_reports_missing_model_context() {
    let mut plan: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
        AccessPath::IndexPrefix {
            index: PUSHDOWN_INDEX,
            values: vec![Value::Text("alpha".to_string())],
        },
        MissingRowPolicy::Ignore,
    );
    plan.order = Some(OrderSpec {
        fields: vec![("id".to_string(), OrderDirection::Asc)],
    });

    assert_eq!(
        plan.explain().order_pushdown,
        ExplainOrderPushdown::MissingModelContext
    );
}

#[test]
#[expect(clippy::too_many_lines)]
fn explain_pushdown_conversion_covers_all_variants() {
    let cases = vec![
        (
            SecondaryOrderPushdownEligibility::Eligible {
                index: "explain::pushdown_tag",
                prefix_len: 1,
            },
            ExplainOrderPushdown::EligibleSecondaryIndex {
                index: "explain::pushdown_tag",
                prefix_len: 1,
            },
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(SecondaryOrderPushdownRejection::NoOrderBy),
            ExplainOrderPushdown::Rejected(SecondaryOrderPushdownRejection::NoOrderBy),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len: 3,
                    index_field_len: 2,
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                    prefix_len: 3,
                    index_field_len: 2,
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                    field: "id".to_string(),
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                    field: "id".to_string(),
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending {
                    field: "id".to_string(),
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending {
                    field: "id".to_string(),
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: "rank".to_string(),
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                    field: "rank".to_string(),
                },
            ),
        ),
        (
            SecondaryOrderPushdownEligibility::Rejected(
                SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                    expected_suffix: vec!["rank".to_string()],
                    expected_full: vec!["group".to_string(), "rank".to_string()],
                    actual: vec!["other".to_string()],
                },
            ),
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                    expected_suffix: vec!["rank".to_string()],
                    expected_full: vec!["group".to_string(), "rank".to_string()],
                    actual: vec!["other".to_string()],
                },
            ),
        ),
    ];

    for (input, expected) in cases {
        assert_eq!(ExplainOrderPushdown::from(input), expected);
    }
}
