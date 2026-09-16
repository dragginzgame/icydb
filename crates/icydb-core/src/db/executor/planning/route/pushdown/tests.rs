//! Borrowed order classification preserves route eligibility and owned rejections.

use super::{
    index_range_limit_pushdown_shape_supported_for_order_contract,
    match_secondary_order_pushdown_core, secondary_order_pushdown_applicability,
};
use crate::db::{
    access::{AccessPlan, SemanticIndexKeyItem, SemanticIndexRangeSpec},
    executor::route::{PushdownApplicability, SecondaryOrderPushdownRejection},
    index::SemanticIndexExpression,
    query::plan::{
        DeterministicSecondaryOrderContract, OrderDirection, OrderSpec, OrderTerm, VisibleIndexes,
        exact_metadata_schema,
    },
    schema::PersistedIndexExpressionOp,
};
use std::{ops::Bound, rc::Rc};

fn contract(terms: &[&str], direction: OrderDirection) -> DeterministicSecondaryOrderContract {
    let order = OrderSpec {
        fields: terms
            .iter()
            .chain([&"tenant", &"id"])
            .map(|name| OrderTerm::field(*name, direction))
            .collect(),
    };
    crate::db::query::preparation::with_preparation_work(|work| {
        DeterministicSecondaryOrderContract::from_order_spec_fields(
            &order,
            Rc::from(vec!["tenant".into(), "id".into()]),
            work,
        )
        .unwrap()
    })
    .unwrap()
}

#[test]
fn prefix_routes_preserve_full_suffix_and_owned_mismatch_details() {
    let expression = SemanticIndexExpression::new(PersistedIndexExpressionOp::Lower, "账户".into());
    let label = expression.canonical_order_text();
    let items = vec![
        SemanticIndexKeyItem::Field("prefix".into()),
        SemanticIndexKeyItem::Expression(expression),
        SemanticIndexKeyItem::Field("tenant".into()),
        SemanticIndexKeyItem::Field("id".into()),
    ];
    let expected_full = vec!["prefix".into(), label.clone(), "tenant".into(), "id".into()];
    for direction in [OrderDirection::Asc, OrderDirection::Desc] {
        for (terms, prefix_len, variable_prefix, expected) in [
            (
                vec!["prefix", &label],
                1,
                true,
                PushdownApplicability::Eligible {
                    index: "by_account".into(),
                    prefix_len: 1,
                },
            ),
            (
                vec![label.as_str()],
                1,
                false,
                PushdownApplicability::Eligible {
                    index: "by_account".into(),
                    prefix_len: 1,
                },
            ),
            (
                vec![label.as_str()],
                1,
                true,
                PushdownApplicability::Rejected(
                    SecondaryOrderPushdownRejection::VariablePrefixSuffixOrderUnsupported {
                        index: "by_account".into(),
                        prefix_len: 1,
                        expected_full: expected_full.clone(),
                        actual: vec![label.clone()],
                    },
                ),
            ),
            // At prefix zero the existing classifier prefers Suffix over Full.
            (
                vec!["prefix", &label],
                0,
                true,
                PushdownApplicability::Rejected(
                    SecondaryOrderPushdownRejection::VariablePrefixSuffixOrderUnsupported {
                        index: "by_account".into(),
                        prefix_len: 0,
                        expected_full: expected_full.clone(),
                        actual: vec!["prefix".into(), label.clone()],
                    },
                ),
            ),
            (
                vec!["missing"],
                1,
                false,
                PushdownApplicability::Rejected(
                    SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                        index: "by_account".into(),
                        prefix_len: 1,
                        expected_suffix: expected_full[1..].to_vec(),
                        expected_full: expected_full.clone(),
                        actual: vec!["missing".into()],
                    },
                ),
            ),
        ] {
            let order = contract(&terms, direction);
            let actual = match_secondary_order_pushdown_core(
                &order,
                "by_account",
                &items,
                prefix_len,
                variable_prefix,
            );
            drop(order);
            assert_eq!(actual, expected);
        }
    }
}

#[test]
fn range_routes_and_limit_checks_preserve_eligibility_and_rejection_shape() {
    let schema = exact_metadata_schema(&[("by_age_rank", &["age", "rank", "id"])], &[]);
    let index = VisibleIndexes::accepted_schema_visible(&schema)
        .unwrap()
        .accepted_semantic_index_contracts()[0]
        .clone();
    for prefix_len in [0, 1, 2] {
        let range = AccessPlan::<crate::value::Value>::index_range(
            SemanticIndexRangeSpec::from_access_contract(
                index.clone(),
                (0..=prefix_len).collect(),
                vec![crate::value::Value::Nat64(3); prefix_len],
                Bound::Unbounded,
                Bound::Unbounded,
            ),
        );
        let facts = range.shape_facts();
        for (terms, eligible) in [
            (vec!["age", "rank"], true),
            (vec!["rank"], prefix_len == 1),
            (vec!["missing"], false),
        ] {
            let order = OrderSpec {
                fields: terms
                    .iter()
                    .chain([&"id"])
                    .map(|name| OrderTerm::field(*name, OrderDirection::Asc))
                    .collect(),
            };
            let order = crate::db::query::preparation::with_preparation_work(|work| {
                DeterministicSecondaryOrderContract::from_order_spec_fields(
                    &order,
                    Rc::from(vec!["id".into()]),
                    work,
                )
                .unwrap()
            })
            .unwrap();
            let expected = if eligible {
                PushdownApplicability::Eligible {
                    index: "by_age_rank".into(),
                    prefix_len,
                }
            } else {
                PushdownApplicability::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                        index: "by_age_rank".into(),
                        prefix_len,
                    },
                )
            };
            assert_eq!(
                secondary_order_pushdown_applicability(&facts, &order),
                expected
            );
            assert_eq!(
                index_range_limit_pushdown_shape_supported_for_order_contract(
                    &facts,
                    Some(&order),
                    true
                ),
                eligible,
            );
        }
        assert!(index_range_limit_pushdown_shape_supported_for_order_contract(&facts, None, false));
        assert!(!index_range_limit_pushdown_shape_supported_for_order_contract(&facts, None, true));
    }
}
