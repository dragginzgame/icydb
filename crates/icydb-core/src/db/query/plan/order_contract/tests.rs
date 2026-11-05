use super::{
    CandidateOrderContract, DeterministicSecondaryOrderContract, GroupedIndexOrderContract,
    GroupedIndexOrderMatch,
};
use crate::db::access::AccessPathKind::{
    IndexBranchSet, IndexMultiLookup, IndexPrefix, IndexRange,
};
use crate::db::query::plan::{OrderDirection, OrderSpec, OrderTerm, expr::Expr};
use crate::retained::RetainedBytes;
use crate::value::Value;
use std::rc::Rc;

#[test]
fn scalar_order_matching_does_not_retry_zero_prefix_comparisons() {
    use super::DeterministicSecondaryIndexOrderMatch as Match;
    use std::cell::Cell;

    for has_primary_key_tail in [false, true] {
        let mut keys = vec!["a", "b"];
        if has_primary_key_tail {
            keys.push("id");
        }
        for (terms, prefix, expected, expected_comparisons) in [
            (["missing", "b"], 0, Match::None, 2),
            (["a", "missing"], 0, Match::None, 3),
            (["a", "b"], 0, Match::Suffix, 3),
            (["a", "b"], 1, Match::Full, 3),
            (["a", "missing"], 1, Match::None, 3),
            (["a", "b"], usize::MAX, Match::Full, 3),
        ] {
            let contract = DeterministicSecondaryOrderContract {
                non_primary_key_terms: terms.into_iter().map(str::to_string).collect(),
                primary_key_terms: Rc::from(["id".to_string()]),
                direction: OrderDirection::Asc,
            };
            let comparisons = Cell::new(0);
            let actual = contract.classify_index_match_by(keys.len(), prefix, |index, term| {
                comparisons.set(comparisons.get() + 1);
                keys[index] == term
            });
            assert_eq!(actual, expected);
            // Includes the single primary-key-tail comparison in either case.
            assert_eq!(comparisons.get(), expected_comparisons);
        }
    }
}

#[test]
fn candidate_order_reuse_preserves_scalar_and_grouped_matching() {
    use crate::db::{access::SemanticIndexKeyItem, query::plan::exact_metadata_schema};

    let schema = exact_metadata_schema(&[], &[]);
    for grouped in [false, true] {
        assert!(
            crate::db::query::preparation::with_preparation_work(|work| {
                CandidateOrderContract::prepare(&schema, None, grouped, work).unwrap()
            })
            .is_none()
        );
        for direction in [OrderDirection::Asc, OrderDirection::Desc] {
            for fields in [vec![], vec!["age"], vec!["age", "id"], vec!["id"]] {
                let order = OrderSpec {
                    fields: fields
                        .iter()
                        .map(|field| OrderTerm::field(*field, direction))
                        .collect(),
                };
                let prepared = crate::db::query::preparation::with_preparation_work(|work| {
                    CandidateOrderContract::prepare(&schema, Some(&order), grouped, work).unwrap()
                });
                let scalar = crate::db::query::preparation::with_preparation_work(|work| {
                    order
                        .deterministic_secondary_order_contract_fields(
                            schema.shared_primary_key_names(),
                            work,
                        )
                        .unwrap()
                });
                let group = crate::db::query::preparation::with_preparation_work(|work| {
                    order.grouped_index_order_contract(work).unwrap()
                });
                assert_eq!(
                    prepared.is_some(),
                    if grouped {
                        group.is_some()
                    } else {
                        scalar.is_some()
                    }
                );
                for fields in [
                    vec![],
                    vec!["age", "id"],
                    vec!["tenant", "age", "id"],
                    vec!["other"],
                ] {
                    let items: Vec<_> = fields
                        .iter()
                        .map(|field| SemanticIndexKeyItem::Field((*field).into()))
                        .collect();
                    for prefix in 0..=items.len() + 1 {
                        let expected = if grouped {
                            group.as_ref().is_some_and(|contract| {
                                super::grouped_index_key_items_satisfied(contract, &items, prefix)
                            })
                        } else {
                            scalar.as_ref().is_some_and(|contract| {
                                super::deterministic_secondary_index_key_items_satisfied(
                                    contract, &items, prefix,
                                )
                            })
                        };
                        for _ in 0..2 {
                            assert_eq!(
                                prepared
                                    .as_ref()
                                    .is_some_and(|contract| contract.satisfies(&items, prefix)),
                                expected
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn key_item_classification_preserves_scalar_and_grouped_label_semantics() {
    use super::DeterministicSecondaryIndexOrderMatch as ScalarMatch;
    use crate::db::{
        access::SemanticIndexKeyItem, index::SemanticIndexExpression,
        schema::PersistedIndexExpressionOp,
    };

    for op in [
        PersistedIndexExpressionOp::Lower,
        PersistedIndexExpressionOp::Upper,
        PersistedIndexExpressionOp::Trim,
        PersistedIndexExpressionOp::LowerTrim,
        PersistedIndexExpressionOp::Date,
        PersistedIndexExpressionOp::Year,
        PersistedIndexExpressionOp::Month,
        PersistedIndexExpressionOp::Day,
    ] {
        let expression = SemanticIndexExpression::new(op, "账户".to_string());
        let label = expression.canonical_order_text();
        let field = |name: &str| SemanticIndexKeyItem::Field(name.to_string());
        for items in [
            vec![],
            vec![SemanticIndexKeyItem::Expression(expression.clone())],
            vec![
                field("prefix"),
                SemanticIndexKeyItem::Expression(expression.clone()),
                field("tenant"),
                field("id"),
            ],
            vec![
                SemanticIndexKeyItem::Expression(expression.clone()),
                field("tenant"),
                field("id"),
            ],
            vec![field(&label), field("tenant"), field("id")],
            vec![
                SemanticIndexKeyItem::Expression(expression.clone()),
                field("id"),
                field("tenant"),
            ],
        ] {
            let rendered = crate::db::query::plan::index_key_item_order_terms(&items);
            for keys in [
                vec!["id".to_string()],
                vec!["tenant".to_string(), "id".to_string()],
            ] {
                // This direct sequence oracle pins suffix stripping and
                // precedence independently of the shared callback matcher.
                let scalar_index = rendered.strip_suffix(keys.as_slice()).unwrap_or(&rendered);
                for terms in [
                    vec![],
                    vec![label.clone()],
                    vec!["prefix".to_string(), label.clone()],
                    vec!["missing".to_string()],
                ] {
                    let scalar = DeterministicSecondaryOrderContract {
                        non_primary_key_terms: terms.clone(),
                        primary_key_terms: Rc::from(keys.clone()),
                        direction: OrderDirection::Asc,
                    };
                    let grouped = GroupedIndexOrderContract {
                        terms: terms.clone(),
                        direction: OrderDirection::Asc,
                    };
                    for prefix in (0..=items.len() + 1).chain([usize::MAX]) {
                        let scalar_expected = if scalar_index
                            .get(prefix..)
                            .is_some_and(|suffix| suffix == terms)
                        {
                            ScalarMatch::Suffix
                        } else if scalar_index == terms {
                            ScalarMatch::Full
                        } else {
                            ScalarMatch::None
                        };
                        let grouped_expected = if prefix > 0
                            && rendered
                                .get(prefix..)
                                .is_some_and(|suffix| suffix.starts_with(&terms))
                        {
                            GroupedIndexOrderMatch::Suffix
                        } else if rendered.starts_with(&terms) {
                            GroupedIndexOrderMatch::Full
                        } else {
                            GroupedIndexOrderMatch::None
                        };
                        assert_eq!(
                            scalar.classify_index_key_items(&items, prefix),
                            scalar_expected
                        );
                        assert_eq!(
                            scalar.classify_index_match(&rendered, prefix),
                            scalar_expected
                        );
                        assert_eq!(
                            grouped.classify_index_key_items(&items, prefix),
                            grouped_expected
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn borrowed_primary_key_names_preserve_exact_tuple_and_suffix_admission() {
    let names: Rc<[String]> = Rc::from(["tenant".to_string(), "id".to_string()]);
    for direction in [OrderDirection::Asc, OrderDirection::Desc] {
        for (fields, exact, suffix) in [
            (vec!["tenant", "id"], true, true),
            (vec!["created", "tenant", "id"], false, true),
            (vec!["id"], false, false),
            (vec!["id", "tenant"], false, false),
            (vec!["tenant", "tenant", "id"], false, false),
            (vec!["id", "created", "tenant", "id"], false, false),
            (vec![], false, false),
        ] {
            let order = OrderSpec {
                fields: fields
                    .iter()
                    .map(|field| OrderTerm::field(*field, direction))
                    .collect(),
            };
            assert_eq!(
                order.primary_key_only_direction_fields(&names),
                exact.then_some(direction)
            );
            let contract = crate::db::query::preparation::with_preparation_work(|work| {
                order
                    .deterministic_secondary_order_contract_fields(Rc::clone(&names), work)
                    .unwrap()
            });
            assert_eq!(contract.is_some(), suffix);
            if let Some(contract) = contract {
                assert_eq!(contract.primary_key_terms, names);
                assert!(Rc::ptr_eq(&contract.primary_key_terms, &names));
                assert_eq!(
                    contract.non_primary_key_terms(),
                    &fields[..fields.len() - names.len()]
                );
                assert_eq!(contract.direction(), direction);
            }
        }
    }
}

#[test]
fn borrowed_primary_key_order_rejects_empty_keys_mixed_directions_and_expressions() {
    let names: Rc<[String]> = Rc::from(["tenant".to_string(), "id".to_string()]);
    let mixed = OrderSpec {
        fields: vec![
            OrderTerm::field("tenant", OrderDirection::Asc),
            OrderTerm::field("id", OrderDirection::Desc),
        ],
    };
    let expression = OrderSpec {
        fields: vec![
            OrderTerm::field("tenant", OrderDirection::Asc),
            OrderTerm::new(Expr::Literal(Value::Nat64(1)), OrderDirection::Asc),
        ],
    };
    for order in [mixed, expression] {
        for names in [Rc::clone(&names), Rc::from([])] {
            assert_eq!(order.primary_key_only_direction_fields(&names), None);
            assert!(
                crate::db::query::preparation::with_preparation_work(|work| order
                    .deterministic_secondary_order_contract_fields(names, work)
                    .unwrap())
                .is_none()
            );
        }
    }
}

#[test]
fn secondary_order_clones_share_detached_key_names_and_count_retention() {
    // Spare string capacity must remain counted even though contracts share
    // the payload. Each resident conservatively accounts for its full share.
    let mut tenant = String::with_capacity(64);
    tenant.push_str("账户");
    let names: Rc<[String]> = Rc::from([tenant, "id".to_string()]);
    let expected = size_of::<DeterministicSecondaryOrderContract>()
        + 2 * size_of::<usize>()
        + size_of_val(names.as_ref())
        + names.iter().map(String::capacity).sum::<usize>();
    let order = OrderSpec {
        fields: names
            .iter()
            .map(|name| OrderTerm::field(name, OrderDirection::Asc))
            .collect(),
    };
    let contract = crate::db::query::preparation::with_preparation_work(|work| {
        order
            .deterministic_secondary_order_contract_fields(Rc::clone(&names), work)
            .unwrap()
    })
    .unwrap();
    let cloned = contract.clone();
    assert!(Rc::ptr_eq(&contract.primary_key_terms, &names));
    assert!(Rc::ptr_eq(&cloned.primary_key_terms, &names));
    drop(names);
    drop(order);
    for resident in [contract, cloned] {
        assert_eq!(
            resident.classify_index_match(&["账户", "id"], 0),
            super::DeterministicSecondaryIndexOrderMatch::Suffix
        );
        assert_eq!(RetainedBytes::measure(&resident, expected), Some(expected));
        assert_eq!(RetainedBytes::measure(&resident, expected - 1), None);
    }
}

fn grouped_contract(terms: &[&str]) -> GroupedIndexOrderContract {
    GroupedIndexOrderContract {
        terms: terms.iter().map(ToString::to_string).collect(),
        direction: OrderDirection::Asc,
    }
}

#[test]
fn grouped_order_accepts_trailing_index_tie_break_terms() {
    let contract = grouped_contract(&["group_key"]);
    let index = ["group_key", "id"].map(|field| super::SemanticIndexKeyItem::Field(field.into()));

    assert_eq!(
        contract.classify_index_key_items(&index, 0),
        GroupedIndexOrderMatch::Full
    );
}

#[test]
fn grouped_order_accepts_trailing_terms_after_equality_prefix() {
    let contract = grouped_contract(&["group_key"]);
    let index = ["tenant_id", "group_key", "id"]
        .map(|field| super::SemanticIndexKeyItem::Field(field.into()));

    assert_eq!(
        contract.classify_index_key_items(&index, 1),
        GroupedIndexOrderMatch::Suffix
    );
}

#[test]
fn grouped_order_rejects_a_gap_before_the_group_key() {
    let contract = grouped_contract(&["group_key"]);
    let index = ["tenant_id", "created_at", "group_key", "id"]
        .map(|field| super::SemanticIndexKeyItem::Field(field.into()));

    assert_eq!(
        contract.classify_index_key_items(&index, 1),
        GroupedIndexOrderMatch::None
    );
}

#[test]
fn variable_prefix_access_kinds_require_a_full_secondary_order_match() {
    for access_kind in [IndexMultiLookup, IndexBranchSet] {
        assert!(
            DeterministicSecondaryOrderContract::access_kind_requires_full_index_order(access_kind,)
        );
    }
    for access_kind in [IndexPrefix, IndexRange] {
        assert!(
            !DeterministicSecondaryOrderContract::access_kind_requires_full_index_order(
                access_kind,
            )
        );
    }
}
