//! Shared index-spec lowering preserves order and rejects exhausted traversal.

use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{
            AccessPlan, LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            SemanticIndexAccessContract, SemanticIndexRangeSpec, lower_access_with_schema_info,
        },
        index::{
            EncodedValue, IndexId, IndexKey, TextPrefixBoundMode,
            build_index_component_range_with_encoded_prefix,
            encode_accepted_index_literal_component, starts_with_component_bounds,
        },
        query::{
            plan::CardinalityTiebreakCandidate,
            preparation::{PreparationWork, with_preparation_work},
        },
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        session::tests::cardinality_tiebreak::ranking_candidates_for_tests,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::ops::Bound;

fn request(steps: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::PredicateExpressionSteps, steps),
    )
}

#[test]
fn mixed_index_specs_preserve_depth_first_leaf_order() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let index = candidates
        .iter()
        .map(CardinalityTiebreakCandidate::index)
        .find(|index| index.name() == "a_common_idx")
        .unwrap();
    let range = AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
        index.clone(),
        vec![
            authority
                .accepted_schema_info()
                .field_slot_index("common")
                .unwrap(),
        ],
        Vec::new(),
        Bound::Included(Value::Text("a".into())),
        Bound::Excluded(Value::Text("z".into())),
    ));
    let multi = AccessPlan::index_multi_lookup_from_contract(
        index.clone(),
        vec![Value::Text("b".into()), Value::Text("c".into())],
    );
    let leaves = [
        candidates[0].access().clone(),
        range.clone(),
        multi.clone(),
        range.clone(),
    ];
    let tree = AccessPlan::Union(vec![
        leaves[0].clone(),
        AccessPlan::Intersection(vec![range.clone(), multi, AccessPlan::by_keys(Vec::new())]),
        range,
    ]);
    let original = tree.clone();
    with_preparation_work(|work| {
        let lower = |access| {
            lower_access_with_schema_info(
                authority.entity_tag(),
                access,
                authority.accepted_schema_info(),
                work,
            )
            .unwrap()
            .into_index_specs()
        };
        let mut expected_prefixes = Vec::new();
        let mut expected_ranges = Vec::new();
        for leaf in &leaves {
            let (prefixes, ranges) = lower(leaf);
            expected_prefixes.extend(prefixes);
            expected_ranges.extend(ranges);
        }
        let actual = lower(&tree);
        assert_eq!(actual, (expected_prefixes, expected_ranges));
        assert_eq!(actual.0.len(), 3);
        assert_eq!(actual.1.len(), 2);
    });
    assert_eq!(tree, original);
}

#[test]
fn traversal_budget_is_exact_cumulative_and_row_free_in_every_lane() {
    let (_, authority, _) = ranking_candidates_for_tests();
    let tree: AccessPlan<Value> = AccessPlan::Union(vec![
        AccessPlan::full_scan(),
        AccessPlan::Intersection(vec![
            AccessPlan::by_keys(Vec::new()),
            AccessPlan::full_scan(),
        ]),
    ]);
    // Five visited nodes. No index encoding or destination spec allocation.
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for limit in [0, 4, 5, 9, 10] {
            let root = request(limit);
            for invocation in 0..2 {
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    lower_access_with_schema_info(
                        authority.entity_tag(),
                        &tree,
                        authority.accepted_schema_info(),
                        work,
                    )
                    .map_err(|error| QueryError::execute(error.into_internal_error()))
                });
                if limit < (invocation + 1) * 5 {
                    let error = result.unwrap_err();
                    assert!(error.diagnostic_facts().contains(&(
                        DiagnosticFactTag::BudgetResource,
                        Resource::PredicateExpressionSteps.raw(),
                    )));
                    assert_eq!(root.observed(Resource::PredicateExpressionSteps), limit + 1);
                    break;
                }
                let (prefixes, ranges) = result.unwrap().into_index_specs();
                assert!(prefixes.is_empty() && ranges.is_empty());
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn traversal_exhaustion_precedes_leaf_encoding_and_keeps_its_error_kind() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let invalid =
        AccessPlan::index_prefix_from_contract(candidates[0].index().clone(), vec![Value::Null]);
    let tree: AccessPlan<Value> = AccessPlan::Union(vec![AccessPlan::full_scan(), invalid]);
    for limit in [2, 3, 4] {
        let root = request(limit);
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            let error = lower_access_with_schema_info(
                authority.entity_tag(),
                &tree,
                authority.accepted_schema_info(),
                work,
            )
            .unwrap_err();
            if limit < 4 {
                assert!(matches!(error, LoweredAccessError::Construction(_)));
                let error = QueryError::execute(error.into_internal_error());
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::PredicateExpressionSteps.raw(),
                )));
            } else {
                assert!(matches!(error, LoweredAccessError::IndexPrefix));
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(
            root.observed(Resource::PredicateExpressionSteps),
            (limit + 1).min(4)
        );
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn spec_backing_exhaustion_precedes_encoding_for_every_index_shape() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let schema = authority.accepted_schema_info();
    let branch_index = crate::db::query::preparation::with_preparation_work(|work| {
        SemanticIndexAccessContract::from_accepted_field_path_index(
            schema
                .field_path_indexes()
                .iter()
                .find(|index| index.name() == "b_wide_branch_idx")
                .unwrap(),
            work,
        )
    })
    .expect("valid accepted index fixture");
    let cases: [(AccessPlan<Value>, usize); 4] = [
        (
            AccessPlan::index_prefix_from_contract(
                candidates[0].index().clone(),
                vec![Value::Null],
            ),
            4 * size_of::<LoweredIndexPrefixSpec>(),
        ),
        (
            AccessPlan::index_multi_lookup_from_contract(
                candidates[0].index().clone(),
                vec![Value::Null; 17],
            ),
            17 * size_of::<LoweredIndexPrefixSpec>(),
        ),
        (
            AccessPlan::index_branch_set_from_contract(
                branch_index,
                vec![Value::Text("all".into())],
                vec![Value::Null; 2],
            ),
            4 * size_of::<LoweredIndexPrefixSpec>(),
        ),
        (
            AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
                candidates[0].index().clone(),
                vec![schema.field_slot_index("common").unwrap()],
                Vec::new(),
                Bound::Included(Value::Null),
                Bound::Unbounded,
            )),
            4 * size_of::<LoweredIndexRangeSpec>(),
        ),
    ];
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for (access, bytes) in &cases {
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::TemporaryBytes, *bytes as u64 - 1),
            );
            let error = PreparationWork::run(&root.scope(), lane, |work| {
                lower_access_with_schema_info(authority.entity_tag(), access, schema, work)
                    .map_err(|error| QueryError::execute(error.into_internal_error()))
            })
            .unwrap_err();
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::TemporaryBytes.raw()
            )));
            assert_eq!(root.observed(Resource::TemporaryBytes), *bytes as u64);
            assert_eq!(root.observed(Resource::PredicateExpressionSteps), 1);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn composite_spec_growth_charges_replacement_backing_and_preserves_order() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let access = AccessPlan::Union(vec![candidates[0].access().clone(); 5]);
    // The fifth prefix grows the buffer from four to eight. Charge the new
    // eight-slot allocation, including the retained prefix, not just four slots.
    let (index, values) = candidates[0]
        .access()
        .as_path()
        .unwrap()
        .as_index_prefix_contract()
        .unwrap();
    let encoded: Vec<_> = values
        .iter()
        .map(|value| EncodedValue::try_from_ref(value).unwrap())
        .collect();
    let raw_bytes = IndexKey::raw_prefix_bounds_retained_capacity(index.key_arity(), &encoded);
    let scalar_bytes: usize = encoded
        .into_iter()
        .map(|value| value.into_bytes().capacity())
        .sum();
    let bytes = ((4 + 8) * size_of::<LoweredIndexPrefixSpec>()
        + 5 * (size_of::<EncodedValue>() + raw_bytes + scalar_bytes)) as u64;
    for limit in [bytes - 1, bytes] {
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, limit),
        );
        let result = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            lower_access_with_schema_info(
                authority.entity_tag(),
                &access,
                authority.accepted_schema_info(),
                work,
            )
            .map_err(|error| QueryError::execute(error.into_internal_error()))
        });
        if limit == bytes {
            let (prefixes, ranges) = result.unwrap().into_index_specs();
            assert_eq!(prefixes.len(), 5);
            assert!(ranges.is_empty());
            assert!(prefixes.windows(2).all(|pair| pair[0] == pair[1]));
        } else {
            assert!(result.is_err());
        }
        assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn multi_lookup_reservation_preserves_deferred_raw_bounds() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let index = candidates[0].index();
    for width in [0_usize, 1, 4, 5, 31, 32, 33] {
        let values: Vec<_> = (0..width)
            .map(|slot| Value::Text(format!("value-{slot}")))
            .collect();
        let access: AccessPlan<Value> =
            AccessPlan::index_multi_lookup_from_contract(index.clone(), values.clone());
        let capacity = if width == 0 { 0 } else { width.max(4) };
        let scalar_bytes: usize = values
            .iter()
            .map(|value| {
                EncodedValue::try_from_ref(value)
                    .unwrap()
                    .into_bytes()
                    .capacity()
            })
            .sum();
        let raw_bytes: usize = if width < 32 {
            values
                .iter()
                .map(|value| {
                    let encoded = EncodedValue::try_from_ref(value).unwrap();
                    IndexKey::raw_prefix_bounds_retained_capacity(index.key_arity(), &[encoded])
                })
                .sum()
        } else {
            0
        };
        let bytes = (capacity * size_of::<LoweredIndexPrefixSpec>()
            + width * size_of::<EncodedValue>()
            + raw_bytes
            + scalar_bytes) as u64;
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, bytes),
        );
        let (prefixes, ranges) = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            lower_access_with_schema_info(
                authority.entity_tag(),
                &access,
                authority.accepted_schema_info(),
                work,
            )
            .map_err(|error| QueryError::execute(error.into_internal_error()))
        })
        .unwrap()
        .into_index_specs();
        assert!(ranges.is_empty());
        assert_eq!(prefixes.len(), width);
        assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
        with_preparation_work(|work| {
            for (spec, value) in prefixes.iter().zip(values) {
                assert_eq!(spec.deferred_cardinality_source().is_some(), width >= 32);
                let single: AccessPlan<Value> =
                    AccessPlan::index_prefix_from_contract(index.clone(), vec![value]);
                let (expected, _) = lower_access_with_schema_info(
                    authority.entity_tag(),
                    &single,
                    authority.accepted_schema_info(),
                    work,
                )
                .unwrap()
                .into_index_specs();
                assert_eq!(spec.prefix_components(), expected[0].prefix_components());
                assert_eq!(
                    spec.raw_bounds(work).unwrap(),
                    expected[0].raw_bounds(work).unwrap()
                );
            }
        });
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn accepted_composite_ranges_preserve_prefix_and_endpoint_contracts() {
    let (_, authority, _) = ranking_candidates_for_tests();
    let schema = authority.accepted_schema_info();
    let accepted = schema
        .field_path_indexes()
        .iter()
        .find(|index| index.name() == "b_wide_branch_idx")
        .unwrap();
    let index = crate::db::query::preparation::with_preparation_work(|work| {
        SemanticIndexAccessContract::from_accepted_field_path_index(accepted, work)
    })
    .expect("valid accepted index fixture");
    let slots: Vec<_> = accepted
        .fields()
        .iter()
        .map(crate::db::schema::SchemaIndexFieldPathInfo::slot)
        .collect();
    let prefix = Value::Text("all".into());
    let cases = [
        (
            Bound::Included(Value::Text("a".into())),
            Bound::Excluded(Value::Text("z".into())),
        ),
        (
            Bound::Excluded(Value::Text("a".into())),
            Bound::Included(Value::Text("z".into())),
        ),
        (Bound::Unbounded, Bound::Unbounded),
        starts_with_component_bounds("é", TextPrefixBoundMode::Strict).unwrap(),
        starts_with_component_bounds("é", TextPrefixBoundMode::LowerOnly).unwrap(),
    ];
    with_preparation_work(|work| {
        for (lower, upper) in cases {
            let encoded = EncodedValue::from_canonical_bytes(
                encode_accepted_index_literal_component(schema, index.name(), 0, &prefix)
                    .unwrap()
                    .unwrap(),
            );
            let expected = build_index_component_range_with_encoded_prefix(
                &IndexId::new_with_generation(
                    authority.entity_tag(),
                    index.ordinal(),
                    index.physical_generation(),
                ),
                index.key_arity(),
                vec![encoded],
                &lower,
                &upper,
                work,
            )
            .unwrap()
            .into_bounds_and_prefix_components();
            let access: AccessPlan<Value> =
                AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
                    index.clone(),
                    slots.clone(),
                    vec![prefix.clone()],
                    lower,
                    upper,
                ));
            let original = access.clone();
            let (prefixes, ranges) =
                lower_access_with_schema_info(authority.entity_tag(), &access, schema, work)
                    .unwrap()
                    .into_index_specs();
            assert!(prefixes.is_empty());
            assert_eq!(ranges.len(), 1);
            assert_eq!(ranges[0].lower(), &expected.0);
            assert_eq!(ranges[0].upper(), &expected.1);
            assert_eq!(ranges[0].prefix_components(), expected.2);
            assert_eq!(access, original);
        }
    });
}

#[test]
fn cardinality_bounds_exhaustion_is_not_unavailable_evidence() {
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let access: AccessPlan<Value> = AccessPlan::index_multi_lookup_from_contract(
        candidates[0].index().clone(),
        (0..32)
            .map(|value| Value::Text(format!("value-{value}")))
            .collect(),
    );
    let (prefixes, _) = with_preparation_work(|work| {
        lower_access_with_schema_info(
            authority.entity_tag(),
            &access,
            authority.accepted_schema_info(),
            work,
        )
    })
    .unwrap()
    .into_index_specs();
    let root = RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, 0),
    );
    let error = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        super::cardinality_lowered_bytes(&prefixes, work)
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert!(
        with_preparation_work(|work| super::cardinality_lowered_bytes(&prefixes, work))
            .unwrap()
            .is_some()
    );
}

#[test]
fn prepared_leaf_bindings_follow_reordered_children_and_share_cloned_bounds() {
    use crate::{db::access::lowering::bind_access_with_schema_info, retained::RetainedBytes};
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let index = candidates
        .iter()
        .map(CardinalityTiebreakCandidate::index)
        .find(|index| index.name() == "a_common_idx")
        .unwrap();
    let first = AccessPlan::<Value>::index_prefix_from_contract(
        index.clone(),
        vec![Value::Text("a".into())],
    );
    let second = AccessPlan::index_multi_lookup_from_contract(
        index.clone(),
        vec![Value::Text("b".into()), Value::Text("c".into())],
    );
    let range = AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
        index.clone(),
        vec![
            authority
                .accepted_schema_info()
                .field_slot_index("common")
                .unwrap(),
        ],
        Vec::new(),
        Bound::Included(Value::Text("d".into())),
        Bound::Excluded(Value::Text("z".into())),
    ));
    let mut tree = AccessPlan::Union(vec![first, AccessPlan::Intersection(vec![second, range])]);
    let semantic = tree.clone();
    let before = RetainedBytes::measure(&tree, usize::MAX).unwrap();
    with_preparation_work(|work| {
        bind_access_with_schema_info(
            authority.entity_tag(),
            &mut tree,
            authority.accepted_schema_info(),
            work,
        )
        .unwrap();
        assert_eq!(
            tree, semantic,
            "physical admission cannot change semantic identity"
        );
        assert!(RetainedBytes::measure(&tree, usize::MAX).unwrap() > before);
        let mut cloned = tree.clone();
        let AccessPlan::Union(children) = &mut cloned else {
            panic!("union fixture")
        };
        children.reverse();
        let AccessPlan::Intersection(nested) = &mut children[0] else {
            panic!("nested fixture")
        };
        nested.reverse();
        let AccessPlan::Intersection(nested) = &children[0] else {
            panic!("nested fixture")
        };
        let AccessPlan::Union(original) = &tree else {
            panic!("original fixture")
        };
        let AccessPlan::Intersection(original_nested) = &original[1] else {
            panic!("original nested fixture")
        };
        for (leaf, source) in [
            (&nested[0], &original_nested[1]),
            (&nested[1], &original_nested[0]),
            (&children[1], &original[0]),
        ] {
            let executable = leaf.executable_contract();
            let actual = executable.index_specs().unwrap().unwrap();
            let expected = lower_access_with_schema_info(
                authority.entity_tag(),
                leaf,
                authority.accepted_schema_info(),
                work,
            )
            .unwrap()
            .into_index_specs();
            assert_eq!(actual.prefixes(), expected.0);
            assert_eq!(actual.ranges(), expected.1);
            assert!(
                std::ptr::eq(actual, source.single_index_specs().unwrap()),
                "clone must share physical bounds rather than re-encode or copy them"
            );
        }
    });
}

#[test]
fn unprepared_index_leaf_is_rejected_but_primary_access_needs_no_binding() {
    let (_, _, candidates) = ranking_candidates_for_tests();
    let access = AccessPlan::path(candidates[0].access().as_path().unwrap().clone());
    let executable = access.executable_contract();
    assert!(executable.index_specs().is_err());
    let primary = AccessPlan::<Value>::full_scan();
    assert!(
        primary
            .executable_contract()
            .index_specs()
            .unwrap()
            .is_none()
    );
}

#[test]
fn prepared_binding_backing_exhaustion_does_not_publish_the_leaf() {
    use crate::db::access::lowering::bind_access_with_schema_info;
    let (_, authority, candidates) = ranking_candidates_for_tests();
    let mut access = AccessPlan::path(candidates[0].access().as_path().unwrap().clone());
    let root = |limit| {
        RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, limit),
        )
    };
    let encoding = root(16_000_000);
    PreparationWork::run(&encoding.scope(), Lane::PublicRead, |work| {
        lower_access_with_schema_info(
            authority.entity_tag(),
            &access,
            authority.accepted_schema_info(),
            work,
        )
        .map(|_| ())
        .map_err(|error| QueryError::execute(error.into_internal_error()))
    })
    .unwrap();
    // Admit all the existing encoding work, but no additional retained owner.
    let rejected = root(encoding.observed(Resource::TemporaryBytes));
    let error = PreparationWork::run(&rejected.scope(), Lane::PublicRead, |work| {
        bind_access_with_schema_info(
            authority.entity_tag(),
            &mut access,
            authority.accepted_schema_info(),
            work,
        )
        .map_err(|error| QueryError::execute(error.into_internal_error()))
    })
    .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    assert!(access.single_index_specs().is_none());
    assert!(access.executable_contract().index_specs().is_err());
}
