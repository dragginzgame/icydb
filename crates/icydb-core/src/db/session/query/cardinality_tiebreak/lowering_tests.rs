//! Shared index-spec lowering preserves order and rejects exhausted traversal.

use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{
            AccessPlan, LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            SemanticIndexAccessContract, SemanticIndexRangeSpec, lower_access_with_schema_info,
        },
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        index::{
            EncodedValue, IndexId, TextPrefixBoundMode,
            build_index_component_range_with_encoded_prefix,
            encode_accepted_index_literal_component, starts_with_component_bounds,
        },
        query::{
            plan::CardinalityTiebreakCandidate,
            preparation::{PreparationWork, with_preparation_work},
        },
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
                .unwrap()
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
                authority.accepted_schema_info().unwrap(),
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
                        authority.accepted_schema_info().unwrap(),
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
                authority.accepted_schema_info().unwrap(),
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
    let schema = authority.accepted_schema_info().unwrap();
    let branch_index = SemanticIndexAccessContract::from_accepted_field_path_index(
        schema
            .field_path_indexes()
            .iter()
            .find(|index| index.name() == "b_wide_branch_idx")
            .unwrap(),
    );
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
    let bytes =
        ((4 + 8) * size_of::<LoweredIndexPrefixSpec>() + 5 * size_of::<EncodedValue>()) as u64;
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
                authority.accepted_schema_info().unwrap(),
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
        let bytes = (capacity * size_of::<LoweredIndexPrefixSpec>()
            + width * size_of::<EncodedValue>()) as u64;
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
                authority.accepted_schema_info().unwrap(),
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
                    authority.accepted_schema_info().unwrap(),
                    work,
                )
                .unwrap()
                .into_index_specs();
                assert_eq!(spec.prefix_components(), expected[0].prefix_components());
                assert_eq!(
                    spec.raw_bounds().unwrap(),
                    expected[0].raw_bounds().unwrap()
                );
            }
        });
        assert_eq!(root.observed(Resource::RowsVisited), 0);
    }
}

#[test]
fn accepted_composite_ranges_preserve_prefix_and_endpoint_contracts() {
    let (_, authority, _) = ranking_candidates_for_tests();
    let schema = authority.accepted_schema_info().unwrap();
    let accepted = schema
        .field_path_indexes()
        .iter()
        .find(|index| index.name() == "b_wide_branch_idx")
        .unwrap();
    let index = SemanticIndexAccessContract::from_accepted_field_path_index(accepted);
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
