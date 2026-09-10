//! Shared prefix construction charges before encoding, including metadata-only calls.

use super::*;
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{
            AccessPlan, LoweredAccessError, LoweredIndexPrefixSpec, LoweredIndexRangeSpec,
            SemanticIndexAccessContract, SemanticIndexRangeSpec, lower_access_with_schema_info,
        },
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        index::{
            EncodedValue, UserIndexPrefixCardinalityKey, encode_accepted_index_literal_component,
        },
        predicate::Predicate,
        query::{plan::VisibleIndexes, preparation::PreparationWork},
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::ops::Bound;

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn component_admission_precedes_invalid_encoding_in_every_index_shape() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let schema = catalog.accepted_schema_info();
    let authority = catalog.accepted_entity_authority();
    let accepted = schema
        .field_path_indexes()
        .iter()
        .find(|index| index.name() == "b_wide_branch_idx")
        .unwrap();
    let index = SemanticIndexAccessContract::from_accepted_field_path_index(accepted);
    let slots = accepted
        .fields()
        .iter()
        .map(crate::db::schema::SchemaIndexFieldPathInfo::slot)
        .collect();
    let cases: [(AccessPlan<Value>, usize, usize, bool); 4] = [
        (
            AccessPlan::index_prefix_from_contract(index.clone(), vec![Value::Null]),
            4 * size_of::<LoweredIndexPrefixSpec>(),
            1,
            false,
        ),
        (
            AccessPlan::index_multi_lookup_from_contract(index.clone(), vec![Value::Null]),
            4 * size_of::<LoweredIndexPrefixSpec>(),
            1,
            false,
        ),
        (
            AccessPlan::index_branch_set_from_contract(
                index.clone(),
                vec![Value::Text("all".into())],
                vec![Value::Null],
            ),
            4 * size_of::<LoweredIndexPrefixSpec>(),
            2,
            false,
        ),
        (
            AccessPlan::index_range(SemanticIndexRangeSpec::from_access_contract(
                index,
                slots,
                vec![Value::Null],
                Bound::Unbounded,
                Bound::Unbounded,
            )),
            4 * size_of::<LoweredIndexRangeSpec>(),
            1,
            true,
        ),
    ];
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for (access, outer_bytes, components, is_range) in &cases {
            for (resource, exact) in [
                (
                    Resource::TemporaryBytes,
                    (outer_bytes + components * size_of::<EncodedValue>()) as u64,
                ),
                (Resource::PredicateExpressionSteps, 1 + *components as u64),
            ] {
                for limit in [exact - 1, exact] {
                    let root = request(resource, limit);
                    PreparationWork::run(&root.scope(), lane, |work| {
                        let error = lower_access_with_schema_info(
                            authority.entity_tag(),
                            access,
                            schema,
                            work,
                        )
                        .unwrap_err();
                        if limit < exact {
                            assert!(matches!(error, LoweredAccessError::Construction(_)));
                            assert!(
                                QueryError::execute(error.into_internal_error())
                                    .diagnostic_facts()
                                    .contains(
                                        &(DiagnosticFactTag::BudgetResource, resource.raw(),)
                                    )
                            );
                        } else if *is_range {
                            assert!(matches!(error, LoweredAccessError::IndexRange));
                        } else {
                            assert!(matches!(error, LoweredAccessError::IndexPrefix));
                        }
                        Ok(())
                    })
                    .unwrap();
                    assert_eq!(root.observed(resource), exact);
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}

#[test]
fn exact_count_prefix_construction_is_cumulative_and_row_free_in_every_lane() {
    let setup = initialize();
    let catalog = setup
        .accepted_schema_catalog_context_for_entity_name(Some(ENTITY_NAME))
        .unwrap();
    let schema = catalog.accepted_schema_info();
    let authority = catalog.accepted_entity_authority();
    let visible = VisibleIndexes::accepted_schema_visible(schema);
    let branches: Vec<_> = (0..17)
        .map(|value| Value::Text(format!("branch-{value:02}")))
        .collect();
    // The maintained metadata-only composite shortcut accepts a wider branch
    // set than ordinary bounded branch scans; cover its owned exact-prefix case.
    let cases = [
        (
            Predicate::eq("common".into(), Value::Text("everyone".into())),
            vec![vec![Value::Text("everyone".into())]],
        ),
        (
            Predicate::in_("common".into(), branches[..3].to_vec()),
            branches[..3]
                .iter()
                .map(|value| vec![value.clone()])
                .collect(),
        ),
        (
            Predicate::And(vec![
                Predicate::eq("wide_fixed".into(), Value::Text("all".into())),
                Predicate::in_("wide_branch".into(), branches.clone()),
            ]),
            branches
                .iter()
                .map(|value| vec![Value::Text("all".into()), value.clone()])
                .collect(),
        ),
    ];
    for (predicate, values) in cases {
        let query =
            StructuralQuery::new(MissingRowPolicy::Ignore).filter_normalized_predicate(predicate);
        let access = query
            .try_build_count_cardinality_prefix_access_with_schema_info(&visible, schema)
            .unwrap()
            .unwrap();
        let index = access.index();
        let index_id = IndexId::new_with_generation(
            authority.entity_tag(),
            index.ordinal(),
            index.physical_generation(),
        );
        let expected: Vec<_> = values
            .iter()
            .map(|prefix| {
                let components = prefix
                    .iter()
                    .enumerate()
                    .map(|(slot, value)| {
                        encode_accepted_index_literal_component(schema, index.name(), slot, value)
                            .unwrap()
                            .unwrap()
                    })
                    .collect();
                UserIndexPrefixCardinalityKey::new(index_id, components)
            })
            .collect();
        let components = values.iter().map(Vec::len).sum::<usize>();
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for (resource, exact) in [
                (
                    Resource::TemporaryBytes,
                    (values.len() * size_of::<UserIndexPrefixCardinalityKey>()
                        + components * size_of::<EncodedValue>()) as u64,
                ),
                (Resource::PredicateExpressionSteps, components as u64),
            ] {
                for limit in [exact - 1, exact, 2 * exact] {
                    let root = request(resource, limit);
                    let session = new_request_session(&root);
                    for invocation in 1..=2 {
                        let result = session
                            .exact_count_cardinality_prefix_keys_for_accepted_authority(
                                &authority, &query, &visible, schema, lane,
                            );
                        if limit < invocation * exact {
                            let error = result.unwrap_err();
                            assert!(
                                error
                                    .diagnostic_facts()
                                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                            );
                            break;
                        }
                        assert_eq!(result.unwrap().unwrap(), expected);
                        assert_eq!(root.observed(resource), invocation * exact);
                    }
                    assert_eq!(root.observed(Resource::RowsVisited), 0);
                }
            }
        }
    }
}
