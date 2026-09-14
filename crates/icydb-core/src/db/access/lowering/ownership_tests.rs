use super::{
    LoweredIndexPrefixRawBounds, LoweredIndexPrefixSpec, LoweredIndexScanContract,
    push_lowered_index_prefix_spec_from_encoded_components,
};
use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        access::{
            SemanticIndexAccessContract, SemanticIndexKeyItem,
            path::SemanticIndexAccessContractInner,
        },
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        index::{
            EncodedValue, IndexId, IndexKeyKind, build_index_prefix_bounds_for_encoded_components,
        },
        query::preparation::{PreparationWork, with_preparation_work},
    },
    retained::RetainedBytes,
    types::EntityTag,
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::sync::Arc;

#[test]
fn prefix_spec_transfers_encoded_payloads_and_accounts_their_capacity() {
    with_preparation_work(|work| {
        let index = SemanticIndexAccessContract {
            inner: Arc::new(SemanticIndexAccessContractInner {
                ordinal: 1,
                physical_generation: 7,
                name: "by_pair".into(),
                store_path: "PairStore".into(),
                key_items: vec![
                    SemanticIndexKeyItem::Field("a".into()),
                    SemanticIndexKeyItem::Field("b".into()),
                ],
                unique: false,
                predicate_semantics: None,
            }),
        };
        let tag = EntityTag::new(254);
        let encoded = [Value::Text("a\0b".into()), Value::Nat64(42)]
            .iter()
            .map(|value| {
                let mut bytes = EncodedValue::try_from_ref(value).unwrap().into_bytes();
                bytes.reserve_exact(64);
                EncodedValue::from_canonical_bytes(bytes)
            })
            .collect::<Vec<_>>();
        let pointers = encoded
            .iter()
            .map(|value| value.encoded().as_ptr())
            .collect::<Vec<_>>();
        let expected_bytes = encoded
            .iter()
            .map(|value| value.encoded().to_vec())
            .collect::<Vec<_>>();
        let expected_bounds = build_index_prefix_bounds_for_encoded_components(
            &IndexId::new_with_generation(tag, index.ordinal(), index.physical_generation()),
            IndexKeyKind::User,
            index.key_arity(),
            &encoded,
            work,
        )
        .unwrap();
        let mut specs = Vec::new();
        push_lowered_index_prefix_spec_from_encoded_components(
            tag,
            &index,
            LoweredIndexScanContract::from_access_contract(index.clone()),
            encoded,
            &mut specs,
            false,
            work,
        )
        .unwrap();
        let mut spec = specs.pop().unwrap();
        assert_eq!(spec.prefix_components(), expected_bytes);
        assert_eq!(
            spec.raw_bounds(work).unwrap(),
            (&expected_bounds.0, &expected_bounds.1)
        );
        for (component, pointer) in spec.prefix_components().iter().zip(pointers) {
            assert_eq!(component.as_ptr(), pointer);
        }
        let before = RetainedBytes::measure(&spec, usize::MAX).unwrap();
        let mut spare = 0;
        // Replace only component backing so raw-bound capacity cannot mask the
        // retained accounting assertion for transferred payloads.
        for component in &mut spec.prefix_components {
            let tight = component.clone();
            spare += component.capacity() - tight.capacity();
            *component = tight;
        }
        assert!(spare >= 128);
        assert_eq!(
            before,
            RetainedBytes::measure(&spec, usize::MAX).unwrap() + spare,
        );
    });
}

#[test]
fn deferred_bounds_reject_before_publication_then_reuse_without_construction() {
    let components = vec![vec![1; 8]];
    let capacity =
        crate::db::index::IndexKey::raw_prefix_bounds_retained_capacity(2, &components) as u64;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
            let spec = LoweredIndexPrefixSpec::from_deferred_component_prefix(
                LoweredIndexScanContract {
                    name: Arc::from("by_pair"),
                    store_path: Arc::from("store"),
                },
                IndexId::new(EntityTag::new(254), 1),
                IndexKeyKind::User,
                2,
                components.clone(),
            );
            let root = |limit| {
                RequestExecutionRoot::new_for_tests(
                    HardExecutionBudget::uniform_for_tests(
                        16_000_000,
                        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                    )
                    .with_limit_for_tests(resource, limit),
                )
            };
            let rejected = root(capacity - 1);
            let error = PreparationWork::run(&rejected.scope(), lane, |work| {
                spec.raw_bounds(work).map_err(QueryError::execute)
            })
            .unwrap_err();
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
            );
            let LoweredIndexPrefixRawBounds::DeferredComponentPrefix { raw_bounds, .. } =
                &spec.raw_bounds
            else {
                panic!("deferred fixture")
            };
            assert!(raw_bounds.get().is_none());

            let admitted = root(capacity);
            let cold = PreparationWork::run(&admitted.scope(), lane, |work| {
                spec.raw_bounds(work).map_err(QueryError::execute)
            })
            .unwrap();
            assert_eq!(admitted.observed(Resource::TemporaryBytes), capacity);
            assert_eq!(
                admitted.observed(Resource::PredicateExpressionSteps),
                capacity
            );
            let warm = root(0);
            let cached = PreparationWork::run(&warm.scope(), lane, |work| {
                spec.raw_bounds(work).map_err(QueryError::execute)
            })
            .unwrap();
            assert!(std::ptr::eq(cold.0, cached.0));
            assert!(std::ptr::eq(cold.1, cached.1));
            assert_eq!(warm.observed(Resource::TemporaryBytes), 0);
            assert_eq!(warm.observed(Resource::PredicateExpressionSteps), 0);
            assert_eq!(warm.observed(Resource::RowsVisited), 0);
        }
    }
}
