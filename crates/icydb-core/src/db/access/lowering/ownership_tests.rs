use super::{LoweredIndexScanContract, push_lowered_index_prefix_spec_from_encoded_components};
use crate::{
    db::{
        access::{
            SemanticIndexAccessContract, SemanticIndexKeyItem,
            path::SemanticIndexAccessContractInner,
        },
        index::{
            EncodedValue, IndexId, IndexKeyKind, build_index_prefix_bounds_for_encoded_components,
        },
    },
    retained::RetainedBytes,
    types::EntityTag,
    value::Value,
};
use std::sync::Arc;

#[test]
fn prefix_spec_transfers_encoded_payloads_and_accounts_their_capacity() {
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
    )
    .unwrap();
    let mut spec = specs.pop().unwrap();
    assert_eq!(spec.prefix_components(), expected_bytes);
    assert_eq!(
        spec.raw_bounds().unwrap(),
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
}
