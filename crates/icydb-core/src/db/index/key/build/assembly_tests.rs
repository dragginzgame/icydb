use super::*;
use crate::db::key_taxonomy::PrimaryKeyComponent;

#[test]
fn complete_user_key_budget_counts_framing_and_primary_identity() {
    let id = IndexId::new_with_generation(EntityTag::new(23), 2, 5);
    let primary_key = PrimaryKeyComponent::Nat64(7).into();
    for second_size in [277, 278] {
        let result =
            build_user_index_key_from_components(id, &primary_key, &[200, second_size], |size| {
                Ok(Some(vec![17; *size]))
            });
        if second_size == 277 {
            assert_eq!(
                result.unwrap().unwrap().to_raw().unwrap().as_bytes().len(),
                512
            );
        } else {
            let error = result.unwrap_err();
            let expected = InternalError::index_key_exceeds_max_size_at(23, 5, 513, 512);
            assert_eq!(error.diagnostic(), expected.diagnostic());
            assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
            assert_eq!(error.class(), crate::error::ErrorClass::Unsupported);
            assert!(
                icydb_diagnostic_code::validate_known_diagnostic_fact_schema(
                    error.diagnostic().error_code(),
                    &error.diagnostic_facts()
                )
                .is_ok()
            );
        }
    }
}

#[test]
fn user_key_limits_preserve_typed_diagnostics() {
    let id = IndexId::new_with_generation(EntityTag::new(23), 2, 5);
    let key = PrimaryKeyComponent::Nat64(7).into();
    let error = build_user_index_key_from_components(id, &key, &[(); MAX_INDEX_FIELDS + 1], |()| {
        panic!("field count must be checked before encoding")
    })
    .unwrap_err();
    let expected = InternalError::index_key_field_count_exceeds_max(
        23,
        5,
        MAX_INDEX_FIELDS + 1,
        MAX_INDEX_FIELDS,
    );
    assert_eq!(error.class(), expected.class());
    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());

    let error = build_user_index_key_from_components(
        id,
        &key,
        &[1, IndexKey::MAX_COMPONENT_SIZE + 1],
        |len| Ok(Some(vec![0; *len])),
    )
    .unwrap_err();
    let expected = InternalError::index_component_exceeds_max_size_at(
        23,
        5,
        1,
        IndexKey::MAX_COMPONENT_SIZE + 1,
        IndexKey::MAX_COMPONENT_SIZE,
    );
    assert_eq!(error.class(), expected.class());
    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
    assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
}

#[test]
fn user_keys_preserve_identity_and_null_short_circuit() {
    let id = IndexId::new_with_generation(EntityTag::new(23), 2, 5);
    let primary_key = PrimaryKeyComponent::Nat64(7).into();
    let items = [Value::Nat64(11), Value::Text("key".into())];
    let key = build_user_index_key_from_components(
        id,
        &primary_key,
        &items,
        encode_value_index_component_ref,
    )
    .unwrap()
    .unwrap();
    assert_eq!(key.index_id, id);
    assert_eq!(key.key_kind, IndexKeyKind::User);
    assert_eq!(
        key.primary_key,
        IndexKey::compact_primary_key_value_bytes(&primary_key).unwrap()
    );
    assert_eq!(
        key.components,
        items
            .iter()
            .map(|item| encode_canonical_index_component(item).unwrap())
            .collect::<Vec<_>>()
    );

    let mut calls = 0;
    let absent = build_user_index_key_from_components(
        id,
        &primary_key,
        &[Value::Nat64(11), Value::Null, Value::Nat64(12)],
        |value| {
            calls += 1;
            encode_value_index_component_ref(value)
        },
    )
    .unwrap();
    assert!(absent.is_none());
    assert_eq!(calls, 2);

    for items in [
        [Value::Text("x".repeat(1024)), Value::Null],
        [Value::Null, Value::Text("x".repeat(1024))],
    ] {
        assert!(
            build_user_index_key_from_components(
                id,
                &primary_key,
                &items,
                encode_value_index_component_ref
            )
            .unwrap()
            .is_none()
        );
    }
}
