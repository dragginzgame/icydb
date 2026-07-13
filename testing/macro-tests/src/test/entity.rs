pub use icydb_testing_test_fixtures::macro_test::entity::*;

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use icydb::{
        db::DbSession,
        model::field::{FieldDatabaseDefault, FieldKind},
        traits::{EntityDeclaration, EntityKey, SingletonEntity},
        types::Ulid,
    };
    use icydb_core::{
        db::{
            EntityKeyBytes, EntityKeyBytesError, KeyValueCodec, PrimaryKeyComponent,
            PrimaryKeyDecode, PrimaryKeyEncode, PrimaryKeyValue,
        },
        entity::EntityValue,
    };
    use icydb_testing_test_fixtures::schema::test::TestCanister;

    fn assert_entity_key<T>()
    where
        T: EntityKey<Key = Ulid>,
    {
    }

    fn assert_singleton_entity<T: SingletonEntity>() {}

    #[test]
    fn internal_primary_key_uses_declared_field_type() {
        assert_entity_key::<Entity>();
    }

    #[test]
    fn unit_primary_key_proves_singleton_entity() {
        assert_singleton_entity::<UnitKey>();
    }

    #[test]
    fn scalar_primary_key_field_shorthand_reaches_entity_model() {
        assert_eq!(Entity::MODEL.primary_key_names(), ["id"]);
    }

    #[test]
    fn scalar_index_field_shorthand_reaches_entity_model() {
        let [index] = Entity::MODEL.indexes() else {
            panic!("entity should expose one shorthand-declared index");
        };

        assert_eq!(index.fields(), ["a"]);
    }

    fn assert_composite_entity_key<T>()
    where
        T: EntityKey<Key = CompositePrimaryKeyEntityKey>,
    {
    }

    #[test]
    fn composite_primary_key_uses_generated_key_type() {
        assert_composite_entity_key::<CompositePrimaryKeyEntity>();
    }

    #[test]
    fn composite_primary_key_model_preserves_ordered_fields() {
        let fields = CompositePrimaryKeyEntity::MODEL
            .primary_key_model()
            .fields()
            .iter()
            .map(icydb_core::model::FieldModel::name)
            .collect::<Vec<_>>();

        assert_eq!(fields, ["tenant_id", "local_id"]);
        assert!(
            !CompositePrimaryKeyEntity::MODEL
                .primary_key_model()
                .is_scalar()
        );
    }

    #[test]
    fn composite_primary_key_type_round_trips_runtime_key_contracts() {
        let key = CompositePrimaryKeyEntityKey {
            tenant_id: 11,
            local_id: 22,
        };

        let value = <CompositePrimaryKeyEntityKey as KeyValueCodec>::to_key_value(&key);
        let from_value = <CompositePrimaryKeyEntityKey as KeyValueCodec>::from_key_value(&value)
            .expect("composite key should decode from its runtime value");
        assert_eq!(from_value, key);

        let primary_key =
            <CompositePrimaryKeyEntityKey as PrimaryKeyEncode>::to_primary_key_value(&key)
                .expect("composite key should encode into primary-key value");
        let PrimaryKeyValue::Composite(composite) = primary_key else {
            panic!("expected composite primary-key value: {primary_key:?}");
        };
        assert_eq!(
            composite.components(),
            [
                PrimaryKeyComponent::Nat64(11),
                PrimaryKeyComponent::Nat64(22)
            ],
        );
        let decoded = <CompositePrimaryKeyEntityKey as PrimaryKeyDecode>::from_primary_key_value(
            &primary_key,
        )
        .expect("composite key should decode from primary-key value");
        assert_eq!(decoded, key);
    }

    #[test]
    fn composite_entity_id_uses_all_primary_key_fields() {
        let entity = CompositePrimaryKeyEntity {
            tenant_id: 31,
            local_id: 42,
            ..Default::default()
        };

        assert_eq!(
            entity.id().key(),
            CompositePrimaryKeyEntityKey {
                tenant_id: 31,
                local_id: 42,
            },
        );
    }

    #[test]
    fn composite_primary_key_bytes_concatenate_components() {
        let key = CompositePrimaryKeyEntityKey {
            tenant_id: 1,
            local_id: 2,
        };
        let mut bytes = vec![0; CompositePrimaryKeyEntityKey::BYTE_LEN];

        EntityKeyBytes::write_bytes(&key, &mut bytes)
            .expect("exact composite-key buffer should encode");

        assert_eq!(bytes, [1_u64.to_be_bytes(), 2_u64.to_be_bytes()].concat(),);
    }

    #[test]
    fn composite_primary_key_bytes_reject_wrong_buffer_length() {
        let key = CompositePrimaryKeyEntityKey {
            tenant_id: 1,
            local_id: 2,
        };
        let mut bytes = [0; CompositePrimaryKeyEntityKey::BYTE_LEN - 1];

        assert_eq!(
            EntityKeyBytes::write_bytes(&key, &mut bytes),
            Err(EntityKeyBytesError::BufferLength {
                expected: CompositePrimaryKeyEntityKey::BYTE_LEN,
                actual: bytes.len(),
            }),
        );
    }

    #[test]
    fn entity_field_name_constants_are_generated() {
        assert_eq!(Entity::ID, "id");
        assert_eq!(Entity::A, "a");
        assert_eq!(BoundedTextEntity::NAME, "name");
    }

    #[expect(dead_code)]
    fn fluent_filter_eq_accepts_generated_field_constants(session: &DbSession<TestCanister>) {
        let _ = session
            .load::<Entity>()
            .filter_eq(Entity::A, 3_i32)
            .try_one();
    }

    #[expect(dead_code)]
    fn fluent_filter_helpers_accept_generated_field_constants(session: &DbSession<TestCanister>) {
        let _ = session
            .load::<Entity>()
            .filter_ne(Entity::A, 4_i32)
            .filter_lt(Entity::A, 10_i32)
            .filter_lte(Entity::A, 10_i32)
            .filter_gt(Entity::A, 1_i32)
            .filter_gte(Entity::A, 1_i32)
            .filter_eq_field(Entity::A, Entity::A)
            .filter_ne_field(Entity::A, Entity::A)
            .filter_lt_field(Entity::A, Entity::A)
            .filter_lte_field(Entity::A, Entity::A)
            .filter_gt_field(Entity::A, Entity::A)
            .filter_gte_field(Entity::A, Entity::A)
            .filter_in(Entity::A, [1_i32, 2_i32])
            .filter_not_in(Entity::A, [3_i32, 4_i32])
            .filter_contains(Entity::A, 3_i32)
            .filter_is_null(Entity::A)
            .filter_is_not_null(Entity::A)
            .filter_is_missing(Entity::A)
            .filter_is_empty(Entity::A)
            .filter_is_not_empty(Entity::A)
            .filter_between(Entity::A, 1_i32, 10_i32)
            .filter_between_fields(Entity::A, Entity::A, Entity::A)
            .filter_not_between(Entity::A, 1_i32, 10_i32)
            .filter_not_between_fields(Entity::A, Entity::A, Entity::A)
            .try_one();

        let _ = session
            .load::<BoundedTextEntity>()
            .filter_text_eq_ci(BoundedTextEntity::NAME, "Ada")
            .filter_text_contains(BoundedTextEntity::NAME, "da")
            .filter_text_contains_ci(BoundedTextEntity::NAME, "DA")
            .filter_text_starts_with(BoundedTextEntity::NAME, "A")
            .filter_text_starts_with_ci(BoundedTextEntity::NAME, "a")
            .filter_text_ends_with(BoundedTextEntity::NAME, "a")
            .filter_text_ends_with_ci(BoundedTextEntity::NAME, "A")
            .try_one();
    }

    #[test]
    fn entity_name_defaults_and_override() {
        assert_eq!(<RenamedEntity as EntityDeclaration>::MODEL.name(), "Potato");
    }

    #[test]
    fn text_max_len_directive_reaches_entity_model() {
        let name = BoundedTextEntity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "name")
            .expect("bounded text field should be present");

        std::assert_matches!(name.kind(), FieldKind::Text { max_len: Some(12) });
    }

    #[test]
    fn blob_max_len_directive_reaches_entity_model() {
        let payload = BoundedBlobEntity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "payload")
            .expect("bounded blob field should be present");

        std::assert_matches!(payload.kind(), FieldKind::Blob { max_len: Some(4) });
    }

    #[test]
    fn generated_fields_without_schema_defaults_emit_no_database_default() {
        let generated_id_default = Entity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "id")
            .expect("generated id field should be present");

        assert_eq!(
            generated_id_default.database_default(),
            FieldDatabaseDefault::None,
        );
    }

    #[test]
    fn supported_schema_defaults_reach_entity_model_as_slot_payloads() {
        let literal_default = Entity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "a")
            .expect("field with schema default should be present");

        assert_eq!(
            literal_default.database_default(),
            FieldDatabaseDefault::EncodedSlotPayload(&[0xFF, 0x01, 3, 0, 0, 0, 0, 0, 0, 0]),
        );
    }

    #[test]
    fn explicit_database_defaults_reach_entity_model_as_slot_payloads() {
        let rank = DatabaseDefaultEntity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "rank")
            .expect("rank field should be present");
        let label = DatabaseDefaultEntity::MODEL
            .fields()
            .iter()
            .find(|field| field.name() == "label")
            .expect("label field should be present");

        assert_eq!(
            rank.database_default(),
            FieldDatabaseDefault::EncodedSlotPayload(&[0xFF, 0x01, 7, 0, 0, 0, 0, 0, 0, 0]),
        );
        assert_eq!(
            label.database_default(),
            FieldDatabaseDefault::EncodedSlotPayload(&[
                0xFF, 0x01, b'u', b'n', b'k', b'n', b'o', b'w', b'n'
            ]),
        );
    }
}
