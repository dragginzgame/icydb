use super::{
    FieldFragment, FieldInsertPolicy, FieldSourceKey, FieldType, ScalarType, SchemaContractError,
    SchemaName,
};

#[test]
fn independently_decoded_field_key_and_name_must_match() {
    let field = FieldFragment {
        source_key: FieldSourceKey::try_new("legacy_name").expect("fixture key should admit"),
        name: SchemaName::try_new("current_name").expect("fixture name should admit"),
        field_type: FieldType::Scalar(ScalarType::Nat64),
        nullable: false,
        insert_policy: FieldInsertPolicy::Required,
        management: None,
    };

    assert_eq!(field.validate(), Err(SchemaContractError::NonCanonical));
}
