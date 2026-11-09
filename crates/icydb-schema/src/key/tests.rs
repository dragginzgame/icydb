use super::{
    ConstraintSourceKey, EntitySourceKey, FieldSourceKey, IndexSourceKey, RelationSourceKey,
    RuleSourceKey, SchemaName, SchemaSubmissionKey, TypeSourceKey,
};

#[test]
fn text_backed_keys_delegate_their_wire_type() {
    macro_rules! check {
        ($($ty:ty),* $(,)?) => { $(
            let value = <$ty>::try_new("sample").unwrap();
            let bytes = crate::wire::encode_one(&value).unwrap();
            assert_eq!(bytes, crate::wire::encode_one("sample").unwrap());
            assert_eq!(crate::wire::decode_one::<$ty>(&bytes).unwrap(), value);
        )* };
    }
    check!(
        EntitySourceKey,
        FieldSourceKey,
        TypeSourceKey,
        ConstraintSourceKey,
        IndexSourceKey,
        RelationSourceKey,
        RuleSourceKey,
        SchemaName,
        SchemaSubmissionKey
    );
}

// Keep persisted source-key bytes independent of the selected SHA-2 provider.
#[test]
fn targeted_rule_source_key_digest_is_stable() {
    let key = ConstraintSourceKey::for_targeted_field_rule(
        &FieldSourceKey::try_new("field").unwrap(),
        &TypeSourceKey::try_new("target").unwrap(),
        &RuleSourceKey::try_new("rule").unwrap(),
    );
    assert_eq!(
        key.as_str(),
        "rule:02aef6a7ef5ac2c3b9f6767e42953f4108a2e5c9e7c96e90d85a1a5dad1c1969"
    );
}
