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
