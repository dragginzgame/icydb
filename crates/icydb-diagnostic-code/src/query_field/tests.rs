use super::*;

#[test]
fn role_registry_is_exact_and_closed() {
    let roles = [
        QueryFieldRole::Predicate,
        QueryFieldRole::Projection,
        QueryFieldRole::GroupBy,
        QueryFieldRole::Having,
        QueryFieldRole::OrderBy,
        QueryFieldRole::AggregateTarget,
    ];

    for (index, role) in roles.into_iter().enumerate() {
        let raw = u8::try_from(index + 1).expect("six roles fit u8");
        assert_eq!(role.raw(), raw);
        assert_eq!(QueryFieldRole::known(raw), Some(role));
    }
    assert_eq!(QueryFieldRole::known(0), None);
    assert_eq!(QueryFieldRole::known(7), None);
    assert_eq!(QueryFieldRole::known(u8::MAX), None);
}

#[test]
fn schema_accepts_only_query_plan_and_bounded_nonempty_fields() {
    for role in [
        QueryFieldRole::Predicate,
        QueryFieldRole::Projection,
        QueryFieldRole::GroupBy,
        QueryFieldRole::Having,
        QueryFieldRole::OrderBy,
        QueryFieldRole::AggregateTarget,
    ] {
        assert_eq!(
            validate_query_field_schema(ErrorCode::QUERY_PLAN, role.raw(), "missing"),
            Ok(role)
        );
    }
    assert_eq!(
        validate_query_field_schema(ErrorCode::QUERY_VALIDATE, 1, "missing"),
        Err(QueryFieldSchemaMismatch::DisallowedCodeRole)
    );
    assert_eq!(
        validate_query_field_schema(ErrorCode::QUERY_PLAN, 0, "missing"),
        Err(QueryFieldSchemaMismatch::UnknownRole)
    );
    assert_eq!(
        validate_query_field_schema(ErrorCode::QUERY_PLAN, 1, ""),
        Err(QueryFieldSchemaMismatch::EmptyField)
    );
}

#[test]
fn schema_uses_utf8_bytes_without_truncation() {
    let exact_ascii = "a".repeat(MAX_PUBLIC_QUERY_FIELD_BYTES);
    let over_ascii = "a".repeat(MAX_PUBLIC_QUERY_FIELD_BYTES + 1);
    let exact_multibyte = "é".repeat(MAX_PUBLIC_QUERY_FIELD_BYTES / 2);
    let over_multibyte = format!("{exact_multibyte}a");

    for field in [&exact_ascii, &exact_multibyte] {
        assert_eq!(
            validate_query_field_schema(ErrorCode::QUERY_PLAN, 5, field),
            Ok(QueryFieldRole::OrderBy)
        );
    }
    for field in [&over_ascii, &over_multibyte] {
        assert_eq!(
            validate_query_field_schema(ErrorCode::QUERY_PLAN, 5, field),
            Err(QueryFieldSchemaMismatch::FieldTooLong)
        );
    }
}
