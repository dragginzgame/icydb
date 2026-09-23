//! Binding diagnostics identify generated sources without retaining schema data.

use super::{
    DynamicTypedBindingError, ENTITY_DESCRIPTOR, ENTITY_SOURCE, ID_SOURCE, REPLACEMENT_SOURCE,
    TypedEntityDescriptor, TypedFieldDescriptor, TypedFieldType, VALUE_SOURCE,
    initialize_typed_session,
};
use crate::db::TypedBindingContext;
use icydb_schema::{MAX_SOURCE_KEY_BYTES, ScalarType};

#[test]
fn typed_binding_unavailable_entity_retains_source_context() {
    let session = initialize_typed_session();
    for source in ["test::MissingEntity", "", "invalid entity"] {
        let descriptor = TypedEntityDescriptor::new(source, &[], &[]);
        let error = session.issue_typed_entity_binding(&descriptor).unwrap_err();
        let DynamicTypedBindingError::SourceUnavailable(context) = error else {
            panic!("expected a source binding failure");
        };
        assert_eq!(context.entity_source(), source);
        assert_eq!(context.field_source(), None);
    }
    assert!(
        session
            .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
            .is_ok()
    );
}

#[test]
fn typed_binding_unavailable_fields_retain_source_context() {
    const MISSING_PRIMARY: TypedEntityDescriptor = TypedEntityDescriptor::new(
        ENTITY_SOURCE,
        &[REPLACEMENT_SOURCE],
        ENTITY_DESCRIPTOR.fields,
    );
    const INVALID_PRIMARY: TypedEntityDescriptor =
        TypedEntityDescriptor::new(ENTITY_SOURCE, &[""], ENTITY_DESCRIPTOR.fields);
    const MISSING_FIELD: TypedEntityDescriptor = TypedEntityDescriptor::new(
        ENTITY_SOURCE,
        &[ID_SOURCE],
        &[TypedFieldDescriptor::new(
            REPLACEMENT_SOURCE,
            TypedFieldType::Scalar(ScalarType::Nat64),
            false,
        )],
    );
    const INVALID_FIELD: TypedEntityDescriptor = TypedEntityDescriptor::new(
        ENTITY_SOURCE,
        &[ID_SOURCE],
        &[TypedFieldDescriptor::new(
            "",
            TypedFieldType::Scalar(ScalarType::Nat64),
            false,
        )],
    );
    const INVALID_NAMED_TYPE: TypedEntityDescriptor = TypedEntityDescriptor::new(
        ENTITY_SOURCE,
        &[ID_SOURCE],
        &[TypedFieldDescriptor::new(
            VALUE_SOURCE,
            TypedFieldType::List(&TypedFieldType::Named("")),
            false,
        )],
    );
    let session = initialize_typed_session();
    for (descriptor, source) in [
        (MISSING_PRIMARY, REPLACEMENT_SOURCE),
        (INVALID_PRIMARY, ""),
        (MISSING_FIELD, REPLACEMENT_SOURCE),
        (INVALID_FIELD, ""),
        (INVALID_NAMED_TYPE, VALUE_SOURCE),
    ] {
        let error = session.issue_typed_entity_binding(&descriptor).unwrap_err();
        let DynamicTypedBindingError::SourceUnavailable(context) = error else {
            panic!("expected a source binding failure");
        };
        assert_eq!(context.entity_source(), ENTITY_SOURCE);
        assert_eq!(context.field_source(), Some(source));
    }
    assert!(
        session
            .issue_typed_entity_binding(&ENTITY_DESCRIPTOR)
            .is_ok()
    );
}

#[test]
fn typed_binding_context_bounds_malformed_keys_at_utf8_boundaries() {
    let exact = Box::leak("x".repeat(MAX_SOURCE_KEY_BYTES).into_boxed_str());
    let oversized =
        Box::leak(format!("{}🦀", "x".repeat(MAX_SOURCE_KEY_BYTES - 1)).into_boxed_str());
    let context = TypedBindingContext::new(exact, Some(oversized));
    assert_eq!(context.entity_source(), exact);
    assert_eq!(
        context.field_source(),
        Some(&oversized[..MAX_SOURCE_KEY_BYTES - 1])
    );
    let session = initialize_typed_session();
    let descriptor = TypedEntityDescriptor::new(oversized, &[], &[]);
    let DynamicTypedBindingError::SourceUnavailable(context) =
        session.issue_typed_entity_binding(&descriptor).unwrap_err()
    else {
        panic!("expected a bounded source binding failure");
    };
    assert_eq!(
        context.entity_source(),
        &oversized[..MAX_SOURCE_KEY_BYTES - 1]
    );
    assert_eq!(context.field_source(), None);
}
