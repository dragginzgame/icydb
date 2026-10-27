//! Aggregate row-size admission rejects before publishing any batch effects.

use super::*;
use crate::{
    db::codec::MAX_ROW_BYTES,
    error::{ErrorClass, ErrorOrigin},
};
use icydb_diagnostic_code::DiagnosticFactTag;

fn blob_row(id: u64, field_bytes: usize) -> DynamicStructuralPatch {
    DynamicStructuralPatch::new(vec![
        ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
        (
            "first".into(),
            DynamicWriteCell::Value(InputValue::blob(vec![7; field_bytes])),
        ),
        (
            "second".into(),
            DynamicWriteCell::Value(InputValue::blob(vec![9; field_bytes])),
        ),
    ])
}

#[test]
fn aggregate_row_limit_rejects_individually_valid_fields_without_batch_effects() {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "first", 1, AcceptedFieldKind::Blob { max_len: None }),
            field(3, "second", 2, AcceptedFieldKind::Blob { max_len: None }),
        ],
        Vec::new(),
    );
    let read = "SELECT id FROM PlannerRow ORDER BY id";
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let session = new_request_session(&root);
    session
        .execute_trusted_dynamic_insert_batch(
            ENTITY_NAME,
            vec![blob_row(1, 8), blob_row(2, 256 * 1024)],
        )
        .unwrap();
    let before = projection_rows(&session, read);
    assert_eq!(before.len(), 2);

    for _ in 0..2 {
        let root = crate::db::RequestExecutionRoot::__new_runtime_root();
        let session = new_request_session(&root);
        let error = session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![blob_row(3, 8), blob_row(4, 3 * 1024 * 1024)],
            )
            .unwrap_err();
        assert_eq!(error.class(), ErrorClass::Unsupported);
        assert_eq!(error.origin(), ErrorOrigin::Serialize);
        let facts = error.diagnostic_facts();
        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].0, DiagnosticFactTag::ActualLength);
        assert!(facts[0].1 > 6 * 1024 * 1024);
        assert_eq!(
            facts[1],
            (DiagnosticFactTag::Limit, u64::from(MAX_ROW_BYTES))
        );
        assert_eq!(projection_rows(&session, read), before);
    }
}
