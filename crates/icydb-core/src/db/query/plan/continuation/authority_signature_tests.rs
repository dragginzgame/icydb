use super::*;

fn accepted_identity(
    entity_byte: u8,
    revision: u64,
    root_byte: u8,
) -> AcceptedContinuationIdentity {
    AcceptedContinuationIdentity {
        entity_schema_fingerprint: [entity_byte; 16],
        accepted_revision: AcceptedSchemaRevision::new(revision),
        accepted_root_fingerprint: AcceptedSchemaFingerprint::new([root_byte; 32]),
    }
}

#[test]
fn continuation_signature_binds_every_accepted_schema_identity_component() {
    let shape = ContinuationSignature::from_bytes([0x11; 32]);
    let baseline = schema_bound_continuation_signature(shape, accepted_identity(0x22, 7, 0x33));

    assert_ne!(
        baseline,
        schema_bound_continuation_signature(shape, accepted_identity(0x23, 7, 0x33)),
    );
    assert_ne!(
        baseline,
        schema_bound_continuation_signature(shape, accepted_identity(0x22, 8, 0x33)),
    );
    assert_ne!(
        baseline,
        schema_bound_continuation_signature(shape, accepted_identity(0x22, 7, 0x34)),
    );
    assert_eq!(
        baseline,
        schema_bound_continuation_signature(shape, accepted_identity(0x22, 7, 0x33)),
    );
}
