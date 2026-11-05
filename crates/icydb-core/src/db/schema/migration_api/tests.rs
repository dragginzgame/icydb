use super::{MAX_SCHEMA_MIGRATION_STATUS_CURSOR_BYTES, SchemaMigrationStatusRequest};
use icydb_schema::wire::{decode_one, encode_one};

#[test]
fn cursor_byte_buffers_preserve_payload_and_application_bounds() {
    for length in [0, 1, 127, 128, 256, 257] {
        let cursor = vec![255; length];
        let request = SchemaMigrationStatusRequest::new(Some(cursor.clone()));
        let bytes = encode_one(&request).expect("encode byte cursor");
        // A one-field record has the same payload as its field. Ordinary
        // vectors remain a byte-identity control, not an accepted client API.
        assert_eq!(bytes, encode_one(Some(cursor)).expect("encode control"));
        let decoded: SchemaMigrationStatusRequest = decode_one(&bytes).expect("decode cursor");
        assert_eq!(decoded, request);
        assert_eq!(
            decoded.validate(),
            length <= MAX_SCHEMA_MIGRATION_STATUS_CURSOR_BYTES
        );
        assert!(decode_one::<SchemaMigrationStatusRequest>(&bytes[..bytes.len() - 1]).is_err());
    }
    let absent = SchemaMigrationStatusRequest::default();
    assert_eq!(encode_one(&absent).expect("encode absent cursor"), [0]);
    assert_eq!(
        decode_one::<SchemaMigrationStatusRequest>(&[0]).expect("decode absent cursor"),
        absent
    );
}
