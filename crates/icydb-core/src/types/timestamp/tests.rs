use super::*;

#[test]
fn current_timestamp_reads_a_nonzero_runtime_clock() {
    assert!(Timestamp::now().as_millis() > 0);
}

#[test]
fn timestamp_runtime_representation_round_trips() {
    let timestamp = Timestamp::from_repr(-42);

    assert_eq!(timestamp.repr(), -42);
}

#[test]
fn timestamp_entity_key_bytes_preserve_signed_payload() {
    let timestamp = Timestamp::from_millis(-42);
    let mut bytes = [0; Timestamp::BYTE_LEN];

    timestamp
        .write_bytes(&mut bytes)
        .expect("exact timestamp key buffer should encode");

    assert_eq!(bytes, (-42_i64).to_be_bytes());
}
