use candid::CandidType;
use serde::{Serialize, de::DeserializeOwned};

use crate::TypeParseError;

use super::{Duration, Timestamp};

fn cbor_bytes<T: Serialize>(value: &T) -> Vec<u8> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).expect("value should CBOR-encode");
    bytes
}

fn cbor_round_trip<T>(value: &T) -> T
where
    T: DeserializeOwned + Serialize,
{
    ciborium::from_reader(cbor_bytes(value).as_slice()).expect("value should CBOR-decode")
}

#[test]
fn time_atoms_keep_primitive_candid_wires() {
    let duration = Duration::from_millis(12_345);
    let timestamp = Timestamp::from_millis(-12_345);
    let candid = candid::encode_args((duration, timestamp)).expect("time atoms should encode");
    let primitive = candid::encode_args((duration.as_millis(), timestamp.as_millis()))
        .expect("primitive time wires should encode");
    let decoded: (Duration, Timestamp) =
        candid::decode_args(&candid).expect("time atoms should decode");

    assert_eq!(Duration::ty(), u64::ty());
    assert_eq!(Timestamp::ty(), i64::ty());
    assert_eq!(candid, primitive);
    assert_eq!(decoded, (duration, timestamp));
}

#[test]
fn time_atoms_keep_primitive_cbor_payloads() {
    let duration = Duration::from_millis(12_345);
    let timestamp = Timestamp::from_millis(-12_345);

    assert_eq!(cbor_bytes(&duration), cbor_bytes(&duration.as_millis()));
    assert_eq!(cbor_bytes(&timestamp), cbor_bytes(&timestamp.as_millis()));
    assert_eq!(cbor_round_trip(&duration), duration);
    assert_eq!(cbor_round_trip(&timestamp), timestamp);
}

#[test]
fn binary_time_atoms_reject_human_readable_text_forms() {
    let duration = cbor_bytes(&"2s");
    let timestamp = cbor_bytes(&"1969-12-31T23:59:59.999Z");

    assert!(ciborium::from_reader::<Duration, _>(duration.as_slice()).is_err());
    assert!(ciborium::from_reader::<Timestamp, _>(timestamp.as_slice()).is_err());
}

#[test]
fn human_readable_time_atoms_retain_flexible_text_input() {
    let duration: Duration = serde_json::from_str("\"2s\"").expect("duration text should decode");
    let timestamp: Timestamp = serde_json::from_str("\"1969-12-31T23:59:59.999Z\"")
        .expect("RFC3339 timestamp should decode");

    assert_eq!(duration, Duration::from_millis(2_000));
    assert_eq!(timestamp, Timestamp::from_millis(-1));
}

#[test]
fn duration_units_and_saturating_arithmetic_are_millisecond_native() {
    assert_eq!(Duration::from_secs(2).as_millis(), 2_000);
    assert_eq!(
        (Duration::MAX + Duration::from_millis(1)).as_millis(),
        u64::MAX,
    );
    assert_eq!((Duration::ZERO - Duration::from_millis(1)).as_millis(), 0);
}

#[test]
fn duration_flexible_parser_rejects_malformed_and_overflowing_text() {
    assert_eq!(
        Duration::parse_flexible("18446744073709551615ms"),
        Ok(Duration::MAX),
    );
    assert_eq!(
        Duration::parse_flexible("18446744073709552s"),
        Err(TypeParseError::InvalidDuration),
    );
    assert_eq!(
        Duration::parse_flexible("1.5s"),
        Err(TypeParseError::InvalidDuration),
    );
}

#[test]
fn timestamp_unit_constructors_are_millisecond_native() {
    assert_eq!(Timestamp::from_secs(42).as_millis(), 42_000);
    assert_eq!(Timestamp::from_millis(1_234).as_millis(), 1_234);
    assert_eq!(Timestamp::from_micros(5_000_000).as_millis(), 5_000);
    assert_eq!(Timestamp::from_nanos(3_000_000_000).as_millis(), 3_000);
}

#[test]
fn timestamp_submillisecond_constructors_floor_before_the_epoch() {
    assert_eq!(Timestamp::from_micros(-1).as_millis(), -1);
    assert_eq!(Timestamp::from_micros(-1_000).as_millis(), -1);
    assert_eq!(Timestamp::from_micros(-1_001).as_millis(), -2);
    assert_eq!(Timestamp::from_nanos(-1).as_millis(), -1);
    assert_eq!(Timestamp::from_nanos(-1_000_000).as_millis(), -1);
    assert_eq!(Timestamp::from_nanos(-1_000_001).as_millis(), -2);
}

#[test]
fn timestamp_seconds_floor_before_the_epoch() {
    assert_eq!(Timestamp::from_millis(-1).as_secs(), -1);
    assert_eq!(Timestamp::from_millis(-1_000).as_secs(), -1);
    assert_eq!(Timestamp::from_millis(-1_001).as_secs(), -2);
}

#[test]
fn timestamp_rfc3339_parsing_preserves_positive_and_offset_instants() {
    let utc =
        Timestamp::parse_rfc3339("2024-03-09T19:45:30Z").expect("valid UTC timestamp should parse");
    let offset = Timestamp::parse_rfc3339("2024-03-09T20:45:30+01:00")
        .expect("valid offset timestamp should parse");

    assert_eq!(utc.as_millis(), 1_710_013_530_000);
    assert_eq!(offset, utc);
}

#[test]
fn timestamp_rfc3339_fractional_parsing_floors_before_the_epoch() {
    let before_epoch = Timestamp::parse_rfc3339("1969-12-31T23:59:59.999999999Z")
        .expect("valid pre-epoch timestamp should parse");
    let positive = Timestamp::parse_rfc3339("2025-01-01T12:30:00.123456789Z")
        .expect("valid fractional timestamp should parse");

    assert_eq!(before_epoch, Timestamp::from_millis(-1));
    assert_eq!(positive, Timestamp::from_millis(1_735_734_600_123));
}

#[test]
fn timestamp_rfc3339_parser_rejects_invalid_text() {
    assert_eq!(
        Timestamp::parse_rfc3339("not-a-timestamp"),
        Err(TypeParseError::InvalidTimestamp),
    );
}

#[test]
fn timestamp_flexible_parser_accepts_integer_and_rfc3339_millis() {
    let numeric = Timestamp::parse_flexible("1710013530000")
        .expect("integer millisecond timestamp should parse");
    let rfc3339 =
        Timestamp::parse_flexible("2024-03-09T19:45:30Z").expect("RFC3339 timestamp should parse");

    assert_eq!(numeric, rfc3339);
    assert_eq!(
        Timestamp::parse_flexible("-12345").expect("negative millis should parse"),
        Timestamp::from_millis(-12_345),
    );
}

#[test]
fn timestamp_fallible_integer_construction_enforces_unsigned_range() {
    assert_eq!(
        Timestamp::try_from_i64(-1),
        Some(Timestamp::from_millis(-1)),
    );
    assert_eq!(
        Timestamp::try_from_u64(i64::MAX.cast_unsigned()),
        Some(Timestamp::MAX),
    );
    assert!(Timestamp::try_from_u64(i64::MAX.cast_unsigned() + 1).is_none());
}

#[test]
fn timestamp_duration_arithmetic_is_exact_within_range() {
    let later = Timestamp::from_millis(5_000);
    let earlier = Timestamp::from_millis(2_000);
    let duration = Duration::from_millis(999);

    assert_eq!(later + duration, Timestamp::from_millis(5_999));
    assert_eq!(later - duration, Timestamp::from_millis(4_001));
    assert_eq!(later - earlier, Duration::from_millis(3_000));
    assert_eq!(earlier - later, Duration::ZERO);
}

#[test]
fn timestamp_duration_arithmetic_uses_the_full_unsigned_domain() {
    assert_eq!(Timestamp::MIN + Duration::MAX, Timestamp::MAX);
    assert_eq!(Timestamp::MAX - Duration::MAX, Timestamp::MIN);
    assert_eq!(Timestamp::EPOCH + Duration::MAX, Timestamp::MAX);
    assert_eq!(Timestamp::EPOCH - Duration::MAX, Timestamp::MIN);
    assert_eq!(Timestamp::MAX - Timestamp::MIN, Duration::MAX);
}

#[test]
fn timestamp_duration_arithmetic_saturates_only_after_the_operation() {
    let one_millisecond = Duration::from_millis(1);

    assert_eq!(Timestamp::MAX + one_millisecond, Timestamp::MAX);
    assert_eq!(Timestamp::MIN - one_millisecond, Timestamp::MIN);
}
