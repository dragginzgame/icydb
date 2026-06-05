use super::*;

#[test]
fn test_from_secs() {
    let t = Timestamp::from_secs(42);
    assert_eq!(t.as_secs(), 42);
    assert_eq!(t.as_millis(), 42_000);
}

#[test]
fn test_explicit_unit_suffix_constructors() {
    assert_eq!(Timestamp::from_secs(42).as_secs(), 42);
    assert_eq!(Timestamp::from_millis(1_234).as_millis(), 1_234);
    assert_eq!(Timestamp::from_micros(5_000_000).as_millis(), 5_000);
    assert_eq!(Timestamp::from_nanos(3_000_000_000).as_millis(), 3_000);
}

#[test]
fn test_parse_rfc3339_manual() {
    // Real RFC-3339 timestamp, exactly how JustTCG returns them.
    let input = "2024-03-09T19:45:30Z";

    let parsed = Timestamp::parse_rfc3339(input).unwrap();

    // Verified UNIX time for that timestamp in milliseconds.
    let expected = 1_710_013_530_000i64;

    assert_eq!(parsed.as_millis(), expected);
}

#[test]
fn test_pre_epoch_timestamp() {
    let parsed = Timestamp::parse_rfc3339("1969-12-31T23:59:59Z").unwrap();
    assert_eq!(parsed.as_millis(), -1_000);
}

#[test]
fn test_try_from_i64_accepts_negative() {
    let t = Timestamp::try_from_i64(-1);
    assert_eq!(t, Some(Timestamp::from_millis(-1)));
}

#[test]
fn test_from_millis() {
    let t = Timestamp::from_millis(1234);
    assert_eq!(t.as_millis(), 1_234);
    assert_eq!(t.as_secs(), 1);
}

#[test]
fn test_from_micros() {
    let t = Timestamp::from_micros(5_000_000);
    assert_eq!(t.as_millis(), 5_000);
    assert_eq!(t.as_secs(), 5);
}

#[test]
fn test_from_nanos() {
    let t = Timestamp::from_nanos(3_000_000_000);
    assert_eq!(t.as_millis(), 3_000);
    assert_eq!(t.as_secs(), 3);
}

#[test]
fn test_parse_flexible_integer() {
    let t = Timestamp::parse_flexible("12345").unwrap();
    assert_eq!(t.as_millis(), 12_345);
}

#[test]
fn test_parse_flexible_numeric_string_negative() {
    let t = Timestamp::parse_flexible("-12345").unwrap();
    assert_eq!(t.as_millis(), -12_345);
}

#[test]
fn timestamp_parse_equivalence_iso_vs_millis() {
    let iso =
        Timestamp::parse_flexible("2024-03-09T19:45:30Z").expect("ISO timestamp should parse");

    let millis =
        Timestamp::parse_flexible("1710013530000").expect("millisecond timestamp should parse");

    assert_eq!(
        iso, millis,
        "ISO and unix-millis representations must parse to identical Timestamp values"
    );
}

#[test]
fn test_parse_flexible_rfc3339_fractional() {
    let t = Timestamp::parse_flexible("2025-01-01T12:30:00.123Z").unwrap();
    assert_eq!(t, Timestamp::from_millis(1_735_734_600_123));
}

#[test]
fn test_parse_rfc3339_with_positive_offset() {
    let parsed = Timestamp::parse_rfc3339("2024-03-09T20:45:30+01:00").unwrap();
    assert_eq!(parsed.as_millis(), 1_710_013_530_000);
}

#[test]
fn test_parse_rfc3339_truncates_sub_millisecond_fraction() {
    let parsed = Timestamp::parse_rfc3339("2025-01-01T12:30:00.123456789Z").unwrap();
    assert_eq!(parsed, Timestamp::from_millis(1_735_734_600_123));
}

#[test]
fn test_parse_rfc3339_invalid() {
    let result = Timestamp::parse_rfc3339("not-a-timestamp");
    assert!(result.is_err());
}

#[test]
fn test_now_is_nonzero() {
    let t = Timestamp::now();
    assert!(t.as_millis() > 0);
}

#[test]
fn test_add_and_sub_with_duration_and_timestamp_difference() {
    let a = Timestamp::from_millis(5_000);
    let b = Timestamp::from_millis(2_000);
    let d = Duration::from_millis(999);

    assert_eq!(a + d, Timestamp::from_millis(5_999));
    assert_eq!(a - d, Timestamp::from_millis(4_001));
    assert_eq!(a - b, Duration::from_millis(3_000));
}

#[test]
fn test_millisecond_precision_in_arithmetic() {
    let t = Timestamp::from_millis(1_500);
    let d = Duration::from_millis(1);
    assert_eq!(t + d, Timestamp::from_millis(1_501));
}

#[test]
fn test_no_truncation_in_timestamp_duration_addition() {
    let t = Timestamp::from_millis(1_000);
    let d = Duration::from_millis(999);
    assert_eq!(t + d, Timestamp::from_millis(1_999));
}

#[test]
fn test_cross_type_timestamp_difference_returns_millisecond_duration() {
    let t1 = Timestamp::from_millis(1_500);
    let t2 = Timestamp::from_millis(1_000);

    assert_eq!(t1 - t2, Duration::from_millis(500));
}

#[test]
fn test_timestamp_arithmetic_saturates_at_bounds() {
    let one_ms = Duration::from_millis(1);
    assert_eq!(Timestamp::MAX + one_ms, Timestamp::MAX);
    assert_eq!(Timestamp::MIN - one_ms, Timestamp::MIN);
}

#[test]
fn test_timestamp_difference_saturates_and_never_goes_negative() {
    assert_eq!(
        Timestamp::from_millis(1_000) - Timestamp::from_millis(2_000),
        Duration::ZERO
    );
    assert_eq!(Timestamp::MAX - Timestamp::MIN, Duration::MAX);
}

#[test]
fn test_numeric_value_roundtrip() {
    let t = Timestamp::from_secs(999);
    let i = u64::try_from(t.as_millis()).unwrap();
    assert_eq!(i, 999_000);

    let t2: Timestamp = i.into();
    assert_eq!(t2, t);
}

#[test]
fn test_runtime_value() {
    let t = Timestamp::from_secs(77);
    let v = t.to_value();
    assert_eq!(v, Value::Timestamp(t));
}
