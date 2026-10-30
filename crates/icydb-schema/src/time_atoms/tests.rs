use crate::TypeParseError;

use super::{Duration, Timestamp};

#[test]
fn time_atoms_keep_primitive_wire_shapes() {
    for pair in [
        (Duration::ZERO, Timestamp::MIN),
        (Duration::MAX, Timestamp::MAX),
        (
            Duration::from_millis(12_345),
            Timestamp::from_millis(-12_345),
        ),
    ] {
        let wire = crate::wire::encode_one(pair).expect("time atoms should encode");
        let primitive = crate::wire::encode_one((pair.0.as_millis(), pair.1.as_millis()))
            .expect("primitive time wires should encode");
        assert_eq!(wire, primitive);
        assert_eq!(
            crate::wire::decode_one::<(Duration, Timestamp)>(&wire).unwrap(),
            pair
        );
        let batch = vec![pair; 1_000];
        let wire = crate::wire::encode_one(&batch).unwrap();
        assert_eq!(
            crate::wire::decode_one::<Vec<(Duration, Timestamp)>>(&wire).unwrap(),
            batch
        );
    }
}

#[test]
fn binary_time_atoms_reject_human_readable_text_forms() {
    let duration = crate::wire::encode_one("2s").unwrap();
    let timestamp = crate::wire::encode_one("1969-12-31T23:59:59.999Z").unwrap();

    assert!(crate::wire::decode_one::<Duration>(&duration).is_err());
    assert!(crate::wire::decode_one::<Timestamp>(&timestamp).is_err());
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
fn timestamp_calendar_offsets_and_fractions_match_the_reference() {
    use time::{Date as ReferenceDate, Month, PrimitiveDateTime, Time, UtcOffset};

    for year in [0, 1, 100, 400, 1900, 1969, 1970, 2000, 2024, 9999] {
        for (month, day) in [(1, 1), (2, 28), (2, 29), (3, 1), (12, 31)] {
            let Ok(date) =
                ReferenceDate::from_calendar_date(year, Month::try_from(month).unwrap(), day)
            else {
                continue;
            };
            for (hour, minute, second) in [(0, 0, 0), (12, 34, 56), (23, 59, 59)] {
                for (fraction, nanos) in [
                    ("", 0),
                    (".0", 0),
                    (".000000001", 1),
                    (".001", 1_000_000),
                    (".123456789", 123_456_789),
                    (".9999999999", 999_999_999),
                ] {
                    let time = Time::from_hms_nano(hour, minute, second, nanos).unwrap();
                    for (suffix, offset_hour, offset_minute) in [
                        ("Z", 0, 0),
                        ("+00:00", 0, 0),
                        ("-00:00", 0, 0),
                        ("+23:59", 23, 59),
                        ("-23:59", -23, -59),
                        ("+01:30", 1, 30),
                        ("-12:45", -12, -45),
                    ] {
                        let offset = UtcOffset::from_hms(offset_hour, offset_minute, 0).unwrap();
                        let expected = PrimitiveDateTime::new(date, time)
                            .assume_offset(offset)
                            .unix_timestamp_nanos()
                            .div_euclid(1_000_000);
                        let input = format!(
                            "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}{fraction}{suffix}"
                        );
                        assert_eq!(
                            Timestamp::parse_rfc3339(&input),
                            Ok(Timestamp::from_millis(i64::try_from(expected).unwrap())),
                            "{input}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn timestamp_calendar_parser_rejects_invalid_components_and_framing() {
    for input in [
        "0000-02-30T00:00:00Z",
        "1900-02-29T00:00:00Z",
        "2024-00-01T00:00:00Z",
        "2024-13-01T00:00:00Z",
        "2024-01-00T00:00:00Z",
        "2024-01-32T00:00:00Z",
        "2024-01-01T24:00:00Z",
        "2024-01-01T00:60:00Z",
        "2024-01-01T00:00:60Z",
        "2024-01-01T00:00:00+24:00",
        "2024-01-01T00:00:00-24:00",
        "2024-01-01T00:00:00+00:60",
        "2024-01-01T00:00:00-00:60",
        "2024-01-01T00:00:00+99:99",
        "2024-01-01T00:00:00.Z",
        "2024-01-01T00:00:00.1xZ",
        "2024-01-01T00:00:00Zsuffix",
        "2024-01-01t00:00:00Z",
        "2024-01-01T00:00:00z",
    ] {
        assert_eq!(
            Timestamp::parse_rfc3339(input),
            Err(TypeParseError::InvalidTimestamp),
            "{input}"
        );
    }
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
