//! Module: types::timestamp
//! Responsibility: module-local ownership and contracts for types::timestamp.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    traits::{
        Atomic, EntityKeyBytes, FieldValue, FieldValueKind, NumCast, NumFromPrimitive,
        NumToPrimitive, Repr, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom,
        Visitable,
    },
    types::{
        Duration,
        parse::{parse_fixed_ascii_i32, parse_fixed_ascii_u8},
    },
    value::Value,
};
use candid::CandidType;
use canic_cdk::utils::time::now_millis;
use derive_more::{Display, FromStr};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt,
    ops::{Add, AddAssign, Sub, SubAssign},
};
use time::{
    Date as TimeDate, Month, OffsetDateTime, PrimitiveDateTime, Time as TimeOfDay, UtcOffset,
    format_description::well_known::Rfc3339,
};

const ERR_INVALID_CALENDAR_DATE: &str = "timestamp parse error: invalid calendar date";
const ERR_INVALID_FRACTIONAL_SECONDS: &str = "timestamp parse error: invalid fractional seconds";
const ERR_INVALID_HOUR: &str = "timestamp parse error: invalid hour";
const ERR_INVALID_MINUTE: &str = "timestamp parse error: invalid minute";
const ERR_INVALID_MONTH: &str = "timestamp parse error: invalid month";
const ERR_INVALID_RFC3339_OFFSET: &str = "timestamp parse error: invalid RFC3339 UTC offset";
const ERR_INVALID_SECOND: &str = "timestamp parse error: invalid second";
const ERR_INVALID_SEPARATOR: &str =
    "timestamp parse error: expected RFC3339 calendar/time separators";
const ERR_INVALID_UTC_OFFSET: &str = "timestamp parse error: invalid UTC offset";
const ERR_INVALID_UTC_OFFSET_HOUR: &str = "timestamp parse error: invalid UTC offset hour";
const ERR_INVALID_UTC_OFFSET_MINUTE: &str = "timestamp parse error: invalid UTC offset minute";
const ERR_INVALID_WALL_CLOCK_TIME: &str = "timestamp parse error: invalid wall-clock time";
const ERR_INVALID_YEAR: &str = "timestamp parse error: invalid year";
const ERR_INVALID_DAY: &str = "timestamp parse error: invalid day";
const ERR_OUT_OF_RANGE_UNIX_MILLIS: &str = "timestamp parse error: out-of-range unix millis";
const ERR_EMPTY_FRACTIONAL_SECONDS: &str = "timestamp parse error: empty fractional seconds";
const ERR_FRACTIONAL_SECONDS_OVERFLOW: &str = "timestamp parse error: fractional seconds overflow";
const ERR_TIMESTAMP_TOO_SHORT: &str = "timestamp parse error: timestamp is too short";

// Invariant:
// Timestamp and Duration are both millisecond-native.
// All arithmetic is millisecond-consistent.
// Binary layout remains transparent fixed-width.

//
// Timestamp
//
// Stored as Unix milliseconds.
// RFC3339 JSON wire format is string-based.
//

#[derive(
    CandidType, Clone, Copy, Debug, Default, Display, Eq, FromStr, PartialEq, Hash, Ord, PartialOrd,
)]
#[repr(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    pub const EPOCH: Self = Self(0);
    pub const MIN: Self = Self(i64::MIN);
    pub const MAX: Self = Self(i64::MAX);

    const MILLIS_PER_SEC: i64 = 1_000;

    /// Construct from seconds (`i64`).
    #[must_use]
    pub const fn from_secs(secs: i64) -> Self {
        Self(secs.saturating_mul(Self::MILLIS_PER_SEC))
    }

    /// Construct from milliseconds (`i64`).
    #[must_use]
    pub const fn from_millis(ms: i64) -> Self {
        Self(ms)
    }

    /// Construct from microseconds (`i64`), truncating to whole milliseconds.
    #[must_use]
    pub fn from_micros(us: i64) -> Self {
        if us < 0 {
            return Self(us / Self::MILLIS_PER_SEC);
        }

        let positive = u64::try_from(us).unwrap_or(u64::MAX);
        let millis = Duration::from_micros_truncating(positive).as_millis();
        match i64::try_from(millis) {
            Ok(value) => Self(value),
            Err(_) => Self::MAX,
        }
    }

    /// Construct from nanoseconds (`i64`), truncating to whole milliseconds.
    #[must_use]
    pub fn from_nanos(ns: i64) -> Self {
        if ns < 0 {
            return Self(ns / 1_000_000);
        }

        let positive = u64::try_from(ns).unwrap_or(u64::MAX);
        let millis = Duration::from_nanos_truncating(positive).as_millis();
        match i64::try_from(millis) {
            Ok(value) => Self(value),
            Err(_) => Self::MAX,
        }
    }

    pub fn parse_rfc3339(s: &str) -> Result<Self, String> {
        // Phase 1: parse one strict RFC3339 timestamp locally so persisted
        // field decode does not retain the full `time` text parser.
        let parsed = parse_rfc3339_parts(s)?;

        // Phase 2: rebuild a validated UTC timestamp through `time`'s date/time
        // constructors rather than its format parser.
        let date = TimeDate::from_calendar_date(parsed.year, parsed.month, parsed.day)
            .map_err(|_| timestamp_parse_error(ERR_INVALID_CALENDAR_DATE))?;
        let time = TimeOfDay::from_hms_nano(
            parsed.hour,
            parsed.minute,
            parsed.second,
            parsed.nanoseconds,
        )
        .map_err(|_| timestamp_parse_error(ERR_INVALID_WALL_CLOCK_TIME))?;
        let offset = UtcOffset::from_hms(
            parsed.offset_sign * i8::try_from(parsed.offset_hour).unwrap_or(i8::MAX),
            parsed.offset_sign * i8::try_from(parsed.offset_minute).unwrap_or(i8::MAX),
            0,
        )
        .map_err(|_| timestamp_parse_error(ERR_INVALID_UTC_OFFSET))?;
        let ts_millis = PrimitiveDateTime::new(date, time)
            .assume_offset(offset)
            .unix_timestamp_nanos()
            / 1_000_000;
        let ts_millis = i64::try_from(ts_millis)
            .map_err(|_| timestamp_parse_error(ERR_OUT_OF_RANGE_UNIX_MILLIS))?;

        Ok(Self::from_millis(ts_millis))
    }

    pub fn parse_flexible(s: &str) -> Result<Self, String> {
        // Try integer milliseconds.
        if let Ok(n) = s.parse::<i64>() {
            return Ok(Self::from_millis(n));
        }

        // Try RFC3339
        Self::parse_rfc3339(s)
    }

    /// Current wall-clock timestamp in milliseconds.
    #[must_use]
    pub fn now() -> Self {
        match i64::try_from(now_millis()) {
            Ok(ms) => Self(ms),
            Err(_) => Self::MAX,
        }
    }

    /// Return Unix milliseconds as `i64`.
    #[must_use]
    pub const fn as_millis(self) -> i64 {
        self.0
    }

    /// Return Unix seconds as `i64`.
    #[must_use]
    pub const fn as_secs(self) -> i64 {
        self.0 / Self::MILLIS_PER_SEC
    }
}

// Duration stores millis as u64; clamp at i64::MAX when adding/subtracting
// against signed timestamps so arithmetic stays saturating and total.
fn duration_millis_to_i64(duration: Duration) -> i64 {
    i64::try_from(duration.repr()).unwrap_or(i64::MAX)
}

//
// ParsedRfc3339Timestamp
//
// ParsedRfc3339Timestamp captures one strictly parsed RFC3339 timestamp
// payload before it is rebuilt through `time` constructors.
// It keeps text-shape validation local to `Timestamp` without exposing parser
// internals outside this module.
//

struct ParsedRfc3339Timestamp {
    year: i32,
    month: Month,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    nanoseconds: u32,
    offset_sign: i8,
    offset_hour: u8,
    offset_minute: u8,
}

// Parse one strict RFC3339 timestamp payload without routing through
// `time`'s format-description parser.
fn parse_rfc3339_parts(s: &str) -> Result<ParsedRfc3339Timestamp, String> {
    let bytes = s.as_bytes();
    if bytes.len() < 20 {
        return Err(timestamp_parse_error(ERR_TIMESTAMP_TOO_SHORT));
    }
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return Err(timestamp_parse_error(ERR_INVALID_SEPARATOR));
    }

    // Phase 1: decode the fixed-width calendar and wall-clock fields.
    let year = parse_required_i32(&bytes[0..4], ERR_INVALID_YEAR)?;
    let month_raw = parse_required_u8(&bytes[5..7], ERR_INVALID_MONTH)?;
    let month = Month::try_from(month_raw).map_err(|_| timestamp_parse_error(ERR_INVALID_MONTH))?;
    let day = parse_required_u8(&bytes[8..10], ERR_INVALID_DAY)?;
    let hour = parse_required_u8(&bytes[11..13], ERR_INVALID_HOUR)?;
    let minute = parse_required_u8(&bytes[14..16], ERR_INVALID_MINUTE)?;
    let second = parse_required_u8(&bytes[17..19], ERR_INVALID_SECOND)?;

    // Phase 2: parse optional fractional seconds and the trailing UTC offset.
    let mut cursor = 19;
    let nanoseconds = if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let fraction_start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        if cursor == fraction_start {
            return Err(timestamp_parse_error(ERR_EMPTY_FRACTIONAL_SECONDS));
        }
        parse_fractional_nanoseconds(&bytes[fraction_start..cursor])?
    } else {
        0
    };

    let (offset_sign, offset_hour, offset_minute) = parse_rfc3339_offset(&bytes[cursor..])?;

    Ok(ParsedRfc3339Timestamp {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanoseconds,
        offset_sign,
        offset_hour,
        offset_minute,
    })
}

// Parse one RFC3339 fractional second suffix into nanoseconds, truncating
// precision beyond nine digits.
fn parse_fractional_nanoseconds(bytes: &[u8]) -> Result<u32, String> {
    let mut value = 0_u32;
    for &byte in bytes.iter().take(9) {
        let digit = byte
            .checked_sub(b'0')
            .filter(|digit| *digit <= 9)
            .ok_or_else(|| timestamp_parse_error(ERR_INVALID_FRACTIONAL_SECONDS))?;
        value = value
            .checked_mul(10)
            .and_then(|current| current.checked_add(<u32 as From<u8>>::from(digit)))
            .ok_or_else(|| timestamp_parse_error(ERR_FRACTIONAL_SECONDS_OVERFLOW))?;
    }
    for _ in bytes.len().min(9)..9 {
        value = value
            .checked_mul(10)
            .ok_or_else(|| timestamp_parse_error(ERR_FRACTIONAL_SECONDS_OVERFLOW))?;
    }

    Ok(value)
}

// Parse one strict RFC3339 UTC offset suffix.
fn parse_rfc3339_offset(bytes: &[u8]) -> Result<(i8, u8, u8), String> {
    match bytes {
        [b'Z'] => Ok((1, 0, 0)),
        [sign @ (b'+' | b'-'), hour0, hour1, b':', minute0, minute1] => {
            let hour = parse_required_u8(&[*hour0, *hour1], ERR_INVALID_UTC_OFFSET_HOUR)?;
            let minute = parse_required_u8(&[*minute0, *minute1], ERR_INVALID_UTC_OFFSET_MINUTE)?;
            let sign = if *sign == b'+' { 1 } else { -1 };

            Ok((sign, hour, minute))
        }
        _ => Err(timestamp_parse_error(ERR_INVALID_RFC3339_OFFSET)),
    }
}

// Build one owned parse error string from a shared static message.
fn timestamp_parse_error(message: &'static str) -> String {
    message.to_owned()
}

// Parse one required fixed-width ASCII integer field into `i32`.
fn parse_required_i32(bytes: &[u8], error: &'static str) -> Result<i32, String> {
    parse_fixed_ascii_i32(bytes).ok_or_else(|| timestamp_parse_error(error))
}

// Parse one required fixed-width ASCII integer field into `u8`.
fn parse_required_u8(bytes: &[u8], error: &'static str) -> Result<u8, String> {
    parse_fixed_ascii_u8(bytes).ok_or_else(|| timestamp_parse_error(error))
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0.saturating_add(duration_millis_to_i64(rhs)))
    }
}

impl AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 = self.0.saturating_add(duration_millis_to_i64(rhs));
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Self;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0.saturating_sub(duration_millis_to_i64(rhs)))
    }
}

impl SubAssign<Duration> for Timestamp {
    fn sub_assign(&mut self, rhs: Duration) {
        self.0 = self.0.saturating_sub(duration_millis_to_i64(rhs));
    }
}

impl Sub for Timestamp {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        if self.0 <= rhs.0 {
            return Duration::ZERO;
        }

        let delta = <i128 as From<i64>>::from(self.0) - <i128 as From<i64>>::from(rhs.0);
        let millis = u64::try_from(delta).unwrap_or(u64::MAX);
        Duration::from_millis(millis)
    }
}

impl Repr for Timestamp {
    type Inner = i64;

    fn repr(&self) -> Self::Inner {
        self.0
    }

    fn from_repr(inner: Self::Inner) -> Self {
        Self(inner)
    }
}

impl Atomic for Timestamp {}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let nanos = <i128 as From<i64>>::from(self.0).saturating_mul(1_000_000);
        let dt =
            OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(serde::ser::Error::custom)?;
        let rendered = dt.format(&Rfc3339).map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(&rendered)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Accept unix-millis integers and RFC3339 / integer strings.
        struct TimestampVisitor;

        impl serde::de::Visitor<'_> for TimestampVisitor {
            type Value = Timestamp;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "unix millis or RFC3339 timestamp")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
                Ok(Timestamp::from_millis(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let millis = i64::try_from(v)
                    .map_err(|_| E::custom("unix millis exceeds i64 timestamp range"))?;
                Ok(Timestamp::from_millis(millis))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Timestamp::parse_flexible(v).map_err(E::custom)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&v)
            }
        }

        deserializer.deserialize_any(TimestampVisitor)
    }
}

impl EntityKeyBytes for Timestamp {
    const BYTE_LEN: usize = 8;

    fn write_bytes(&self, out: &mut [u8]) {
        assert_eq!(out.len(), Self::BYTE_LEN);
        out.copy_from_slice(&self.0.to_be_bytes());
    }
}

impl FieldValue for Timestamp {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Timestamp(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Timestamp(v) => Some(*v),
            _ => None,
        }
    }
}

impl NumCast for Timestamp {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_i64().map(Self)
    }
}

impl NumFromPrimitive for Timestamp {
    fn from_i64(n: i64) -> Option<Self> {
        Some(Self(n))
    }

    fn from_u64(n: u64) -> Option<Self> {
        i64::try_from(n).ok().map(Self)
    }
}

impl From<u64> for Timestamp {
    fn from(n: u64) -> Self {
        match i64::try_from(n) {
            Ok(ms) => Self(ms),
            Err(_) => Self::MAX,
        }
    }
}

impl From<i64> for Timestamp {
    fn from(n: i64) -> Self {
        Self(n)
    }
}

impl NumToPrimitive for Timestamp {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
}

impl SanitizeAuto for Timestamp {}

impl SanitizeCustom for Timestamp {}

impl ValidateAuto for Timestamp {}

impl ValidateCustom for Timestamp {}

impl Visitable for Timestamp {}

//
// TESTS
//

#[cfg(test)]
mod tests {
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
    fn test_from_i64_accepts_negative() {
        let t = <Timestamp as NumFromPrimitive>::from_i64(-1);
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
    fn test_num_cast_roundtrip() {
        let t = Timestamp::from_secs(999);
        let i = t.to_u64().unwrap();
        assert_eq!(i, 999_000);

        let t2: Timestamp = i.into();
        assert_eq!(t2, t);
    }

    #[test]
    fn test_field_value() {
        let t = Timestamp::from_secs(77);
        let v = t.to_value();
        assert_eq!(v, Value::Timestamp(t));
    }

    #[test]
    fn test_json_iso_serialization() {
        let t = Timestamp::from_millis(1_710_013_530_123);
        let json = serde_json::to_string(&t).unwrap();
        assert_eq!(json, "\"2024-03-09T19:45:30.123Z\"");
    }

    #[test]
    fn test_json_unix_deserialization() {
        let unquoted: Timestamp = serde_json::from_str("1710013530000").unwrap();
        assert_eq!(unquoted, Timestamp::from_millis(1_710_013_530_000));

        let quoted: Timestamp = serde_json::from_str("\"1710013530000\"").unwrap();
        assert_eq!(quoted, Timestamp::from_millis(1_710_013_530_000));
    }

    #[test]
    fn test_json_iso_deserialization() {
        let parsed: Timestamp = serde_json::from_str("\"2024-03-09T19:45:30Z\"").unwrap();
        assert_eq!(parsed, Timestamp::from_millis(1_710_013_530_000));
    }

    #[test]
    fn test_json_rejects_invalid_iso_and_out_of_range_u64() {
        let iso_err = serde_json::from_str::<Timestamp>("\"not-a-timestamp\"").unwrap_err();
        assert!(iso_err.to_string().contains("timestamp parse error"));

        let overflow_u64_err =
            serde_json::from_str::<Timestamp>("18446744073709551615").unwrap_err();
        assert!(
            overflow_u64_err
                .to_string()
                .contains("exceeds i64 timestamp range")
        );
    }

    #[test]
    fn test_json_pre_epoch_roundtrip() {
        let ts = Timestamp::from_millis(-1_000);
        let json = serde_json::to_string(&ts).unwrap();
        let parsed: Timestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ts);
    }

    #[test]
    fn test_json_roundtrip() {
        let t = Timestamp::from_millis(1_710_013_530_000);
        let json = serde_json::to_string(&t).unwrap();
        let parsed: Timestamp = serde_json::from_str(&json).unwrap();
        assert_eq!(t, parsed);
    }

    #[test]
    fn test_serde_cbor_boundary_uses_rfc3339_text_not_millis_number() {
        let t = Timestamp::from_millis(1_710_013_530_123);

        let bytes = serde_cbor::to_vec(&t).expect("timestamp serialization should succeed");
        let wire: serde_cbor::Value =
            serde_cbor::from_slice(&bytes).expect("timestamp cbor decode should succeed");

        match wire {
            serde_cbor::Value::Text(rendered) => {
                assert_eq!(rendered, "2024-03-09T19:45:30.123Z");
            }
            other => panic!("timestamp wire shape must remain RFC3339 text, got {other:?}"),
        }

        let decoded: Timestamp =
            serde_cbor::from_slice(&bytes).expect("timestamp decode should succeed");
        assert_eq!(decoded, t);
    }

    #[test]
    fn test_json_extreme_timestamps_fail_cleanly_for_iso_rendering() {
        assert!(serde_json::to_string(&Timestamp::MIN).is_err());
        assert!(serde_json::to_string(&Timestamp::MAX).is_err());
    }
}
