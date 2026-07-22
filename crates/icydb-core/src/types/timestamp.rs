//! Module: types::timestamp
//! Defines the millisecond-native timestamp type used by typed values,
//! arithmetic with durations, and RFC3339 wire conversion.

use crate::runtime::now_millis;
use crate::{
    db::{EntityKeyBytes, EntityKeyBytesError, validate_entity_key_bytes_buffer},
    traits::Repr,
    types::{
        Decimal, Duration, NumericValue, TypeParseError,
        parse::{parse_fixed_ascii_i32, parse_fixed_ascii_u8},
    },
    value::{RuntimeValueDecode, RuntimeValueEncode, RuntimeValueKind, RuntimeValueMeta, Value},
    visitor::{SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable},
};
use candid::CandidType;
use serde::{Deserialize, Deserializer};
use std::{
    fmt,
    ops::{Add, AddAssign, Sub, SubAssign},
};
use time::{Date as TimeDate, Month, PrimitiveDateTime, Time as TimeOfDay, UtcOffset};

// Invariant:
// Timestamp and Duration are both millisecond-native.
// All arithmetic is millisecond-consistent.
// Binary layout remains transparent fixed-width.

//
// Timestamp
//
// Stored as Unix milliseconds.
// API/JSON deserialization accepts RFC3339 strings and unix-millis numbers.
//

#[derive(CandidType, Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
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

    /// Fallible conversion from `i64` milliseconds.
    #[must_use]
    pub const fn try_from_i64(millis: i64) -> Option<Self> {
        Some(Self(millis))
    }

    /// Fallible conversion from `u64` milliseconds.
    #[must_use]
    pub fn try_from_u64(millis: u64) -> Option<Self> {
        i64::try_from(millis).ok().map(Self)
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

    pub fn parse_rfc3339(s: &str) -> Result<Self, TypeParseError> {
        // Phase 1: parse one strict RFC3339 timestamp locally so persisted
        // field decode does not retain the full `time` text parser.
        let parsed = parse_rfc3339_components(s)?;

        // Phase 2: rebuild a validated UTC timestamp through `time`'s date/time
        // constructors rather than its format parser.
        let date = TimeDate::from_calendar_date(parsed.year, parsed.month, parsed.day)
            .map_err(|_| TypeParseError::InvalidTimestamp)?;
        let time = TimeOfDay::from_hms_nano(
            parsed.hour,
            parsed.minute,
            parsed.second,
            parsed.nanoseconds,
        )
        .map_err(|_| TypeParseError::InvalidTimestamp)?;
        let offset = UtcOffset::from_hms(
            parsed.offset_sign * i8::try_from(parsed.offset_hour).unwrap_or(i8::MAX),
            parsed.offset_sign * i8::try_from(parsed.offset_minute).unwrap_or(i8::MAX),
            0,
        )
        .map_err(|_| TypeParseError::InvalidTimestamp)?;
        let ts_millis = PrimitiveDateTime::new(date, time)
            .assume_offset(offset)
            .unix_timestamp_nanos()
            / 1_000_000;
        let ts_millis = i64::try_from(ts_millis).map_err(|_| TypeParseError::InvalidTimestamp)?;

        Ok(Self::from_millis(ts_millis))
    }

    pub fn parse_flexible(s: &str) -> Result<Self, TypeParseError> {
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

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// Duration stores millis as u64; clamp at i64::MAX when adding/subtracting
// against signed timestamps so arithmetic stays saturating and total.
fn duration_millis_to_i64(duration: Duration) -> i64 {
    i64::try_from(duration.repr()).unwrap_or(i64::MAX)
}

//
// Rfc3339TimestampComponents
//
// Rfc3339TimestampComponents captures one strictly parsed RFC3339 timestamp
// payload before it is rebuilt through `time` constructors.
// It keeps text-shape validation local to `Timestamp` without exposing parser
// internals outside this module.
//

struct Rfc3339TimestampComponents {
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
fn parse_rfc3339_components(s: &str) -> Result<Rfc3339TimestampComponents, TypeParseError> {
    let bytes = s.as_bytes();
    if bytes.len() < 20 {
        return Err(TypeParseError::InvalidTimestamp);
    }
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return Err(TypeParseError::InvalidTimestamp);
    }

    // Phase 1: decode the fixed-width calendar and wall-clock fields.
    let year = parse_required_i32(&bytes[0..4])?;
    let month_raw = parse_required_u8(&bytes[5..7])?;
    let month = Month::try_from(month_raw).map_err(|_| TypeParseError::InvalidTimestamp)?;
    let day = parse_required_u8(&bytes[8..10])?;
    let hour = parse_required_u8(&bytes[11..13])?;
    let minute = parse_required_u8(&bytes[14..16])?;
    let second = parse_required_u8(&bytes[17..19])?;

    // Phase 2: parse optional fractional seconds and the trailing UTC offset.
    let mut cursor = 19;
    let nanoseconds = if bytes.get(cursor) == Some(&b'.') {
        cursor += 1;
        let fraction_start = cursor;
        while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
            cursor += 1;
        }
        if cursor == fraction_start {
            return Err(TypeParseError::InvalidTimestamp);
        }
        parse_fractional_nanoseconds(&bytes[fraction_start..cursor])?
    } else {
        0
    };

    let (offset_sign, offset_hour, offset_minute) = parse_rfc3339_offset(&bytes[cursor..])?;

    Ok(Rfc3339TimestampComponents {
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
fn parse_fractional_nanoseconds(bytes: &[u8]) -> Result<u32, TypeParseError> {
    let mut value = 0_u32;
    for &byte in bytes.iter().take(9) {
        let digit = byte
            .checked_sub(b'0')
            .filter(|digit| *digit <= 9)
            .ok_or(TypeParseError::InvalidTimestamp)?;
        value = value
            .checked_mul(10)
            .and_then(|current| current.checked_add(u32::from(digit)))
            .ok_or(TypeParseError::InvalidTimestamp)?;
    }
    for _ in bytes.len().min(9)..9 {
        value = value
            .checked_mul(10)
            .ok_or(TypeParseError::InvalidTimestamp)?;
    }

    Ok(value)
}

// Parse one strict RFC3339 UTC offset suffix.
fn parse_rfc3339_offset(bytes: &[u8]) -> Result<(i8, u8, u8), TypeParseError> {
    match bytes {
        [b'Z'] => Ok((1, 0, 0)),
        [sign @ (b'+' | b'-'), hour0, hour1, b':', minute0, minute1] => {
            let hour = parse_required_u8(&[*hour0, *hour1])?;
            let minute = parse_required_u8(&[*minute0, *minute1])?;
            let sign = if *sign == b'+' { 1 } else { -1 };

            Ok((sign, hour, minute))
        }
        _ => Err(TypeParseError::InvalidTimestamp),
    }
}

// Parse one required fixed-width ASCII integer field into `i32`.
fn parse_required_i32(bytes: &[u8]) -> Result<i32, TypeParseError> {
    parse_fixed_ascii_i32(bytes).ok_or(TypeParseError::InvalidTimestamp)
}

// Parse one required fixed-width ASCII integer field into `u8`.
fn parse_required_u8(bytes: &[u8]) -> Result<u8, TypeParseError> {
    parse_fixed_ascii_u8(bytes).ok_or(TypeParseError::InvalidTimestamp)
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

        let delta = i128::from(self.0) - i128::from(rhs.0);
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

    fn write_bytes(&self, out: &mut [u8]) -> Result<(), EntityKeyBytesError> {
        validate_entity_key_bytes_buffer(out, Self::BYTE_LEN)?;
        out.copy_from_slice(&self.0.to_be_bytes());

        Ok(())
    }
}

impl RuntimeValueMeta for Timestamp {
    fn kind() -> RuntimeValueKind {
        RuntimeValueKind::Atomic
    }
}

impl RuntimeValueEncode for Timestamp {
    fn to_value(&self) -> Value {
        Value::Timestamp(*self)
    }
}

impl RuntimeValueDecode for Timestamp {
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Timestamp(v) => Some(*v),
            _ => None,
        }
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

impl NumericValue for Timestamp {
    fn try_to_decimal(&self) -> Option<Decimal> {
        Decimal::from_i64(self.0)
    }

    fn try_from_decimal(value: Decimal) -> Option<Self> {
        value.to_i64().map(Self)
    }
}

impl SanitizeAuto for Timestamp {}

impl SanitizeCustom for Timestamp {}

impl ValidateAuto for Timestamp {}

impl ValidateCustom for Timestamp {}

impl Visitable for Timestamp {
    fn requires_application_write_callbacks() -> bool {
        false
    }
}

//
// TESTS
//

#[cfg(test)]
mod tests;
