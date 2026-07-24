//! Canonical millisecond-native time atoms.

use std::{
    fmt::{self, Display, Formatter},
    ops::{Add, AddAssign, Sub, SubAssign},
};

use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize};
use time::{Date as TimeDate, Month, PrimitiveDateTime, Time as TimeOfDay, UtcOffset};

/// Compact scalar parsing failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TypeParseError {
    /// Date text is invalid.
    InvalidDate,
    /// Decimal text is invalid.
    InvalidDecimal,
    /// Duration text is invalid.
    InvalidDuration,
    /// Float text is invalid.
    InvalidFloat,
    /// Signed big-integer text is invalid.
    InvalidIntBig,
    /// Timestamp text is invalid.
    InvalidTimestamp,
    /// ULID text is invalid.
    InvalidUlid,
}

impl Display for TypeParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("type parse")
    }
}

/// Canonical millisecond duration.
#[derive(CandidType, Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Duration(u64);

impl Duration {
    /// Zero milliseconds.
    pub const ZERO: Self = Self(0);
    /// Minimum duration.
    pub const MIN: Self = Self(u64::MIN);
    /// Maximum duration.
    pub const MAX: Self = Self(u64::MAX);

    const MS_PER_SEC: u64 = 1_000;
    const SECS_PER_MIN: u64 = 60;
    const MINS_PER_HOUR: u64 = 60;
    const HOURS_PER_DAY: u64 = 24;
    const DAYS_PER_WEEK: u64 = 7;

    /// Construct from milliseconds.
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Convert nonnegative signed milliseconds.
    #[must_use]
    pub const fn try_from_i64(millis: i64) -> Option<Self> {
        if millis < 0 {
            None
        } else {
            Some(Self(millis.cast_unsigned()))
        }
    }

    /// Convert unsigned milliseconds.
    #[must_use]
    pub const fn try_from_u64(millis: u64) -> Option<Self> {
        Some(Self(millis))
    }

    /// Construct from microseconds, truncating sub-millisecond precision.
    #[must_use]
    pub const fn from_micros_truncating(micros: u64) -> Self {
        Self(micros / Self::MS_PER_SEC)
    }

    /// Construct from nanoseconds, truncating sub-millisecond precision.
    #[must_use]
    pub const fn from_nanos_truncating(nanos: u64) -> Self {
        Self(nanos / 1_000_000)
    }

    /// Construct from seconds with saturation.
    #[must_use]
    pub const fn from_secs(seconds: u64) -> Self {
        Self(seconds.saturating_mul(Self::MS_PER_SEC))
    }

    /// Construct from minutes with saturation.
    #[must_use]
    pub const fn from_minutes(minutes: u64) -> Self {
        Self(
            minutes
                .saturating_mul(Self::SECS_PER_MIN)
                .saturating_mul(Self::MS_PER_SEC),
        )
    }

    /// Construct from hours with saturation.
    #[must_use]
    pub const fn from_hours(hours: u64) -> Self {
        Self(
            hours
                .saturating_mul(Self::MINS_PER_HOUR)
                .saturating_mul(Self::SECS_PER_MIN)
                .saturating_mul(Self::MS_PER_SEC),
        )
    }

    /// Construct from days with saturation.
    #[must_use]
    pub const fn from_days(days: u64) -> Self {
        Self(
            days.saturating_mul(Self::HOURS_PER_DAY)
                .saturating_mul(Self::MINS_PER_HOUR)
                .saturating_mul(Self::SECS_PER_MIN)
                .saturating_mul(Self::MS_PER_SEC),
        )
    }

    /// Construct from weeks with saturation.
    #[must_use]
    pub const fn from_weeks(weeks: u64) -> Self {
        Self(
            weeks
                .saturating_mul(Self::DAYS_PER_WEEK)
                .saturating_mul(Self::HOURS_PER_DAY)
                .saturating_mul(Self::MINS_PER_HOUR)
                .saturating_mul(Self::SECS_PER_MIN)
                .saturating_mul(Self::MS_PER_SEC),
        )
    }

    /// Return milliseconds.
    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Return whole seconds.
    #[must_use]
    pub const fn as_secs(self) -> u64 {
        self.0 / Self::MS_PER_SEC
    }

    /// Return whole minutes.
    #[must_use]
    pub const fn as_minutes(self) -> u64 {
        self.0 / (Self::SECS_PER_MIN * Self::MS_PER_SEC)
    }

    /// Return whole hours.
    #[must_use]
    pub const fn as_hours(self) -> u64 {
        self.0 / (Self::MINS_PER_HOUR * Self::SECS_PER_MIN * Self::MS_PER_SEC)
    }

    /// Return whole days.
    #[must_use]
    pub const fn as_days(self) -> u64 {
        self.0 / (Self::HOURS_PER_DAY * Self::MINS_PER_HOUR * Self::SECS_PER_MIN * Self::MS_PER_SEC)
    }

    /// Return whole weeks.
    #[must_use]
    pub const fn as_weeks(self) -> u64 {
        self.0
            / (Self::DAYS_PER_WEEK
                * Self::HOURS_PER_DAY
                * Self::MINS_PER_HOUR
                * Self::SECS_PER_MIN
                * Self::MS_PER_SEC)
    }

    /// Parse integer milliseconds or `ms`, `s`, `m`, `h`, or `d` suffixes.
    ///
    /// # Errors
    ///
    /// Returns [`TypeParseError::InvalidDuration`] for malformed or overflowing
    /// input.
    pub fn parse_flexible(input: &str) -> Result<Self, TypeParseError> {
        let (digits, multiplier) = if let Some(value) = input.strip_suffix("ms") {
            (value, 1)
        } else if let Some(value) = input.strip_suffix('s') {
            (value, Self::MS_PER_SEC)
        } else if let Some(value) = input.strip_suffix('m') {
            (value, Self::SECS_PER_MIN * Self::MS_PER_SEC)
        } else if let Some(value) = input.strip_suffix('h') {
            (
                value,
                Self::MINS_PER_HOUR * Self::SECS_PER_MIN * Self::MS_PER_SEC,
            )
        } else if let Some(value) = input.strip_suffix('d') {
            (
                value,
                Self::HOURS_PER_DAY * Self::MINS_PER_HOUR * Self::SECS_PER_MIN * Self::MS_PER_SEC,
            )
        } else {
            (input, 1)
        };
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(TypeParseError::InvalidDuration);
        }
        let value = digits
            .parse::<u64>()
            .map_err(|_| TypeParseError::InvalidDuration)?;
        Ok(Self(value.saturating_mul(multiplier)))
    }
}

impl Add for Duration {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        Self(self.0.saturating_add(other.0))
    }
}

impl AddAssign for Duration {
    fn add_assign(&mut self, other: Self) {
        self.0 = self.0.saturating_add(other.0);
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DurationVisitor;

        impl serde::de::Visitor<'_> for DurationVisitor {
            type Value = Duration;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("milliseconds or duration string")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Duration::try_from_i64(value)
                    .ok_or_else(|| E::custom("duration must be non-negative"))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
                Ok(Duration::from_millis(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Duration::parse_flexible(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(DurationVisitor)
    }
}

impl From<u64> for Duration {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Serialize for Duration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl Sub for Duration {
    type Output = Self;

    fn sub(self, other: Self) -> Self::Output {
        Self(self.0.saturating_sub(other.0))
    }
}

impl SubAssign for Duration {
    fn sub_assign(&mut self, other: Self) {
        self.0 = self.0.saturating_sub(other.0);
    }
}

/// Canonical Unix-millisecond timestamp.
#[derive(CandidType, Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    /// Unix epoch.
    pub const EPOCH: Self = Self(0);
    /// Minimum timestamp.
    pub const MIN: Self = Self(i64::MIN);
    /// Maximum timestamp.
    pub const MAX: Self = Self(i64::MAX);

    const MILLIS_PER_SEC: i64 = 1_000;

    /// Construct from seconds with saturation.
    #[must_use]
    pub const fn from_secs(seconds: i64) -> Self {
        Self(seconds.saturating_mul(Self::MILLIS_PER_SEC))
    }

    /// Construct from milliseconds.
    #[must_use]
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Convert signed milliseconds.
    #[must_use]
    pub const fn try_from_i64(millis: i64) -> Option<Self> {
        Some(Self(millis))
    }

    /// Convert unsigned milliseconds.
    #[must_use]
    pub fn try_from_u64(millis: u64) -> Option<Self> {
        i64::try_from(millis).ok().map(Self)
    }

    /// Construct from microseconds, truncating sub-millisecond precision.
    #[must_use]
    pub fn from_micros(micros: i64) -> Self {
        if micros < 0 {
            return Self(micros / Self::MILLIS_PER_SEC);
        }
        Self::from_positive_submillis(micros, 1_000)
    }

    /// Construct from nanoseconds, truncating sub-millisecond precision.
    #[must_use]
    pub fn from_nanos(nanos: i64) -> Self {
        if nanos < 0 {
            return Self(nanos / 1_000_000);
        }
        Self::from_positive_submillis(nanos, 1_000_000)
    }

    fn from_positive_submillis(value: i64, divisor: u64) -> Self {
        let value = u64::try_from(value).unwrap_or(u64::MAX) / divisor;
        i64::try_from(value).map_or(Self::MAX, Self)
    }

    /// Parse strict RFC3339 text.
    ///
    /// # Errors
    ///
    /// Returns [`TypeParseError::InvalidTimestamp`] for malformed or
    /// out-of-range input.
    pub fn parse_rfc3339(input: &str) -> Result<Self, TypeParseError> {
        let bytes = input.as_bytes();
        if bytes.len() < 20
            || bytes.get(4) != Some(&b'-')
            || bytes.get(7) != Some(&b'-')
            || bytes.get(10) != Some(&b'T')
            || bytes.get(13) != Some(&b':')
            || bytes.get(16) != Some(&b':')
        {
            return Err(TypeParseError::InvalidTimestamp);
        }
        let year = parse_i32(&bytes[0..4])?;
        let month = Month::try_from(parse_u8(&bytes[5..7])?)
            .map_err(|_| TypeParseError::InvalidTimestamp)?;
        let day = parse_u8(&bytes[8..10])?;
        let hour = parse_u8(&bytes[11..13])?;
        let minute = parse_u8(&bytes[14..16])?;
        let second = parse_u8(&bytes[17..19])?;

        let mut cursor = 19;
        let nanoseconds = if bytes.get(cursor) == Some(&b'.') {
            cursor += 1;
            let start = cursor;
            while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                cursor += 1;
            }
            if cursor == start {
                return Err(TypeParseError::InvalidTimestamp);
            }
            parse_fractional_nanoseconds(&bytes[start..cursor])?
        } else {
            0
        };
        let (sign, offset_hour, offset_minute) = parse_offset(&bytes[cursor..])?;
        let date = TimeDate::from_calendar_date(year, month, day)
            .map_err(|_| TypeParseError::InvalidTimestamp)?;
        let time = TimeOfDay::from_hms_nano(hour, minute, second, nanoseconds)
            .map_err(|_| TypeParseError::InvalidTimestamp)?;
        let offset = UtcOffset::from_hms(
            sign * i8::try_from(offset_hour).unwrap_or(i8::MAX),
            sign * i8::try_from(offset_minute).unwrap_or(i8::MAX),
            0,
        )
        .map_err(|_| TypeParseError::InvalidTimestamp)?;
        let millis = PrimitiveDateTime::new(date, time)
            .assume_offset(offset)
            .unix_timestamp_nanos()
            / 1_000_000;
        i64::try_from(millis)
            .map(Self)
            .map_err(|_| TypeParseError::InvalidTimestamp)
    }

    /// Parse integer milliseconds or strict RFC3339 text.
    ///
    /// # Errors
    ///
    /// Returns a typed timestamp parse error.
    pub fn parse_flexible(input: &str) -> Result<Self, TypeParseError> {
        input
            .parse::<i64>()
            .map(Self)
            .or_else(|_| Self::parse_rfc3339(input))
    }

    /// Return Unix milliseconds.
    #[must_use]
    pub const fn as_millis(self) -> i64 {
        self.0
    }

    /// Return whole Unix seconds.
    #[must_use]
    pub const fn as_secs(self) -> i64 {
        self.0 / Self::MILLIS_PER_SEC
    }
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, duration: Duration) -> Self::Output {
        Self(
            self.0
                .saturating_add(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)),
        )
    }
}

impl AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, duration: Duration) {
        *self = *self + duration;
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TimestampVisitor;

        impl serde::de::Visitor<'_> for TimestampVisitor {
            type Value = Timestamp;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str("unix millis or RFC3339 timestamp")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
                Ok(Timestamp::from_millis(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Timestamp::try_from_u64(value)
                    .ok_or_else(|| E::custom("unix millis exceeds i64 timestamp range"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Timestamp::parse_flexible(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(TimestampVisitor)
    }
}

impl Display for Timestamp {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

impl From<i64> for Timestamp {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<u64> for Timestamp {
    fn from(value: u64) -> Self {
        i64::try_from(value).map_or(Self::MAX, Self)
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i64(self.0)
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Self;

    fn sub(self, duration: Duration) -> Self::Output {
        Self(
            self.0
                .saturating_sub(i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)),
        )
    }
}

impl Sub for Timestamp {
    type Output = Duration;

    fn sub(self, other: Self) -> Self::Output {
        if self.0 <= other.0 {
            return Duration::ZERO;
        }
        Duration::from_millis(
            u64::try_from(i128::from(self.0) - i128::from(other.0)).unwrap_or(u64::MAX),
        )
    }
}

impl SubAssign<Duration> for Timestamp {
    fn sub_assign(&mut self, duration: Duration) {
        *self = *self - duration;
    }
}

fn parse_i32(bytes: &[u8]) -> Result<i32, TypeParseError> {
    bytes
        .iter()
        .try_fold(0_i32, |value, byte| {
            byte.checked_sub(b'0')
                .filter(|digit| *digit <= 9)
                .and_then(|digit| value.checked_mul(10)?.checked_add(i32::from(digit)))
        })
        .ok_or(TypeParseError::InvalidTimestamp)
}

fn parse_u8(bytes: &[u8]) -> Result<u8, TypeParseError> {
    bytes
        .iter()
        .try_fold(0_u8, |value, byte| {
            byte.checked_sub(b'0')
                .filter(|digit| *digit <= 9)
                .and_then(|digit| value.checked_mul(10)?.checked_add(digit))
        })
        .ok_or(TypeParseError::InvalidTimestamp)
}

fn parse_fractional_nanoseconds(bytes: &[u8]) -> Result<u32, TypeParseError> {
    let mut value = 0_u32;
    for byte in bytes.iter().take(9) {
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

fn parse_offset(bytes: &[u8]) -> Result<(i8, u8, u8), TypeParseError> {
    match bytes {
        [b'Z'] => Ok((1, 0, 0)),
        [sign @ (b'+' | b'-'), hour0, hour1, b':', minute0, minute1] => Ok((
            if *sign == b'+' { 1 } else { -1 },
            parse_u8(&[*hour0, *hour1])?,
            parse_u8(&[*minute0, *minute1])?,
        )),
        _ => Err(TypeParseError::InvalidTimestamp),
    }
}
