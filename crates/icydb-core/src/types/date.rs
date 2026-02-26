use crate::{
    traits::{
        AsView, Atomic, FieldValue, FieldValueKind, NumCast, NumFromPrimitive, NumToPrimitive,
        SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use derive_more::{Add, AddAssign, FromStr, Sub, SubAssign};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Debug, Display},
    sync::OnceLock,
};
use time::{Date as TimeDate, Duration as TimeDuration, Month, format_description::FormatItem};

static FORMAT: OnceLock<Vec<FormatItem<'static>>> = OnceLock::new();

///
/// Date
///

#[derive(
    Add,
    AddAssign,
    CandidType,
    Clone,
    Copy,
    Default,
    Eq,
    FromStr,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    Sub,
    SubAssign,
)]
#[repr(transparent)]
pub struct Date(i32);

impl Date {
    pub const EPOCH: Self = Self(0);
    pub const MIN: Self = Self(i32::MIN);
    pub const MAX: Self = Self(i32::MAX);

    const fn epoch_date() -> TimeDate {
        // Safe: constant valid date
        match TimeDate::from_calendar_date(1970, Month::January, 1) {
            Ok(d) => d,
            Err(_) => unreachable!(),
        }
    }

    #[must_use]
    pub fn new(y: i32, m: u8, d: u8) -> Self {
        let m = m.clamp(1, 12);

        let Ok(month) = Month::try_from(m) else {
            return Self::EPOCH;
        };

        let last_valid_day = (28..=31)
            .rev()
            .find(|&day| TimeDate::from_calendar_date(y, month, day).is_ok());

        let Some(last_valid_day) = last_valid_day else {
            return Self::EPOCH;
        };

        let d = d.clamp(1, last_valid_day);

        match TimeDate::from_calendar_date(y, month, d) {
            Ok(date) => Self::from_time_date(date),
            Err(_) => Self::EPOCH,
        }
    }

    #[must_use]
    pub fn new_checked(y: i32, m: u8, d: u8) -> Option<Self> {
        let month = Month::try_from(m).ok()?;
        let date = TimeDate::from_calendar_date(y, month, d).ok()?;
        Some(Self::from_time_date(date))
    }

    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }

    /// Returns the year component (e.g. 2025)
    #[must_use]
    pub fn year(self) -> i32 {
        self.to_time_date().year()
    }

    /// Returns the month component (1–12)
    #[must_use]
    pub fn month(self) -> u8 {
        self.to_time_date().month().into()
    }

    /// Returns the day-of-month component (1–31)
    #[must_use]
    pub fn day(self) -> u8 {
        self.to_time_date().day()
    }

    /// Parse an ISO `YYYY-MM-DD` string into a `Date`.
    pub fn parse(s: &str) -> Option<Self> {
        let format =
            FORMAT.get_or_init(|| time::format_description::parse("[year]-[month]-[day]").unwrap());

        TimeDate::parse(s, format).ok().map(Self::from_time_date)
    }

    #[expect(clippy::cast_possible_truncation)]
    fn from_time_date(date: TimeDate) -> Self {
        let epoch = Self::epoch_date();
        let days = (date - epoch).whole_days();
        Self(days as i32)
    }

    fn to_time_date(self) -> TimeDate {
        let epoch = Self::epoch_date();
        let delta = TimeDuration::days(self.0.into());
        epoch.checked_add(delta).unwrap_or({
            if self.0 >= 0 {
                TimeDate::MAX
            } else {
                TimeDate::MIN
            }
        })
    }
}

impl AsView for Date {
    type ViewType = Self;

    fn as_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Atomic for Date {}

impl Debug for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Date({self})")
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.to_time_date();
        let month: u8 = d.month().into();
        write!(f, "{:04}-{:02}-{:02}", d.year(), month, d.day())
    }
}

impl FieldValue for Date {
    fn kind() -> FieldValueKind {
        FieldValueKind::Atomic
    }

    fn to_value(&self) -> Value {
        Value::Date(*self)
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Date(v) => Some(*v),
            _ => None,
        }
    }
}

impl NumCast for Date {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_i32().map(Self)
    }
}

impl NumFromPrimitive for Date {
    #[expect(clippy::cast_possible_truncation)]
    fn from_i64(n: i64) -> Option<Self> {
        Some(Self(n as i32))
    }

    #[expect(clippy::cast_possible_truncation)]
    fn from_u64(n: u64) -> Option<Self> {
        if i32::try_from(n).is_ok() {
            Some(Self(n as i32))
        } else {
            None
        }
    }
}

impl NumToPrimitive for Date {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
}

impl SanitizeAuto for Date {}

impl SanitizeCustom for Date {}

impl Serialize for Date {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Date {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).ok_or_else(|| serde::de::Error::custom(format!("invalid date: {s}")))
    }
}

impl ValidateAuto for Date {}

impl ValidateCustom for Date {}

impl Visitable for Date {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_ymd_and_to_naive_date_round_trip() {
        let date = Date::new(2024, 10, 19);
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month(), 10);
        assert_eq!(date.day(), 19);
    }

    #[test]
    fn invalid_date_parse_returns_none() {
        assert!(Date::parse("2025-13-40").is_none());
        assert!(Date::new_checked(2025, 2, 30).is_none());
    }

    #[test]
    fn new_out_of_range_year_defaults_to_epoch() {
        let date = Date::new(i32::MAX, 1, 1);
        assert_eq!(date, Date::EPOCH);
    }

    #[test]
    fn overflow_protection_in_from_u64() {
        // i32::MAX + 1 should safely fail
        let too_large = (i32::MAX as u64) + 1;
        assert!(Date::from_u64(too_large).is_none());
    }

    #[test]
    fn ordering_and_equality_work() {
        let d1 = Date::new_checked(2020, 1, 1).unwrap();
        let d2 = Date::new_checked(2021, 1, 1).unwrap();
        assert!(d1 < d2);
        assert_eq!(d1, d1);
    }

    #[test]
    fn display_formats_as_iso_date() {
        let date = Date::new_checked(2025, 10, 19).unwrap();
        assert_eq!(format!("{date}"), "2025-10-19");
    }
}
