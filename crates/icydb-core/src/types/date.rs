use crate::{
    db::primitives::{Int64ListFilterKind, Int64RangeFilterKind},
    traits::{
        FieldValue, Filterable, Inner, NumCast, NumFromPrimitive, NumToPrimitive, SanitizeAuto,
        SanitizeCustom, UpdateView, ValidateAuto, ValidateCustom, View, Visitable,
    },
    value::Value,
};
use candid::CandidType;
use chrono::{Datelike, Duration as ChronoDuration, NaiveDate};
use derive_more::{Add, AddAssign, FromStr, Sub, SubAssign};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

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
pub struct Date(pub i32);

impl Date {
    pub const EPOCH: Self = Self(0);
    pub const MIN: Self = Self(i32::MIN);
    pub const MAX: Self = Self(i32::MAX);

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn new(y: i32, m: u32, d: u32) -> Self {
        // clamp month
        let m = m.clamp(1, 12);

        // clamp day
        let last_valid_day = (28..=31)
            .rev()
            .find(|&d| NaiveDate::from_ymd_opt(y, m, d).is_some())
            .unwrap();
        let d = d.clamp(1, last_valid_day);

        match NaiveDate::from_ymd_opt(y, m, d) {
            Some(date) => {
                Self((date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
            }
            None => Self::EPOCH, // default to 1970-01-01
        }
    }

    #[must_use]
    pub fn new_checked(y: i32, m: u32, d: u32) -> Option<Self> {
        NaiveDate::from_ymd_opt(y, m, d).map(Self::from_naive_date)
    }

    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }

    /// Returns the year component (e.g. 2025)
    #[must_use]
    pub fn year(self) -> i32 {
        self.to_naive_date().year()
    }

    /// Returns the month component (1–12)
    #[must_use]
    pub fn month(self) -> u32 {
        self.to_naive_date().month()
    }

    /// Returns the day-of-month component (1–31)
    #[must_use]
    pub fn day(self) -> u32 {
        self.to_naive_date().day()
    }

    pub fn parse(s: &str) -> Option<Self> {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .ok()
            .map(Self::from_naive_date)
    }

    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    fn from_naive_date(date: NaiveDate) -> Self {
        Self((date - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
    }

    #[must_use]
    fn to_naive_date(self) -> NaiveDate {
        NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + ChronoDuration::days(self.0.into())
    }
}

impl fmt::Debug for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Date({self})")
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = self.to_naive_date();
        write!(f, "{:04}-{:02}-{:02}", d.year(), d.month(), d.day())
    }
}

impl FieldValue for Date {
    fn to_value(&self) -> Value {
        Value::Date(*self)
    }
}

impl Filterable for Date {
    type Filter = Int64RangeFilterKind;
    type ListFilter = Int64ListFilterKind;
}

impl From<i32> for Date {
    fn from(n: i32) -> Self {
        Self(n)
    }
}

impl Inner<Self> for Date {
    fn inner(&self) -> &Self {
        self
    }

    fn into_inner(self) -> Self {
        self
    }
}

impl NumCast for Date {
    fn from<T: NumToPrimitive>(n: T) -> Option<Self> {
        n.to_i32().map(Self)
    }
}

impl NumFromPrimitive for Date {
    #[allow(clippy::cast_possible_truncation)]
    fn from_i64(n: i64) -> Option<Self> {
        Some(Self(n as i32))
    }

    #[allow(clippy::cast_possible_truncation)]
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

impl UpdateView for Date {
    type UpdateViewType = Self;

    fn merge(&mut self, v: Self::UpdateViewType) {
        *self = v;
    }
}

impl ValidateAuto for Date {}

impl ValidateCustom for Date {}

impl View for Date {
    type ViewType = Self;

    fn to_view(&self) -> Self::ViewType {
        *self
    }

    fn from_view(view: Self::ViewType) -> Self {
        view
    }
}

impl Visitable for Date {}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn from_ymd_and_to_naive_date_round_trip() {
        let date = Date::new(2024, 10, 19);
        assert_eq!(date.year(), 2024);
        assert_eq!(date.month(), 10);
        assert_eq!(date.day(), 19);
    }

    #[test]
    fn from_naive_date_and_back_are_consistent() {
        let naive = NaiveDate::from_ymd_opt(1999, 12, 31).unwrap();
        let date = Date::from_naive_date(naive);
        let round_trip = date.to_naive_date();
        assert_eq!(round_trip, naive);
    }

    #[test]
    fn parse_and_format_work() {
        let parsed = Date::parse("2025-03-28").unwrap();
        let naive = parsed.to_naive_date();
        assert_eq!(naive.year(), 2025);
        assert_eq!(naive.month(), 3);
        assert_eq!(naive.day(), 28);
    }

    #[test]
    fn epoch_is_1970_01_01() {
        let epoch = Date::EPOCH;
        let naive = epoch.to_naive_date();
        assert_eq!(naive, NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
        assert_eq!(epoch.get(), 0);
    }

    #[test]
    fn invalid_date_parse_returns_none() {
        assert!(Date::parse("2025-13-40").is_none());
        assert!(Date::new_checked(2025, 2, 30).is_none());
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

    #[test]
    fn serialize_to_json_string() {
        let date = Date::new_checked(2024, 2, 29).unwrap();
        let json = serde_json::to_string(&date).unwrap();
        // should be a quoted ISO date string
        assert_eq!(json, "\"2024-02-29\"");
    }

    #[test]
    fn deserialize_from_json_string() {
        let json = "\"2023-07-15\"";
        let date: Date = serde_json::from_str(json).unwrap();
        assert_eq!(date.year(), 2023);
        assert_eq!(date.month(), 7);
        assert_eq!(date.day(), 15);
    }

    #[test]
    fn serde_round_trip_preserves_value() {
        let original = Date::new_checked(2000, 1, 1).unwrap();
        let json = serde_json::to_string(&original).unwrap();
        let parsed: Date = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, original);
    }

    #[test]
    fn serialize_invalid_date_fails_deserialize() {
        let bad_json = "\"2024-02-30\""; // invalid date
        let result: Result<Date, _> = serde_json::from_str(bad_json);
        assert!(result.is_err(), "invalid date should fail to deserialize");
    }
}
