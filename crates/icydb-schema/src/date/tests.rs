use super::*;

#[test]
fn every_supported_calendar_day_matches_the_reference_calendar() {
    use time::{Date as ReferenceDate, Month};

    let epoch = ReferenceDate::from_calendar_date(1970, Month::January, 1).unwrap();
    for days in Date::MIN.as_days_since_epoch()..=Date::MAX.as_days_since_epoch() {
        let reference = ReferenceDate::from_julian_day(epoch.to_julian_day() + days).unwrap();
        let (year, month, day) = reference.to_calendar_date();
        let month = u8::from(month);
        let date = Date::try_from_days_since_epoch(days).unwrap();
        assert_eq!((date.year(), date.month(), date.day()), (year, month, day));
        assert_eq!(Date::try_new(year, month, day), Some(date));
    }
}

#[test]
fn calendar_admission_matches_reference_at_month_and_leap_boundaries() {
    use time::{Date as ReferenceDate, Month};

    let epoch = ReferenceDate::from_calendar_date(1970, Month::January, 1).unwrap();
    for year in [
        0, 1, 4, 100, 400, 1582, 1900, 1969, 1970, 2000, 2024, 2100, 2400, 9999,
    ] {
        for month in 0..=13 {
            for day in 0..=32 {
                let expected = Month::try_from(month)
                    .ok()
                    .and_then(|month| ReferenceDate::from_calendar_date(year, month, day).ok())
                    .map(|date| (date - epoch).whole_days());
                assert_eq!(
                    Date::try_new(year, month, day)
                        .map(|date| i64::from(date.as_days_since_epoch())),
                    expected,
                    "{year:04}-{month:02}-{day:02}"
                );
            }
        }
    }
}

#[test]
fn serde_calendar_boundaries_and_batches_use_compact_days() {
    let today = Date::try_new(2026, 9, 12).unwrap();
    for (date, width) in [(Date::EPOCH, 1), (today, 3), (Date::MIN, 3), (Date::MAX, 4)] {
        let json = serde_json::to_string(&date).unwrap();
        assert_eq!(json, format!("\"{date}\""));
        assert_eq!(serde_json::from_str::<Date>(&json).unwrap(), date);

        let wire = crate::wire::encode_one(date).unwrap();
        assert_eq!(wire.len(), width);
        assert_eq!(
            wire,
            crate::wire::encode_one(date.as_days_since_epoch()).unwrap()
        );
        assert_eq!(crate::wire::decode_one::<Date>(&wire).unwrap(), date);

        let batch = vec![date; 1_000];
        let encoded = crate::wire::encode_one(&batch).unwrap();
        assert_eq!(encoded.len(), 2 + 1_000 * width);
        assert_eq!(
            crate::wire::decode_one::<Vec<Date>>(&encoded).unwrap(),
            batch
        );
    }
}

#[test]
fn binary_serde_rejects_days_outside_calendar_domain() {
    for day in [
        i64::MIN,
        i64::from(Date::MIN.as_days_since_epoch()) - 1,
        i64::from(Date::MAX.as_days_since_epoch()) + 1,
        i64::MAX,
    ] {
        let encoded = crate::wire::encode_one(day).unwrap();
        assert!(crate::wire::decode_one::<Date>(&encoded).is_err());
    }
}

// Internal semantic/storage representation behavior.

#[test]
fn from_ymd_and_to_naive_date_round_trip() {
    let date = Date::try_new(2024, 10, 19).expect("valid calendar date should construct");
    assert_eq!(date.year(), 2024);
    assert_eq!(date.month(), 10);
    assert_eq!(date.day(), 19);
}

#[test]
fn try_new_rejects_out_of_range_month_and_day() {
    assert!(Date::try_new(2025, 13, 99).is_none());
}

#[test]
fn invalid_date_parse_returns_none() {
    assert!(Date::parse("2025-13-40").is_none());
    assert!(Date::try_new(2025, 2, 30).is_none());
}

#[test]
fn try_new_rejects_out_of_range_year() {
    assert!(Date::try_new(-1, 1, 1).is_none());
    assert!(Date::try_new(10_000, 1, 1).is_none());
    assert!(Date::try_new(i32::MAX, 1, 1).is_none());
}

#[test]
fn overflow_protection_in_try_from_u64() {
    // i32::MAX + 1 should safely fail
    let too_large = (i32::MAX as u64) + 1;
    assert!(Date::try_from_u64(too_large).is_none());
}

#[test]
fn ordering_and_equality_follow_internal_day_count() {
    let d1 = Date::try_new(2020, 1, 1).unwrap();
    let d2 = Date::try_new(2021, 1, 1).unwrap();

    assert!(d1 < d2);
    assert!(d1.as_days_since_epoch() < d2.as_days_since_epoch());
    assert_eq!(d1, d1);
}

#[test]
fn internal_day_count_helpers_round_trip() {
    let days = -365;
    let date = Date::try_from_days_since_epoch(days).expect("bounded day should construct");
    assert_eq!(date.as_days_since_epoch(), days);
}

#[test]
fn raw_day_construction_rejects_values_outside_calendar_bounds() {
    assert_eq!(
        Date::try_from_days_since_epoch(Date::MIN.as_days_since_epoch()),
        Some(Date::MIN),
    );
    assert_eq!(
        Date::try_from_days_since_epoch(Date::MAX.as_days_since_epoch()),
        Some(Date::MAX),
    );
    assert!(Date::try_from_days_since_epoch(Date::MIN.as_days_since_epoch() - 1).is_none(),);
    assert!(Date::try_from_days_since_epoch(Date::MAX.as_days_since_epoch() + 1).is_none(),);
}

#[test]
fn checked_day_arithmetic_obeys_calendar_bounds() {
    let leap_day = Date::try_new(2024, 2, 29).expect("leap day should construct");
    let march_first = Date::try_new(2024, 3, 1).expect("next day should construct");

    assert_eq!(leap_day.checked_add_days(1), Some(march_first));
    assert_eq!(march_first.checked_sub_days(1), Some(leap_day));
    assert_eq!(march_first.days_since(leap_day), 1);
    assert!(Date::MAX.checked_add_days(1).is_none());
    assert!(Date::MIN.checked_sub_days(1).is_none());
    assert!(Date::EPOCH.checked_add_days(i64::MAX).is_none());
    assert!(Date::EPOCH.checked_sub_days(i64::MIN).is_none());
}

#[test]
fn display_formats_as_iso_date() {
    let date = Date::try_new(2025, 10, 19).unwrap();
    assert_eq!(format!("{date}"), "2025-10-19");
}

#[test]
fn parse_stays_iso_strict() {
    assert_eq!(Date::parse("2025-10-19"), Date::try_new(2025, 10, 19));
    assert!(Date::parse("10/19/2025").is_none());
    assert!(Date::parse("2025-10-19T00:00:00Z").is_none());
}

#[test]
fn parse_supports_pre_epoch_and_leap_year_cases() {
    assert_eq!(
        Date::parse("1900-01-01"),
        Date::try_new(1900, 1, 1),
        "expected non-leap-century date to parse",
    );
    assert_eq!(
        Date::parse("1969-12-31"),
        Date::try_new(1969, 12, 31),
        "expected pre-epoch date to parse",
    );
    assert_eq!(
        Date::parse("2000-02-29"),
        Date::try_new(2000, 2, 29),
        "expected leap-day date to parse",
    );
}

#[test]
fn parse_rejects_invalid_non_leap_day() {
    assert!(Date::parse("1900-02-29").is_none());
}

#[test]
fn calendar_boundaries_format_and_parse_exactly() {
    assert_eq!(Date::MIN.to_string(), "0000-01-01");
    assert_eq!(Date::MAX.to_string(), "9999-12-31");
    assert_eq!(Date::parse(Date::MIN.to_string().as_str()), Some(Date::MIN));
    assert_eq!(Date::parse(Date::MAX.to_string().as_str()), Some(Date::MAX));
}

#[test]
fn wire_decode_rejects_out_of_range_epoch_days() {
    let min = crate::wire::encode_one(Date::MIN.as_days_since_epoch())
        .expect("minimum day should encode");
    let max = crate::wire::encode_one(Date::MAX.as_days_since_epoch())
        .expect("maximum day should encode");
    let below = crate::wire::encode_one(Date::MIN.as_days_since_epoch() - 1)
        .expect("out-of-range day should encode as raw i32");
    let above = crate::wire::encode_one(Date::MAX.as_days_since_epoch() + 1)
        .expect("out-of-range day should encode as raw i32");

    assert_eq!(
        crate::wire::decode_one::<Date>(&min).expect("minimum day should decode"),
        Date::MIN,
    );
    assert_eq!(
        crate::wire::decode_one::<Date>(&max).expect("maximum day should decode"),
        Date::MAX,
    );
    assert!(crate::wire::decode_one::<Date>(&below).is_err());
    assert!(crate::wire::decode_one::<Date>(&above).is_err());
}
