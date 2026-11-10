use super::*;

#[test]
fn sqlite_average_real_overlap_is_exact_and_bounded() {
    for (value, mantissa, scale) in [
        (0.0, 0, 0),
        (3.0, 3, 0),
        (1.5, 15, 1),
        (1.25, 125, 2),
        (-0.5, -5, 1),
    ] {
        assert_eq!(
            sqlite_reference_value(
                "AVG boundary",
                0,
                SqliteReferenceColumnKind::Decimal,
                ValueRef::Real(value)
            )
            .unwrap(),
            SqliteReferenceValue::Decimal { mantissa, scale }
        );
    }
    for value in [
        1.0 / 3.0,
        1.125,
        f64::NAN,
        f64::INFINITY,
        f64::from(i32::MAX) + 1.0,
    ] {
        assert_eq!(
            sqlite_reference_value(
                "AVG boundary",
                0,
                SqliteReferenceColumnKind::Decimal,
                ValueRef::Real(value)
            )
            .unwrap_err()
            .kind(),
            SqliteAdapterErrorKind::Result
        );
    }
}
