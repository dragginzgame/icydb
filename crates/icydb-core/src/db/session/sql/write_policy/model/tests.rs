use super::SqlWriteExposureClass;

#[test]
fn exposure_class_owns_its_staged_row_bound() {
    assert_eq!(
        SqlWriteExposureClass::PublicPrimaryKeyOnly.max_staged_rows(Some(10)),
        Some(1),
    );
    assert_eq!(
        SqlWriteExposureClass::PublicBoundedDeterministic.max_staged_rows(Some(10)),
        Some(10),
    );
    assert_eq!(
        SqlWriteExposureClass::PublicBoundedDeterministic.max_staged_rows(None),
        None,
    );
}
