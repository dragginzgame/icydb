use crate::{
    db::executor::planning::route::ensure_index_range_fast_path_spec_arity, error::ErrorClass,
};

#[test]
fn index_range_fast_path_spec_arity_accepts_zero_or_one_spec() {
    for count in [0, 1] {
        assert!(ensure_index_range_fast_path_spec_arity(true, count).is_ok());
    }
    assert!(ensure_index_range_fast_path_spec_arity(false, 2).is_ok());
}

#[test]
fn fast_path_spec_arity_rejects_multiple_range_specs_for_index_range() {
    let err = ensure_index_range_fast_path_spec_arity(true, 2)
        .expect_err("index-range fast-path must reject multiple index-range specs");

    assert_eq!(
        err.class,
        ErrorClass::InvariantViolation,
        "range-spec arity violation must classify as invariant violation"
    );
    assert_eq!(
        err.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation,
        "range-spec arity violation must return the invariant diagnostic code"
    );
}
