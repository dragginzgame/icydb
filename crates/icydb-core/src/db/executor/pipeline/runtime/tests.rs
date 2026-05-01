use crate::{db::executor::planning::route::ensure_load_fast_path_spec_arity, error::ErrorClass};

#[test]
fn fast_path_spec_arity_accepts_single_spec_shapes() {
    let result = ensure_load_fast_path_spec_arity(true, 1, true, 1);

    assert!(result.is_ok(), "single fast-path specs should be accepted");
}

#[test]
fn fast_path_spec_arity_rejects_multiple_prefix_specs_for_secondary() {
    let err = ensure_load_fast_path_spec_arity(true, 2, false, 0)
        .expect_err("secondary fast-path must reject multiple index-prefix specs");

    assert_eq!(
        err.class,
        ErrorClass::InvariantViolation,
        "prefix-spec arity violation must classify as invariant violation"
    );
    assert!(
        err.message
            .contains("secondary fast-path resolution expects at most one index-prefix spec"),
        "prefix-spec arity violation must return a clear invariant message"
    );
}

#[test]
fn fast_path_spec_arity_rejects_multiple_range_specs_for_index_range() {
    let err = ensure_load_fast_path_spec_arity(false, 0, true, 2)
        .expect_err("index-range fast-path must reject multiple index-range specs");

    assert_eq!(
        err.class,
        ErrorClass::InvariantViolation,
        "range-spec arity violation must classify as invariant violation"
    );
    assert!(
        err.message
            .contains("index-range fast-path resolution expects at most one index-range spec"),
        "range-spec arity violation must return a clear invariant message"
    );
}
