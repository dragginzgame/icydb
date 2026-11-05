use crate::db::executor::planning::route::widened_residual_filter_predicate_pushdown_fetch;

#[test]
fn widened_residual_fetch_grows_underfilled_bounded_probe() {
    assert_eq!(
        widened_residual_filter_predicate_pushdown_fetch(3, 0, 0),
        None,
        "zero keep windows should not enter residual retry",
    );
    assert_eq!(
        widened_residual_filter_predicate_pushdown_fetch(3, 2, 0),
        Some(5),
        "zero-match underfill should widen the bounded fetch enough to look past the missing keep window",
    );
    assert_eq!(
        widened_residual_filter_predicate_pushdown_fetch(3, 2, 1),
        Some(5),
        "partial underfill should widen by the observed discard gap instead of falling back immediately",
    );
    assert_eq!(
        widened_residual_filter_predicate_pushdown_fetch(256, 2, 0),
        None,
        "an underfilled probe at the safety cap should request the controller's unbounded fallback",
    );
}
