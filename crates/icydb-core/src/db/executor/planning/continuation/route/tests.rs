use crate::db::{
    executor::{AccessWindow, ContinuationMode, RouteContinuationPlan},
    query::plan::ContinuationPolicy,
};

fn route_continuation(
    access_window_keep: AccessWindow,
    access_window_fetch: AccessWindow,
) -> RouteContinuationPlan {
    RouteContinuationPlan::new(
        ContinuationMode::Initial,
        ContinuationPolicy::new(true, true, true),
        access_window_keep,
        access_window_fetch,
    )
}

#[test]
fn route_continuation_access_window_limit_zero_projects_zero_fetch_limit() {
    let continuation = route_continuation(
        AccessWindow::new(4, Some(0), Some(4)),
        AccessWindow::new(4, Some(0), Some(0)),
    );
    let access_window = continuation.fetch_access_window();

    assert_eq!(continuation.effective_offset(), 4);
    assert_eq!(access_window.lower_bound(), 4);
    assert_eq!(access_window.page_limit(), Some(0));
    assert_eq!(access_window.fetch_limit(), Some(0));
    assert!(
        access_window.is_zero_window(),
        "LIMIT 0 route windows must project zero-fetch access windows",
    );
}

#[test]
fn route_continuation_access_window_bounded_limit_projects_offset_and_fetch_counts() {
    let continuation = route_continuation(
        AccessWindow::new(3, Some(2), Some(5)),
        AccessWindow::new(3, Some(2), Some(6)),
    );
    let keep_window = continuation.keep_access_window();
    let fetch_window = continuation.fetch_access_window();

    assert_eq!(continuation.effective_offset(), 3);
    assert_eq!(keep_window.lower_bound(), 3);
    assert_eq!(keep_window.page_limit(), Some(2));
    assert_eq!(keep_window.fetch_limit(), Some(5));
    assert!(!keep_window.is_zero_window());
    assert_eq!(fetch_window.fetch_limit(), Some(6));
}

#[test]
fn route_continuation_access_window_unbounded_limit_projects_unbounded_fetch() {
    let continuation = route_continuation(
        AccessWindow::new(0, None, None),
        AccessWindow::new(0, None, None),
    );
    let access_window = continuation.fetch_access_window();

    assert_eq!(continuation.effective_offset(), 0);
    assert_eq!(access_window.lower_bound(), 0);
    assert_eq!(access_window.page_limit(), None);
    assert_eq!(access_window.fetch_limit(), None);
    assert!(!access_window.is_zero_window());
}

#[test]
fn route_continuation_initial_mode_applies_policy_flags() {
    let continuation =
        RouteContinuationPlan::initial_with_policy(ContinuationPolicy::new(true, false, false));

    #[cfg(feature = "sql")]
    assert_eq!(continuation.mode(), ContinuationMode::Initial);
    assert!(!continuation.applied());
    assert!(continuation.strict_advance_required_when_applied());
    assert!(continuation.grouped_safe_when_applied());
    assert!(continuation.index_range_limit_pushdown_allowed());
}
