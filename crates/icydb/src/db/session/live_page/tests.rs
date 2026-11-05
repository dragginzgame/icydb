use super::*;

fn page(continuation: Option<&str>) -> LiveQueryPageOutput {
    LiveQueryPageOutput {
        entity: "app::User".to_string(),
        columns: vec!["id".to_string()],
        rows: Vec::new(),
        row_count: 0,
        continuation: continuation.map(str::to_string),
        work: crate::db::ScalarPageWork {
            envelope_identity: 1,
            entries_visited: 0,
            result_rows: 0,
        },
    }
}

#[test]
fn live_page_step_commits_progress_only_after_processing() {
    let mut continuation = Some("prior".to_string());
    let advanced = prepare_live_page_step(page(Some("next")), continuation.as_deref())
        .expect("a new continuation should prepare a step");
    assert!(!advanced.is_exhausted());
    assert_eq!(advanced.page().continuation.as_deref(), Some("next"));
    assert_eq!(continuation.as_deref(), Some("prior"));

    assert!(!advanced.commit(&mut continuation));
    assert_eq!(continuation.as_deref(), Some("next"));

    let exhausted = prepare_live_page_step(page(None), continuation.as_deref())
        .expect("a missing continuation should prepare terminal step");
    assert!(exhausted.is_exhausted());
    assert!(exhausted.page().continuation.is_none());
    assert_eq!(continuation.as_deref(), Some("next"));

    assert!(exhausted.commit(&mut continuation));
    assert!(continuation.is_none());
}

#[test]
fn dropped_live_page_step_preserves_retry_position_and_page_contract() {
    let continuation = Some("prior".to_string());
    let step = prepare_live_page_step(page(Some("next")), continuation.as_deref())
        .expect("a new continuation should prepare a step");

    drop(step);
    assert_eq!(continuation.as_deref(), Some("prior"));

    let retry = prepare_live_page_step(page(Some("next")), continuation.as_deref())
        .expect("the unchanged position should retry the same page");
    let retry_page = retry.into_page();
    assert_eq!(retry_page.continuation.as_deref(), Some("next"));
}

#[test]
fn live_page_step_rejects_non_progressing_continuation_before_commit() {
    let continuation = Some("same".to_string());
    let error = prepare_live_page_step(page(Some("same")), continuation.as_deref())
        .expect_err("a repeated continuation must fail");

    assert_eq!(
        error.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::RuntimeInvariantViolation
    );
    assert_eq!(error.origin(), ErrorOrigin::Cursor);
    assert_eq!(continuation.as_deref(), Some("same"));
}
