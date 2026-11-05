use super::ExecutionOutcomeMetrics;

#[test]
fn retry_metrics_accumulate_scan_work_and_keep_latest_output_count() {
    for (previous_scan, previous_rows, latest_scan, latest_rows, total_scan) in [
        (7, 3, 11, 1, 18),
        (usize::MAX, 4, 1, 0, usize::MAX),
        (0, 0, 0, 5, 0),
    ] {
        let merged = ExecutionOutcomeMetrics {
            rows_scanned: previous_scan,
            post_access_rows: previous_rows,
        }
        .merge_residual_retry_attempt(ExecutionOutcomeMetrics {
            rows_scanned: latest_scan,
            post_access_rows: latest_rows,
        });

        assert_eq!(merged.rows_scanned, total_scan);
        assert_eq!(merged.post_access_rows, latest_rows);
    }
}
