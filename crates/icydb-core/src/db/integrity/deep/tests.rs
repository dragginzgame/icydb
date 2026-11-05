use super::*;
use crate::db::integrity::progress_codec::current_job_codec_fixture;

#[test]
fn terminal_page_counters_refuse_overflow_without_partial_advance() {
    let mut job = current_job_codec_fixture();
    let sequence = job.last_receipt.receipt.page_sequence();
    let pages = job.pages_completed;
    assert_eq!(next_terminal_page_sequence(&mut job).unwrap(), sequence + 1);
    assert_eq!(job.pages_completed, pages + 1);
    assert_eq!(job.last_receipt.receipt.page_sequence(), sequence);

    job.pages_completed = u64::MAX;
    assert!(matches!(
        next_terminal_page_sequence(&mut job),
        Err(IntegrityDeepError::Job(IntegrityJobError::CounterExhausted))
    ));
    assert_eq!(job.pages_completed, u64::MAX);
    assert_eq!(job.last_receipt.receipt.page_sequence(), sequence);
}
