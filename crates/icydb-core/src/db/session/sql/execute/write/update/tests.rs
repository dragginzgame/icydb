use super::{SqlExactUpdatePolicy, SqlExactUpdatePolicyRejection, sql_exact_update_policy_error};
use icydb_diagnostic_code::DiagnosticFactTag;

#[test]
fn exact_update_assertion_errors_retain_requested_and_bound_counts() {
    let required =
        sql_exact_update_policy_error(0, SqlExactUpdatePolicyRejection::AssertionRequired);
    assert_eq!(
        required.diagnostic_facts(),
        vec![
            (DiagnosticFactTag::ActualCount, 0),
            (DiagnosticFactTag::Minimum, 1),
        ],
    );

    let requested = SqlExactUpdatePolicy::max_affected_rows() + 1;
    let too_high =
        sql_exact_update_policy_error(requested, SqlExactUpdatePolicyRejection::AssertionTooHigh);
    assert_eq!(
        too_high.diagnostic_facts(),
        vec![
            (DiagnosticFactTag::ActualCount, u64::from(requested)),
            (
                DiagnosticFactTag::Limit,
                u64::from(SqlExactUpdatePolicy::max_affected_rows()),
            ),
        ],
    );
}
