use super::{StructuralProjectionScanBudget, sql_scan_budget_exceeded_error};
use icydb_diagnostic_code::DiagnosticFactTag;

#[test]
fn sql_scan_budget_error_retains_exact_usage_and_limit() {
    let budget = StructuralProjectionScanBudget::try_new(4)
        .expect("positive non-max scan budget should be valid");
    let error = sql_scan_budget_exceeded_error(budget, 5);

    assert_eq!(
        error.diagnostic_facts(),
        vec![
            (DiagnosticFactTag::ActualCount, 5),
            (DiagnosticFactTag::Limit, 4),
        ],
    );
}
