use super::sql_insert_select_width_mismatch_error;
use icydb_diagnostic_code::DiagnosticFactTag;

#[test]
fn insert_select_width_error_retains_expected_and_actual_arity() {
    let error = sql_insert_select_width_mismatch_error(3, 2);

    assert_eq!(
        error.diagnostic_facts(),
        vec![
            (DiagnosticFactTag::ExpectedArity, 3),
            (DiagnosticFactTag::ActualArity, 2),
        ],
    );
}
