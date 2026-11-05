use super::SqlLoweringError;
use crate::db::QueryError;
use icydb_diagnostic_code::DiagnosticFactTag;

#[test]
fn lowering_projection_error_retains_projection_index() {
    let error = SqlLoweringError::grouped_projection_references_non_group_field(4);
    assert_eq!(
        error.diagnostic_facts(),
        vec![(DiagnosticFactTag::ProjectionIndex, 4)],
    );

    let query_error = QueryError::from_sql_lowering_error(error);
    assert_eq!(
        query_error.diagnostic_facts(),
        vec![(DiagnosticFactTag::ProjectionIndex, 4)],
    );
}

#[test]
fn lowering_parameter_error_retains_parameter_index_only_when_known() {
    let known = SqlLoweringError::unsupported_parameter_placement(Some(3));
    assert_eq!(
        known.diagnostic_facts(),
        vec![(DiagnosticFactTag::ParameterIndex, 3)],
    );

    let unknown = SqlLoweringError::unsupported_parameter_placement(None);
    assert!(unknown.diagnostic_facts().is_empty());
}
