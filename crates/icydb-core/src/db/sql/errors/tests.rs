use super::*;

#[test]
fn sql_unknown_field_attachment_accepts_only_lowering_roles() {
    for role in [
        QueryFieldRole::Predicate,
        QueryFieldRole::Projection,
        QueryFieldRole::GroupBy,
        QueryFieldRole::AggregateTarget,
    ] {
        let error = PlanError::from_sql_unknown_field(role, "missing".to_string())
            .expect("lowering role should be admitted");
        assert_eq!(error.query_field_context(), Some((role, "missing")));
    }
    for role in [QueryFieldRole::Having, QueryFieldRole::OrderBy] {
        assert!(
            PlanError::from_sql_unknown_field(role, "missing".to_string()).is_none(),
            "non-lowering role {role:?} must fail closed"
        );
    }
}
