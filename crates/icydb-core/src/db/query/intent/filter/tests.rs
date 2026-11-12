use crate::db::query::plan::exact_metadata_schema;

#[test]
fn primary_key_summary_preserves_scope_and_propagates_exhaustion() {
    use crate::{
        db::{
            RequestExecutionRoot,
            predicate::Predicate,
            query::preparation::PreparationWork,
            resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        },
        value::Value,
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
        DiagnosticFactTag,
    };

    let schema = exact_metadata_schema(&[], &[]);
    let predicate = Predicate::And(vec![
        Predicate::in_("id".into(), vec![Value::Int64(1), Value::Int64(2)]),
        Predicate::Or(vec![Predicate::in_("id".into(), vec![Value::Int64(3)])]),
        Predicate::in_("age".into(), vec![Value::Int64(4)]),
    ]);
    // AND + matching compare/name/two values + OR shell + other compare.
    let exact = 8;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for limit in [exact - 1, exact] {
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::PredicateExpressionSteps, limit)
                .with_limit_for_tests(Resource::TemporaryBytes, 0),
            );
            let result = PreparationWork::run(&root.scope(), lane, |work| {
                super::primary_key_input_resource_from_predicate(&schema, Some(&predicate), work)
            });
            if limit == exact {
                let summary = result.unwrap().unwrap();
                assert_eq!(summary.raw_term_count(), 2);
                assert_eq!(summary.estimated_payload_bytes(), 16);
            } else {
                assert!(result.unwrap_err().diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::PredicateExpressionSteps.raw(),
                )));
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
    }
}

#[test]
fn admitted_filter_rejects_other_schema_roots_before_planning_and_exact_count() {
    use crate::db::{
        predicate::MissingRowPolicy,
        query::{
            intent::QueryModel,
            plan::{
                VisibleIndexes, expr::Expr,
                try_build_count_cardinality_prefix_access_from_query_model,
            },
            preparation::with_preparation_work,
        },
    };
    use crate::value::Value;
    use std::rc::Rc;

    let schema = Rc::new(exact_metadata_schema(&[("age_idx", &["age"])], &[]));
    let other = Rc::new(exact_metadata_schema(&[("age_idx", &["age"])], &[]));
    with_preparation_work(|work| {
        let query = QueryModel::new(MissingRowPolicy::Ignore)
            .filter_expr(
                &schema,
                Expr::membership(Expr::Field("age".into()), vec![Value::Int64(1)], false),
                work,
            )
            .unwrap();
        assert!(
            query
                .prepare_scalar_planning_state_with_schema_info(schema.clone(), work)
                .is_ok()
        );
        assert!(
            query
                .prepare_scalar_planning_state_with_schema_info(other.clone(), work)
                .is_err()
        );
        let visible = VisibleIndexes::accepted_schema_visible(&other, work).unwrap();
        assert!(
            try_build_count_cardinality_prefix_access_from_query_model(
                &query, &visible, &other, work
            )
            .is_err()
        );
    });
}

#[test]
fn prepared_filter_borrows_admission_and_preserves_both_input_summaries() {
    use crate::db::{
        predicate::MissingRowPolicy,
        query::{intent::QueryModel, plan::expr::Expr, preparation::with_preparation_work},
    };
    use crate::value::Value;
    use std::rc::Rc;

    let schema = Rc::new(exact_metadata_schema(&[], &[]));
    with_preparation_work(|work| {
        let clause = |values: &[i64]| {
            Expr::membership(
                Expr::Field("id".into()),
                values.iter().copied().map(Value::Int64).collect(),
                false,
            )
        };
        let query = QueryModel::new(MissingRowPolicy::Ignore)
            .filter_expr(&schema, clause(&[1, 2]), work)
            .unwrap()
            .filter_expr(&schema, clause(&[2, 3]), work)
            .unwrap();
        let filter = query.admitted_filter(&schema).unwrap().unwrap();
        assert_eq!(
            filter
                .primary_key_input_resource()
                .unwrap()
                .raw_term_count(),
            4
        );
        let prepared = query
            .prepare_scalar_planning_state_with_schema_info(schema.clone(), work)
            .unwrap();
        assert!(std::ptr::eq(
            prepared.normalized_predicate().unwrap(),
            filter.predicate_subset().unwrap()
        ));
    });
}
