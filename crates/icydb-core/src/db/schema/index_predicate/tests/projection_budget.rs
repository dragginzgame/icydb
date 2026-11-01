//! Accepted index projection keeps conjunction work with its operation owner.

use super::{K, P, catalog, snapshot};
use crate::db::{
    QueryError, RequestExecutionRoot,
    access::SemanticIndexAccessContract,
    query::{plan::VisibleIndexes, preparation::PreparationWork},
    schema::{
        AcceptedSchemaSnapshot, PersistedIndexExpressionOp, PersistedIndexExpressionSnapshot,
        PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
        PersistedIndexSnapshot, PersistedSchemaSnapshot, SchemaIndexId, SchemaInfo,
    },
    test_support::request_with_limit,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::rc::Rc;

fn accepted_indexes() -> Rc<AcceptedSchemaSnapshot> {
    let kind = K::Text { max_len: None };
    let base = snapshot(kind.clone());
    let predicate = P::bind_test_sql("left != 'a' AND left != 'b' AND left != 'c'", base.fields());
    let path = |offset: usize| {
        let field = &base.fields()[offset];
        PersistedIndexFieldPathSnapshot::new(
            field.id(),
            field.slot(),
            vec![field.name().into()],
            kind.clone(),
            false,
        )
    };
    let keys = [
        PersistedIndexKeySnapshot::from_field_paths(vec![path(1)]),
        PersistedIndexKeySnapshot::new(vec![PersistedIndexKeyItemSnapshot::Expression(Box::new(
            PersistedIndexExpressionSnapshot::new(
                PersistedIndexExpressionOp::Lower,
                path(2),
                kind.clone(),
                kind,
                "expr:v1:LOWER(right)".into(),
            ),
        ))]),
    ];
    let indexes = keys
        .into_iter()
        .enumerate()
        .map(|(offset, key)| {
            let id = u16::try_from(offset + 1).unwrap();
            PersistedIndexSnapshot::new(
                SchemaIndexId::new(u32::from(id)).unwrap(),
                id,
                format!("filtered_{id}"),
                "tests::Filtered::filtered".into(),
                false,
                key,
                Some(predicate.clone()),
            )
        })
        .collect();
    Rc::new(
        AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new_with_indexes(
            base.version(),
            base.entity_path().into(),
            base.entity_name().into(),
            base.primary_key_field_ids().to_vec(),
            base.row_layout().clone(),
            base.fields().to_vec(),
            indexes,
        ))
        .unwrap(),
    )
}

fn visible(
    root: &RequestExecutionRoot,
    lane: Lane,
    schema: &SchemaInfo,
) -> Result<VisibleIndexes, QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        VisibleIndexes::accepted_schema_visible(schema, work).map_err(QueryError::execute)
    })
}

fn assert_resource(facts: &[(DiagnosticFactTag, u64)], limit: u64, actual: u64) {
    for expected in [
        (
            DiagnosticFactTag::BudgetResource,
            Resource::PredicateExpressionSteps.raw(),
        ),
        (DiagnosticFactTag::Limit, limit),
        (DiagnosticFactTag::Actual, actual),
    ] {
        assert!(facts.contains(&expected));
    }
}

#[test]
fn visible_index_normalization_shares_request_limits_across_both_index_kinds() {
    let accepted = accepted_indexes();
    let schema = SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog());
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let root = request_with_limit(Resource::PredicateExpressionSteps, 5);
        let facts = visible(&root, lane, &schema)
            .unwrap_err()
            .diagnostic_facts();
        assert_resource(&facts, 5, 6);
        assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
        assert_eq!(root.observed(Resource::RowsVisited), 0);
        assert_eq!(root.observed(Resource::KeyIndexEntriesVisited), 0);

        let root = request_with_limit(Resource::PredicateExpressionSteps, 12);
        let first = visible(&root, lane, &schema).unwrap();
        let second = visible(&root, lane, &schema).unwrap();
        assert_eq!(
            first.accepted_semantic_index_contracts(),
            second.accepted_semantic_index_contracts()
        );
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), 12);
        assert_resource(
            &visible(&root, lane, &schema)
                .unwrap_err()
                .diagnostic_facts(),
            12,
            13,
        );
    }
}

#[test]
fn each_index_projection_keeps_its_canonical_predicate_at_the_exact_boundary() {
    let accepted = accepted_indexes();
    let schema = SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog());
    let expected = crate::db::predicate::normalize(
        accepted.persisted_snapshot().indexes()[0]
            .predicate()
            .unwrap()
            .to_predicate(accepted.persisted_snapshot().fields(), &catalog())
            .unwrap(),
    );
    for expression in [false, true] {
        for (limit, admitted) in [(2, false), (3, true), (4, true)] {
            let root = request_with_limit(Resource::PredicateExpressionSteps, limit);
            let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                let result = if expression {
                    SemanticIndexAccessContract::from_accepted_expression_index(
                        &schema.expression_indexes()[0],
                        work,
                    )
                } else {
                    SemanticIndexAccessContract::from_accepted_field_path_index(
                        &schema.field_path_indexes()[0],
                        work,
                    )
                };
                result.map_err(QueryError::execute)
            });
            if admitted {
                assert_eq!(result.unwrap().predicate_semantics(), Some(&expected));
            } else {
                assert_resource(&result.unwrap_err().diagnostic_facts(), 2, 3);
            }
        }
    }
}
