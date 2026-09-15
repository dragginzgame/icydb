//! Group-field framing and request admission without operand reconstruction.

use super::hash_group_field_slots;
use crate::db::{
    QueryError, RequestExecutionRoot,
    codec::{new_hash_sha256, write_hash_str_u32, write_hash_tag_u8, write_hash_u32},
    executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    predicate::MissingRowPolicy,
    query::{
        builder::count,
        fingerprint::finalize_sha256_digest,
        plan::{
            AccessPlannedQuery, FieldSlot, GroupAggregateSpec, GroupField, GroupFieldSet,
            GroupPlan, GroupSpec, GroupedExecutionConfig, LogicalPlan,
        },
        preparation::PreparationWork,
    },
    schema::AcceptedFieldKind,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn hash_fields(
    fields: &GroupFieldSet,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<[u8; 32], QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        let mut hasher = new_hash_sha256();
        hash_group_field_slots(&mut hasher, fields, work).map_err(QueryError::execute)?;
        Ok(finalize_sha256_digest(hasher))
    })
}

#[test]
fn group_field_hash_preserves_direct_and_segmented_path_framing() {
    let owner =
        FieldSlot::from_test_accepted_kind(9, "owner", AcceptedFieldKind::Text { max_len: None });
    let fields = GroupFieldSet::PathAware(vec![
        GroupField::Direct(owner.clone()),
        GroupField::scalar_path_for_test(
            "meta.国家",
            "meta",
            vec!["国家".into()],
            3,
            AcceptedFieldKind::Text { max_len: None },
        ),
        GroupField::scalar_path_for_test(
            "meta.a.b",
            "meta",
            vec!["a".into(), "b".into()],
            3,
            AcceptedFieldKind::Text { max_len: None },
        ),
    ]);
    let mut expected = new_hash_sha256();
    write_hash_u32(&mut expected, 3);
    write_hash_tag_u8(&mut expected, 0x82);
    write_hash_u32(&mut expected, 9);
    write_hash_str_u32(&mut expected, "owner");
    for segments in [&["国家"][..], &["a", "b"][..]] {
        write_hash_tag_u8(&mut expected, 0x83);
        write_hash_u32(&mut expected, 3);
        write_hash_str_u32(&mut expected, "meta");
        write_hash_u32(&mut expected, segments.len() as u32);
        for segment in segments {
            write_hash_str_u32(&mut expected, segment);
        }
    }
    let expected = finalize_sha256_digest(expected);
    let exact = 3 + "owner".len() as u64 + "meta.国家".len() as u64 + "meta.a.b".len() as u64;
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        for limit in [exact - 1, exact, 2 * exact] {
            let root = request(Resource::PredicateExpressionSteps, limit);
            for attempt in 1..=3 {
                let result = hash_fields(&fields, &root, lane);
                if attempt * exact <= limit {
                    assert_eq!(result.unwrap(), expected);
                } else {
                    let facts = result.unwrap_err().diagnostic_facts();
                    assert!(facts.contains(&(
                        DiagnosticFactTag::BudgetResource,
                        Resource::PredicateExpressionSteps.raw()
                    )));
                    assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
                    break;
                }
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), 0);
            assert_eq!(root.observed(Resource::NestedValueSteps), 0);
            assert_eq!(root.observed(Resource::RowsVisited), 0);
        }
        assert_eq!(
            hash_fields(
                &fields,
                &request(Resource::PredicateExpressionSteps, exact),
                lane
            )
            .unwrap(),
            expected
        );
    }
    let root = request(Resource::TemporaryBytes, 0);
    assert_eq!(
        hash_fields(
            &GroupFieldSet::Direct(vec![owner.clone()]),
            &root,
            Lane::Diagnostic
        )
        .unwrap(),
        hash_fields(
            &GroupFieldSet::PathAware(vec![GroupField::Direct(owner)]),
            &root,
            Lane::Diagnostic
        )
        .unwrap(),
    );
    // Component framing distinguishes a literal dot from a path boundary even
    // when raw structural fixtures carry the same rendered label.
    let path = |segments: Vec<String>| {
        GroupFieldSet::PathAware(vec![GroupField::scalar_path_for_test(
            "meta.a.b",
            "meta",
            segments,
            3,
            AcceptedFieldKind::Text { max_len: None },
        )])
    };
    assert_ne!(
        hash_fields(&path(vec!["a.b".into()]), &root, Lane::Diagnostic).unwrap(),
        hash_fields(&path(vec!["a".into(), "b".into()]), &root, Lane::Diagnostic).unwrap(),
    );
}

#[test]
fn empty_group_field_sets_need_no_work_or_backing() {
    let root = request(Resource::PredicateExpressionSteps, 0);
    let mut expected = new_hash_sha256();
    write_hash_u32(&mut expected, 0);
    let expected = finalize_sha256_digest(expected);
    for fields in [
        GroupFieldSet::Direct(vec![]),
        GroupFieldSet::PathAware(vec![]),
    ] {
        assert_eq!(
            hash_fields(&fields, &root, Lane::Diagnostic).unwrap(),
            expected
        );
    }
    assert_eq!(root.observed(Resource::PredicateExpressionSteps), 0);
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
}

#[test]
fn group_field_failure_prevents_continuation_publication_and_retry_preserves_identity() {
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    plan.logical = LogicalPlan::Grouped(GroupPlan {
        scalar: plan.scalar_plan().clone(),
        group: GroupSpec {
            group_fields: GroupFieldSet::Direct(vec![FieldSlot::from_test_accepted_kind(
                0,
                "key",
                AcceptedFieldKind::Int32,
            )]),
            aggregates: vec![GroupAggregateSpec::from_aggregate_expr(count())],
            execution: GroupedExecutionConfig::planner_default_bounded(),
        },
        having_expr: None,
    });
    let build = |root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            plan.planned_continuation_contract_with_accepted_identity("tests::Entity", None, work)
                .map_err(QueryError::execute)
        })
    };
    let rejected = request(Resource::PredicateExpressionSteps, 3);
    assert!(build(&rejected).unwrap_err().diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::PredicateExpressionSteps.raw(),
    )));
    // The access-plan visit precedes the group-field visit and its three bytes.
    assert_eq!(rejected.observed(Resource::PredicateExpressionSteps), 5);
    let measured = request(Resource::PredicateExpressionSteps, 16_000_000);
    let expected = build(&measured).unwrap().unwrap().continuation_signature();
    let exact = measured.observed(Resource::PredicateExpressionSteps);
    assert!(build(&request(Resource::PredicateExpressionSteps, exact - 1)).is_err());
    let root = request(Resource::PredicateExpressionSteps, 2 * exact);
    for _ in 0..2 {
        assert_eq!(
            build(&root).unwrap().unwrap().continuation_signature(),
            expected
        );
    }
    assert!(build(&root).is_err());
    assert_eq!(
        build(&request(Resource::PredicateExpressionSteps, exact))
            .unwrap()
            .unwrap()
            .continuation_signature(),
        expected
    );
    assert_eq!(root.observed(Resource::RowsVisited), 0);
    assert_eq!(root.observed(Resource::QueryExecutions), 0);
}
