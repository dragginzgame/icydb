use super::{
    AcceptedContinuationIdentity, ExecutionFamily, ExecutionPreparation, PreparedExecutionPlanCore,
    PreparedExecutionPlanResidents, retain_lazy,
};
use crate::db::{
    QueryError, RequestExecutionRoot,
    cursor::CursorPlanError,
    index::{IndexCompilePolicy, compile_index_program},
    predicate::{
        CoercionId, CoercionSpec, CompareOp, ExecutableComparePredicate, ExecutablePredicate,
        MissingRowPolicy,
    },
    query::{
        plan::AccessPlannedQuery,
        preparation::{PreparationWork, with_preparation_work},
    },
    resource::bounded_cache::CacheEntryWeight,
    resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
};
use crate::value::Value;
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};
use std::{rc::Rc, sync::OnceLock};

fn accepted_identity() -> AcceptedContinuationIdentity {
    let schema = crate::db::query::plan::exact_metadata_schema(&[], &[]);
    AcceptedContinuationIdentity::new([0x22; 16], schema.value_catalog_handle().authority())
}

#[test]
fn execution_family_uses_retained_ordering_and_preserves_missing_contract_error() {
    use crate::db::{
        query::{
            builder::count,
            plan::{
                FieldSlot, GroupAggregateSpec, GroupFieldSet, GroupPlan, GroupSpec,
                GroupedExecutionConfig, LogicalPlan, OrderDirection, OrderSpec, OrderTerm,
            },
        },
        schema::AcceptedFieldKind,
    };

    for (grouped, ordered, expected) in [
        (false, false, ExecutionFamily::PrimaryKey),
        (false, true, ExecutionFamily::Ordered),
        (true, false, ExecutionFamily::Grouped),
        (true, true, ExecutionFamily::Grouped),
    ] {
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
            unreachable!()
        };
        scalar.order = ordered.then(|| OrderSpec {
            fields: vec![OrderTerm::field("key", OrderDirection::Desc)],
        });
        if grouped {
            plan.logical =
                LogicalPlan::Grouped(GroupPlan {
                    scalar: scalar.clone(),
                    group: GroupSpec {
                        group_fields: GroupFieldSet::Direct(vec![
                            FieldSlot::from_test_accepted_kind(0, "key", AcceptedFieldKind::Int32),
                        ]),
                        aggregates: vec![GroupAggregateSpec::from_aggregate_expr(count())],
                        execution: GroupedExecutionConfig::planner_default_bounded(),
                    },
                    having_expr: None,
                });
        }
        let continuation = with_preparation_work(|work| {
            plan.planned_continuation_contract_with_accepted_identity(
                "tests::Entity",
                Some(accepted_identity()),
                work,
            )
        })
        .unwrap();
        let plan = Rc::new(plan);
        let core =
            PreparedExecutionPlanCore::new(Rc::clone(&plan), 0, accepted_identity(), continuation);
        for _ in 0..3 {
            assert_eq!(core.execution_family().unwrap(), expected);
        }

        let missing = PreparedExecutionPlanCore::new(plan, 0, accepted_identity(), None);
        let error = missing.execution_family().unwrap_err();
        let expected = CursorPlanError::continuation_cursor_invariant().into_internal_error();
        assert_eq!(error.diagnostic(), expected.diagnostic());
        assert_eq!(error.diagnostic_facts(), expected.diagnostic_facts());
    }
}

#[test]
fn scalar_preparation_shares_completed_programs_without_requiring_retention() {
    assert_shared_preparation(
        IndexCompilePolicy::ConservativeSubset,
        PreparedExecutionPlanCore::get_or_init_scalar_execution_preparation,
        |residents| &residents.scalar_execution_preparation,
    );
}

#[cfg(feature = "sql")]
#[test]
fn aggregate_preparation_shares_strict_programs_without_requiring_retention() {
    assert_shared_preparation(
        IndexCompilePolicy::StrictAllOrNone,
        PreparedExecutionPlanCore::get_or_init_aggregate_execution_preparation,
        |residents| &residents.aggregate_execution_preparation,
    );
}

fn assert_shared_preparation(
    policy: IndexCompilePolicy,
    prepare: fn(
        &PreparedExecutionPlanCore,
    ) -> Result<Rc<ExecutionPreparation>, crate::error::InternalError>,
    resident: fn(&PreparedExecutionPlanResidents) -> &OnceLock<Rc<ExecutionPreparation>>,
) {
    use crate::db::{
        predicate::Predicate,
        query::plan::{LogicalPlan, exact_metadata_schema},
    };
    use crate::retained::RetainedBytes;

    let schema = exact_metadata_schema(&[], &[]);
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!()
    };
    scalar.predicate = Some(Predicate::eq("age".into(), Value::Int64(1)));
    with_preparation_work(|work| {
        let projection = plan.prepare_projection(&schema, work)?;
        plan.finalize_static_execution_planning_contract_with_schema(&schema, projection, work)
    })
    .unwrap();
    plan.static_execution_planning_contract
        .as_mut()
        .unwrap()
        .slot_map = Some(vec![1]);

    for retain in [false, true] {
        let core =
            PreparedExecutionPlanCore::new(Rc::new(plan.clone()), 0, accepted_identity(), None);
        let (entry, total) = CacheEntryWeight::for_tests(32, if retain { 16_000 } else { 32 });
        core.attach_cache_retention(&entry);
        let cell = resident(&core.residents);

        let error = preparation_with_budget(|| prepare(&core), 0).unwrap_err();
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw(),
        )));
        assert!(cell.get().is_none());
        assert_eq!(total.get(), 32);

        let first = preparation_with_budget(|| prepare(&core), 16_000_000).unwrap();
        assert!(first.prepared_index_program(policy).is_some());
        // The resident's inline pointer slot was charged at entry admission;
        // lazy retention adds only its shared allocation and owned payload.
        let bytes = RetainedBytes::measure(&first, usize::MAX).unwrap() - size_of_val(&first);
        assert!(bytes > size_of_val(first.as_ref()));
        let weight = if retain { 32 + bytes } else { 32 };
        assert_eq!(total.get(), weight);

        // Warm reuse needs no literal construction; declined retention
        // leaves a usable result and permits fresh preparation next time.
        let again = preparation_with_budget(|| prepare(&core), if retain { 0 } else { 16_000_000 })
            .unwrap();
        assert_eq!(Rc::ptr_eq(&first, &again), retain);
        assert_eq!(
            first.prepared_index_program(policy),
            again.prepared_index_program(policy)
        );
        assert_eq!(total.get(), weight);
        assert_eq!(cell.get().is_some(), retain);
        if retain {
            assert!(Rc::ptr_eq(&first, cell.get().unwrap()));
        }

        drop(entry);
        assert_eq!(total.get(), 0);
        drop(core);
        assert!(first.prepared_index_program(policy).is_some());
    }
}

fn preparation_with_budget<T>(
    prepare: impl FnOnce() -> Result<T, crate::error::InternalError>,
    limit: u64,
) -> Result<T, QueryError> {
    use crate::db::resource::budget::{
        HardExecutionContext, with_query_execution_budget_for_tests,
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetScope;

    with_query_execution_budget_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(Resource::TemporaryBytes, limit),
        HardExecutionContext::new(
            DiagnosticExecutionBudgetScope::Execution,
            Lane::PublicRead,
            0,
        ),
        || prepare().map_err(QueryError::execute),
    )
}

#[test]
fn retained_lazy_attachments_reserve_once_and_leave_oversize_results_usable() {
    let (entry, total) = CacheEntryWeight::for_tests(32, 64);
    let handle = Rc::downgrade(&entry);
    let accepted = OnceLock::new();
    retain_lazy(&handle, &accepted, &vec![1_u8; 16]);
    assert_eq!(accepted.get().map(Vec::len), Some(16));
    assert_eq!(total.get(), 48);
    retain_lazy(&handle, &accepted, &vec![2_u8; 32]);
    assert_eq!(total.get(), 48);

    let declined = OnceLock::new();
    let result = vec![3_u8; 17];
    retain_lazy(&handle, &declined, &result);
    assert!(declined.get().is_none());
    assert_eq!(result.len(), 17);
    assert_eq!(total.get(), 48);
    drop(entry);
    assert_eq!(total.get(), 0);
    retain_lazy(&handle, &declined, &result);
    assert_eq!(declined.get(), Some(&result));
}

#[test]
fn failed_optional_compilation_is_not_published_or_charged_as_retention() {
    for (op, value, limit) in [
        (CompareOp::Eq, Value::Nat64(1), 8),
        (CompareOp::StartsWith, Value::Text("abc".into()), 6),
    ] {
        let core = PreparedExecutionPlanCore::new(
            Rc::new(AccessPlannedQuery::full_scan_for_test(
                MissingRowPolicy::Error,
            )),
            0,
            accepted_identity(),
            None,
        );
        let (entry, total) = CacheEntryWeight::for_tests(32, 4096);
        core.attach_cache_retention(&entry);
        let predicate = ExecutablePredicate::Compare(ExecutableComparePredicate::field_literal(
            Some(0),
            op,
            value,
            CoercionSpec::new(CoercionId::Strict),
        ));
        let cell = OnceLock::new();
        let request = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, limit),
        );
        let result = PreparationWork::run(&request.scope(), Lane::PublicRead, |work| {
            core.try_initialize_lazy(&cell, || {
                compile_index_program(
                    &predicate,
                    &[0],
                    IndexCompilePolicy::ConservativeSubset,
                    work,
                )
            })
            .map_err(QueryError::execute)
        });
        assert!(result.unwrap_err().diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw()
        ),));
        assert!(cell.get().is_none());
        assert_eq!(total.get(), 32);

        // Retry under a fresh request can complete; a warm lookup skips its builder.
        let program = with_preparation_work(|work| {
            core.try_initialize_lazy(&cell, || {
                compile_index_program(
                    &predicate,
                    &[0],
                    IndexCompilePolicy::ConservativeSubset,
                    work,
                )
            })
        })
        .unwrap();
        assert!(program.is_some());
        let weight = total.get();
        assert!(weight > 32);
        assert_eq!(
            core.try_initialize_lazy(&cell, || panic!("warm compilation must be skipped"))
                .unwrap(),
            program
        );
        assert_eq!(total.get(), weight);

        // Unsupported is also a completed result and remains reusable.
        let absent = OnceLock::new();
        with_preparation_work(|work| {
            core.try_initialize_lazy(&absent, || {
                compile_index_program(
                    &ExecutablePredicate::IsNull {
                        field_slot: Some(0),
                    },
                    &[0],
                    IndexCompilePolicy::ConservativeSubset,
                    work,
                )
            })
        })
        .unwrap();
        assert!(matches!(absent.get(), Some(None)));
        assert!(
            core.try_initialize_lazy(&absent, || panic!("warm absence must be reused"))
                .unwrap()
                .is_none()
        );
    }
}
