//! Source classification uses the compiler caller's cumulative authority.

use super::*;
use crate::db::{
    QueryError, query::preparation::PreparationWork, tests::support::request_with_limit,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
};

fn check_resolution_schema(extra_fields: u16) -> AcceptedSchemaSnapshot {
    let mut fields = snapshot().fields().to_vec();
    for ordinal in 0..extra_fields {
        fields.push(field(
            6 + u32::from(ordinal),
            5 + ordinal,
            &format!("unused_{ordinal}"),
            AcceptedFieldKind::Int64,
            true,
            LeafCodec::Scalar(ScalarCodec::Int64),
        ));
    }
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::Resolution".into(),
        "Resolution".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    let constraints = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check(
            "resolution_policy".into(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::Compare {
                left: AcceptedCheckValueExprV1::Field(FieldId::new(2)),
                op: AcceptedCheckCompareOpV1::Gte,
                right: AcceptedCheckValueExprV1::Field(FieldId::new(2)),
            },
        )
        .unwrap();
    AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraints)).unwrap()
}

#[test]
fn compiled_field_resolution_admits_its_metadata_scan_extent() {
    let narrow = check_resolution_schema(0);
    let wide = check_resolution_schema(6);
    for lane in [
        Lane::PublicRead,
        Lane::TrustedRead,
        Lane::Diagnostic,
        Lane::Mutation,
    ] {
        let compile = |schema: &AcceptedSchemaSnapshot, limit| {
            let root = request_with_limit(Resource::PredicateExpressionSteps, limit);
            let result = PreparationWork::run(&root.scope(), lane, |work| {
                CompiledAcceptedRowConstraints::compile(schema, &value_catalog(), FINGERPRINT, work)
                    .map_err(QueryError::execute)
            });
            (root.observed(Resource::PredicateExpressionSteps), result)
        };
        let (narrow_steps, narrow_program) = compile(&narrow, u64::MAX);
        let (wide_steps, wide_program) = compile(&wide, u64::MAX);
        let narrow_program = narrow_program.unwrap();
        let wide_program = wide_program.unwrap();
        // Extra nullable fields add no row constraints or required slots, but
        // each compiled CHECK/NOT NULL field lookup scans this metadata owner.
        assert_eq!(
            narrow_program.required_slots(),
            wide_program.required_slots()
        );
        assert!(wide_steps > narrow_steps);
        assert_eq!(compile(&wide, wide_steps).1.unwrap(), wide_program);
        for limit in [narrow_steps, wide_steps - 1] {
            let error = compile(&wide, limit).1.unwrap_err();
            for fact in [
                (
                    DiagnosticFactTag::BudgetResource,
                    Resource::PredicateExpressionSteps.raw(),
                ),
                (DiagnosticFactTag::ExecutionLane, lane.raw()),
                (DiagnosticFactTag::Limit, limit),
            ] {
                assert!(error.diagnostic_facts().contains(&fact));
            }
            if limit == wide_steps - 1 {
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::Actual, wide_steps))
                );
            }
        }
        for (schema, program) in [(&narrow, &narrow_program), (&wide, &wide_program)] {
            let mut row = values(1, Value::Text("Ada".into()), Vec::new());
            row.resize(
                schema
                    .persisted_snapshot()
                    .row_layout()
                    .allocated_slot_count(),
                None,
            );
            program
                .evaluate(FINGERPRINT, &borrowed_values(&row))
                .unwrap();
        }
    }
}

fn check_tree_snapshot(expression: AcceptedCheckExprV1, accepted: bool) -> PersistedSchemaSnapshot {
    let snapshot = snapshot();
    let constraints = snapshot.constraint_catalog().clone();
    let constraints = if accepted {
        constraints.with_added_check(
            "tree_policy".into(),
            ConstraintOrigin::Generated,
            expression,
        )
    } else {
        constraints.with_added_check_activation(
            "tree_policy".into(),
            ConstraintOrigin::Generated,
            expression,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            2,
        )
    }
    .unwrap();
    snapshot.with_constraint_catalog(constraints)
}

fn check_tree_cases() -> [(AcceptedCheckExprV1, AcceptedCheckExprV1, bool); 3] {
    [
        (
            AcceptedCheckExprV1::True,
            AcceptedCheckExprV1::Not(Box::new(AcceptedCheckExprV1::Not(Box::new(
                AcceptedCheckExprV1::True,
            )))),
            true,
        ),
        (
            AcceptedCheckExprV1::False,
            AcceptedCheckExprV1::And(vec![AcceptedCheckExprV1::True, AcceptedCheckExprV1::False]),
            false,
        ),
        (
            AcceptedCheckExprV1::True,
            AcceptedCheckExprV1::Or(vec![AcceptedCheckExprV1::True, AcceptedCheckExprV1::False]),
            true,
        ),
    ]
}

#[test]
fn check_tree_structure_requires_its_own_construction_allowance() {
    for (plain, tree, succeeds) in check_tree_cases() {
        for accepted in [false, true] {
            let plain =
                AcceptedSchemaSnapshot::try_new(check_tree_snapshot(plain.clone(), accepted))
                    .unwrap();
            let tree = AcceptedSchemaSnapshot::try_new(check_tree_snapshot(tree.clone(), accepted))
                .unwrap();
            for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
                for lane in [
                    Lane::PublicRead,
                    Lane::TrustedRead,
                    Lane::Diagnostic,
                    Lane::Mutation,
                ] {
                    let compile = |schema: &AcceptedSchemaSnapshot, limit| {
                        let root = request_with_limit(resource, limit);
                        let result = PreparationWork::run(&root.scope(), lane, |work| {
                            CompiledAcceptedRowConstraints::compile(
                                schema,
                                &value_catalog(),
                                FINGERPRINT,
                                work,
                            )
                            .map_err(QueryError::execute)
                        });
                        (root.observed(resource), result)
                    };
                    let (plain_amount, plain_program) = compile(&plain, u64::MAX);
                    let (tree_amount, tree_program) = compile(&tree, u64::MAX);
                    let plain_program = plain_program.unwrap();
                    let tree_program = tree_program.unwrap();
                    assert!(tree_amount > plain_amount);
                    assert_eq!(compile(&tree, tree_amount).1.unwrap(), tree_program);
                    for limit in [plain_amount, tree_amount - 1] {
                        let error = compile(&tree, limit).1.unwrap_err();
                        for fact in [
                            (DiagnosticFactTag::BudgetResource, resource.raw()),
                            (DiagnosticFactTag::ExecutionLane, lane.raw()),
                            (DiagnosticFactTag::Limit, limit),
                        ] {
                            assert!(error.diagnostic_facts().contains(&fact));
                        }
                        if limit == tree_amount - 1 {
                            assert!(
                                error
                                    .diagnostic_facts()
                                    .contains(&(DiagnosticFactTag::Actual, tree_amount))
                            );
                        }
                    }
                    let row = values(1, Value::Text("Ada".into()), Vec::new());
                    let row = borrowed_values(&row);
                    let result = tree_program.evaluate(FINGERPRINT, &row);
                    assert_eq!(result.is_ok(), succeeds);
                    assert_eq!(result, plain_program.evaluate(FINGERPRINT, &row));
                    if !accepted {
                        let activation = CompiledAcceptedRowConstraints::compile_activation(
                            &tree,
                            &value_catalog(),
                            FINGERPRINT,
                            tree.persisted_snapshot().constraint_activations()[0].id(),
                            AcceptedRowConstraintViolationKind::Check,
                        )
                        .unwrap();
                        assert_eq!(activation.evaluate(FINGERPRINT, &row), result);
                    }
                }
            }
        }
    }
}

#[test]
fn malformed_check_trees_refuse_before_recursive_construction_admission() {
    let mut too_deep = AcceptedCheckExprV1::True;
    for _ in 0..MAX_CHECK_EXPR_V1_DEPTH {
        too_deep = AcceptedCheckExprV1::Not(Box::new(too_deep));
    }
    for (expression, expected) in [
        (too_deep, AcceptedCheckExprV1Error::DepthExceeded),
        (
            AcceptedCheckExprV1::And(vec![AcceptedCheckExprV1::True, AcceptedCheckExprV1::True]),
            AcceptedCheckExprV1Error::NonCanonical,
        ),
        (
            AcceptedCheckExprV1::IsNull(AcceptedCheckValueExprV1::Field(FieldId::new(99))),
            AcceptedCheckExprV1Error::UnknownField,
        ),
    ] {
        for accepted in [false, true] {
            // Bypass snapshot acceptance solely to exercise the compiler's
            // defensive validation before any structural recursive admission.
            let schema =
                AcceptedSchemaSnapshot::new(check_tree_snapshot(expression.clone(), accepted));
            let snapshot = schema.persisted_snapshot();
            let budget = MaintenanceConstructionBudget::new();
            let error = CompiledAcceptedRowConstraints::compile(
                &schema,
                &value_catalog(),
                FINGERPRINT,
                &budget,
            )
            .unwrap_err();
            assert_eq!(
                error.diagnostic(),
                crate::error::InternalError::accepted_row_constraint_program_corrupt().diagnostic()
            );
            assert_eq!(
                budget.observed_for_tests(Resource::PredicateExpressionSteps),
                (snapshot.constraints().len() + snapshot.constraint_activations().len()) as u64
            );
            if !accepted {
                assert_eq!(
                    CompiledAcceptedRowConstraints::compile_activation(
                        &schema,
                        &value_catalog(),
                        FINGERPRINT,
                        snapshot.constraint_activations()[0].id(),
                        AcceptedRowConstraintViolationKind::Check,
                    )
                    .unwrap_err(),
                    AcceptedRowConstraintEvaluationError::InvalidExpression(expected)
                );
            }
        }
    }
}

#[test]
fn singleton_row_program_needs_no_sort_allowance() {
    let mut snapshot = snapshot();
    let field = snapshot.fields()[0].clone();
    snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::Singleton".into(),
        "Singleton".into(),
        field.id(),
        SchemaRowLayout::initial(vec![(field.id(), field.slot())]),
        vec![field],
    );
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot).unwrap();
    for resource in [
        Resource::SortEntries,
        Resource::SortComparisons,
        Resource::SortTemporaryBytes,
    ] {
        let root = request_with_limit(resource, 0);
        let program = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            CompiledAcceptedRowConstraints::compile(&accepted, &value_catalog(), FINGERPRINT, work)
                .map_err(QueryError::execute)
        })
        .unwrap();
        assert_eq!(program.required_slots(), &[0]);
        assert_eq!(program.integrity_constraint_count(), 0);
        assert_eq!(root.observed(resource), 0);
    }
}

fn unique_barrier_sort_fixture() -> ([AcceptedSchemaSnapshot; 2], ConstraintId) {
    let single = unique_activation_predicate_fixture("nickname IS NOT NULL");
    let id = SchemaIndexId::new(2).unwrap();
    let second = PersistedIndexSnapshot::new(
        id,
        2,
        "unique_payload".into(),
        "tests::CheckedEntity::unique_payload".into(),
        true,
        PersistedIndexKeySnapshot::from_field_paths(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(5),
            SchemaFieldSlot::new(4),
            vec!["payload".into()],
            AcceptedFieldKind::Blob { max_len: Some(64) },
            false,
        )]),
        None,
    )
    .clone_with_schema_identity(id, 2, 10);
    let constraints = single
        .constraint_catalog()
        .clone()
        .with_added_unique_activation(&second, AcceptedSchemaFingerprint::new([0xA5; 32]), 10)
        .unwrap();
    let second_constraint = constraints.activations().last().unwrap().id();
    let double = single
        .clone()
        .with_constraint_catalog(constraints)
        .with_constraint_candidates(
            vec![single.candidate_indexes()[0].clone(), second],
            Vec::new(),
        );
    (
        [single, double].map(|snapshot| AcceptedSchemaSnapshot::try_new(snapshot).unwrap()),
        second_constraint,
    )
}

#[test]
fn multiple_unique_barriers_admit_their_final_sort_before_returning_a_program() {
    let ([single, double], second_constraint) = unique_barrier_sort_fixture();
    for resource in [
        Resource::SortEntries,
        Resource::SortComparisons,
        Resource::SortTemporaryBytes,
    ] {
        let compile = |schema: &AcceptedSchemaSnapshot, limit| {
            let root = request_with_limit(resource, limit);
            let result = PreparationWork::run(&root.scope(), Lane::Mutation, |work| {
                CompiledAcceptedRowConstraints::compile(schema, &value_catalog(), FINGERPRINT, work)
                    .map_err(QueryError::execute)
            });
            (root.observed(resource), result)
        };
        let (single_amount, single_program) = compile(&single, u64::MAX);
        let (double_amount, double_program) = compile(&double, u64::MAX);
        let single_program = single_program.unwrap();
        let double_program = double_program.unwrap();
        let additional = match resource {
            Resource::SortEntries | Resource::SortComparisons => 2,
            Resource::SortTemporaryBytes => icydb_schema::compact_sort_scratch_bytes(2),
            _ => unreachable!(),
        };
        assert_eq!(double_amount, single_amount + additional);
        assert_eq!(compile(&double, double_amount).1.unwrap(), double_program);
        for limit in [single_amount, double_amount - 1] {
            let error = compile(&double, limit).1.unwrap_err();
            for fact in [
                (DiagnosticFactTag::BudgetResource, resource.raw()),
                (DiagnosticFactTag::ExecutionLane, Lane::Mutation.raw()),
                (DiagnosticFactTag::Limit, limit),
                (DiagnosticFactTag::Actual, double_amount),
            ] {
                assert!(error.diagnostic_facts().contains(&fact));
            }
        }
        let mut provenance = vec![Some(AcceptedFieldWriteProvenance::Preserved); 5];
        provenance[4] = Some(AcceptedFieldWriteProvenance::Authored);
        assert!(
            single_program
                .unique_activation_write_blocker(MutationMode::Update, &provenance)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            double_program
                .unique_activation_write_blocker(MutationMode::Update, &provenance)
                .unwrap()
                .unwrap()
                .constraint_id(),
            second_constraint
        );
        provenance[1] = Some(AcceptedFieldWriteProvenance::Authored);
        assert_eq!(
            double_program
                .unique_activation_write_blocker(MutationMode::Update, &provenance)
                .unwrap()
                .unwrap()
                .constraint_id(),
            single_program
                .unique_activation_write_blocker(MutationMode::Update, &provenance)
                .unwrap()
                .unwrap()
                .constraint_id(),
        );
    }
}

#[test]
fn compiled_dependency_slots_preserve_nested_fields_and_activation_scope() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let expression = bind_check_expr_v1(
        CheckExprV1Input::Not(Box::new(CheckExprV1Input::And(vec![
            CheckExprV1Input::Or(vec![
                CheckExprV1Input::IsNull(CheckValueExprV1Input::Field("nickname".into())),
                CheckExprV1Input::Compare {
                    left: CheckValueExprV1Input::Literal(InputValue::nat64(0)),
                    op: AcceptedCheckCompareOpV1::Lte,
                    right: CheckValueExprV1Input::CharLength("nickname".into()),
                },
            ]),
            CheckExprV1Input::Compare {
                left: CheckValueExprV1Input::Cardinality("tags".into()),
                op: AcceptedCheckCompareOpV1::Gte,
                right: CheckValueExprV1Input::OctetLength("payload".into()),
            },
            CheckExprV1Input::IsNotNull(CheckValueExprV1Input::Field("score".into())),
        ]))),
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .unwrap();
    let expected = expression
        .dependencies()
        .into_iter()
        .map(|field| usize::from(snapshot.row_layout().slot_for_field(field).unwrap().get()))
        .collect::<Vec<_>>();
    assert_eq!(expected, [1, 2, 3, 4]);
    let constraints = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check_activation(
            "nested_dependencies".into(),
            ConstraintOrigin::Generated,
            expression,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            2,
        )
        .unwrap();
    let activation_id = constraints.activations()[0].id();
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraints)).unwrap();
    let whole = CompiledAcceptedRowConstraints::compile(
        &accepted,
        &catalog,
        FINGERPRINT,
        &MaintenanceConstructionBudget::new(),
    )
    .unwrap();
    assert_eq!(whole.required_slots(), &[0, 1, 2, 3, 4]);
    let activation = CompiledAcceptedRowConstraints::compile_activation(
        &accepted,
        &catalog,
        FINGERPRINT,
        activation_id,
        AcceptedRowConstraintViolationKind::Check,
    )
    .unwrap();
    assert_eq!(activation.required_slots(), expected);
}

#[test]
fn compiled_dependency_backing_uses_the_same_cumulative_caller_authority() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let [constant, dependent] = [false, true].map(|dependent| {
        let mut constraints = snapshot.constraint_catalog().clone();
        for ordinal in 0..16 {
            constraints = constraints
                .with_added_check(
                    format!("dependency_{ordinal}"),
                    ConstraintOrigin::Generated,
                    if dependent {
                        AcceptedCheckExprV1::IsNotNull(AcceptedCheckValueExprV1::Field(
                            FieldId::new(2),
                        ))
                    } else {
                        AcceptedCheckExprV1::True
                    },
                )
                .unwrap();
        }
        AcceptedSchemaSnapshot::try_new(snapshot.clone().with_constraint_catalog(constraints))
            .unwrap()
    });
    for lane in [
        Lane::PublicRead,
        Lane::TrustedRead,
        Lane::Diagnostic,
        Lane::Mutation,
    ] {
        let compile = |schema: &AcceptedSchemaSnapshot, limit| {
            let root = request_with_limit(Resource::TemporaryBytes, limit);
            let result = PreparationWork::run(&root.scope(), lane, |work| {
                CompiledAcceptedRowConstraints::compile(schema, &catalog, FINGERPRINT, work)
                    .map_err(QueryError::execute)
            });
            (root.observed(Resource::TemporaryBytes), result)
        };
        let (constant_bytes, constant_program) = compile(&constant, u64::MAX);
        let (dependent_bytes, dependent_program) = compile(&dependent, u64::MAX);
        let constant_program = constant_program.unwrap();
        let dependent_program = dependent_program.unwrap();
        // NOT NULL already needs this slot. Repeated CHECK dependencies must
        // still admit collection backing before the final sorted deduplication.
        assert_eq!(
            constant_program.required_slots(),
            dependent_program.required_slots()
        );
        assert!(dependent_bytes > constant_bytes);
        assert_eq!(
            compile(&dependent, dependent_bytes).1.unwrap(),
            dependent_program,
        );
        for limit in [constant_bytes, dependent_bytes - 1] {
            let error = compile(&dependent, limit).1.unwrap_err();
            for fact in [
                (
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw(),
                ),
                (DiagnosticFactTag::ExecutionLane, lane.raw()),
                (DiagnosticFactTag::Limit, limit),
            ] {
                assert!(error.diagnostic_facts().contains(&fact));
            }
            if limit == dependent_bytes - 1 {
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::Actual, dependent_bytes,))
                );
            }
        }
        let row = values(1, Value::Text("Ada".into()), Vec::new());
        dependent_program
            .evaluate(FINGERPRINT, &borrowed_values(&row))
            .unwrap();
    }
}

fn integrity_scope_schemas(
    fixture: &TargetedRuleFixture,
    targeted: bool,
) -> [AcceptedSchemaSnapshot; 2] {
    let snapshot = fixture.accepted.persisted_snapshot();
    [false, true].map(|accepted| {
        let catalog = snapshot.constraint_catalog().clone();
        let target = AcceptedRuleTarget::new(
            FieldId::new(8),
            AcceptedNamedTypeIdentity::Composite(fixture.label_type),
        );
        let operation = AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 5 };
        let fingerprint = AcceptedSchemaFingerprint::new([0xA5; 32]);
        let catalog = match (targeted, accepted) {
            (false, false) => catalog.with_added_check_activation(
                "scope_check".into(),
                ConstraintOrigin::Generated,
                AcceptedCheckExprV1::True,
                fingerprint,
                2,
            ),
            (false, true) => catalog.with_added_check(
                "scope_check".into(),
                ConstraintOrigin::Generated,
                AcceptedCheckExprV1::True,
            ),
            (true, false) => catalog.with_added_targeted_rule_activation(
                "scope_targeted".into(),
                ConstraintOrigin::Generated,
                target,
                operation,
                fingerprint,
                2,
            ),
            (true, true) => catalog.with_added_targeted_rule(
                "scope_targeted".into(),
                ConstraintOrigin::Generated,
                target,
                operation,
            ),
        }
        .unwrap();
        AcceptedSchemaSnapshot::try_new(snapshot.clone().with_constraint_catalog(catalog)).unwrap()
    })
}

#[test]
fn accepted_integrity_entries_need_backing_beyond_pending_write_gates() {
    let fixture = targeted_rule_fixture();
    for targeted in [false, true] {
        let [pending, accepted] = integrity_scope_schemas(&fixture, targeted);
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            let compile = |schema: &AcceptedSchemaSnapshot, limit| {
                let root = request_with_limit(Resource::TemporaryBytes, limit);
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    CompiledAcceptedRowConstraints::compile(
                        schema,
                        &fixture.value_catalog,
                        FINGERPRINT,
                        work,
                    )
                    .map_err(QueryError::execute)
                });
                (root.observed(Resource::TemporaryBytes), result)
            };
            let (pending_bytes, pending_program) = compile(&pending, u64::MAX);
            let pending_program = pending_program.unwrap();
            let (accepted_bytes, accepted_program) = compile(&accepted, u64::MAX);
            let accepted_program = accepted_program.unwrap();
            assert_eq!(
                accepted_program.integrity_constraint_count(),
                pending_program.integrity_constraint_count() + 1,
            );
            assert!(accepted_bytes > pending_bytes);
            let values = borrowed_values(&fixture.values);
            pending_program.evaluate(FINGERPRINT, &values).unwrap();
            accepted_program.evaluate(FINGERPRINT, &values).unwrap();
            assert_eq!(compile(&pending, pending_bytes).1.unwrap(), pending_program);
            assert_eq!(
                compile(&accepted, accepted_bytes).1.unwrap(),
                accepted_program
            );
            for limit in [pending_bytes, accepted_bytes - 1] {
                let error = compile(&accepted, limit).1.unwrap_err();
                for fact in [
                    (
                        DiagnosticFactTag::BudgetResource,
                        Resource::TemporaryBytes.raw(),
                    ),
                    (DiagnosticFactTag::ExecutionLane, lane.raw()),
                    (DiagnosticFactTag::Limit, limit),
                    (DiagnosticFactTag::Actual, accepted_bytes),
                ] {
                    assert!(error.diagnostic_facts().contains(&fact));
                }
            }
        }
    }
}

#[test]
fn compiled_checks_require_backing_even_when_source_capacity_is_reused() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let mut constraints = snapshot.constraint_catalog().clone();
    let mut previous_bytes = None;
    for count in 1..=4 {
        constraints = constraints
            .with_added_check(
                format!("true_{count}"),
                ConstraintOrigin::Generated,
                AcceptedCheckExprV1::True,
            )
            .unwrap();
        let accepted = AcceptedSchemaSnapshot::try_new(
            snapshot
                .clone()
                .with_constraint_catalog(constraints.clone()),
        )
        .unwrap();
        let budget = MaintenanceConstructionBudget::new();
        let program = budget
            .run(
                |work| {
                    CompiledAcceptedRowConstraints::compile(&accepted, &catalog, FINGERPRINT, work)
                },
                std::convert::identity,
            )
            .unwrap();
        assert_eq!(program.integrity_constraint_count(), count);
        let bytes = budget.observed_for_tests(Resource::TemporaryBytes);
        // The first four CHECK sources share the source owner's initial
        // capacity. Every additional compiled CHECK still needs program backing.
        if let Some(previous) = previous_bytes {
            assert!(bytes > previous);
        }
        previous_bytes = Some(bytes);
    }
}

fn mixed_source_fixture() -> TargetedRuleFixture {
    let mut fixture = targeted_rule_fixture();
    let snapshot = fixture.accepted.persisted_snapshot();
    let fingerprint = AcceptedSchemaFingerprint::new([0xA5; 32]);
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check(
            "accepted_true".into(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::Not(Box::new(AcceptedCheckExprV1::Not(Box::new(
                AcceptedCheckExprV1::True,
            )))),
        )
        .unwrap()
        .with_added_check_activation(
            "pending_true".into(),
            ConstraintOrigin::SqlDdl,
            AcceptedCheckExprV1::Or(vec![AcceptedCheckExprV1::True, AcceptedCheckExprV1::False]),
            fingerprint,
            2,
        )
        .unwrap()
        .with_added_not_null_activation(&snapshot.fields()[7], fingerprint, 2)
        .unwrap()
        .with_added_targeted_rule_activation(
            "pending_label_length".into(),
            ConstraintOrigin::Generated,
            AcceptedRuleTarget::new(
                FieldId::new(8),
                AcceptedNamedTypeIdentity::Composite(fixture.label_type),
            ),
            AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 5 },
            fingerprint,
            2,
        )
        .unwrap();
    fixture.accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.clone().with_constraint_catalog(catalog)).unwrap();
    fixture
}

#[test]
fn mixed_sources_obey_exact_short_and_cumulative_caller_limits() {
    let fixture = mixed_source_fixture();
    for resource in [
        Resource::PredicateExpressionSteps,
        Resource::TemporaryBytes,
        Resource::SortEntries,
        Resource::SortComparisons,
        Resource::SortTemporaryBytes,
    ] {
        for lane in [
            Lane::PublicRead,
            Lane::TrustedRead,
            Lane::Diagnostic,
            Lane::Mutation,
        ] {
            let baseline = request_with_limit(resource, u64::MAX);
            let compile = |work: &PreparationWork<'_>| {
                CompiledAcceptedRowConstraints::compile(
                    &fixture.accepted,
                    &fixture.value_catalog,
                    FINGERPRINT,
                    work,
                )
                .map_err(QueryError::execute)
            };
            let expected = PreparationWork::run(&baseline.scope(), lane, compile).unwrap();
            let exact = baseline.observed(resource);
            assert!(exact > 0);
            for limit in [0, exact - 1, exact, 2 * exact] {
                let root = request_with_limit(resource, limit);
                for attempt in 1..=3 {
                    let result = PreparationWork::run(&root.scope(), lane, compile);
                    if attempt * exact <= limit {
                        assert_eq!(result.unwrap(), expected);
                        assert_eq!(root.observed(resource), attempt * exact);
                    } else {
                        let error = result.unwrap_err();
                        for fact in [
                            (DiagnosticFactTag::BudgetResource, resource.raw()),
                            (DiagnosticFactTag::ExecutionLane, lane.raw()),
                            (DiagnosticFactTag::Limit, limit),
                        ] {
                            assert!(error.diagnostic_facts().contains(&fact));
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn mixed_sources_share_maintenance_capacity_and_keep_program_semantics() {
    let fixture = mixed_source_fixture();
    let baseline = MaintenanceConstructionBudget::new();
    let compile = |work: &MaintenanceConstructionBudget| {
        CompiledAcceptedRowConstraints::compile(
            &fixture.accepted,
            &fixture.value_catalog,
            FINGERPRINT,
            work,
        )
    };
    let expected = baseline.run(compile, std::convert::identity).unwrap();
    expected
        .evaluate(FINGERPRINT, &borrowed_values(&fixture.values))
        .unwrap();
    for resource in [
        Resource::PredicateExpressionSteps,
        Resource::TemporaryBytes,
        Resource::SortEntries,
        Resource::SortComparisons,
        Resource::SortTemporaryBytes,
    ] {
        let exact = baseline.observed_for_tests(resource);
        assert!(exact > 0);
        for limit in [0, exact - 1, exact, 2 * exact] {
            let budget = MaintenanceConstructionBudget::with_limit_for_tests(resource, limit);
            for attempt in 1..=3 {
                let result = budget.run(compile, std::convert::identity);
                if attempt * exact <= limit {
                    assert_eq!(result.unwrap(), expected);
                } else {
                    let error = result.unwrap_err();
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw(),))
                    );
                }
            }
        }
    }
}
