use super::*;

#[test]
fn append_only_field_mutation_plan_is_no_rebuild() {
    let field = nullable_text_field("nickname", 3, 2);
    let plan = MutationPlan::append_only_fields(&[field]);

    assert_eq!(
        plan.compatibility(),
        MutationCompatibility::MetadataOnlySafe
    );
    assert_eq!(
        plan.rebuild_requirement(),
        RebuildRequirement::NoRebuildRequired
    );
    assert_eq!(plan.added_field_count(), 1);
    assert_eq!(
        plan.mutations(),
        &[SchemaMutation::AddNullableField {
            field_id: FieldId::new(3),
            name: "nickname".to_string(),
            slot: SchemaFieldSlot::new(2),
        }]
    );
}

#[test]
fn mutation_plan_fingerprint_is_deterministic_and_semantic() {
    let nickname = nullable_text_field("nickname", 3, 2);
    let handle = nullable_text_field("handle", 3, 2);
    let first = MutationPlan::append_only_fields(std::slice::from_ref(&nickname));
    let second = MutationPlan::append_only_fields(&[nickname]);
    let changed = MutationPlan::append_only_fields(&[handle]);

    assert_eq!(first.fingerprint(), second.fingerprint());
    assert_ne!(first.fingerprint(), changed.fingerprint());
}

#[test]
fn index_mutation_plans_are_rebuild_gated() {
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let expression =
        SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
            .expect("accepted expression index should lower")
            .lower_to_plan();
    let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
        &non_unique_name_index(),
    )
    .expect("non-unique secondary index should lower to drop cleanup")
    .lower_to_plan();

    for plan in [&field_path, &expression, &drop] {
        assert_eq!(plan.compatibility(), MutationCompatibility::RequiresRebuild);
        assert_eq!(
            plan.rebuild_requirement(),
            RebuildRequirement::IndexRebuildRequired
        );
        assert_eq!(
            plan.publication_status(),
            MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
                MutationCompatibility::RequiresRebuild,
            )),
        );
    }
}

#[test]
fn rebuild_plan_derives_physical_index_actions() {
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let expression =
        SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
            .expect("accepted expression index should lower")
            .lower_to_plan();
    let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
        &non_unique_name_index(),
    )
    .expect("non-unique secondary index should lower to drop cleanup")
    .lower_to_plan();

    let field_path_rebuild = field_path.rebuild_plan();
    let [SchemaRebuildAction::BuildFieldPathIndex { target }] = field_path_rebuild.actions() else {
        panic!("field-path index addition should derive one field-path rebuild target");
    };
    assert_eq!(target.ordinal(), 1);
    assert_eq!(target.name(), "by_name");
    assert_eq!(target.store(), "test::mutation::by_name");
    assert!(!target.unique());
    assert_eq!(target.predicate_sql(), Some("name IS NOT NULL"));
    let [key_path] = target.key_paths() else {
        panic!("field-path rebuild target should carry one accepted key path");
    };
    assert_eq!(key_path.field_id(), FieldId::new(2));
    assert_eq!(key_path.slot(), SchemaFieldSlot::new(1));
    assert_eq!(key_path.path(), &["name".to_string()]);
    assert_eq!(key_path.kind(), &PersistedFieldKind::Text { max_len: None });
    assert!(!key_path.nullable());
    let expression_rebuild = expression.rebuild_plan();
    let [SchemaRebuildAction::BuildExpressionIndex { target }] = expression_rebuild.actions()
    else {
        panic!("expression index addition should derive one expression rebuild target");
    };
    assert_eq!(target.ordinal(), 2);
    assert_eq!(target.name(), "by_lower_name");
    assert_eq!(target.store(), "test::mutation::by_lower_name");
    assert!(!target.unique());
    assert_eq!(target.predicate_sql(), Some("LOWER(name) IS NOT NULL"));
    let [super::SchemaExpressionIndexRebuildKey::Expression(expression)] = target.key_items()
    else {
        panic!("expression rebuild target should carry one expression key");
    };
    assert_eq!(expression.op(), PersistedIndexExpressionOp::Lower);
    assert_eq!(expression.canonical_text(), "expr:v1:LOWER(name)");
    assert_eq!(
        expression.input_kind(),
        &PersistedFieldKind::Text { max_len: None }
    );
    assert_eq!(
        expression.output_kind(),
        &PersistedFieldKind::Text { max_len: None }
    );
    assert_eq!(expression.source().field_id(), FieldId::new(2));
    assert_eq!(expression.source().slot(), SchemaFieldSlot::new(1));
    let drop_rebuild = drop.rebuild_plan();
    let [SchemaRebuildAction::DropSecondaryIndex { target }] = drop_rebuild.actions() else {
        panic!("secondary index drop should derive one cleanup target");
    };
    assert_eq!(target.ordinal(), 1);
    assert_eq!(target.name(), "by_name");
    assert_eq!(target.store(), "test::mutation::by_name");
    assert!(!target.unique());
    assert_eq!(target.predicate_sql(), Some("name IS NOT NULL"));
}

#[test]
fn execution_plan_keeps_metadata_only_mutations_publishable_without_steps() {
    let field = nullable_text_field("nickname", 3, 2);
    let plan = MutationPlan::append_only_fields(&[field]);
    let execution = plan.execution_plan();

    assert_eq!(
        execution.readiness(),
        super::SchemaMutationExecutionReadiness::PublishableNow,
    );
    assert!(execution.steps().is_empty());
    assert!(execution.runner_capabilities().is_empty());
    assert_eq!(
        execution.execution_gate(),
        super::SchemaMutationExecutionGate::ReadyToPublish,
    );
    assert_eq!(
        execution.admit_runner_capabilities(&[]),
        super::SchemaMutationExecutionAdmission::PublishableNow,
    );
    assert_eq!(
        plan.publication_status(),
        MutationPublicationStatus::Publishable,
    );
}

#[test]
fn execution_plan_schedules_index_work_before_validation_and_invalidation() {
    let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
        &non_unique_name_index(),
    )
    .expect("non-unique secondary index should lower to drop cleanup")
    .lower_to_plan();
    let execution = drop.execution_plan();

    assert_eq!(
        execution.readiness(),
        super::SchemaMutationExecutionReadiness::RequiresPhysicalRunner(
            RebuildRequirement::IndexRebuildRequired,
        ),
    );
    assert_eq!(
        execution.execution_gate(),
        super::SchemaMutationExecutionGate::AwaitingPhysicalWork {
            requirement: RebuildRequirement::IndexRebuildRequired,
            step_count: 3,
        },
    );
    let [
        super::SchemaMutationExecutionStep::DropSecondaryIndex { target },
        super::SchemaMutationExecutionStep::ValidatePhysicalWork,
        super::SchemaMutationExecutionStep::InvalidateRuntimeState,
    ] = execution.steps()
    else {
        panic!("drop execution should schedule cleanup, validation, and invalidation");
    };
    assert_eq!(target.name(), "by_name");
    assert_eq!(target.store(), "test::mutation::by_name");
}

#[test]
fn execution_plan_reports_runner_capabilities_without_duplicates() {
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let expression =
        SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
            .expect("accepted expression index should lower")
            .lower_to_plan();
    let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
        &non_unique_name_index(),
    )
    .expect("non-unique secondary index should lower to drop cleanup")
    .lower_to_plan();

    assert_eq!(
        field_path.execution_plan().runner_capabilities(),
        vec![
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert_eq!(
        expression.execution_plan().runner_capabilities(),
        vec![
            super::SchemaMutationRunnerCapability::BuildExpressionIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert_eq!(
        drop.execution_plan().runner_capabilities(),
        vec![
            super::SchemaMutationRunnerCapability::DropSecondaryIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
}

#[test]
fn execution_admission_fails_closed_on_missing_runner_capabilities() {
    let drop = SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(
        &non_unique_name_index(),
    )
    .expect("non-unique secondary index should lower to drop cleanup")
    .lower_to_plan();
    let execution = drop.execution_plan();

    assert_eq!(
        execution.admit_runner_capabilities(&[]),
        super::SchemaMutationExecutionAdmission::MissingRunnerCapabilities {
            missing: vec![
                super::SchemaMutationRunnerCapability::DropSecondaryIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        },
    );
    assert_eq!(
        execution.admit_runner_capabilities(&[
            super::SchemaMutationRunnerCapability::DropSecondaryIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
        ]),
        super::SchemaMutationExecutionAdmission::MissingRunnerCapabilities {
            missing: vec![super::SchemaMutationRunnerCapability::InvalidateRuntimeState],
        },
    );
    assert_eq!(
        execution.admit_runner_capabilities(&[
            super::SchemaMutationRunnerCapability::DropSecondaryIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ]),
        super::SchemaMutationExecutionAdmission::RunnerReady {
            required: vec![
                super::SchemaMutationRunnerCapability::DropSecondaryIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        },
    );
}

#[test]
fn runner_contract_preflight_deduplicates_capabilities_and_preserves_gate() {
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let execution = field_path.execution_plan();
    let runner = super::SchemaMutationRunnerContract::new(&[
        super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
        super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
        super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
        super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
    ]);

    assert_eq!(
        runner.capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert_eq!(
        runner.preflight(&execution),
        super::SchemaMutationRunnerPreflight::Ready {
            step_count: 3,
            required: vec![
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        },
    );
    assert_eq!(
        execution.execution_gate(),
        super::SchemaMutationExecutionGate::AwaitingPhysicalWork {
            requirement: RebuildRequirement::IndexRebuildRequired,
            step_count: 3,
        },
    );
}

#[test]
fn runner_contract_preflight_keeps_no_work_and_rejections_non_executable() {
    let metadata_only = MutationPlan::append_only_fields(&[nullable_text_field("nickname", 3, 2)]);
    let rewrite = SchemaMutationRequest::Incompatible.lower_to_plan();
    let unsupported = SchemaMutationRequest::AlterNullability {
        field_id: FieldId::new(2),
    }
    .lower_to_plan();
    let runner = super::SchemaMutationRunnerContract::new(&[
        super::SchemaMutationRunnerCapability::RewriteAllRows,
    ]);

    assert_eq!(
        runner.preflight(&metadata_only.execution_plan()),
        super::SchemaMutationRunnerPreflight::NoPhysicalWork,
    );
    assert_eq!(
        runner.preflight(&rewrite.execution_plan()),
        super::SchemaMutationRunnerPreflight::Rejected {
            requirement: RebuildRequirement::FullDataRewriteRequired,
        },
    );
    assert_eq!(
        runner.preflight(&unsupported.execution_plan()),
        super::SchemaMutationRunnerPreflight::Rejected {
            requirement: RebuildRequirement::Unsupported,
        },
    );
}

#[test]
fn runner_outcome_reports_no_work_and_ready_physical_work() {
    let metadata_only = MutationPlan::append_only_fields(&[nullable_text_field("nickname", 3, 2)]);
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let runner = super::SchemaMutationRunnerContract::new(&[
        super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
        super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
        super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
    ]);

    let super::SchemaMutationRunnerOutcome::NoPhysicalWork(no_work) =
        runner.outcome(&metadata_only.execution_plan())
    else {
        panic!("metadata-only mutation should not require physical work");
    };
    assert_eq!(no_work.step_count(), 0);
    assert!(no_work.required_capabilities().is_empty());
    assert_eq!(
        no_work.completed_phases(),
        &[super::SchemaMutationRunnerPhase::Preflight],
    );
    assert!(no_work.has_completed_phase(super::SchemaMutationRunnerPhase::Preflight));
    assert_eq!(no_work.store_visibility(), None);
    assert_eq!(no_work.rows_scanned(), 0);
    assert_eq!(no_work.rows_skipped(), 0);
    assert_eq!(no_work.index_keys_written(), 0);
    assert!(!no_work.physical_work_allows_publication());

    let super::SchemaMutationRunnerOutcome::ReadyForPhysicalWork(ready) =
        runner.outcome(&field_path.execution_plan())
    else {
        panic!("field-path index mutation should be ready for staged physical work");
    };
    assert_eq!(ready.step_count(), 3);
    assert_eq!(
        ready.required_capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
    assert_eq!(
        ready.completed_phases(),
        &[super::SchemaMutationRunnerPhase::Preflight],
    );
    assert_eq!(
        ready.store_visibility(),
        Some(super::SchemaMutationStoreVisibility::StagedOnly),
    );
    assert_eq!(ready.rows_scanned(), 0);
    assert_eq!(ready.rows_skipped(), 0);
    assert_eq!(ready.index_keys_written(), 0);
    assert!(!ready.physical_work_allows_publication());
}

#[test]
fn runner_outcome_classifies_missing_capabilities_and_unsupported_requirements() {
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();
    let no_runner = super::SchemaMutationRunnerContract::new(&[]);

    let super::SchemaMutationRunnerOutcome::Rejected(missing) =
        no_runner.outcome(&field_path.execution_plan())
    else {
        panic!("missing runner capabilities should reject before physical work");
    };
    assert_eq!(missing.phase(), super::SchemaMutationRunnerPhase::Preflight);
    assert_eq!(
        missing.kind(),
        super::SchemaMutationRunnerRejectionKind::MissingCapabilities,
    );
    assert_eq!(
        missing.requirement(),
        Some(RebuildRequirement::IndexRebuildRequired),
    );
    assert_eq!(
        missing.missing_capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );

    let super::SchemaMutationRunnerOutcome::Rejected(unsupported) =
        no_runner.outcome(&incompatible.execution_plan())
    else {
        panic!("full rewrite should remain rejected by runner outcome");
    };
    assert_eq!(
        unsupported.kind(),
        super::SchemaMutationRunnerRejectionKind::UnsupportedRequirement,
    );
    assert_eq!(
        unsupported.requirement(),
        Some(RebuildRequirement::FullDataRewriteRequired),
    );
    assert!(unsupported.missing_capabilities().is_empty());
}

#[test]
fn runner_input_binds_accepted_snapshots_to_execution_plan() {
    let before = base_snapshot();
    let added = nullable_text_field("nickname", 3, 2);
    let after = append_fields_snapshot(&before, std::slice::from_ref(&added));
    let plan = MutationPlan::append_only_fields(&[added]);
    let input = super::SchemaMutationRunnerInput::new(&before, &after, plan.execution_plan())
        .expect("same-entity accepted snapshots should build runner input");
    let runner = super::SchemaMutationRunnerContract::new(&[]);

    assert_eq!(input.accepted_before().entity_path(), before.entity_path());
    assert_eq!(
        input.accepted_after().fields().len(),
        before.fields().len() + 1,
    );
    assert_eq!(
        input.execution_plan().readiness(),
        super::SchemaMutationExecutionReadiness::PublishableNow,
    );
    assert!(matches!(
        input.outcome(&runner),
        super::SchemaMutationRunnerOutcome::NoPhysicalWork(_),
    ));
}

#[test]
fn runner_input_rejects_cross_entity_snapshot_pairs() {
    let before = base_snapshot();
    let wrong_entity = PersistedSchemaSnapshot::new(
        before.version(),
        "test::OtherEntity".to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_id(),
        before.row_layout().clone(),
        before.fields().to_vec(),
    );
    let wrong_name = PersistedSchemaSnapshot::new(
        before.version(),
        before.entity_path().to_string(),
        "OtherEntity".to_string(),
        before.primary_key_field_id(),
        before.row_layout().clone(),
        before.fields().to_vec(),
    );
    let wrong_pk = PersistedSchemaSnapshot::new(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        FieldId::new(99),
        before.row_layout().clone(),
        before.fields().to_vec(),
    );

    assert_eq!(
        super::SchemaMutationRunnerInput::new(
            &before,
            &wrong_entity,
            MutationPlan::exact_match().execution_plan(),
        ),
        Err(super::SchemaMutationRunnerInputError::EntityPath),
    );
    assert_eq!(
        super::SchemaMutationRunnerInput::new(
            &before,
            &wrong_name,
            MutationPlan::exact_match().execution_plan(),
        ),
        Err(super::SchemaMutationRunnerInputError::EntityName),
    );
    assert_eq!(
        super::SchemaMutationRunnerInput::new(
            &before,
            &wrong_pk,
            MutationPlan::exact_match().execution_plan(),
        ),
        Err(super::SchemaMutationRunnerInputError::PrimaryKeyField),
    );
}

#[test]
fn noop_runner_accepts_metadata_only_input_and_rejects_physical_work() {
    let before = base_snapshot();
    let added = nullable_text_field("nickname", 3, 2);
    let metadata_after = append_fields_snapshot(&before, std::slice::from_ref(&added));
    let metadata_input = super::SchemaMutationRunnerInput::new(
        &before,
        &metadata_after,
        MutationPlan::append_only_fields(&[added]).execution_plan(),
    )
    .expect("metadata-only same-entity input should build");
    let index_after = snapshot_with_indexes(&before, vec![non_unique_name_index()]);
    let index_plan =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let index_input =
        super::SchemaMutationRunnerInput::new(&before, &index_after, index_plan.execution_plan())
            .expect("index same-entity input should build");
    let runner = super::SchemaMutationNoopRunner::new();

    assert!(matches!(
        runner.run(&metadata_input),
        super::SchemaMutationRunnerOutcome::NoPhysicalWork(_),
    ));

    let super::SchemaMutationRunnerOutcome::Rejected(rejection) = runner.run(&index_input) else {
        panic!("no-op runner must reject physical index work");
    };
    assert_eq!(
        rejection.kind(),
        super::SchemaMutationRunnerRejectionKind::MissingCapabilities,
    );
    assert_eq!(
        rejection.requirement(),
        Some(RebuildRequirement::IndexRebuildRequired),
    );
    assert_eq!(
        rejection.missing_capabilities(),
        &[
            super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
            super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
            super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ],
    );
}

#[test]
fn runtime_epoch_identity_tracks_accepted_snapshot_changes() {
    let before = base_snapshot();
    let repeated_before = base_snapshot();
    let added = nullable_text_field("nickname", 3, 2);
    let after = append_fields_snapshot(&before, &[added]);

    let before_epoch = super::SchemaMutationRuntimeEpoch::from_snapshot(&before)
        .expect("base snapshot should hash into runtime epoch");
    let repeated_epoch = super::SchemaMutationRuntimeEpoch::from_snapshot(&repeated_before)
        .expect("same snapshot should hash into runtime epoch");
    let after_epoch = super::SchemaMutationRuntimeEpoch::from_snapshot(&after)
        .expect("changed snapshot should hash into runtime epoch");

    assert_eq!(before_epoch, repeated_epoch);
    assert_ne!(before_epoch, after_epoch);
    assert_eq!(before_epoch.entity_path(), before.entity_path());
    assert_eq!(after_epoch.schema_version(), after.version());
    assert_ne!(
        before_epoch.snapshot_fingerprint(),
        after_epoch.snapshot_fingerprint(),
    );
}

#[test]
fn publication_identity_keeps_staged_epoch_invisible_until_published() {
    let before = base_snapshot();
    let added = nullable_text_field("nickname", 3, 2);
    let after = append_fields_snapshot(&before, std::slice::from_ref(&added));
    let input = super::SchemaMutationRunnerInput::new(
        &before,
        &after,
        MutationPlan::append_only_fields(&[added]).execution_plan(),
    )
    .expect("same-entity metadata input should build");

    let staged = super::SchemaMutationPublicationIdentity::from_input(
        &input,
        super::SchemaMutationStoreVisibility::StagedOnly,
    )
    .expect("staged publication identity should derive from snapshots");
    let published = super::SchemaMutationPublicationIdentity::from_input(
        &input,
        super::SchemaMutationStoreVisibility::Published,
    )
    .expect("published publication identity should derive from snapshots");

    assert!(staged.changes_epoch());
    assert_eq!(
        staged.store_visibility(),
        super::SchemaMutationStoreVisibility::StagedOnly,
    );
    assert_eq!(staged.visible_epoch(), staged.before_epoch());
    assert_eq!(staged.published_epoch(), None);
    assert_eq!(
        published.store_visibility(),
        super::SchemaMutationStoreVisibility::Published,
    );
    assert_eq!(published.visible_epoch(), published.after_epoch());
    assert_eq!(published.published_epoch(), Some(published.after_epoch()));
}

#[test]
fn execution_plan_keeps_full_rewrite_and_unsupported_non_executable() {
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();
    let rewrite_execution = incompatible.execution_plan();

    assert_eq!(
        rewrite_execution.readiness(),
        super::SchemaMutationExecutionReadiness::Unsupported(
            RebuildRequirement::FullDataRewriteRequired,
        ),
    );
    assert_eq!(
        rewrite_execution.execution_gate(),
        super::SchemaMutationExecutionGate::Rejected {
            requirement: RebuildRequirement::FullDataRewriteRequired,
        },
    );
    assert_eq!(
        rewrite_execution.steps(),
        &[super::SchemaMutationExecutionStep::RewriteAllRows],
    );
    assert_eq!(
        rewrite_execution.runner_capabilities(),
        vec![super::SchemaMutationRunnerCapability::RewriteAllRows],
    );
    assert_eq!(
        rewrite_execution
            .admit_runner_capabilities(&[super::SchemaMutationRunnerCapability::RewriteAllRows,]),
        super::SchemaMutationExecutionAdmission::Rejected {
            requirement: RebuildRequirement::FullDataRewriteRequired,
        },
    );

    let nullability = SchemaMutationRequest::AlterNullability {
        field_id: FieldId::new(2),
    }
    .lower_to_plan();
    let unsupported_execution = nullability.execution_plan();

    assert_eq!(
        unsupported_execution.readiness(),
        super::SchemaMutationExecutionReadiness::Unsupported(RebuildRequirement::Unsupported),
    );
    assert_eq!(
        unsupported_execution.execution_gate(),
        super::SchemaMutationExecutionGate::Rejected {
            requirement: RebuildRequirement::Unsupported,
        },
    );
    assert_eq!(
        unsupported_execution.steps(),
        &[super::SchemaMutationExecutionStep::Unsupported {
            reason: "alter nullability requires data proof or rewrite",
        }],
    );
    assert!(unsupported_execution.runner_capabilities().is_empty());
}

#[test]
fn field_path_index_request_lowering_fails_closed_for_unsupported_indexes() {
    let unique = PersistedIndexSnapshot::new(
        1,
        "unique_name".to_string(),
        "test::mutation::unique_name".to_string(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        None,
    );
    let explicit_items = PersistedIndexSnapshot::new(
        2,
        "items_name".to_string(),
        "test::mutation::items_name".to_string(),
        false,
        PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::FieldPath(
            name_key_path(),
        )]),
        None,
    );
    let empty = PersistedIndexSnapshot::new(
        3,
        "empty_name".to_string(),
        "test::mutation::empty_name".to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(Vec::new()),
        None,
    );

    assert_eq!(
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&unique),
        Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&explicit_items),
        Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&empty),
        Err(AcceptedSchemaMutationError::EmptyIndexKey),
    );
}

#[test]
fn expression_index_request_lowering_fails_closed_for_unsupported_indexes() {
    let unique = PersistedIndexSnapshot::new(
        1,
        "unique_lower_name".to_string(),
        "test::mutation::unique_lower_name".to_string(),
        true,
        expression_name_index().key().clone(),
        None,
    );
    let field_path_only = non_unique_name_index();
    let items_without_expression = PersistedIndexSnapshot::new(
        2,
        "items_name".to_string(),
        "test::mutation::items_name".to_string(),
        false,
        PersistedIndexKeySnapshot::Items(vec![PersistedIndexKeyItemSnapshot::FieldPath(
            name_key_path(),
        )]),
        None,
    );
    let empty = PersistedIndexSnapshot::new(
        3,
        "empty_expression".to_string(),
        "test::mutation::empty_expression".to_string(),
        false,
        PersistedIndexKeySnapshot::Items(Vec::new()),
        None,
    );

    assert_eq!(
        SchemaMutationRequest::from_accepted_expression_index(&unique),
        Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_expression_index(&field_path_only),
        Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_expression_index(&items_without_expression),
        Err(AcceptedSchemaMutationError::ExpressionIndexRequiresExpressionKey),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_expression_index(&empty),
        Err(AcceptedSchemaMutationError::EmptyIndexKey),
    );
}

#[test]
fn secondary_index_drop_request_lowering_fails_closed_for_unique_indexes() {
    let unique = PersistedIndexSnapshot::new(
        1,
        "unique_name".to_string(),
        "test::mutation::unique_name".to_string(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        None,
    );

    assert_eq!(
        SchemaMutationRequest::from_accepted_non_unique_secondary_index_drop(&unique),
        Err(AcceptedSchemaMutationError::UniqueIndexRequiresDedicatedValidation),
    );
}

#[test]
fn rebuild_plan_keeps_unsupported_and_full_rewrite_shapes_explicit() {
    let nullability = SchemaMutationRequest::AlterNullability {
        field_id: FieldId::new(2),
    }
    .lower_to_plan();
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

    assert_eq!(
        nullability.rebuild_plan().actions(),
        &[SchemaRebuildAction::Unsupported {
            reason: "alter nullability requires data proof or rewrite",
        }],
    );
    assert_eq!(
        incompatible.rebuild_plan().actions(),
        &[SchemaRebuildAction::RewriteAllRows],
    );
}

#[test]
fn unsupported_mutation_plans_fail_closed() {
    let alteration = SchemaMutationRequest::AlterNullability {
        field_id: FieldId::new(2),
    }
    .lower_to_plan();
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

    assert_eq!(
        alteration.compatibility(),
        MutationCompatibility::UnsupportedPreOne
    );
    assert_eq!(
        alteration.rebuild_requirement(),
        RebuildRequirement::Unsupported
    );
    assert_eq!(
        alteration.publication_status(),
        MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
            MutationCompatibility::UnsupportedPreOne,
        )),
    );
    assert_eq!(
        incompatible.compatibility(),
        MutationCompatibility::Incompatible
    );
    assert_eq!(
        incompatible.rebuild_requirement(),
        RebuildRequirement::FullDataRewriteRequired
    );
}

#[test]
fn publication_gate_allows_only_metadata_safe_no_rebuild_plans() {
    let field = nullable_text_field("nickname", 3, 2);
    let append_only = MutationPlan::append_only_fields(&[field]);
    let metadata_safe_but_rebuild_required = MutationPlan {
        mutations: Vec::new(),
        compatibility: MutationCompatibility::MetadataOnlySafe,
        rebuild: RebuildRequirement::IndexRebuildRequired,
    };
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

    assert_eq!(
        append_only.publication_status(),
        MutationPublicationStatus::Publishable
    );
    assert_eq!(
        metadata_safe_but_rebuild_required.publication_status(),
        MutationPublicationStatus::Blocked(MutationPublicationBlocker::RebuildRequired(
            RebuildRequirement::IndexRebuildRequired,
        )),
    );
    assert_eq!(
        incompatible.publication_status(),
        MutationPublicationStatus::Blocked(MutationPublicationBlocker::NotMetadataSafe(
            MutationCompatibility::Incompatible,
        )),
    );
}

#[test]
fn publication_preflight_requires_runner_readiness_before_physical_work() {
    let field = nullable_text_field("nickname", 3, 2);
    let append_only = MutationPlan::append_only_fields(&[field]);
    let field_path =
        SchemaMutationRequest::from_accepted_non_unique_field_path_index(&non_unique_name_index())
            .expect("non-unique field-path index should lower")
            .lower_to_plan();
    let no_runner = super::SchemaMutationRunnerContract::new(&[]);
    let index_runner = super::SchemaMutationRunnerContract::new(&[
        super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
        super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
        super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
    ]);
    let incompatible = SchemaMutationRequest::Incompatible.lower_to_plan();

    assert_eq!(
        append_only.publication_preflight(&no_runner),
        super::MutationPublicationPreflight::PublishableNow,
    );
    assert_eq!(
        field_path.publication_preflight(&no_runner),
        super::MutationPublicationPreflight::MissingRunnerCapabilities {
            missing: vec![
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        },
    );
    assert_eq!(
        field_path.publication_preflight(&index_runner),
        super::MutationPublicationPreflight::PhysicalWorkReady {
            step_count: 3,
            required: vec![
                super::SchemaMutationRunnerCapability::BuildFieldPathIndex,
                super::SchemaMutationRunnerCapability::ValidatePhysicalWork,
                super::SchemaMutationRunnerCapability::InvalidateRuntimeState,
            ],
        },
    );
    assert_eq!(
        incompatible.publication_preflight(&index_runner),
        super::MutationPublicationPreflight::Rejected {
            requirement: RebuildRequirement::FullDataRewriteRequired,
        },
    );
}

#[test]
fn snapshot_delta_classifier_names_append_only_fields() {
    let stored = base_snapshot();
    let mut fields = stored.fields().to_vec();
    fields.push(nullable_text_field("nickname", 3, 2));
    let generated = PersistedSchemaSnapshot::new(
        stored.version(),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.primary_key_field_id(),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ],
        ),
        fields,
    );

    let SchemaMutationDelta::AppendOnlyFields(added_fields) =
        classify_schema_mutation_delta(&stored, &generated)
    else {
        panic!("append-only snapshot change should classify as appended fields");
    };

    assert_eq!(added_fields.len(), 1);
    assert_eq!(added_fields[0].name(), "nickname");
}

#[test]
fn snapshot_delta_request_lowers_append_only_fields_to_mutation_plan() {
    let stored = base_snapshot();
    let mut fields = stored.fields().to_vec();
    fields.push(nullable_text_field("nickname", 3, 2));
    let generated = PersistedSchemaSnapshot::new(
        stored.version(),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.primary_key_field_id(),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
                (FieldId::new(3), SchemaFieldSlot::new(2)),
            ],
        ),
        fields,
    );

    let SchemaMutationRequest::AppendOnlyFields(added_fields) =
        schema_mutation_request_for_snapshots(&stored, &generated)
    else {
        panic!("append-only snapshot change should lower into append-only request");
    };

    let plan = SchemaMutationRequest::AppendOnlyFields(added_fields).lower_to_plan();
    assert_eq!(plan.added_field_count(), 1);
    assert_eq!(
        plan.publication_status(),
        MutationPublicationStatus::Publishable
    );
}

#[test]
fn snapshot_delta_classifier_rejects_non_prefix_field_changes() {
    let stored = base_snapshot();
    let mut generated_fields = stored.fields().to_vec();
    generated_fields[1] = nullable_text_field("renamed", 2, 1);
    let generated = PersistedSchemaSnapshot::new(
        stored.version(),
        stored.entity_path().to_string(),
        stored.entity_name().to_string(),
        stored.primary_key_field_id(),
        stored.row_layout().clone(),
        generated_fields,
    );

    assert_eq!(
        classify_schema_mutation_delta(&stored, &generated),
        SchemaMutationDelta::Incompatible
    );
}
