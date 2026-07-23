use super::*;

#[test]
fn sql_owned_check_activation_is_not_relowered_as_generated_authority() {
    let generated = base_snapshot();
    let catalog = generated
        .constraint_catalog()
        .clone()
        .with_added_check_activation(
            "ddl_check".to_string(),
            ConstraintOrigin::SqlDdl,
            AcceptedCheckExprV1::True,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            2,
        )
        .expect("SQL-owned test activation should build");
    let accepted = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        SchemaVersion::new(2),
        generated.entity_path().to_string(),
        generated.entity_name().to_string(),
        generated.primary_key_field_ids().to_vec(),
        generated.row_layout().clone(),
        generated.fields().to_vec(),
        generated.indexes().to_vec(),
    )
    .with_constraint_catalog(catalog)
    .with_relations(generated.relations().to_vec());

    assert_eq!(
        derive_generated_accepted_candidate(
            &accepted,
            &generated,
            Some(GeneratedConstraintActivationContext::new(
                AcceptedSchemaFingerprint::new([0x5A; 32]),
                3,
            )),
        ),
        Ok(None),
        "SQL DDL activation must remain accepted-only authority",
    );
}

#[test]
fn metadata_mutation_plan_is_immediately_publishable() {
    let field = nullable_text_field("nickname", 3, 2);
    let fields = [field];
    let plan: MutationPlan = SchemaMutationRequest::AppendOnlyFields(&fields).into();

    assert_eq!(
        plan.publication_preflight(),
        super::MutationPublicationPreflight::PublishableNow,
    );
}

#[test]
fn index_mutation_plans_preserve_the_current_physical_target() {
    let field_path: MutationPlan =
        SchemaMutationRequest::from_accepted_field_path_index(&non_unique_name_index())
            .expect("field-path index should lower")
            .into();
    let expression: MutationPlan =
        SchemaMutationRequest::from_accepted_expression_index(&expression_name_index())
            .expect("expression index should lower")
            .into();

    assert_eq!(
        field_path.publication_preflight(),
        super::MutationPublicationPreflight::RequiresPhysicalWork,
    );
    let field_path_target = field_path
        .field_path_index_target()
        .expect("field-path plan should expose its runner target");
    assert_eq!(field_path_target.name(), "by_name");
    assert_eq!(field_path_target.store(), "test::mutation::by_name");
    assert_eq!(field_path_target.predicate_sql(), Some("name IS NOT NULL"));
    let [field_path_key] = field_path_target.key_paths() else {
        panic!("test field-path index should retain one accepted key");
    };
    assert_eq!(field_path_key.field_id(), FieldId::new(2));
    assert_eq!(
        field_path_key.kind(),
        &AcceptedFieldKind::Text { max_len: None },
    );
    assert!(!field_path_key.nullable());
    assert!(field_path.expression_index_target().is_none());

    assert_eq!(
        expression.publication_preflight(),
        super::MutationPublicationPreflight::RequiresPhysicalWork,
    );
    let expression_target = expression
        .expression_index_target()
        .expect("expression plan should expose its runner target");
    assert_eq!(expression_target.name(), "by_lower_name");
    assert_eq!(expression_target.store(), "test::mutation::by_lower_name");
    assert_eq!(
        expression_target.predicate_sql(),
        Some("LOWER(name) IS NOT NULL"),
    );
    let [super::SchemaExpressionIndexRebuildKey::Expression(expression_key)] =
        expression_target.key_items()
    else {
        panic!("test expression index should retain one expression key");
    };
    assert_eq!(
        expression_key.input_kind(),
        &AcceptedFieldKind::Text { max_len: None },
    );
    assert_eq!(
        expression_key.output_kind(),
        &AcceptedFieldKind::Text { max_len: None },
    );
    assert_eq!(expression_key.canonical_text(), "expr:v1:LOWER(name)");
    assert!(expression.field_path_index_target().is_none());
}

#[test]
fn field_path_index_request_lowering_fails_closed_for_unsupported_indexes() {
    let unique = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).expect("test index identity should be non-zero"),
        1,
        "unique_name".to_string(),
        "test::mutation::unique_name".to_string(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        None,
    );
    let explicit_items = PersistedIndexSnapshot::new(
        SchemaIndexId::new(2).expect("test index identity should be non-zero"),
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
        SchemaIndexId::new(3).expect("test index identity should be non-zero"),
        3,
        "empty_name".to_string(),
        "test::mutation::empty_name".to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(Vec::new()),
        None,
    );

    let SchemaMutationRequest::AddFieldPathIndex { target } =
        SchemaMutationRequest::from_accepted_field_path_index(&unique)
            .expect("unique field-path indexes should lower")
    else {
        panic!("unique field-path index should preserve its target");
    };
    assert!(target.unique());
    assert_eq!(
        SchemaMutationRequest::from_accepted_field_path_index(&explicit_items),
        Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape),
    );
    assert_eq!(
        SchemaMutationRequest::from_accepted_field_path_index(&empty),
        Err(AcceptedSchemaMutationError::EmptyIndexKey),
    );
}

#[test]
fn expression_index_request_lowering_fails_closed_for_unsupported_indexes() {
    let field_path_only = non_unique_name_index();
    let items_without_expression = PersistedIndexSnapshot::new(
        SchemaIndexId::new(2).expect("test index identity should be non-zero"),
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
        SchemaIndexId::new(3).expect("test index identity should be non-zero"),
        3,
        "empty_expression".to_string(),
        "test::mutation::empty_expression".to_string(),
        false,
        PersistedIndexKeySnapshot::Items(Vec::new()),
        None,
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
fn snapshot_delta_request_lowers_only_current_plan_shapes() {
    let stored = base_snapshot();
    let added = nullable_text_field("nickname", 3, 2);
    let generated = append_fields_snapshot(&stored, std::slice::from_ref(&added));

    let Some(SchemaMutationRequest::AppendOnlyFields(added_fields)) =
        schema_mutation_request_for_snapshots(&stored, &generated)
    else {
        panic!("append-only delta should lower into a request");
    };
    let plan: MutationPlan = SchemaMutationRequest::AppendOnlyFields(added_fields).into();
    assert_eq!(
        plan.publication_preflight(),
        super::MutationPublicationPreflight::PublishableNow,
    );

    let with_index = snapshot_with_indexes(&stored, vec![non_unique_name_index()]);
    let Some(SchemaMutationRequest::AddFieldPathIndex { target }) =
        schema_mutation_request_for_snapshots(&stored, &with_index)
    else {
        panic!("single field-path index delta should lower into a request");
    };
    assert_eq!(target.name(), "by_name");

    let multiple = snapshot_with_indexes(
        &stored,
        vec![non_unique_name_index(), expression_name_index()],
    );
    assert_eq!(
        classify_schema_mutation_delta(&stored, &multiple),
        SchemaMutationDelta::Incompatible,
    );
    assert_eq!(
        schema_mutation_request_for_snapshots(&stored, &multiple),
        None
    );

    let indexed_stored = snapshot_with_indexes(&stored, vec![non_unique_name_index()]);
    let preserved_append = append_fields_snapshot(&indexed_stored, std::slice::from_ref(&added));
    let append_that_drops_index = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        preserved_append.version(),
        preserved_append.entity_path().to_string(),
        preserved_append.entity_name().to_string(),
        preserved_append.primary_key_field_ids().to_vec(),
        preserved_append.row_layout().clone(),
        preserved_append.fields().to_vec(),
        Vec::new(),
    )
    .with_constraint_catalog(preserved_append.constraint_catalog().clone())
    .with_relations(preserved_append.relations().to_vec());
    assert_eq!(
        classify_schema_mutation_delta(&indexed_stored, &append_that_drops_index),
        SchemaMutationDelta::Incompatible,
        "append-only classification must keep unrelated accepted indexes exact",
    );

    let allocator_drift = snapshot_with_indexes(
        &stored
            .clone_with_version(stored.version())
            .with_constraint_catalog(AcceptedConstraintCatalog::from_persisted_parts(
                ConstraintIdAllocator::new(
                    stored
                        .constraint_id_allocator()
                        .high_water()
                        .saturating_add(1),
                ),
                stored.constraints().to_vec(),
                Vec::new(),
            )),
        vec![non_unique_name_index()],
    );
    assert_eq!(
        classify_schema_mutation_delta(&stored, &allocator_drift),
        SchemaMutationDelta::Incompatible,
        "index-addition classification must keep constraint allocator state exact",
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
        stored.first_primary_key_field_id(),
        stored.row_layout().clone(),
        generated_fields,
    );

    assert_eq!(
        classify_schema_mutation_delta(&stored, &generated),
        SchemaMutationDelta::Incompatible,
    );
    assert_eq!(
        schema_mutation_request_for_snapshots(&stored, &generated),
        None
    );
}

#[test]
#[cfg(feature = "sql")]
fn physical_field_changes_preserve_row_layout_exhaustion_causes() {
    let base = base_snapshot();
    let max_layout_snapshot = PersistedSchemaSnapshot::new(
        base.version(),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new(
            crate::db::schema::RowLayoutVersion::new(u32::MAX)
                .expect("maximum layout version should be valid"),
            crate::db::schema::RowLayoutVersion::INITIAL,
            base.row_layout().field_to_slot().to_vec(),
        ),
        base.fields().to_vec(),
    );
    let accepted = crate::db::schema::AcceptedSchemaSnapshot::try_new(max_layout_snapshot.clone())
        .expect("maximum-version snapshot should remain internally valid");

    assert_eq!(
        derive_sql_ddl_field_drop_accepted_after(&accepted, "name"),
        Err(SchemaDdlMutationAdmissionError::RowLayoutVersionExhausted),
        "DROP COLUMN must not collapse layout exhaustion into a missing runner",
    );

    let added = nullable_text_field("nickname", 3, 2);
    let generated = PersistedSchemaSnapshot::new(
        SchemaVersion::new(2),
        base.entity_path().to_string(),
        base.entity_name().to_string(),
        base.primary_key_field_ids().to_vec(),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
            (FieldId::new(3), SchemaFieldSlot::new(2)),
        ]),
        [base.fields(), std::slice::from_ref(&added)].concat(),
    );
    assert_eq!(
        derive_generated_accepted_candidate(&max_layout_snapshot, &generated, None),
        Err(GeneratedAcceptedCandidateError::RowLayoutVersionExhausted),
        "generated additive reconciliation must preserve the same typed cause",
    );
}

#[test]
#[cfg(feature = "sql")]
fn metadata_default_changes_remain_available_at_row_layout_exhaustion() {
    let base = base_snapshot();
    let maximum_layout = crate::db::schema::RowLayoutVersion::new(u32::MAX)
        .expect("maximum layout version should be valid");
    let ddl_field = PersistedFieldSnapshot::new_initial_with_write_policy_and_origin(
        FieldId::new(3),
        "nickname".to_string(),
        SchemaFieldSlot::new(2),
        AcceptedFieldKind::Text { max_len: None },
        Vec::new(),
        true,
        SchemaInsertDefault::None,
        crate::db::schema::SchemaFieldWritePolicy::none(),
        crate::db::schema::PersistedFieldOrigin::SqlDdl,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Text),
    );
    let accepted =
        crate::db::schema::AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.primary_key_field_ids().to_vec(),
            SchemaRowLayout::new(
                maximum_layout,
                crate::db::schema::RowLayoutVersion::INITIAL,
                [
                    base.row_layout().field_to_slot(),
                    &[(FieldId::new(3), SchemaFieldSlot::new(2))],
                ]
                .concat(),
            ),
            [base.fields(), std::slice::from_ref(&ddl_field)].concat(),
        ))
        .expect("maximum-version snapshot should remain internally valid");

    let set_default = derive_sql_ddl_field_default_accepted_after(
        &accepted,
        "nickname",
        SchemaInsertDefault::SlotPayload(vec![0xFF, 0x01, b'A', b'd', b'a']),
    )
    .expect("metadata-only SET DEFAULT must not allocate a row layout version");
    assert_eq!(
        set_default
            .accepted_after()
            .persisted_snapshot()
            .row_layout()
            .current_version(),
        maximum_layout,
    );

    let drop_default = derive_sql_ddl_field_default_accepted_after(
        set_default.accepted_after(),
        "nickname",
        SchemaInsertDefault::None,
    )
    .expect("metadata-only DROP DEFAULT must not allocate a row layout version");
    assert_eq!(
        drop_default
            .accepted_after()
            .persisted_snapshot()
            .row_layout()
            .current_version(),
        maximum_layout,
    );
}

#[test]
#[cfg(feature = "sql")]
fn accepted_after_derivations_preserve_structural_identity_state() {
    let before = base_snapshot().with_relations(vec![PersistedRelationEdgeSnapshot::new(
        RelationId::new(9).expect("test relation identity should be non-zero"),
        "owner".to_string(),
        "test::Owner".to_string(),
        vec![FieldId::new(2)],
    )]);
    let initial_catalog =
        AcceptedConstraintCatalog::initial(before.fields(), before.indexes(), before.relations())
            .expect("test structural constraints should build");
    let before = before.with_constraint_catalog(AcceptedConstraintCatalog::from_persisted_parts(
        ConstraintIdAllocator::new(17),
        initial_catalog.constraints().to_vec(),
        Vec::new(),
    ));
    let accepted = crate::db::schema::AcceptedSchemaSnapshot::try_new(before)
        .expect("test accepted schema should be internally valid");

    let renamed = derive_sql_ddl_field_rename_accepted_after(&accepted, "name", "display_name")
        .expect("field rename should derive an accepted-after snapshot");
    let renamed_snapshot = renamed.accepted_after().persisted_snapshot();
    assert_eq!(renamed_snapshot.constraint_id_allocator().high_water(), 17);
    assert_eq!(renamed_snapshot.relations()[0].id().get(), 9);
    assert_eq!(
        renamed_snapshot
            .constraints()
            .iter()
            .find(|constraint| {
                matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::NotNull { field_id }
                        if *field_id == FieldId::new(2)
                )
            })
            .expect("renamed field should retain its not-null constraint")
            .name(),
        "__icydb_not_null_2",
        "field rename must not silently rename accepted constraint identity",
    );

    let indexed =
        derive_sql_ddl_field_path_index_accepted_after(&accepted, non_unique_name_index())
            .expect("index addition should derive an accepted-after snapshot");
    let indexed_snapshot = indexed.accepted_after().persisted_snapshot();
    assert_eq!(indexed_snapshot.constraint_id_allocator().high_water(), 17);
    assert_eq!(indexed_snapshot.relations()[0].id().get(), 9);
}

#[test]
#[cfg(feature = "sql")]
fn structural_constraint_mutations_allocate_once_and_never_reuse_ids() {
    let before = base_snapshot();
    let initial_high_water = before.constraint_id_allocator().high_water();
    let accepted = crate::db::schema::AcceptedSchemaSnapshot::try_new(before)
        .expect("test accepted schema should be internally valid");
    let unique = PersistedIndexSnapshot::new_sql_ddl(
        SchemaIndexId::new(1).expect("test index identity should be non-zero"),
        1,
        "unique_name".to_string(),
        "test::mutation::unique_name".to_string(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        None,
    );

    let indexed = derive_sql_ddl_field_path_index_accepted_after(&accepted, unique.clone())
        .expect("unique index addition should derive its paired constraint");
    let indexed_snapshot = indexed.accepted_after().persisted_snapshot();
    assert_eq!(
        indexed_snapshot.constraint_id_allocator().high_water(),
        initial_high_water + 1,
    );
    let unique_constraint = indexed_snapshot
        .constraints()
        .iter()
        .find(|constraint| {
            matches!(
                constraint.kind(),
                AcceptedConstraintKind::Unique { index_id } if *index_id == unique.schema_id()
            )
        })
        .expect("unique accepted index should have one paired constraint");
    assert_eq!(unique_constraint.name(), unique.name());
    assert_eq!(unique_constraint.origin(), ConstraintOrigin::SqlDdl);
    let retired_id = unique_constraint.id();

    let dropped =
        derive_sql_ddl_secondary_index_drop_accepted_after(indexed.accepted_after(), &unique)
            .expect("unique index drop should remove its paired constraint");
    let dropped_snapshot = dropped.accepted_after().persisted_snapshot();
    assert_eq!(
        dropped_snapshot.constraint_id_allocator().high_water(),
        retired_id.get(),
        "drop must retire rather than compact the stable constraint identity",
    );
    assert!(dropped_snapshot.constraints().iter().all(|constraint| {
        !matches!(
            constraint.kind(),
            AcceptedConstraintKind::Unique { index_id } if *index_id == unique.schema_id()
        )
    }));

    let recreated =
        derive_sql_ddl_field_path_index_accepted_after(dropped.accepted_after(), unique)
            .expect("recreated unique index should allocate fresh constraint identity");
    let recreated_constraint = recreated
        .accepted_after()
        .persisted_snapshot()
        .constraints()
        .iter()
        .find(|constraint| matches!(constraint.kind(), AcceptedConstraintKind::Unique { .. }))
        .expect("recreated unique index should have one paired constraint");
    assert!(recreated_constraint.id().get() > retired_id.get());
}

#[test]
#[cfg(feature = "sql")]
fn nullability_mutations_add_and_retire_one_stable_constraint() {
    let before = base_snapshot();
    let initial_high_water = before.constraint_id_allocator().high_water();
    let accepted = crate::db::schema::AcceptedSchemaSnapshot::try_new(before)
        .expect("test accepted schema should be internally valid");

    let nullable = derive_sql_ddl_field_nullability_accepted_after(&accepted, "name", true)
        .expect("dropping not-null should remove the paired constraint");
    let nullable_snapshot = nullable.accepted_after().persisted_snapshot();
    assert_eq!(
        nullable_snapshot.constraint_id_allocator().high_water(),
        initial_high_water,
    );
    assert!(nullable_snapshot.constraints().iter().all(|constraint| {
        !matches!(
            constraint.kind(),
            AcceptedConstraintKind::NotNull { field_id } if *field_id == FieldId::new(2)
        )
    }));

    let required =
        derive_sql_ddl_field_nullability_accepted_after(nullable.accepted_after(), "name", false)
            .expect("restoring not-null should allocate a fresh paired constraint");
    let required_snapshot = required.accepted_after().persisted_snapshot();
    assert_eq!(
        required_snapshot.constraint_id_allocator().high_water(),
        initial_high_water + 1,
    );
    assert_eq!(
        required_snapshot
            .constraints()
            .iter()
            .filter(|constraint| {
                matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::NotNull { field_id }
                        if *field_id == FieldId::new(2)
                )
            })
            .count(),
        1,
    );
}

#[test]
#[cfg(feature = "sql")]
fn sql_ddl_index_identity_exhaustion_is_typed_and_fail_closed() {
    let exhausted_index = PersistedIndexSnapshot::new(
        SchemaIndexId::new(u32::MAX).expect("maximum logical index identity is non-zero"),
        1,
        "exhausted".to_string(),
        "test::mutation::exhausted".to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        None,
    );
    let before = snapshot_with_indexes(&base_snapshot(), vec![exhausted_index]);
    let accepted = crate::db::schema::AcceptedSchemaSnapshot::try_new(before)
        .expect("maximum non-zero logical index identity should decode as current authority");
    let key = SchemaDdlSecondaryIndexKeyIntent::FieldPath(
        SchemaDdlSecondaryIndexFieldPathIntent::new("name".to_string(), Vec::new()),
    );

    let error = build_sql_ddl_secondary_index_candidate(
        &accepted,
        "next_index".to_string(),
        "test::mutation::next_index".to_string(),
        false,
        &[key],
        None,
    )
    .expect_err("logical index identity exhaustion must reject instead of panicking");

    assert_eq!(
        error,
        SchemaDdlSecondaryIndexKeyCandidateError::IndexIdentityExhausted,
    );
}

#[test]
#[cfg(feature = "sql")]
fn secondary_index_drop_compacts_only_physical_ordinals() {
    let first = non_unique_name_index();
    let dropped = expression_name_index();
    let surviving = PersistedIndexSnapshot::new(
        SchemaIndexId::new(3).expect("test index identity should be non-zero"),
        3,
        "by_name_copy".to_string(),
        "test::mutation::by_name_copy".to_string(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        None,
    );
    let before = snapshot_with_indexes(&base_snapshot(), vec![first, dropped.clone(), surviving]);
    let accepted = crate::db::schema::AcceptedSchemaSnapshot::try_new(before)
        .expect("test accepted schema should be internally valid");

    let derivation = derive_sql_ddl_secondary_index_drop_accepted_after(&accepted, &dropped)
        .expect("middle index drop should derive dense physical ordinals");
    let indexes = derivation.accepted_after().persisted_snapshot().indexes();

    assert_eq!(
        indexes
            .iter()
            .map(|index| (index.schema_id().get(), index.ordinal()))
            .collect::<Vec<_>>(),
        vec![(1, 1), (3, 2)],
    );
}
