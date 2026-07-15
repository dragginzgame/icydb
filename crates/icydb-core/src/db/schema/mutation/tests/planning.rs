use super::*;

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
fn runner_input_binds_accepted_snapshots_to_the_same_mutation_plan() {
    let before = base_snapshot();
    let added = nullable_text_field("nickname", 3, 2);
    let after = append_fields_snapshot(&before, std::slice::from_ref(&added));
    let plan: MutationPlan =
        SchemaMutationRequest::AppendOnlyFields(std::slice::from_ref(&added)).into();
    let input = super::SchemaMutationRunnerInput::new(&before, &after, plan.clone())
        .expect("same-entity accepted snapshots should build runner input");

    assert_eq!(
        input.accepted_after().fields().len(),
        before.fields().len() + 1,
    );
    assert_eq!(input.mutation_plan(), &plan);
}

#[test]
fn runner_input_rejects_cross_entity_snapshot_pairs() {
    let before = base_snapshot();
    let wrong_entity = PersistedSchemaSnapshot::new(
        before.version(),
        "test::OtherEntity".to_string(),
        before.entity_name().to_string(),
        before.first_primary_key_field_id(),
        before.row_layout().clone(),
        before.fields().to_vec(),
    );
    let wrong_name = PersistedSchemaSnapshot::new(
        before.version(),
        before.entity_path().to_string(),
        "OtherEntity".to_string(),
        before.first_primary_key_field_id(),
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
        super::SchemaMutationRunnerInput::new(&before, &wrong_entity, MutationPlan::exact_match(),),
        Err(super::SchemaMutationRunnerInputError::EntityPath),
    );
    assert_eq!(
        super::SchemaMutationRunnerInput::new(&before, &wrong_name, MutationPlan::exact_match(),),
        Err(super::SchemaMutationRunnerInputError::EntityName),
    );
    assert_eq!(
        super::SchemaMutationRunnerInput::new(&before, &wrong_pk, MutationPlan::exact_match()),
        Err(super::SchemaMutationRunnerInputError::PrimaryKeyField),
    );
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
