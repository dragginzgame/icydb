//! Fail-closed admission of the explicit entity-rename companion.

use super::*;
use icydb_diagnostic_code::{DiagnosticDetail, SchemaMigrationCode};

fn recompose(
    base: &SchemaProposal,
    entities: Vec<EntityFragment>,
    assignments: Vec<EntityStoreAssignment>,
    transitions: Vec<EntityMigration>,
) -> SchemaProposal {
    SchemaProposal::try_compose(
        vec![
            SchemaCapability::RESTRICTIVE_RELATIONS,
            SchemaCapability::VERSIONED_MIGRATIONS,
        ],
        base.target_database(),
        base.submission_key().clone(),
        base.expected_head().clone(),
        vec![SchemaFragment::try_new(entities, Vec::new()).unwrap()],
        assignments,
        Vec::new(),
        Some(SchemaMigrationPlan::try_new(transitions).unwrap()),
    )
    .unwrap()
}

fn assert_rejected(
    db: &Db<MigrationExecutionCanister>,
    proposal: &SchemaProposal,
    reason: SchemaMigrationCode,
) {
    let head = schema_application_target(db).unwrap();
    let state = physical_state(db);
    let lineage = load_entity_source_lineage_catalog().unwrap();
    let error = advance(db, proposal).expect_err("invalid rename must reject before publication");
    assert_eq!(
        error.diagnostic().detail(),
        Some(&DiagnosticDetail::SchemaMigration { reason })
    );
    assert_eq!(schema_application_target(db).unwrap(), head);
    assert_eq!(load_entity_source_lineage_catalog().unwrap(), lineage);
    assert_eq!(physical_state(db), state);
}

#[test]
fn companion_requires_complete_rename_explanation_and_explicit_transition() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root, true);
    let candidate = proposal(&schema_application_target(&db).unwrap(), true, true);
    let transitions = candidate.migration().unwrap().transitions().to_vec();
    let entities = candidate.fragments()[0].entities().to_vec();
    let assignments = candidate.assignments().to_vec();
    let missing = recompose(
        &candidate,
        entities.clone(),
        assignments.clone(),
        transitions
            .iter()
            .filter(|t| t.entity() != &entity("Holder"))
            .cloned()
            .collect(),
    );
    assert_rejected(&db, &missing, SchemaMigrationCode::MissingMigration);

    let changed = entities
        .iter()
        .map(|definition| {
            if definition.source_key() != &entity("Holder") {
                return definition.clone();
            }
            let mut fields = definition.fields().to_vec();
            fields.push(scalar_field("extra", ScalarType::Nat64, true));
            EntityFragment::try_new(
                definition.name().clone(),
                definition.version(),
                fields,
                definition.primary_key().to_vec(),
                definition.indexes().to_vec(),
                definition.relations().to_vec(),
                definition.constraints().to_vec(),
            )
            .unwrap()
        })
        .collect();
    let unexplained = recompose(
        &candidate,
        changed,
        assignments.clone(),
        transitions.clone(),
    );
    assert_rejected(
        &db,
        &unexplained,
        SchemaMigrationCode::UnexplainedSchemaDifference,
    );

    let bad_predecessor = transitions
        .iter()
        .map(|transition| {
            if transition.entity() == &entity("Holder") {
                return transition.clone();
            }
            EntityMigration::try_new(
                transition.entity().clone(),
                version_one(),
                Some(entity("Absent")),
                Vec::new(),
                Vec::new(),
            )
            .unwrap()
        })
        .collect();
    let missing = recompose(&candidate, entities.clone(), assignments, bad_predecessor);
    assert_rejected(&db, &missing, SchemaMigrationCode::UnknownFromObject);

    let wrong_store = candidate
        .assignments()
        .iter()
        .map(|assignment| {
            EntityStoreAssignment::new(
                assignment.entity().clone(),
                TargetStoreIdentity::from_bytes([0x99; 32]),
            )
        })
        .collect();
    let misplaced = recompose(&candidate, entities, wrong_store, transitions);
    assert_rejected(&db, &misplaced, SchemaMigrationCode::KindMismatch);
}

#[test]
fn dependency_only_declaration_cannot_authorize_meaningless_version_bump() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root, true);
    let base = proposal(&schema_application_target(&db).unwrap(), false, true);
    let entities = base.fragments()[0]
        .entities()
        .iter()
        .map(|definition| {
            EntityFragment::try_new(
                definition.name().clone(),
                DeclaredEntityVersion::try_new(if definition.source_key() == &entity("Holder") {
                    2
                } else {
                    1
                })
                .unwrap(),
                definition.fields().to_vec(),
                definition.primary_key().to_vec(),
                definition.indexes().to_vec(),
                definition.relations().to_vec(),
                definition.constraints().to_vec(),
            )
            .unwrap()
        })
        .collect();
    let candidate = recompose(
        &base,
        entities,
        base.assignments().to_vec(),
        vec![
            EntityMigration::try_new(
                entity("Holder"),
                version_one(),
                None,
                Vec::new(),
                Vec::new(),
            )
            .unwrap(),
        ],
    );
    assert_rejected(&db, &candidate, SchemaMigrationCode::EmptyEntityVersionBump);
}
