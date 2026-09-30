//! Mixed proposals publish migrated and fresh entities through one accepted head.

use super::*;
use crate::db::{
    commit::DatabaseControlOp,
    data::RawDataStoreKey,
    index::{IndexEntryValue, IndexId, IndexKey, IndexKeyKind},
    key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
    schema::{
        EntitySourceLineageCatalogOp, MigrationRewriteInterruption, SchemaMigrationCommand,
        SchemaMigrationPhase, SchemaMigrationRecordOp, SchemaMigrationStatusPage,
        application::{
            accepted_head_after_candidates, ensure_generated_schema_application_admitted,
            existing_proposal_stores, lineage_after_planned, load_current_application_bundles,
            load_entity_source_lineage_catalog, load_schema_migration_record, migrate_schema,
            migration_submission_key,
        },
        ensure_schema_migration_ready_for_ordinary_operations, interrupt_next_migration_rewrite_at,
        migration_lineage::AcceptedEntitySourceLineageState,
        migration_planner::plan_schema_migration,
        migration_record::PersistedSchemaMigrationPhase,
    },
};
use icydb_diagnostic_code::{DiagnosticDetail, SchemaMigrationCode};
use icydb_schema::{RecordFieldFragment, RecordTypeFragment, TypeSourceKey};

#[derive(Clone, Copy)]
enum Shape {
    Rename,
    Fill,
}

fn migrated_item(shape: Shape) -> EntityFragment {
    let mut fields = vec![scalar("id", ScalarType::Nat64)];
    let mut indexes = declaration("Item", 1).indexes().to_vec();
    let item_name = match shape {
        Shape::Rename => "Archive",
        Shape::Fill => {
            fields.push(scalar("coins", ScalarType::Nat64));
            indexes.push(
                IndexFragment::try_new(
                    name("coins_lookup"),
                    vec![IndexKeyFragment::Field(field("coins"))],
                    false,
                    None,
                )
                .unwrap(),
            );
            "Item"
        }
    };
    EntityFragment::try_new(
        name(item_name),
        DeclaredEntityVersion::try_new(2).unwrap(),
        fields,
        vec![field("id")],
        indexes,
        Vec::new(),
        Vec::new(),
    )
    .unwrap()
}

fn quest(shape: Shape, version: u32) -> EntityFragment {
    let base = declaration("Quest", version);
    let mut fields = base.fields().to_vec();
    fields.push(FieldFragment::new(
        name("details"),
        FieldType::Named(TypeSourceKey::try_new("QuestDetails").unwrap()),
        false,
        FieldInsertPolicy::Required,
        None,
    ));
    EntityFragment::try_new(
        name("Quest"),
        base.version(),
        fields,
        vec![field("id")],
        base.indexes().to_vec(),
        vec![
            RelationFragment::try_new(
                name("item"),
                RelationSourceFragment::direct(vec![field("item_id")]),
                entity(match shape {
                    Shape::Rename => "Archive",
                    Shape::Fill => "Item",
                }),
                vec![field("id")],
                RelationDeleteAction::Restrict,
            )
            .unwrap(),
        ],
        base.constraints().to_vec(),
    )
    .unwrap()
}

fn mixed_proposal(
    target: &SchemaApplicationTarget,
    shape: Shape,
    new_version: u32,
) -> SchemaProposal {
    let item = migrated_item(shape);
    let transition = EntityMigration::try_new(
        item.source_key().clone(),
        version_one(),
        matches!(shape, Shape::Rename).then(|| entity("Item")),
        Vec::new(),
        matches!(shape, Shape::Fill)
            .then(|| SchemaMigrationTransform::Fill {
                to: field("coins"),
                literal: ScalarLiteral::Nat(5),
            })
            .into_iter()
            .collect(),
    )
    .unwrap();
    let mut entities = vec![item, quest(shape, new_version)];
    if matches!(shape, Shape::Rename) {
        // The populated predecessor's name is reused for a fresh empty entity.
        entities.push(declaration("Item", 1));
    }
    let assignments = entities
        .iter()
        .map(|entity| {
            EntityStoreAssignment::new(entity.source_key().clone(), target.stores()[0].identity())
        })
        .collect();
    let details = RecordTypeFragment::try_new(
        name("QuestDetails"),
        vec![RecordFieldFragment::new(
            name("label"),
            FieldType::Scalar(ScalarType::Text { max_len: Some(64) }),
            false,
        )],
    )
    .unwrap();
    SchemaProposal::try_compose(
        vec![
            SchemaCapability::VERSIONED_MIGRATIONS,
            SchemaCapability::SECONDARY_INDEXES,
            SchemaCapability::ACCEPTED_CHECKS,
            SchemaCapability::RESTRICTIVE_RELATIONS,
        ],
        target.database_identity(),
        SchemaSubmissionKey::try_new("mixed-creation").unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(entities, vec![NamedTypeFragment::Record(details)]).unwrap()],
        assignments,
        Vec::new(),
        Some(SchemaMigrationPlan::try_new(vec![transition]).unwrap()),
    )
    .unwrap()
}

fn advance(db: &Db<EvolutionCanister>, proposal: &SchemaProposal) -> SchemaMigrationStatusPage {
    migrate_schema(db, proposal, command(proposal)).unwrap()
}

fn command(proposal: &SchemaProposal) -> SchemaMigrationCommand {
    SchemaMigrationCommand::Advance {
        expected_database: proposal.target_database(),
        expected_head: proposal.expected_head().clone(),
        expected_plan: proposal.migration().unwrap().digest(),
        acknowledged_finding_page: None,
    }
}

fn tag(db: &Db<EvolutionCanister>, path: &str) -> crate::types::EntityTag {
    db.accepted_runtime_entity_for_path(path)
        .unwrap()
        .entity_tag()
}

fn assert_lineage(db: &Db<EvolutionCanister>, proposal: &SchemaProposal) {
    let target = schema_application_target(db).unwrap();
    let lineage = load_entity_source_lineage_catalog().unwrap().unwrap();
    let entities = proposal.fragments()[0].entities();
    assert_eq!(lineage.entries().len(), entities.len());
    for entity in entities {
        let tag = db
            .accepted_runtime_entity_for_path(entity.name().as_str())
            .unwrap()
            .entity_tag();
        let entry = lineage.get(target.stores()[0].identity(), tag).unwrap();
        assert_eq!(entry.publication_head(), target.accepted_head());
        let AcceptedEntitySourceLineageState::Adopted {
            version,
            source_digest,
        } = entry.state()
        else {
            panic!("every published entity must have adopted lineage");
        };
        assert_eq!(version.get(), entity.version().get());
        assert_eq!(
            *source_digest,
            proposal.entity_source_digest(entity.source_key()).unwrap()
        );
    }
}

fn assert_pending(db: &Db<EvolutionCanister>, proposal: &SchemaProposal) {
    let error = apply_schema(db, proposal).unwrap_err();
    assert_eq!(
        error.diagnostic().detail(),
        Some(&DiagnosticDetail::SchemaMigration {
            reason: SchemaMigrationCode::MigrationInProgress
        })
    );
    assert!(db.accepted_runtime_entity_for_path("Quest").is_err());
    assert_eq!(
        schema_application_target(db).unwrap().accepted_head(),
        proposal.expected_head()
    );
}

fn assert_new_writes(root: &RequestExecutionRoot, item_name: &str) {
    let session = DbSession::<EvolutionCanister>::new(&EVOLUTION_REGISTRY, root);
    let values = |id, target, score| {
        vec![
            ("id", InputValue::nat64(id)),
            ("item_id", InputValue::nat64(target)),
            ("score", InputValue::int64(score)),
            (
                "details",
                InputValue::map(vec![(
                    InputValue::text("label".into()),
                    InputValue::text("quest".into()),
                )]),
            ),
        ]
    };
    insert(&session, "Quest", values(7, 1, 5)).unwrap();
    let page = session
        .execute_trusted_live_page(
            &DynamicQuery::new("Quest")
                .select(["id", "details"])
                .filter(FieldRef::new("id").eq(InputValue::nat64(7))),
            None,
        )
        .unwrap();
    assert_eq!(page.rows.len(), 1);
    assert_eq!(page.rows[0][0], OutputValue::nat64(7));
    assert!(insert(&session, "Quest", values(8, 999, 5)).is_err());
    assert!(insert(&session, "Quest", values(8, 1, -1)).is_err());
    assert!(
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
                entity: item_name.into(),
                key: InputValue::nat64(1),
            })
            .is_err()
    );
}

#[test]
fn metadata_migration_and_creation_preserve_rows_and_reuse_predecessor_name() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let physical = physical_state(&db);
    let old_tag = tag(&db, "Item");
    let proposal = mixed_proposal(&schema_application_target(&db).unwrap(), Shape::Rename, 1);
    assert_pending(&db, &proposal);
    let applied = advance(&db, &proposal);
    assert_eq!(applied.phase(), SchemaMigrationPhase::Applied);
    assert_eq!(physical_state(&db), physical);
    assert_eq!(tag(&db, "Archive"), old_tag);
    assert_ne!(tag(&db, "Item"), old_tag);
    assert_lineage(&db, &proposal);
    assert_eq!(advance(&db, &proposal), applied);
    let session = DbSession::<EvolutionCanister>::new(&EVOLUTION_REGISTRY, &root);
    assert!(
        session
            .execute_trusted_live_page(&DynamicQuery::new("Item").select(["id"]), None)
            .unwrap()
            .rows
            .is_empty()
    );
    let archived = session
        .execute_trusted_live_page(&DynamicQuery::new("Archive").select(["id"]), None)
        .unwrap();
    assert_eq!(archived.rows.len(), 2);
    insert(&session, "Item", vec![("id", InputValue::nat64(1))]).unwrap();
    assert_new_writes(&root, "Archive");
}

fn to_publishing(db: &Db<EvolutionCanister>, proposal: &SchemaProposal) {
    for phase in [
        SchemaMigrationPhase::Prepared,
        SchemaMigrationPhase::Validating,
        SchemaMigrationPhase::ReadyToRewrite,
        SchemaMigrationPhase::RewritingRows,
        SchemaMigrationPhase::RebuildingIndexes,
        SchemaMigrationPhase::FinalValidation,
        SchemaMigrationPhase::Publishing,
    ] {
        assert_eq!(advance(db, proposal).phase(), phase);
        assert!(db.accepted_runtime_entity_for_path("Quest").is_err());
        assert_eq!(
            schema_application_target(db).unwrap().accepted_head(),
            proposal.expected_head()
        );
    }
}

#[test]
fn physical_migration_creates_named_entity_after_rewriting_and_recovery() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let old_tag = tag(&db, "Item");
    let proposal = mixed_proposal(&schema_application_target(&db).unwrap(), Shape::Fill, 1);
    assert_pending(&db, &proposal);
    for phase in [
        SchemaMigrationPhase::Prepared,
        SchemaMigrationPhase::Validating,
        SchemaMigrationPhase::ReadyToRewrite,
        SchemaMigrationPhase::RewritingRows,
    ] {
        assert_eq!(advance(&db, &proposal).phase(), phase);
    }
    assert!(ensure_schema_migration_ready_for_ordinary_operations().is_err());
    interrupt_next_migration_rewrite_at(MigrationRewriteInterruption::JournalPublished);
    assert!(migrate_schema(&db, &proposal, command(&proposal)).is_err());
    forget_recovered_domain_for_tests(&db).unwrap();
    drive_startup_recovery_to_completion(&db);
    assert!(db.accepted_runtime_entity_for_path("Quest").is_err());
    assert_eq!(
        advance(&db, &proposal).phase(),
        SchemaMigrationPhase::RebuildingIndexes
    );
    assert_eq!(
        advance(&db, &proposal).phase(),
        SchemaMigrationPhase::FinalValidation
    );
    assert_eq!(
        advance(&db, &proposal).phase(),
        SchemaMigrationPhase::Publishing
    );
    let applied = advance(&db, &proposal);
    assert_eq!(applied.phase(), SchemaMigrationPhase::Applied);
    assert_eq!(applied.rows_rewritten(), 2);
    assert_eq!(tag(&db, "Item"), old_tag);
    assert_lineage(&db, &proposal);
    ensure_schema_migration_ready_for_ordinary_operations().unwrap();
    assert_eq!(advance(&db, &proposal), applied);
    let session = DbSession::<EvolutionCanister>::new(&EVOLUTION_REGISTRY, &root);
    let page = session
        .execute_trusted_live_page(
            &DynamicQuery::new("Item")
                .select(["id", "coins"])
                .filter(FieldRef::new("coins").eq(InputValue::nat64(5))),
            None,
        )
        .unwrap();
    assert_eq!(page.rows.len(), 2);
    assert!(page.rows.iter().all(|row| row[1] == OutputValue::nat64(5)));
    assert_new_writes(&root, "Item");
}

fn interrupt_mixed_publication(
    db: &Db<EvolutionCanister>,
    proposal: &SchemaProposal,
    receipt_first: bool,
) {
    let authorities = application_authorities(db);
    let before = load_current_application_bundles(&authorities).unwrap();
    let stores = existing_proposal_stores(proposal.target_database(), &authorities, &before);
    let lineage_before = load_entity_source_lineage_catalog().unwrap().unwrap();
    let planned = plan_schema_migration(proposal, &stores, &lineage_before).unwrap();
    let head = accepted_head_after_candidates(&authorities, planned.candidates()).unwrap();
    let after = lineage_after_planned(&lineage_before, planned.lineage(), &head).unwrap();
    let receipt = SchemaChangeReceipt::new(
        proposal.target_database(),
        migration_submission_key(Some(proposal.migration().unwrap().digest())).unwrap(),
        proposal.digest().unwrap(),
        proposal.expected_head().clone(),
        SchemaChangeOutcome::Applied {
            accepted_head: head,
        },
    )
    .unwrap();
    let application = SchemaApplicationRecordOp::insert(
        &SchemaApplicationRecord::new(receipt, Vec::new()).unwrap(),
    )
    .unwrap();
    let mut controls = vec![
        DatabaseControlOp::SchemaApplication(application.clone()),
        DatabaseControlOp::EntitySourceLineage(
            EntitySourceLineageCatalogOp::replace(Some(&lineage_before), &after).unwrap(),
        ),
    ];
    if let Some(record) = load_schema_migration_record().unwrap() {
        assert_eq!(record.phase(), PersistedSchemaMigrationPhase::Publishing);
        let applied = record
            .transition(
                PersistedSchemaMigrationPhase::Applied,
                record.progress().clone(),
            )
            .unwrap();
        controls.push(DatabaseControlOp::SchemaMigration(
            SchemaMigrationRecordOp::replace(&record, &applied).unwrap(),
        ));
    }
    recovery::interrupt_candidate_publication(
        db,
        before[0].as_ref().unwrap(),
        &planned.candidates()[0],
        controls,
        application,
        receipt_first,
    );
}

fn assert_publication_recovery(shape: Shape, receipt_first: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let proposal = mixed_proposal(&schema_application_target(&db).unwrap(), shape, 1);
    if matches!(shape, Shape::Fill) {
        to_publishing(&db, &proposal);
    }
    let physical = physical_state(&db);
    interrupt_mixed_publication(&db, &proposal, receipt_first);
    for _ in 0..2 {
        forget_recovered_domain_for_tests(&db).unwrap();
        drive_startup_recovery_to_completion(&db);
        assert_eq!(
            advance(&db, &proposal).phase(),
            SchemaMigrationPhase::Applied
        );
        assert_eq!(physical_state(&db), physical);
        assert_lineage(&db, &proposal);
        ensure_schema_migration_ready_for_ordinary_operations().unwrap();
        let current = mixed_proposal(&schema_application_target(&db).unwrap(), shape, 1);
        ensure_generated_schema_application_admitted(&db, &current).unwrap();
        assert!(matches!(
            apply_generated_schema(&db, &current).unwrap().outcome(),
            SchemaChangeOutcome::NoOp { .. }
        ));
    }
    assert_new_writes(
        &root,
        if matches!(shape, Shape::Rename) {
            "Archive"
        } else {
            "Item"
        },
    );
}

#[test]
fn metadata_mixed_publication_recovers_all_authorities() {
    assert_publication_recovery(Shape::Rename, false);
}

#[test]
fn metadata_mixed_publication_recovers_receipt_first() {
    assert_publication_recovery(Shape::Rename, true);
}

#[test]
fn physical_mixed_publication_recovers_all_authorities() {
    assert_publication_recovery(Shape::Fill, false);
}

#[test]
fn physical_mixed_publication_recovers_receipt_first() {
    assert_publication_recovery(Shape::Fill, true);
}

#[test]
fn new_entity_version_gap_rejects_mixed_proposal_atomically() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    let physical = physical_state(&db);
    let lineage = load_entity_source_lineage_catalog().unwrap();
    let proposal = mixed_proposal(&target, Shape::Fill, 2);
    let error = migrate_schema(&db, &proposal, command(&proposal)).unwrap_err();
    assert_eq!(
        error.diagnostic().detail(),
        Some(&DiagnosticDetail::SchemaMigration {
            reason: SchemaMigrationCode::VersionGap
        })
    );
    assert_eq!(physical_state(&db), physical);
    assert_eq!(load_entity_source_lineage_catalog().unwrap(), lineage);
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert!(load_schema_migration_record().unwrap().is_none());
}

#[test]
fn stale_mixed_proposal_rejects_without_partial_creation() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let old = schema_application_target(&db).unwrap();
    apply_schema(&db, &proposal(&old, &[("Achievement", 1)], "advance-head")).unwrap();
    let current = schema_application_target(&db).unwrap();
    let physical = physical_state(&db);
    let mixed = mixed_proposal(&old, Shape::Fill, 1);
    let error = migrate_schema(&db, &mixed, command(&mixed)).unwrap_err();
    assert_eq!(
        error.diagnostic().detail(),
        Some(&DiagnosticDetail::SchemaMigration {
            reason: SchemaMigrationCode::StaleAcceptedHead
        })
    );
    assert_eq!(physical_state(&db), physical);
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        current.accepted_head()
    );
    assert!(db.accepted_runtime_entity_for_path("Quest").is_err());
}

#[test]
fn creation_does_not_explain_unplanned_existing_field_additions() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    let base = mixed_proposal(&target, Shape::Rename, 1);
    let mut entities = base.fragments()[0].entities().to_vec();
    let existing = entities
        .iter_mut()
        .find(|proposed| proposed.source_key() == &entity("Archive"))
        .unwrap();
    let mut fields = existing.fields().to_vec();
    fields.push(scalar("unplanned", ScalarType::Nat64));
    *existing = EntityFragment::try_new(
        name("Archive"),
        existing.version(),
        fields,
        vec![field("id")],
        existing.indexes().to_vec(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let mixed = SchemaProposal::try_compose(
        base.capabilities().to_vec(),
        base.target_database(),
        base.submission_key().clone(),
        base.expected_head().clone(),
        vec![SchemaFragment::try_new(entities, base.fragments()[0].types().to_vec()).unwrap()],
        base.assignments().to_vec(),
        Vec::new(),
        base.migration().cloned(),
    )
    .unwrap();
    let physical = physical_state(&db);
    let lineage = load_entity_source_lineage_catalog().unwrap();
    let error = migrate_schema(&db, &mixed, command(&mixed)).unwrap_err();
    assert_eq!(
        error.diagnostic().detail(),
        Some(&DiagnosticDetail::SchemaMigration {
            reason: SchemaMigrationCode::UnexplainedSchemaDifference
        })
    );
    assert_eq!(physical_state(&db), physical);
    assert_eq!(load_entity_source_lineage_catalog().unwrap(), lineage);
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert!(db.accepted_runtime_entity_for_path("Quest").is_err());
}

fn assert_occupied_domain(index_domain: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    let tag = crate::types::EntityTag::new(tag(&db, "Item").value() + 1);
    let store = db.store_handle(EVOLUTION_STORE_PATH).unwrap();
    if index_domain {
        let key = IndexKey::new_from_components_with_primary_key_value(
            &IndexId::new(tag, 0),
            IndexKeyKind::User,
            &[7_u64.to_be_bytes()],
            &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
        )
        .unwrap()
        .to_raw()
        .unwrap();
        store.with_index_mut(|index| index.insert(key, IndexEntryValue::presence()));
    } else {
        let mut occupied = None;
        store
            .with_data(|data| {
                data.visit_entries(|key, row| {
                    occupied = Some((
                        RawDataStoreKey::from_entity_and_primary_key_bytes(
                            tag,
                            &key.as_bytes()[8..],
                        ),
                        row.clone(),
                    ));
                    Ok::<_, InternalError>(StoreVisit::Stop)
                })
            })
            .unwrap();
        let (key, row) = occupied.unwrap();
        store.with_data_mut(|data| data.insert_raw_for_test(key, row));
    }
    let physical = physical_state(&db);
    let lineage = load_entity_source_lineage_catalog().unwrap();
    let mixed = mixed_proposal(&target, Shape::Fill, 1);
    assert!(migrate_schema(&db, &mixed, command(&mixed)).is_err());
    assert_eq!(physical_state(&db), physical);
    assert_eq!(load_entity_source_lineage_catalog().unwrap(), lineage);
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert!(load_schema_migration_record().unwrap().is_none());
}

#[test]
fn occupied_new_rows_reject_before_physical_preparation() {
    assert_occupied_domain(false);
}

#[test]
fn occupied_new_indexes_reject_before_physical_preparation() {
    assert_occupied_domain(true);
}

#[test]
fn abort_mixed_physical_plan_leaves_creation_unpublished() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = initialize(&root);
    let target = schema_application_target(&db).unwrap();
    let physical = physical_state(&db);
    let lineage = load_entity_source_lineage_catalog().unwrap();
    let mixed = mixed_proposal(&target, Shape::Fill, 1);
    assert_eq!(advance(&db, &mixed).phase(), SchemaMigrationPhase::Prepared);
    assert_eq!(
        advance(&db, &mixed).phase(),
        SchemaMigrationPhase::Validating
    );
    let abort = SchemaMigrationCommand::Abort {
        expected_database: mixed.target_database(),
        expected_head: mixed.expected_head().clone(),
        expected_plan: mixed.migration().unwrap().digest(),
    };
    let aborted = migrate_schema(&db, &mixed, abort.clone()).unwrap();
    assert_eq!(aborted.phase(), SchemaMigrationPhase::Aborted);
    forget_recovered_domain_for_tests(&db).unwrap();
    drive_startup_recovery_to_completion(&db);
    assert_eq!(migrate_schema(&db, &mixed, abort).unwrap(), aborted);
    assert_eq!(physical_state(&db), physical);
    assert_eq!(load_entity_source_lineage_catalog().unwrap(), lineage);
    assert_eq!(
        schema_application_target(&db).unwrap().accepted_head(),
        target.accepted_head()
    );
    assert!(db.accepted_runtime_entity_for_path("Quest").is_err());
    ensure_schema_migration_ready_for_ordinary_operations().unwrap();
}
