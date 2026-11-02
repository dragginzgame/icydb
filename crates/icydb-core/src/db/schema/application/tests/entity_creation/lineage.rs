//! Ordinary accepted edits and retained source lineage publish together.

use super::*;
use crate::db::schema::{
    EntitySourceLineageCatalogOp, apply_entity_source_lineage_catalog_op,
    live_schema_checkpoint::load_entity_source_lineage_catalog,
    migration_lineage::{
        AcceptedEntitySourceLineage, AcceptedEntitySourceLineageCatalog,
        AcceptedEntitySourceLineageState, AcceptedEntitySourceVersion,
    },
};
use icydb_schema::SchemaMigrationPlan;
use std::collections::BTreeMap;

fn quest_with_default(version: u32, value: i64) -> EntityFragment {
    let base = declaration("Quest", version);
    let fields = base
        .fields()
        .iter()
        .map(|field| {
            if field.name().as_str() == "score" {
                FieldFragment::new(
                    name("score"),
                    FieldType::Scalar(ScalarType::Int64),
                    false,
                    FieldInsertPolicy::Default(ScalarLiteral::Int(value.into())),
                    None,
                )
            } else {
                field.clone()
            }
        })
        .collect();
    EntityFragment::try_new(
        name("Quest"),
        base.version(),
        fields,
        vec![field("id")],
        base.indexes().to_vec(),
        base.relations().to_vec(),
        base.constraints().to_vec(),
    )
    .unwrap()
}

fn compose(
    db: &Db<EvolutionCanister>,
    entities: Vec<EntityFragment>,
    key: &str,
    plan: Option<SchemaMigrationPlan>,
) -> SchemaProposal {
    let target = schema_application_target(db).unwrap();
    let assignments = entities
        .iter()
        .map(|entity| {
            EntityStoreAssignment::new(entity.source_key().clone(), target.stores()[0].identity())
        })
        .collect();
    let mut capabilities = vec![
        SchemaCapability::ACCEPTED_CHECKS,
        SchemaCapability::RESTRICTIVE_RELATIONS,
        SchemaCapability::SECONDARY_INDEXES,
    ];
    if plan.is_some() {
        capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
    }
    SchemaProposal::try_compose(
        capabilities,
        target.database_identity(),
        SchemaSubmissionKey::try_new(key).unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(entities, Vec::new()).unwrap()],
        assignments,
        Vec::new(),
        plan,
    )
    .unwrap()
}

fn prepare(root: &RequestExecutionRoot) -> Db<EvolutionCanister> {
    let db = initialize(root);
    let proposal = compose(
        &db,
        vec![declaration("Item", 1), quest_with_default(1, 1)],
        "quest",
        None,
    );
    apply_schema(&db, &proposal).unwrap();
    // Model an existing installation that had migration support in a prior
    // binary. Feature-disabled builds must maintain its accepted lineage too.
    if load_entity_source_lineage_catalog().unwrap().is_none() {
        let target = schema_application_target(&db).unwrap();
        let entries = proposal.fragments()[0]
            .entities()
            .iter()
            .map(|entity| {
                let tag = db
                    .accepted_runtime_entity_for_path(entity.name().as_str())
                    .unwrap()
                    .entity_tag();
                (
                    (target.stores()[0].identity(), tag),
                    AcceptedEntitySourceLineage::adopted(
                        target.accepted_head().clone(),
                        AcceptedEntitySourceVersion::try_new(1).unwrap(),
                        proposal.entity_source_digest(entity.source_key()).unwrap(),
                    )
                    .unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let catalog = AcceptedEntitySourceLineageCatalog::try_new(entries).unwrap();
        apply_entity_source_lineage_catalog_op(
            &EntitySourceLineageCatalogOp::replace(None, &catalog).unwrap(),
        )
        .unwrap();
    }
    db
}

fn assert_quest_lineage(db: &Db<EvolutionCanister>, proposal: &SchemaProposal) {
    let target = schema_application_target(db).unwrap();
    let tag = db
        .accepted_runtime_entity_for_path("Quest")
        .unwrap()
        .entity_tag();
    let catalog = load_entity_source_lineage_catalog().unwrap().unwrap();
    let entry = catalog
        .entries()
        .get(&(target.stores()[0].identity(), tag))
        .unwrap();
    let AcceptedEntitySourceLineageState::Adopted {
        version,
        source_digest,
    } = entry.state()
    else {
        panic!("existing adopted lineage must remain adopted");
    };
    assert_eq!(version.get(), 1);
    assert_eq!(
        *source_digest,
        proposal.entity_source_digest(&entity("Quest")).unwrap()
    );
}

#[test]
fn ordinary_default_change_refreshes_retained_lineage_without_a_plan() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = prepare(&root);
    let physical = physical_state(&db);
    let changed = compose(
        &db,
        vec![declaration("Item", 1), quest_with_default(1, 7)],
        "default",
        None,
    );
    apply_schema(&db, &changed).unwrap();
    assert_quest_lineage(&db, &changed);
    assert_eq!(physical_state(&db), physical);
}

#[test]
fn admitted_noop_repairs_a_previously_stale_source_digest() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = prepare(&root);
    let stale = load_entity_source_lineage_catalog().unwrap().unwrap();
    let changed = compose(
        &db,
        vec![declaration("Item", 1), quest_with_default(1, 7)],
        "changed",
        None,
    );
    apply_schema(&db, &changed).unwrap();
    let current = load_entity_source_lineage_catalog().unwrap().unwrap();
    apply_entity_source_lineage_catalog_op(
        &EntitySourceLineageCatalogOp::replace(Some(&current), &stale).unwrap(),
    )
    .unwrap();
    let target = schema_application_target(&db).unwrap();
    let physical = physical_state(&db);
    let repair = compose(
        &db,
        vec![declaration("Item", 1), quest_with_default(1, 7)],
        "repair",
        None,
    );
    assert!(matches!(
        apply_schema(&db, &repair).unwrap().outcome(),
        SchemaChangeOutcome::NoOp { .. }
    ));
    assert_quest_lineage(&db, &repair);
    assert_eq!(schema_application_target(&db).unwrap(), target);
    assert_eq!(physical_state(&db), physical);
}

fn quest_with_upper_check(version: u32) -> EntityFragment {
    let base = quest_with_default(version, 1);
    let mut constraints = base.constraints().to_vec();
    constraints.push(ConstraintFragment::check(
        name("upper_score"),
        SourceCheckExpr::try_new(vec![
            SourceCheckInstruction::Field(field("score")),
            SourceCheckInstruction::Literal(ScalarLiteral::Int(8)),
            SourceCheckInstruction::LessThanOrEqual,
        ])
        .unwrap(),
    ));
    EntityFragment::try_new(
        name("Quest"),
        base.version(),
        base.fields().to_vec(),
        vec![field("id")],
        base.indexes().to_vec(),
        base.relations().to_vec(),
        constraints,
    )
    .unwrap()
}

fn populate_quests(db: &Db<EvolutionCanister>, root: &RequestExecutionRoot, rows: u64) {
    if rows != 0 {
        let session = DbSession::<EvolutionCanister>::new(&EVOLUTION_REGISTRY, root);
        let mutations = (1..=rows)
            .map(|id| DynamicMutation::Insert {
                entity: "Quest".into(),
                patch: DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "item_id".into(),
                        DynamicWriteCell::Value(InputValue::nat64(1)),
                    ),
                ]),
            })
            .collect();
        session
            .execute_trusted_dynamic_mutation_batch(mutations)
            .unwrap();
        drive_startup_recovery_to_completion(db);
    }
}

fn assert_check_lineage(rows: u64) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = prepare(&root);
    let reference = compose(
        &db,
        vec![declaration("Item", 1), quest_with_default(1, 1)],
        "reference",
        None,
    );
    populate_quests(&db, &root, rows);
    let changed = compose(
        &db,
        vec![declaration("Item", 1), quest_with_upper_check(1)],
        "check",
        None,
    );
    let receipt = apply_schema(&db, &changed).unwrap();
    if let SchemaChangeOutcome::Pending { job, .. } = receipt.outcome() {
        assert!(rows > 256);
        assert_quest_lineage(&db, &reference);
        let retry = compose(
            &db,
            vec![declaration("Item", 1), quest_with_upper_check(1)],
            "pending-reapply",
            None,
        );
        let error = apply_schema(&db, &retry).unwrap_err();
        assert_eq!(
            error.diagnostic(),
            InternalError::store_unsupported().diagnostic()
        );
        assert_quest_lineage(&db, &reference);
        abort_schema_application(&db, job.id(), None).unwrap();
        assert_quest_lineage(&db, &reference);
    } else {
        assert!(rows <= 256);
        assert_quest_lineage(&db, &changed);
    }
}

#[test]
fn empty_check_publication_refreshes_its_source_digest() {
    assert_check_lineage(0);
}

#[test]
fn directly_validated_check_publication_refreshes_its_source_digest() {
    assert_check_lineage(1);
}

#[test]
fn deferred_check_and_abort_preserve_the_prior_source_digest() {
    assert_check_lineage(257);
}

#[test]
fn ordinary_version_changes_refuse_without_rewriting_lineage_or_rows() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = prepare(&root);
    let physical = physical_state(&db);
    let target = schema_application_target(&db).unwrap();
    let before = load_entity_source_lineage_catalog().unwrap();
    let changed = compose(
        &db,
        vec![declaration("Item", 1), quest_with_default(2, 7)],
        "version",
        None,
    );
    let error = apply_schema(&db, &changed).unwrap_err();
    assert_eq!(
        error.diagnostic().error_code(),
        icydb_diagnostic_code::ErrorCode::SCHEMA_MIGRATION_MISSING_MIGRATION
    );
    assert_eq!(load_entity_source_lineage_catalog().unwrap(), before);
    assert_eq!(schema_application_target(&db).unwrap(), target);
    assert_eq!(physical_state(&db), physical);
}

fn assert_deferred_version_change_refuses(item_version: u32, quest_version: u32) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = prepare(&root);
    // One extra row beyond direct validation forces the pending route.
    populate_quests(&db, &root, 257);
    let before = load_entity_source_lineage_catalog().unwrap();
    let target = schema_application_target(&db).unwrap();
    let physical = physical_state(&db);
    let changed = compose(
        &db,
        vec![
            declaration("Item", item_version),
            quest_with_upper_check(quest_version),
        ],
        "deferred-version",
        None,
    );
    for _ in 0..2 {
        let error = apply_schema(&db, &changed).unwrap_err();
        assert_eq!(
            error.diagnostic().error_code(),
            icydb_diagnostic_code::ErrorCode::SCHEMA_MIGRATION_MISSING_MIGRATION
        );
        assert_eq!(schema_application_target(&db).unwrap(), target);
        assert_eq!(load_entity_source_lineage_catalog().unwrap(), before);
        assert_eq!(physical_state(&db), physical);
    }
    // A refused submission must leave admission available to a valid one.
    let corrected = compose(
        &db,
        vec![declaration("Item", 1), quest_with_upper_check(1)],
        "corrected-deferred-version",
        None,
    );
    let receipt = apply_schema(&db, &corrected).unwrap();
    let SchemaChangeOutcome::Pending { job, .. } = receipt.outcome() else {
        panic!("same-version validation should start normally");
    };
    // A valid publication may advance provenance, while pending validation
    // preserves the admitted versions and digests of every assigned source.
    let admitted = load_entity_source_lineage_catalog().unwrap().unwrap();
    let prior = before.as_ref().unwrap();
    assert_eq!(admitted.entries().len(), prior.entries().len());
    for (identity, entry) in prior.entries() {
        assert_eq!(admitted.entries()[identity].state(), entry.state());
    }
    abort_schema_application(&db, job.id(), None).unwrap();
}

#[test]
fn deferred_validation_refuses_its_own_unplanned_source_version_change() {
    assert_deferred_version_change_refuses(1, 2);
}

#[test]
fn deferred_validation_refuses_another_entitys_unplanned_source_version_change() {
    assert_deferred_version_change_refuses(2, 1);
}

#[test]
fn ordinary_edit_does_not_implicitly_adopt_an_unadopted_source() {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = prepare(&root);
    let target = schema_application_target(&db).unwrap();
    let tag = db
        .accepted_runtime_entity_for_path("Quest")
        .unwrap()
        .entity_tag();
    let before = load_entity_source_lineage_catalog().unwrap().unwrap();
    let mut entries = before.entries().clone();
    entries.insert(
        (target.stores()[0].identity(), tag),
        AcceptedEntitySourceLineage::unadopted(target.accepted_head().clone()).unwrap(),
    );
    let unadopted = AcceptedEntitySourceLineageCatalog::try_new(entries).unwrap();
    apply_entity_source_lineage_catalog_op(
        &EntitySourceLineageCatalogOp::replace(Some(&before), &unadopted).unwrap(),
    )
    .unwrap();
    let changed = compose(
        &db,
        vec![declaration("Item", 1), quest_with_default(1, 7)],
        "unadopted",
        None,
    );
    apply_schema(&db, &changed).unwrap();
    let after = load_entity_source_lineage_catalog().unwrap().unwrap();
    assert!(matches!(
        after.entries()[&(target.stores()[0].identity(), tag)].state(),
        AcceptedEntitySourceLineageState::Unadopted
    ));
}

#[cfg(feature = "migration")]
#[test]
fn ordinary_default_change_does_not_block_a_later_unrelated_migration() {
    use crate::db::schema::{
        SchemaMigrationCommand, SchemaMigrationPhase, application::migrate_schema,
    };
    use icydb_schema::{EntityMigration, SchemaMigrationTransform};
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = prepare(&root);
    let changed = compose(
        &db,
        vec![declaration("Item", 1), quest_with_default(1, 7)],
        "before-plan",
        None,
    );
    apply_schema(&db, &changed).unwrap();
    let base = declaration("Item", 1);
    let item = EntityFragment::try_new(
        name("Item"),
        DeclaredEntityVersion::try_new(2).unwrap(),
        vec![
            scalar("id", ScalarType::Nat64),
            scalar("coins", ScalarType::Nat64),
        ],
        vec![field("id")],
        base.indexes().to_vec(),
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let transition = EntityMigration::try_new(
        entity("Item"),
        version_one(),
        None,
        Vec::new(),
        vec![SchemaMigrationTransform::Fill {
            to: field("coins"),
            literal: ScalarLiteral::Nat(5),
        }],
    )
    .unwrap();
    let plan = SchemaMigrationPlan::try_new(vec![transition]).unwrap();
    let digest = plan.digest();
    let proposal = compose(
        &db,
        vec![item, quest_with_default(1, 7)],
        "unrelated",
        Some(plan),
    );
    for _ in 0..64 {
        let status = migrate_schema(
            &db,
            &proposal,
            SchemaMigrationCommand::Advance {
                expected_database: proposal.target_database(),
                expected_head: proposal.expected_head().clone(),
                expected_plan: digest,
            },
        )
        .unwrap();
        if status.phase() == SchemaMigrationPhase::Applied {
            assert_quest_lineage(&db, &proposal);
            assert_eq!(physical_state(&db).rows.len(), 2);
            return;
        }
    }
    panic!("the unrelated migration must finish within the fixture bound");
}

#[cfg(feature = "migration")]
fn assert_default_recovery(receipt_first: bool) {
    let root = RequestExecutionRoot::__new_runtime_root();
    let db = prepare(&root);
    let physical = physical_state(&db);
    let changed = compose(
        &db,
        vec![declaration("Item", 1), quest_with_default(1, 7)],
        "interrupted",
        None,
    );
    super::recovery::interrupt_publication(&db, &changed, receipt_first);
    for _ in 0..2 {
        forget_recovered_domain_for_tests(&db).unwrap();
        drive_startup_recovery_to_completion(&db);
        apply_schema(&db, &changed).unwrap();
        assert_quest_lineage(&db, &changed);
        assert_eq!(physical_state(&db), physical);
    }
}

#[cfg(feature = "migration")]
#[test]
fn ordinary_default_lineage_recovers_with_its_schema_marker() {
    assert_default_recovery(false);
}

#[cfg(feature = "migration")]
#[test]
fn ordinary_default_lineage_recovers_after_receipt_publication() {
    assert_default_recovery(true);
}
