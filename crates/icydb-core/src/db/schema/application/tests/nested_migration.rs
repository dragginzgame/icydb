//! Physical migration proofs for retained nested relations and isolated generations.
//! Reuses the application boundary's journaled store and recovery fixture.

mod schema_admission;
mod terminal_handoff;

use super::*;
use crate::{
    db::{
        DynamicMutation, DynamicQuery, DynamicStructuralPatch, DynamicWriteCell,
        RequestExecutionRoot,
        data::StoreVisit,
        index::{IndexKey, IndexKeyKind, IndexStoreVisit},
        key_taxonomy::{EncodedPrimaryKey, PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            MigrationRewriteInterruption, PersistedRelationEdgeSnapshot, SchemaMigrationCommand,
            SchemaMigrationFindingKind, SchemaMigrationPhase, SchemaMigrationStatusPage,
            ensure_schema_migration_ready_for_ordinary_operations,
            interrupt_next_migration_rewrite_at, migrate_schema,
        },
        session::DbSession,
    },
    error::InternalError,
    value::{InputValue, OutputValue},
};
use icydb_schema::{RelationPathStepFragment, RelationSourceFragment, SchemaMigrationRename};

fn field(value: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(value).expect("fixture field should admit")
}

fn proposal(
    current: bool,
    head: ExpectedAcceptedHead,
    database: TargetDatabaseIdentity,
    store: TargetStoreIdentity,
) -> SchemaProposal {
    proposal_for_version(if current { 2 } else { 1 }, head, database, store)
}

// A copied repeated root changes targets without changing relation identity.
// Renaming that root also proves the planner uses resolved field IDs.
#[expect(
    clippy::too_many_lines,
    reason = "the predecessor and candidate fixture share one exact relation contract"
)]
fn proposal_for_version(
    version: u32,
    head: ExpectedAcceptedHead,
    database: TargetDatabaseIdentity,
    store: TargetStoreIdentity,
) -> SchemaProposal {
    let source = EntitySourceKey::try_new("Source").expect("source key should admit");
    let target = EntitySourceKey::try_new("Target").expect("target key should admit");
    let current = version > 1;
    let root = if current { "links" } else { "refs" };
    let scalar = FieldType::Scalar(ScalarType::Nat64);
    let nested = FieldType::List(Box::new(FieldType::List(Box::new(scalar.clone()))));
    let fields = [
        ("id", scalar.clone()),
        (root, nested.clone()),
        ("backup", nested),
        ("stable", scalar.clone()),
    ]
    .into_iter()
    .map(|(name_, kind)| {
        FieldFragment::new(name(name_), kind, false, FieldInsertPolicy::Required, None)
    })
    .collect();
    let relations = [
        (
            "copied_targets",
            RelationSourceFragment::Nested {
                root: field(root),
                steps: vec![RelationPathStepFragment::ListItems; 2],
            },
        ),
        (
            "stable_target",
            RelationSourceFragment::direct(vec![field("stable")]),
        ),
    ]
    .into_iter()
    .map(|(name_, source)| {
        RelationFragment::try_new(
            name(name_),
            source,
            target.clone(),
            vec![field("id")],
            RelationDeleteAction::Restrict,
        )
        .expect("relation should admit")
    })
    .collect();
    let entities = vec![
        EntityFragment::try_new(
            name("Source"),
            DeclaredEntityVersion::try_new(version).expect("version should admit"),
            fields,
            vec![field("id")],
            Vec::new(),
            relations,
            Vec::new(),
        )
        .expect("source entity should admit"),
        EntityFragment::try_new(
            name("Target"),
            version_one(),
            vec![FieldFragment::new(
                name("id"),
                scalar,
                false,
                FieldInsertPolicy::Required,
                None,
            )],
            vec![field("id")],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("target entity should admit"),
    ];
    let migration = current.then(|| {
        SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                source.clone(),
                DeclaredEntityVersion::try_new(version - 1)
                    .expect("predecessor version should admit"),
                None,
                (version == 2)
                    .then(|| SchemaMigrationRename::Field {
                        from: field("refs"),
                        to: field("links"),
                    })
                    .into_iter()
                    .collect(),
                vec![SchemaMigrationTransform::Copy {
                    from: field("backup"),
                    to: field("links"),
                }],
            )
            .expect("transition should admit"),
        ])
        .expect("plan should admit")
    });
    let mut capabilities = vec![SchemaCapability::RESTRICTIVE_RELATIONS];
    if current {
        capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
    }
    SchemaProposal::try_compose(
        capabilities,
        database,
        SchemaSubmissionKey::try_new(if current {
            "nested-migration"
        } else {
            "nested-initial"
        })
        .expect("submission key should admit"),
        head,
        vec![SchemaFragment::try_new(entities, Vec::new()).expect("fragment should admit")],
        vec![
            EntityStoreAssignment::new(source, store),
            EntityStoreAssignment::new(target, store),
        ],
        Vec::new(),
        migration,
    )
    .expect("proposal should compose")
}

fn nested_value(target: u64) -> InputValue {
    InputValue::list(vec![InputValue::list(vec![InputValue::nat64(target); 2])])
}

fn initialize(
    backup: u64,
) -> (
    Db<MigrationExecutionCanister>,
    DbSession<MigrationExecutionCanister>,
    SchemaProposal,
) {
    let db = Db::new(
        &MIGRATION_EXECUTION_REGISTRY,
        RequestExecutionRoot::__new_runtime_root().scope(),
    );
    drive_startup_recovery_to_completion(&db);
    let initial = schema_application_target(&db).expect("initial target should issue");
    let store = initial.stores()[0].identity();
    apply_schema(
        &db,
        &proposal(
            false,
            initial.accepted_head().clone(),
            initial.database_identity(),
            store,
        ),
    )
    .expect("initial schema should publish");
    let session = DbSession::new(
        &MIGRATION_EXECUTION_REGISTRY,
        &RequestExecutionRoot::__new_runtime_root(),
    );
    for id in 1..=3 {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "Target".to_string(),
                patch: DynamicStructuralPatch::new(vec![(
                    "id".to_string(),
                    DynamicWriteCell::Value(InputValue::nat64(id)),
                )]),
            })
            .expect("target should insert");
    }
    for id in 10..=12 {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: "Source".to_string(),
                patch: DynamicStructuralPatch::new(vec![
                    (
                        "id".to_string(),
                        DynamicWriteCell::Value(InputValue::nat64(id)),
                    ),
                    ("refs".to_string(), DynamicWriteCell::Value(nested_value(1))),
                    (
                        "backup".to_string(),
                        DynamicWriteCell::Value(nested_value(backup)),
                    ),
                    (
                        "stable".to_string(),
                        DynamicWriteCell::Value(InputValue::nat64(3)),
                    ),
                ]),
            })
            .expect("source should insert");
    }
    let target = schema_application_target(&db).expect("migration target should issue");
    let candidate = proposal(
        true,
        target.accepted_head().clone(),
        target.database_identity(),
        store,
    );
    (db, session, candidate)
}

fn advance(
    db: &Db<MigrationExecutionCanister>,
    proposal: &SchemaProposal,
) -> Result<SchemaMigrationStatusPage, InternalError> {
    migrate_schema(
        db,
        proposal,
        SchemaMigrationCommand::Advance {
            expected_database: proposal.target_database(),
            expected_head: proposal.expected_head().clone(),
            expected_plan: proposal.migration().expect("plan should exist").digest(),
            acknowledged_finding_page: None,
        },
    )
}

fn accepted_relations(db: &Db<MigrationExecutionCanister>) -> Vec<PersistedRelationEdgeSnapshot> {
    let source = db
        .accepted_runtime_entity_for_path("Source")
        .expect("source should resolve");
    db.store_handle(MIGRATION_EXECUTION_STORE_PATH)
        .expect("store should resolve")
        .with_schema(|schema| {
            schema.current_accepted_catalog_selection(
                source.entity_tag(),
                "Source",
                MIGRATION_EXECUTION_STORE_PATH,
            )
        })
        .expect("selection should decode")
        .expect("selection should exist")
        .decode_verified()
        .expect("snapshot should decode")
        .persisted_snapshot()
        .relations()
        .to_vec()
}

fn row_bytes(db: &Db<MigrationExecutionCanister>) -> Vec<Vec<u8>> {
    let mut rows = Vec::new();
    db.store_handle(MIGRATION_EXECUTION_STORE_PATH)
        .expect("store should resolve")
        .with_data(|data| {
            data.visit_entries(|_, row| {
                rows.push(row.as_bytes().to_vec());
                Ok::<_, InternalError>(StoreVisit::Continue)
            })
        })
        .expect("rows should scan");
    rows
}

// Ignore retired generations, but require exact domain, target and source pairs
// in every accepted generation, including the untouched direct relation.
fn assert_edges(
    db: &Db<MigrationExecutionCanister>,
    relations: &[PersistedRelationEdgeSnapshot],
    target: u64,
) {
    let source = db
        .accepted_runtime_entity_for_path("Source")
        .expect("source should resolve");
    let mut actual = Vec::new();
    db.store_handle(MIGRATION_EXECUTION_STORE_PATH)
        .expect("store should resolve")
        .with_index(|index| {
            index.visit_entries(|raw, _| {
                let key = IndexKey::try_from_raw(raw).expect("key should decode");
                if key.key_kind() == IndexKeyKind::System
                    && key.index_id().entity_tag() == source.entity_tag()
                {
                    for relation in relations {
                        if key.index_id().generation() == relation.physical_generation()
                            && key.component(0)
                                == Some(relation.id().get().to_be_bytes().as_slice())
                        {
                            actual.push((
                                relation.id().get(),
                                key.component(1).expect("target should exist").to_vec(),
                                raw.decode()
                                    .expect("key should decode")
                                    .primary_key()
                                    .as_bytes()
                                    .to_vec(),
                            ));
                        }
                    }
                }
                Ok::<_, InternalError>(IndexStoreVisit::Continue)
            })
        })
        .expect("edges should scan");
    let encode = |id| {
        EncodedPrimaryKey::encode(PrimaryKeyValue::Scalar(PrimaryKeyComponent::Nat64(id)))
            .expect("key should encode")
            .as_bytes()
            .to_vec()
    };
    let mut expected = Vec::new();
    for relation in relations {
        let target = if relation.name() == "copied_targets" {
            target
        } else {
            3
        };
        for source in 10..=12 {
            expected.push((relation.id().get(), encode(target), encode(source)));
        }
    }
    actual.sort();
    expected.sort();
    assert_eq!(
        actual, expected,
        "active reverse edges must match rewritten source rows exactly"
    );
}

fn assert_hidden(
    db: &Db<MigrationExecutionCanister>,
    proposal: &SchemaProposal,
    before: &[PersistedRelationEdgeSnapshot],
) {
    assert_eq!(
        schema_application_target(db)
            .expect("target should issue")
            .accepted_head(),
        proposal.expected_head()
    );
    assert_eq!(accepted_relations(db), before);
}

// Three one-row interruptions exercise the whole nested row/edge batch at each
// existing rewrite checkpoint; the uninterrupted case also proves publication
// cannot depend on recovery rebuilding an omitted domain.
#[expect(
    clippy::too_many_lines,
    reason = "validation, rewrite, publication and replay form one migration scenario"
)]
fn run_copy(interrupted: bool) {
    let (db, session, proposal) = initialize(2);
    let before = accepted_relations(&db);
    let rows = row_bytes(&db);
    assert_edges(&db, &before, 1);
    for phase in [
        SchemaMigrationPhase::Prepared,
        SchemaMigrationPhase::Validating,
        SchemaMigrationPhase::ReadyToRewrite,
    ] {
        assert_eq!(
            advance(&db, &proposal)
                .expect("validation should advance")
                .phase(),
            phase
        );
        assert_hidden(&db, &proposal, &before);
        if phase != SchemaMigrationPhase::Prepared {
            ensure_schema_migration_ready_for_ordinary_operations()
                .expect_err("validation must gate ordinary operations");
        }
        assert_eq!(
            row_bytes(&db),
            rows,
            "validation must not rewrite accepted rows"
        );
        assert_edges(&db, &before, 1);
    }
    assert_eq!(
        advance(&db, &proposal)
            .expect("rewrite should start")
            .phase(),
        SchemaMigrationPhase::RewritingRows
    );
    if interrupted {
        for checkpoint in [
            MigrationRewriteInterruption::MarkerPersisted,
            MigrationRewriteInterruption::JournalPublished,
            MigrationRewriteInterruption::PhysicalApplied,
        ] {
            interrupt_next_migration_rewrite_at(checkpoint);
            advance(&db, &proposal).expect_err("rewrite should be interrupted");
            forget_recovered_domain_for_tests(&db).expect("recovery ownership should reset");
            drive_startup_recovery_to_completion(&db);
            assert_hidden(&db, &proposal, &before);
            ensure_schema_migration_ready_for_ordinary_operations()
                .expect_err("recovery must retain the migration gate");
        }
    }
    for _ in 0..8 {
        assert_hidden(&db, &proposal, &before);
        if advance(&db, &proposal)
            .expect("migration should advance")
            .phase()
            == SchemaMigrationPhase::Publishing
        {
            break;
        }
    }
    assert_hidden(&db, &proposal, &before);
    ensure_schema_migration_ready_for_ordinary_operations()
        .expect_err("candidate must remain gated until publication");
    let applied = advance(&db, &proposal).expect("candidate should publish");
    assert_eq!(applied.phase(), SchemaMigrationPhase::Applied);
    assert_eq!(applied.rows_rewritten(), 3);
    let after = accepted_relations(&db);
    assert_edges(&db, &after, 2);
    assert_eq!(
        applied.indexes_rebuilt(),
        1,
        "only the transformed relation needs a fresh domain"
    );
    for old in &before {
        let new = after
            .iter()
            .find(|new| new.id() == old.id())
            .expect("relation identity must survive");
        assert_eq!(new.source(), old.source());
        if old.name() == "copied_targets" {
            assert_ne!(new.physical_generation(), old.physical_generation());
        } else {
            assert_eq!(new.physical_generation(), old.physical_generation());
        }
    }
    let expected = (10..=12)
        .map(|id| {
            vec![
                OutputValue::nat64(id),
                OutputValue::from_public(nested_value(2).into_public()),
            ]
        })
        .collect::<Vec<_>>();
    let query = DynamicQuery::new("Source")
        .select(["id", "links"])
        .order_by(crate::db::asc("id"))
        .limit(16);
    let mut actual = Vec::new();
    let mut continuation = None;
    for _ in 0..4 {
        let page = session
            .execute_trusted_live_page(&query, continuation.as_deref())
            .expect("candidate rows should query");
        actual.extend(page.rows);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert!(
        continuation.is_none(),
        "the bounded query must exhaust all pages"
    );
    assert_eq!(actual, expected);
    let delete = |id| {
        session.execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: "Target".to_string(),
            key: InputValue::nat64(id),
        })
    };
    for target in [2, 3] {
        let error = delete(target).expect_err("active references must restrict target deletion");
        assert!(error.diagnostic_facts().contains(&(
            icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
            icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
        )));
    }
    delete(1).expect("the replaced nested target must be released");
    let published_rows = row_bytes(&db);
    for _ in 0..2 {
        forget_recovered_domain_for_tests(&db).expect("recovery ownership should reset");
        drive_startup_recovery_to_completion(&db);
        assert_eq!(row_bytes(&db), published_rows);
        assert_eq!(accepted_relations(&db), after);
        assert_edges(&db, &after, 2);
    }
}

#[test]
fn nested_relation_copy_publishes_exact_reverse_edges_without_recovery() {
    run_copy(false);
}

#[test]
fn nested_relation_copy_recovers_each_rewrite_checkpoint() {
    run_copy(true);
}

#[test]
fn nested_relation_copy_missing_target_rejects_without_rewriting() {
    let (db, _, proposal) = initialize(99);
    let rows = row_bytes(&db);
    let before = accepted_relations(&db);
    for phase in [
        SchemaMigrationPhase::Prepared,
        SchemaMigrationPhase::Validating,
        SchemaMigrationPhase::Rejected,
    ] {
        let page = advance(&db, &proposal).expect("validation should report a typed finding");
        assert_eq!(page.phase(), phase);
        if phase == SchemaMigrationPhase::Rejected {
            assert_eq!(page.findings().len(), 3);
            assert!(
                page.findings()
                    .iter()
                    .all(|finding| finding.kind() == SchemaMigrationFindingKind::Relation)
            );
        }
        assert_hidden(&db, &proposal, &before);
        assert_eq!(row_bytes(&db), rows);
        assert_edges(&db, &before, 1);
    }
}

#[test]
fn nested_relation_copy_abort_preserves_the_accepted_generation() {
    let (db, _, proposal) = initialize(2);
    let rows = row_bytes(&db);
    let before = accepted_relations(&db);
    for phase in [
        SchemaMigrationPhase::Prepared,
        SchemaMigrationPhase::Validating,
        SchemaMigrationPhase::ReadyToRewrite,
    ] {
        assert_eq!(
            advance(&db, &proposal)
                .expect("validation should advance")
                .phase(),
            phase
        );
    }
    let store = db
        .store_handle(MIGRATION_EXECUTION_STORE_PATH)
        .expect("store should resolve");
    assert_eq!(
        store.with_index(IndexStore::len),
        9,
        "only candidate edges are staged beside accepted edges"
    );
    let aborted = migrate_schema(
        &db,
        &proposal,
        SchemaMigrationCommand::Abort {
            expected_database: proposal.target_database(),
            expected_head: proposal.expected_head().clone(),
            expected_plan: proposal.migration().expect("plan should exist").digest(),
        },
    )
    .expect("pre-rewrite abort should complete");
    assert_eq!(aborted.phase(), SchemaMigrationPhase::Aborted);
    assert_eq!(store.with_index(IndexStore::len), 6);
    assert_eq!(row_bytes(&db), rows);
    assert_eq!(accepted_relations(&db), before);
    assert_edges(&db, &before, 1);
    ensure_schema_migration_ready_for_ordinary_operations().expect("abort should clear the gate");
}

#[test]
fn nested_relation_copy_final_validation_requires_the_candidate_reverse_edge() {
    let (db, _, proposal) = initialize(2);
    let before = accepted_relations(&db);
    for phase in [
        SchemaMigrationPhase::Prepared,
        SchemaMigrationPhase::Validating,
        SchemaMigrationPhase::ReadyToRewrite,
        SchemaMigrationPhase::RewritingRows,
        SchemaMigrationPhase::RebuildingIndexes,
        SchemaMigrationPhase::FinalValidation,
    ] {
        assert_eq!(
            advance(&db, &proposal)
                .expect("migration should advance")
                .phase(),
            phase
        );
    }
    // Remove one candidate witness after rewrite, leaving all predecessor keys
    // intact. Final validation must inspect the transformed retained relation.
    let store = db
        .store_handle(MIGRATION_EXECUTION_STORE_PATH)
        .expect("store should resolve");
    let mut candidate_key = None;
    store
        .with_index(|index| {
            index.visit_entries(|raw, _| {
                let key = IndexKey::try_from_raw(raw).expect("key should decode");
                if key.key_kind() == IndexKeyKind::System
                    && before.iter().all(|relation| {
                        relation.physical_generation() != key.index_id().generation()
                    })
                {
                    candidate_key = Some(raw.clone());
                    return Ok::<_, InternalError>(IndexStoreVisit::Stop);
                }
                Ok(IndexStoreVisit::Continue)
            })
        })
        .expect("candidate edges should scan");
    let candidate_key =
        candidate_key.expect("transformed relation must have a candidate generation");
    assert!(
        store
            .with_index_mut(|index| index.remove(&candidate_key))
            .is_some()
    );
    let error =
        advance(&db, &proposal).expect_err("missing candidate witness must prevent publication");
    assert_eq!(
        error.diagnostic().detail(),
        Some(&icydb_diagnostic_code::DiagnosticDetail::SchemaMigration {
            reason: icydb_diagnostic_code::SchemaMigrationCode::CandidateMismatch,
        })
    );
    assert_hidden(&db, &proposal, &before);
    ensure_schema_migration_ready_for_ordinary_operations()
        .expect_err("failed final proof must retain the migration gate");
}
