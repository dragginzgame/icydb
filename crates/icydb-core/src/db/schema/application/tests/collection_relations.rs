//! Collection relations admitted through the public schema proposal boundary.

use super::*;
use crate::db::DynamicQuery;
use icydb_schema::{RelationDeleteAction, RelationFragment, RelationSourceFragment};

#[test]
fn direct_collection_relation_admits_and_enforces_targets() {
    assert_collection_relation(false);
}

#[test]
fn nullable_direct_collection_relation_admits_and_enforces_targets() {
    assert_collection_relation(true);
}

fn collection_proposal(
    target: &crate::db::schema::SchemaApplicationTarget,
    nullable: bool,
) -> SchemaProposal {
    let id = FieldSourceKey::try_new("id").unwrap();
    let resources = FieldSourceKey::try_new("resource_ids").unwrap();
    let resource = EntitySourceKey::try_new("Resource").unwrap();
    let location = EntitySourceKey::try_new("Location").unwrap();
    let id_field = FieldFragment::new(
        name("id"),
        FieldType::Scalar(ScalarType::Ulid),
        false,
        FieldInsertPolicy::Required,
        None,
    );
    let entities = vec![
        EntityFragment::try_new(
            name("Resource"),
            version_one(),
            vec![id_field.clone()],
            vec![id.clone()],
            vec![],
            vec![],
            vec![],
        )
        .unwrap(),
        EntityFragment::try_new(
            name("Location"),
            version_one(),
            vec![
                id_field,
                FieldFragment::new(
                    name("resource_ids"),
                    FieldType::List(Box::new(FieldType::Scalar(ScalarType::Ulid))),
                    nullable,
                    if nullable {
                        FieldInsertPolicy::Nullable
                    } else {
                        FieldInsertPolicy::Required
                    },
                    None,
                ),
            ],
            vec![id.clone()],
            vec![],
            vec![
                RelationFragment::try_new(
                    name("resource_ids"),
                    RelationSourceFragment::direct(vec![resources]),
                    resource.clone(),
                    vec![id],
                    RelationDeleteAction::Restrict,
                )
                .unwrap(),
            ],
            vec![],
        )
        .unwrap(),
    ];
    let store = target.stores()[0].identity();
    SchemaProposal::try_compose(
        vec![
            SchemaCapability::EXACT_COMPOSITE_TYPES,
            SchemaCapability::RESTRICTIVE_RELATIONS,
        ],
        target.database_identity(),
        SchemaSubmissionKey::try_new("collection_relation").unwrap(),
        target.accepted_head().clone(),
        vec![SchemaFragment::try_new(entities, vec![]).unwrap()],
        vec![
            EntityStoreAssignment::new(resource, store),
            EntityStoreAssignment::new(location, store),
        ],
        vec![],
        None,
    )
    .unwrap()
}

fn ulid(value: u128) -> InputValue {
    InputValue::ulid(crate::types::Ulid::from_u128(value))
}

fn assert_collection_relation(nullable: bool) {
    let root = crate::db::RequestExecutionRoot::__new_runtime_root();
    let db = Db::<AbortCanister>::new(&ABORT_REGISTRY, root.scope());
    drive_startup_recovery_to_completion(&db);
    let target = schema_application_target(&db).expect("application target");
    let proposal = collection_proposal(&target, nullable);
    apply_schema(&db, &proposal).expect("direct collection schema should publish");
    let session = DbSession::<AbortCanister>::new(&ABORT_REGISTRY, &root);
    let query = DynamicQuery::new("Resource").select(["id"]).limit(10);
    assert!(
        session
            .execute_trusted_live_page(&query, None)
            .expect("first query should admit")
            .rows
            .is_empty()
    );
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "Resource".to_string(),
            patch: DynamicStructuralPatch::new(vec![(
                "id".to_string(),
                DynamicWriteCell::Value(ulid(1)),
            )]),
        })
        .expect("first operation should admit the collection relation");

    let insert = |id, targets| {
        session.execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: "Location".to_string(),
            patch: DynamicStructuralPatch::new(vec![
                ("id".to_string(), DynamicWriteCell::Value(ulid(id))),
                ("resource_ids".to_string(), DynamicWriteCell::Value(targets)),
            ]),
        })
    };
    let error = insert(10, InputValue::list(vec![ulid(1), ulid(2)]))
        .expect_err("missing targets must reject atomically");
    assert_relation_failure(&error);
    insert(10, InputValue::list(vec![ulid(1), ulid(1)]))
        .expect("valid duplicate references should insert");
    insert(11, InputValue::list(vec![])).expect("empty collection should insert");
    if nullable {
        insert(12, InputValue::null()).expect("null collection should insert");
    }
    let query = DynamicQuery::new("Location")
        .select(["id", "resource_ids"])
        .order_by(crate::db::asc("id"))
        .limit(10);
    let mut rows = Vec::new();
    let mut continuation = None;
    for _ in 0..4 {
        let page = session
            .execute_trusted_live_page(&query, continuation.as_deref())
            .expect("collection rows should query");
        rows.extend(page.rows);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert!(continuation.is_none(), "bounded fixture query must exhaust");
    assert_eq!(rows.len(), if nullable { 3 } else { 2 });
    assert_eq!(
        rows[0][1],
        crate::value::OutputValue::from_public(
            InputValue::list(vec![ulid(1), ulid(1)]).into_public()
        )
    );
    let delete = || {
        session.execute_trusted_dynamic_mutation(&DynamicMutation::Delete {
            entity: "Resource".to_string(),
            key: ulid(1),
        })
    };
    assert_relation_failure(&delete().expect_err("referenced target deletion must reject"));
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: "Location".to_string(),
            key: ulid(10),
            patch: DynamicStructuralPatch::new(vec![(
                "resource_ids".to_string(),
                DynamicWriteCell::Value(if nullable {
                    InputValue::null()
                } else {
                    InputValue::list(vec![])
                }),
            )]),
        })
        .expect("clearing references should update the reverse index");
    delete().expect("cleared references must release target deletion");
}

fn assert_relation_failure(error: &crate::error::InternalError) {
    assert!(error.diagnostic_facts().contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
    )));
}
