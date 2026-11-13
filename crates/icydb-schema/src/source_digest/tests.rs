use crate::{
    DeclaredEntityVersion, EntityFragment, EntityMigration, EntitySourceKey, EntityStoreAssignment,
    ExpectedAcceptedHead, FieldFragment, FieldInsertPolicy, FieldSourceKey, FieldType,
    NamedTypeFragment, RelationDeleteAction, RelationFragment, RelationSourceFragment, ScalarType,
    SchemaCapability, SchemaFragment, SchemaMigrationPlan, SchemaName, SchemaProposal,
    SchemaSubmissionKey, TargetDatabaseIdentity, TargetStoreIdentity, TypeSourceKey,
};

fn proposal(version: u32, field_name: &str) -> SchemaProposal {
    let id = FieldFragment::new(
        SchemaName::try_new(field_name).expect("field name should admit"),
        FieldType::Scalar(crate::ScalarType::Nat64),
        false,
        FieldInsertPolicy::Required,
        None,
    );
    let entity = EntityFragment::try_new(
        SchemaName::try_new("User").expect("entity name should admit"),
        DeclaredEntityVersion::try_new(version).expect("version should admit"),
        vec![id],
        vec![FieldSourceKey::try_new(field_name).expect("field key should admit")],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .expect("entity should admit");
    SchemaProposal::try_compose(
        Vec::new(),
        TargetDatabaseIdentity::from_bytes([1; 32]),
        SchemaSubmissionKey::try_new("source-digest").expect("submission should admit"),
        ExpectedAcceptedHead::Empty,
        vec![SchemaFragment::try_new(vec![entity], Vec::new()).expect("fragment should admit")],
        vec![EntityStoreAssignment::new(
            EntitySourceKey::try_new("User").expect("entity key should admit"),
            TargetStoreIdentity::from_bytes([2; 32]),
        )],
        Vec::new(),
        None,
    )
    .expect("proposal should admit")
}

#[test]
fn source_digest_ignores_declared_version_but_not_entity_meaning() {
    let source = EntitySourceKey::try_new("User").expect("source should admit");
    assert_eq!(
        proposal(1, "id")
            .entity_source_digest(&source)
            .expect("digest should derive"),
        proposal(7, "id")
            .entity_source_digest(&source)
            .expect("digest should derive"),
    );
    assert_ne!(
        proposal(1, "id")
            .entity_source_digest(&source)
            .expect("digest should derive"),
        proposal(1, "other")
            .entity_source_digest(&source)
            .expect("digest should derive"),
    );
}

// Zebra -> Alpha crosses Middle in canonical target order. Payload keeps a
// reachable named-type contract in the owner's source meaning.
fn dependency_proposal(
    renamed: bool,
    payload: ScalarType,
    target: &FieldInsertPolicy,
) -> SchemaProposal {
    let name = |value| SchemaName::try_new(value).unwrap();
    let key = |value| EntitySourceKey::try_new(value).unwrap();
    let field = |value| FieldSourceKey::try_new(value).unwrap();
    let id = |policy| {
        FieldFragment::new(
            name("id"),
            FieldType::Scalar(ScalarType::Nat64),
            false,
            policy,
            None,
        )
    };
    let target_name = if renamed { "Alpha" } else { "Zebra" };
    let mut entities = Vec::new();
    for entity in [target_name, "Middle", "Holder"] {
        let mut fields = vec![id(if entity == target_name {
            target.clone()
        } else {
            FieldInsertPolicy::Required
        })];
        let relations = if entity == "Holder" {
            fields.push(FieldFragment::new(
                name("payload"),
                FieldType::Named(TypeSourceKey::try_new("Payload").unwrap()),
                false,
                FieldInsertPolicy::Required,
                None,
            ));
            [target_name, "Middle"]
                .into_iter()
                .enumerate()
                .map(|(ordinal, target)| {
                    RelationFragment::try_new(
                        name(if ordinal == 0 { "first" } else { "second" }),
                        RelationSourceFragment::direct(vec![field("id")]),
                        key(target),
                        vec![field("id")],
                        RelationDeleteAction::Restrict,
                    )
                    .unwrap()
                })
                .collect()
        } else {
            Vec::new()
        };
        entities.push(
            EntityFragment::try_new(
                name(entity),
                DeclaredEntityVersion::try_new(if renamed && entity != "Middle" { 2 } else { 1 })
                    .unwrap(),
                fields,
                vec![field("id")],
                Vec::new(),
                relations,
                Vec::new(),
            )
            .unwrap(),
        );
    }
    let migration = renamed.then(|| {
        SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                key("Alpha"),
                DeclaredEntityVersion::try_new(1).unwrap(),
                Some(key("Zebra")),
                Vec::new(),
                Vec::new(),
            )
            .unwrap(),
            EntityMigration::try_new(
                key("Holder"),
                DeclaredEntityVersion::try_new(1).unwrap(),
                None,
                Vec::new(),
                Vec::new(),
            )
            .unwrap(),
        ])
        .unwrap()
    });
    let assignments = entities
        .iter()
        .map(|entity| {
            EntityStoreAssignment::new(
                entity.source_key().clone(),
                TargetStoreIdentity::from_bytes([2; 32]),
            )
        })
        .collect();
    let mut capabilities = vec![SchemaCapability::RESTRICTIVE_RELATIONS];
    if renamed {
        capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
    }
    SchemaProposal::try_compose(
        capabilities,
        TargetDatabaseIdentity::from_bytes([1; 32]),
        SchemaSubmissionKey::try_new("dependency").unwrap(),
        ExpectedAcceptedHead::Empty,
        vec![
            SchemaFragment::try_new(
                entities,
                vec![NamedTypeFragment::newtype(
                    name("Payload"),
                    FieldType::Scalar(payload),
                )],
            )
            .unwrap(),
        ],
        assignments,
        Vec::new(),
        migration,
    )
    .unwrap()
}

#[test]
fn entity_rename_dependency_proof_preserves_canonical_order_and_complete_meaning() {
    let holder = EntitySourceKey::try_new("Holder").unwrap();
    let before = dependency_proposal(false, ScalarType::Nat64, &FieldInsertPolicy::Required)
        .entity_source_digest(&holder)
        .unwrap();
    let renamed = dependency_proposal(true, ScalarType::Nat64, &FieldInsertPolicy::Required);
    assert_ne!(before, renamed.entity_source_digest(&holder).unwrap());
    assert_eq!(
        before,
        renamed
            .entity_source_digest_before_entity_renames(&holder)
            .unwrap()
    );
    for (payload, target) in [
        (ScalarType::Nat32, FieldInsertPolicy::Required),
        (
            ScalarType::Nat64,
            FieldInsertPolicy::Default(crate::ScalarLiteral::Nat(1)),
        ),
    ] {
        assert_ne!(
            before,
            dependency_proposal(true, payload, &target)
                .entity_source_digest_before_entity_renames(&holder)
                .unwrap()
        );
    }
}
