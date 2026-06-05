use crate::{
    db::schema::{
        FieldId, PersistedFieldKind, PersistedIndexExpressionOp, PersistedIndexKeyItemSnapshot,
        PersistedIndexKeySnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaVersion,
        compiled_schema_proposal_for_model,
    },
    model::{
        entity::{EntityModel, PrimaryKeyModel, RelationEdgeModel},
        field::{
            FieldDatabaseDefault, FieldKind, FieldModel, FieldStorageDecode, LeafCodec, ScalarCodec,
        },
        index::{IndexExpression, IndexKeyItem, IndexModel},
    },
    testing::entity_model_from_static,
};

static PROFILE_FIELDS: [FieldModel; 2] = [
    FieldModel::generated("nickname", FieldKind::Text { max_len: None }),
    FieldModel::generated("score", FieldKind::Nat64),
];
static FIELDS: [FieldModel; 4] = [
    FieldModel::generated("id", FieldKind::Ulid),
    FieldModel::generated_with_storage_decode_and_nullability(
        "name",
        FieldKind::Text { max_len: None },
        FieldStorageDecode::ByKind,
        true,
    ),
    FieldModel::generated("rank", FieldKind::Nat64),
    FieldModel::generated_with_storage_decode_nullability_write_policies_and_nested_fields(
        "profile",
        FieldKind::Structured { queryable: true },
        FieldStorageDecode::Value,
        false,
        None,
        None,
        &PROFILE_FIELDS,
    ),
];
static NAME_INDEX: IndexModel =
    IndexModel::generated_with_ordinal(1, "idx_entity__name", "entity::name", &["name"], false);
static PROFILE_NICKNAME_INDEX: IndexModel = IndexModel::generated_with_ordinal(
    2,
    "idx_entity__profile_nickname",
    "entity::profile_nickname",
    &["profile.nickname"],
    false,
);
static EXPRESSION_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("name"))];
static EXPRESSION_INDEX: IndexModel = IndexModel::generated_with_ordinal_and_key_items(
    3,
    "idx_entity__lower_name",
    "entity::lower_name",
    &["name"],
    &EXPRESSION_KEY_ITEMS,
    false,
);
static INDEXES: [&IndexModel; 3] = [&NAME_INDEX, &PROFILE_NICKNAME_INDEX, &EXPRESSION_INDEX];
static RELATION_LOCAL_FIELDS: [&FieldModel; 1] = [&FIELDS[2]];
static RELATIONS: [RelationEdgeModel; 1] = [RelationEdgeModel::generated(
    "score_owner",
    "schema::proposal::tests::ScoreOwner",
    &RELATION_LOCAL_FIELDS,
)];
static MODEL: EntityModel = entity_model_from_static(
    "schema::proposal::tests::Entity",
    "Entity",
    &FIELDS[0],
    0,
    &FIELDS,
    &INDEXES,
);
static RELATION_MODEL: EntityModel = EntityModel::generated_with_primary_key_model_and_relations(
    "schema::proposal::tests::RelationEntity",
    "RelationEntity",
    1,
    PrimaryKeyModel::scalar(&FIELDS[0]),
    0,
    &FIELDS,
    &INDEXES,
    &RELATIONS,
);
static VERSIONED_MODEL: EntityModel = EntityModel::generated(
    "schema::proposal::tests::VersionedEntity",
    "VersionedEntity",
    4,
    &FIELDS[0],
    0,
    &FIELDS,
    &INDEXES,
);
static COMPOSITE_PRIMARY_KEY_FIELDS: [&FieldModel; 2] = [&FIELDS[0], &FIELDS[2]];
static COMPOSITE_MODEL: EntityModel = EntityModel::generated_with_primary_key_model(
    "schema::proposal::tests::CompositeEntity",
    "CompositeEntity",
    1,
    PrimaryKeyModel::ordered(&COMPOSITE_PRIMARY_KEY_FIELDS),
    0,
    &FIELDS,
    &INDEXES,
);

#[test]
fn compiled_schema_proposal_assigns_initial_field_ids_from_slots() {
    let proposal = compiled_schema_proposal_for_model(&MODEL);

    assert_eq!(proposal.entity_path(), "schema::proposal::tests::Entity");
    assert_eq!(proposal.entity_name(), "Entity");
    assert_eq!(proposal.declared_schema_version(), SchemaVersion::initial());
    assert_eq!(proposal.first_primary_key_field_id(), FieldId::new(1));
    assert_eq!(proposal.primary_key_field_ids(), &[FieldId::new(1)]);
    assert_eq!(proposal.fields().len(), 4);
    assert_eq!(
        proposal.indexes().len(),
        3,
        "field-path and expression indexes should both have accepted-index proposals",
    );

    let ids = proposal
        .fields()
        .iter()
        .map(super::CompiledFieldProposal::id)
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        vec![
            FieldId::new(1),
            FieldId::new(2),
            FieldId::new(3),
            FieldId::new(4),
        ],
    );
}

#[test]
fn compiled_schema_proposal_carries_declared_schema_version() {
    let proposal = compiled_schema_proposal_for_model(&VERSIONED_MODEL);
    let snapshot = proposal.initial_persisted_schema_snapshot();

    assert_eq!(proposal.declared_schema_version(), SchemaVersion::new(4));
    assert_eq!(
        proposal.initial_row_layout().version(),
        SchemaVersion::new(4)
    );
    assert_eq!(snapshot.version(), SchemaVersion::new(4));
    assert_eq!(snapshot.row_layout().version(), SchemaVersion::new(4));
}

#[test]
fn compiled_schema_proposal_preserves_ordered_primary_key_field_ids() {
    let proposal = compiled_schema_proposal_for_model(&COMPOSITE_MODEL);

    assert_eq!(proposal.first_primary_key_field_id(), FieldId::new(1));
    assert_eq!(
        proposal.primary_key_field_ids(),
        &[FieldId::new(1), FieldId::new(3)],
    );
}

#[test]
fn compiled_schema_proposal_preserves_generated_relation_edges() {
    let proposal = compiled_schema_proposal_for_model(&RELATION_MODEL);

    assert_eq!(proposal.relations().len(), 1);
    assert_eq!(proposal.relations()[0].name(), "score_owner");
    assert_eq!(
        proposal.relations()[0].target_path(),
        "schema::proposal::tests::ScoreOwner"
    );
    assert_eq!(
        proposal.relations()[0].local_field_ids(),
        &[FieldId::new(3)]
    );

    let snapshot = proposal.initial_persisted_schema_snapshot();
    assert_eq!(snapshot.relations().len(), 1);
    assert_eq!(snapshot.relations()[0].name(), "score_owner");
    assert_eq!(
        snapshot.relations()[0].target_path(),
        "schema::proposal::tests::ScoreOwner"
    );
    assert_eq!(
        snapshot.relations()[0].local_field_ids(),
        &[FieldId::new(3)]
    );
}

#[test]
fn compiled_schema_proposal_preserves_generated_storage_contracts() {
    let proposal = compiled_schema_proposal_for_model(&MODEL);
    let name = &proposal.fields()[1];

    assert_eq!(name.name(), "name");
    assert_eq!(name.slot(), SchemaFieldSlot::from_generated_index(1));
    std::assert_matches!(name.kind(), FieldKind::Text { max_len: None });
    assert!(name.nullable());
    assert_eq!(name.database_default(), FieldDatabaseDefault::None);
    assert_eq!(name.storage_decode(), FieldStorageDecode::ByKind);
    assert_eq!(name.leaf_codec(), LeafCodec::Scalar(ScalarCodec::Text));
}

#[test]
fn compiled_schema_proposal_builds_initial_row_layout() {
    let proposal = compiled_schema_proposal_for_model(&MODEL);
    let layout = proposal.initial_row_layout();

    assert_eq!(layout.version(), SchemaVersion::initial());
    assert_eq!(
        layout.field_to_slot(),
        &[
            (FieldId::new(1), SchemaFieldSlot::from_generated_index(0)),
            (FieldId::new(2), SchemaFieldSlot::from_generated_index(1)),
            (FieldId::new(3), SchemaFieldSlot::from_generated_index(2)),
            (FieldId::new(4), SchemaFieldSlot::from_generated_index(3)),
        ]
    );
}

#[test]
fn compiled_schema_proposal_builds_initial_persisted_snapshot() {
    let proposal = compiled_schema_proposal_for_model(&MODEL);
    let snapshot = proposal.initial_persisted_schema_snapshot();

    assert_eq!(snapshot.version(), SchemaVersion::initial());
    assert_eq!(snapshot.entity_path(), "schema::proposal::tests::Entity");
    assert_eq!(snapshot.entity_name(), "Entity");
    assert_eq!(snapshot.first_primary_key_field_id(), FieldId::new(1));
    assert_eq!(snapshot.fields().len(), 4);
    assert_eq!(snapshot.indexes().len(), 3);

    let name = &snapshot.fields()[1];
    assert_eq!(name.id(), FieldId::new(2));
    assert_eq!(name.name(), "name");
    assert_eq!(name.slot(), SchemaFieldSlot::from_generated_index(1));
    std::assert_matches!(name.kind(), PersistedFieldKind::Text { max_len: None });
    assert!(name.nullable());
    assert_eq!(name.default(), &SchemaFieldDefault::None);
    assert_eq!(name.storage_decode(), FieldStorageDecode::ByKind);
    assert_eq!(name.leaf_codec(), LeafCodec::Scalar(ScalarCodec::Text));

    let profile = &snapshot.fields()[3];
    assert_eq!(profile.name(), "profile");
    assert_eq!(profile.nested_leaves().len(), 2);
    assert_eq!(profile.nested_leaves()[0].path(), &["nickname".to_string()],);
    std::assert_matches!(
        profile.nested_leaves()[0].kind(),
        PersistedFieldKind::Text { max_len: None }
    );
    assert_eq!(profile.nested_leaves()[1].path(), &["score".to_string()]);
    std::assert_matches!(profile.nested_leaves()[1].kind(), PersistedFieldKind::Nat64);

    let name_index = &snapshot.indexes()[0];
    assert_eq!(name_index.ordinal(), 1);
    assert_eq!(name_index.name(), "idx_entity__name");
    assert!(!name_index.unique());
    assert_eq!(name_index.key().field_paths().len(), 1);
    assert_eq!(
        name_index.key().field_paths()[0].field_id(),
        FieldId::new(2)
    );
    assert_eq!(
        name_index.key().field_paths()[0].slot(),
        SchemaFieldSlot::from_generated_index(1)
    );
    assert_eq!(
        name_index.key().field_paths()[0].path(),
        &["name".to_string()]
    );

    let nested_index = &snapshot.indexes()[1];
    assert_eq!(nested_index.name(), "idx_entity__profile_nickname");
    assert_eq!(
        nested_index.key().field_paths()[0].field_id(),
        FieldId::new(4)
    );
    assert_eq!(
        nested_index.key().field_paths()[0].path(),
        &["profile".to_string(), "nickname".to_string()]
    );
    std::assert_matches!(
        nested_index.key().field_paths()[0].kind(),
        PersistedFieldKind::Text { max_len: None }
    );

    let expression_index = &snapshot.indexes()[2];
    assert_eq!(expression_index.ordinal(), 3);
    assert_eq!(expression_index.name(), "idx_entity__lower_name");
    let PersistedIndexKeySnapshot::Items(items) = expression_index.key() else {
        panic!("expression index should preserve explicit key items");
    };
    assert_eq!(items.len(), 1);
    let PersistedIndexKeyItemSnapshot::Expression(expression) = &items[0] else {
        panic!("expression index key should persist an accepted expression item");
    };
    assert_eq!(expression.op(), PersistedIndexExpressionOp::Lower);
    assert_eq!(expression.source().field_id(), FieldId::new(2));
    assert_eq!(
        expression.source().slot(),
        SchemaFieldSlot::from_generated_index(1)
    );
    assert_eq!(expression.source().path(), &["name".to_string()]);
    std::assert_matches!(
        expression.input_kind(),
        PersistedFieldKind::Text { max_len: None }
    );
    std::assert_matches!(
        expression.output_kind(),
        PersistedFieldKind::Text { max_len: None }
    );
    assert_eq!(expression.canonical_text(), "expr:v1:LOWER(name)");
}
