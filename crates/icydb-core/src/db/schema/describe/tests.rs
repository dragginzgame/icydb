use std::collections::BTreeMap;

use super::{
    EntityFieldDescription, EntityIdentityDescription, EntityRelationCardinality,
    EntityRelationDescription, MAX_SCHEMA_VALUE_RENDER_CHARS, SqlColumnDefault, SqlColumnExtra,
    SqlColumnKey, SqlColumnSummary, SqlDescribeOutput, SqlShowRelationsOutput,
    classify_compact_column_key, compact_column_capacity_from_counts, compact_column_extras,
    describe_accepted_constraint, describe_compact_columns_with_persisted_schema,
    describe_constraint_activation, describe_entity_constraints_with_persisted_schema,
    describe_entity_fields_with_persisted_schema, describe_entity_relations_with_persisted_schema,
    nested_path_nullable, persisted_relation_cardinality,
};
use crate::db::schema::{
    AcceptedCheckExprV1, AcceptedCheckValueExprV1, AcceptedCompositeCatalog,
    AcceptedConstraintCatalog, AcceptedConstraintKind, AcceptedConstraintSnapshot,
    AcceptedFieldKind, AcceptedIndexPredicate, AcceptedNamedTypeIdentity, AcceptedRuleOperation,
    AcceptedRuleTarget, AcceptedSchemaFingerprint, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
    AcceptedValueCatalogHandle, CompositeFieldId, CompositeTypeId, ConstraintActivationKind,
    ConstraintActivationSnapshot, ConstraintActivationState, ConstraintId, ConstraintOrigin,
    ConstraintValidationJob, FieldId, FieldStorageDecode, LeafCodec, MAX_SCHEMA_SNAPSHOT_BYTES,
    PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot,
    PersistedIndexSnapshot, PersistedNestedLeafSnapshot, PersistedRelationEdgeSnapshot,
    PersistedRelationPathStepSnapshot, PersistedSchemaSnapshot, RelationId, ScalarCodec,
    SchemaFieldSlot, SchemaIndexId, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
    composite_catalog::{
        AcceptedCompositeElement, AcceptedCompositeField, AcceptedCompositeShape,
        decode_accepted_composite_catalog, encode_accepted_composite_catalog,
    },
    decode_persisted_schema_snapshot, empty_accepted_enum_catalog_for_tests,
    encode_persisted_schema_snapshot,
};

const REACHABLE_COMPACT_REPLY_COMPOSITE_FIELDS: usize = icydb_schema::MAX_FRAGMENT_FIELDS;
const REACHABLE_COMPACT_REPLY_TOP_LEVEL_COMPOSITES: usize = 95;
const IC_QUERY_REPLY_BYTES: usize = 3 * 1024 * 1024;

#[test]
fn field_kind_summaries_bound_cycles_and_reused_type_expansion() {
    for (levels, cycle) in [(1, false), (1, true), (20, false)] {
        let definitions = (1..=levels)
            .map(|level| {
                let kind = if cycle {
                    AcceptedFieldKind::Composite {
                        type_id: CompositeTypeId::new(1).unwrap(),
                    }
                } else if level == levels {
                    AcceptedFieldKind::Nat64
                } else {
                    AcceptedFieldKind::Composite {
                        type_id: CompositeTypeId::new(level + 1).unwrap(),
                    }
                };
                (
                    CompositeTypeId::new(level).unwrap(),
                    (
                        format!("tests::T{level}"),
                        AcceptedCompositeShape::Tuple(vec![
                            AcceptedCompositeElement::new(kind.clone(), false),
                            AcceptedCompositeElement::new(kind, false),
                        ]),
                    ),
                )
            })
            .collect();
        let enums = empty_accepted_enum_catalog_for_tests();
        let composites =
            AcceptedCompositeCatalog::from_initial_definitions(definitions, &enums).unwrap();
        let values = AcceptedValueCatalogHandle::new_for_tests(
            enums,
            composites,
            AcceptedSchemaRevision::INITIAL,
        );
        let kind = AcceptedFieldKind::Composite {
            type_id: CompositeTypeId::new(1).unwrap(),
        };
        let schema = summary_schema(kind.clone());
        for result in [
            describe_entity_fields_with_persisted_schema(&schema, &values).map(|_| ()),
            describe_compact_columns_with_persisted_schema(&schema, &values).map(|_| ()),
        ] {
            if levels == 1 && !cycle {
                result.unwrap();
            } else {
                assert_eq!(
                    result.unwrap_err().class(),
                    crate::error::ErrorClass::Unsupported
                );
            }
        }
        let result = super::summarize_persisted_field_kind(&kind, &values);
        if levels == 1 && !cycle {
            assert_eq!(
                result.unwrap(),
                "composite(path=tests::T1, codec=structural_v1, shape=tuple<nat64, nat64>)"
            );
        } else {
            assert_eq!(
                result.unwrap_err().class(),
                crate::error::ErrorClass::Unsupported
            );
        }
    }
}

fn summary_schema(kind: AcceptedFieldKind) -> AcceptedSchemaSnapshot {
    let (snapshot, _) = constraint_description_fixture();
    let field = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "value".into(),
        SchemaFieldSlot::new(1),
        kind,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    );
    let snapshot = snapshot
        .clone()
        .with_fields(vec![snapshot.fields()[0].clone(), field])
        .with_indexes(Vec::new());
    let constraints = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .unwrap();
    AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraints)).unwrap()
}

fn constraint_description_fixture() -> (PersistedSchemaSnapshot, AcceptedValueCatalogHandle) {
    let snapshot = PersistedSchemaSnapshot::new_with_indexes(
        SchemaVersion::initial(),
        "tests::Account".to_string(),
        "Account".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(vec![
            (FieldId::new(1), SchemaFieldSlot::new(0)),
            (FieldId::new(2), SchemaFieldSlot::new(1)),
        ]),
        vec![
            PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            ),
            PersistedFieldSnapshot::new_initial(
                FieldId::new(2),
                "email".to_string(),
                SchemaFieldSlot::new(1),
                AcceptedFieldKind::Text { max_len: None },
                Vec::new(),
                true,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).expect("test index identity should be non-zero"),
            1,
            "account_email".to_string(),
            "tests::Account::account_email".to_string(),
            true,
            PersistedIndexKeySnapshot::from_field_paths(vec![
                PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(2),
                    SchemaFieldSlot::new(1),
                    vec!["email".to_string()],
                    AcceptedFieldKind::Text { max_len: None },
                    true,
                ),
            ]),
            Some(crate::db::schema::AcceptedIndexPredicate::test_non_null(2)),
        )],
    );
    let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    (snapshot, value_catalog)
}

#[test]
fn filtered_unique_constraint_description_exposes_partial_backing_contract() {
    let (snapshot, value_catalog) = constraint_description_fixture();
    let constraint = snapshot
        .constraints()
        .iter()
        .find(|constraint| constraint.name() == "account_email")
        .expect("unique constraint should exist");

    let description = describe_accepted_constraint(&snapshot, &value_catalog, constraint)
        .expect("accepted unique constraint should describe");
    assert_eq!(description.index_id(), Some(1));
    assert_eq!(description.index(), Some("account_email"));
    assert_eq!(description.predicate_sql(), Some("email IS NOT NULL"));
    assert_eq!(description.semantics(), "partial_unique_index_v1");
}

#[test]
fn constraint_descriptions_preserve_live_progress_and_reject_unbound_jobs() {
    let (snapshot, values) = constraint_description_fixture();
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check_activation(
            "pending_check".into(),
            ConstraintOrigin::SqlDdl,
            AcceptedCheckExprV1::True,
            AcceptedSchemaFingerprint::new([1; 32]),
            7,
        )
        .unwrap();
    let id = catalog.activations()[0].id();
    let catalog = catalog.with_validation_started(id).unwrap();
    let job = ConstraintValidationJob::start(
        crate::types::EntityTag::new(1),
        snapshot.entity_path().into(),
        &catalog.activations()[0],
        None,
    )
    .unwrap();
    let schema = AcceptedSchemaSnapshot::new(snapshot.clone().with_constraint_catalog(catalog));
    let descriptions = describe_entity_constraints_with_persisted_schema(
        &schema,
        &values,
        std::slice::from_ref(&job),
    )
    .unwrap();
    assert!(
        descriptions
            .windows(2)
            .all(|pair| pair[0].id() < pair[1].id())
    );
    let active = descriptions
        .iter()
        .find(|row| row.id() == id.get())
        .unwrap();
    assert_eq!(active.validation_state(), "validating");
    assert_eq!(active.validation_progress().unwrap().phase(), "forward");
    assert_eq!(active.validation_progress().unwrap().rows_scanned(), 0);

    let missing_job =
        describe_entity_constraints_with_persisted_schema(&schema, &values, &[]).unwrap_err();
    assert_eq!(
        missing_job.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
    );
    let unbound_job = describe_entity_constraints_with_persisted_schema(
        &AcceptedSchemaSnapshot::new(snapshot),
        &values,
        &[job],
    )
    .unwrap_err();
    assert_eq!(
        unbound_job.diagnostic_code(),
        icydb_diagnostic_code::DiagnosticCode::StoreInvariantViolation,
    );
}

#[test]
fn row_local_constraint_descriptions_preserve_meaning_across_activation() {
    let (snapshot, values) = constraint_description_fixture();
    let field_id = FieldId::new(2);
    let expression = Box::new(AcceptedCheckExprV1::IsNotNull(
        AcceptedCheckValueExprV1::Field(field_id),
    ));
    let target = AcceptedRuleTarget::new(
        field_id,
        AcceptedNamedTypeIdentity::Composite(CompositeTypeId::new(1).unwrap()),
    );
    let operation = Box::new(AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 10 });
    for (accepted, activating, semantics) in [
        (
            AcceptedConstraintKind::NotNull { field_id },
            ConstraintActivationKind::NotNull { field_id },
            "not_null_v1",
        ),
        (
            AcceptedConstraintKind::Check {
                expression: expression.clone(),
            },
            ConstraintActivationKind::Check { expression },
            "check_expr_v1",
        ),
        (
            AcceptedConstraintKind::TargetedRule {
                target,
                operation: operation.clone(),
            },
            ConstraintActivationKind::TargetedRule { target, operation },
            "targeted_length_range_v1",
        ),
    ] {
        assert_constraint_description_lifecycle(
            &snapshot, &values, accepted, activating, semantics,
        );
    }
}

fn assert_constraint_description_lifecycle(
    snapshot: &PersistedSchemaSnapshot,
    values: &AcceptedValueCatalogHandle,
    accepted: AcceptedConstraintKind,
    activating: ConstraintActivationKind,
    semantics: &str,
) {
    let id = ConstraintId::new(10).unwrap();
    let constraint =
        AcceptedConstraintSnapshot::new(id, "rule".into(), ConstraintOrigin::Generated, accepted);
    let mut expected = describe_accepted_constraint(snapshot, values, &constraint).unwrap();
    assert_eq!(expected.semantics(), semantics);
    assert_eq!(expected.fields(), &["email"]);
    assert_eq!(expected.validation_state(), "validated");
    for state in [
        ConstraintActivationState::EnforcingNewWrites,
        ConstraintActivationState::Validating,
    ] {
        let activation = ConstraintActivationSnapshot::new(
            id,
            "rule".into(),
            ConstraintOrigin::Generated,
            activating.clone(),
            state,
            AcceptedSchemaFingerprint::new([1; 32]),
            1,
        );
        let job = (state == ConstraintActivationState::Validating).then(|| {
            ConstraintValidationJob::start(
                crate::types::EntityTag::new(1),
                snapshot.entity_path().into(),
                &activation,
                None,
            )
            .unwrap()
        });
        if job.is_some() {
            assert!(describe_constraint_activation(snapshot, values, &activation, None).is_err());
        }
        let description =
            describe_constraint_activation(snapshot, values, &activation, job.as_ref()).unwrap();
        assert_eq!(
            description.validation_state(),
            if job.is_some() {
                "validating"
            } else {
                "enforcing_new_writes"
            }
        );
        assert_eq!(description.validation_progress().is_some(), job.is_some());
        expected
            .validation_state
            .clone_from(&description.validation_state);
        expected
            .validation_progress
            .clone_from(&description.validation_progress);
        assert_eq!(description, expected);
    }
}

#[test]
fn relation_descriptions_use_the_owner_for_their_lifecycle_state() {
    let (snapshot, values) = constraint_description_fixture();
    let relation_id = RelationId::new(1).unwrap();
    let accepted = PersistedRelationEdgeSnapshot::new_direct(
        relation_id,
        "accepted_relation".into(),
        "AcceptedTarget".into(),
        vec![FieldId::new(1)],
    );
    let candidate = PersistedRelationEdgeSnapshot::new_direct(
        relation_id,
        "candidate_relation".into(),
        "CandidateTarget".into(),
        vec![FieldId::new(2)],
    );
    let snapshot = snapshot
        .with_relations(vec![accepted])
        .with_constraint_candidates(Vec::new(), vec![candidate]);
    let id = ConstraintId::new(10).unwrap();
    let constraint = AcceptedConstraintSnapshot::new(
        id,
        "relation".into(),
        ConstraintOrigin::SqlDdl,
        AcceptedConstraintKind::Relation { relation_id },
    );
    let activation = ConstraintActivationSnapshot::new(
        id,
        "relation".into(),
        ConstraintOrigin::SqlDdl,
        ConstraintActivationKind::Relation { relation_id },
        ConstraintActivationState::EnforcingNewWrites,
        AcceptedSchemaFingerprint::new([1; 32]),
        1,
    );
    let accepted = describe_accepted_constraint(&snapshot, &values, &constraint).unwrap();
    let candidate = describe_constraint_activation(&snapshot, &values, &activation, None).unwrap();
    assert_eq!(accepted.relation(), Some("accepted_relation"));
    assert_eq!(accepted.target_entity(), Some("AcceptedTarget"));
    assert_eq!(accepted.fields(), &["id"]);
    assert_eq!(candidate.relation(), Some("candidate_relation"));
    assert_eq!(candidate.target_entity(), Some("CandidateTarget"));
    assert_eq!(candidate.fields(), &["email"]);
    assert_eq!(candidate.action(), Some("restrict"));
    assert_eq!(candidate.semantics(), "relation_pk_restrict_v1");
}

#[test]
fn identity_description_reports_exact_remaining_capacity_and_exhaustion() {
    let available = EntityIdentityDescription::new("id".to_string(), "nat8".to_string(), 255, 254)
        .expect("in-domain Identity description should build");
    assert_eq!(available.minimum(), 1);
    assert_eq!(available.maximum(), 255);
    assert_eq!(available.high_water(), 254);
    assert_eq!(available.remaining(), 1);
    assert!(!available.exhausted());

    let exhausted = EntityIdentityDescription::new("id".to_string(), "nat8".to_string(), 255, 255)
        .expect("exact-domain exhaustion should remain describable");
    assert_eq!(exhausted.remaining(), 0);
    assert!(exhausted.exhausted());

    assert!(
        EntityIdentityDescription::new("id".to_string(), "nat8".to_string(), 255, 256).is_err(),
        "state beyond the accepted domain must not be described",
    );
}

#[test]
fn compact_key_contract_distinguishes_single_unique_from_compound_membership() {
    assert_eq!(
        classify_compact_column_key(true, [(true, 1), (false, 2)]),
        SqlColumnKey::Primary
    );
    assert_eq!(
        classify_compact_column_key(false, [(true, 2)]),
        SqlColumnKey::Multiple,
        "compound unique membership must not imply independent uniqueness",
    );
    assert_eq!(
        classify_compact_column_key(false, [(false, 1), (true, 1)]),
        SqlColumnKey::Unique,
        "single-field unique membership has precedence over non-unique membership",
    );
    assert_eq!(
        classify_compact_column_key(false, std::iter::empty()),
        SqlColumnKey::None
    );
}

#[test]
fn descriptive_column_keys_require_unconditional_uniqueness() {
    let (snapshot, values) = constraint_description_fixture();
    // An unconditional unique control needs a non-null source; nullable
    // unique admission deliberately requires an explicit omission guard.
    let email = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "email".into(),
        SchemaFieldSlot::new(1),
        AcceptedFieldKind::Text { max_len: None },
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Text),
    );
    let snapshot = snapshot
        .clone()
        .with_fields(vec![snapshot.fields()[0].clone(), email]);
    let partial = snapshot.indexes()[0]
        .clone_with_top_level_field_nullability(FieldId::new(2), false)
        .clone_with_predicate_for_tests(Some(AcceptedIndexPredicate::bind_test_sql(
            "email <> 'excluded'",
            snapshot.fields(),
        )));
    let key = partial.key().clone();
    let unconditional = |ordinal| {
        PersistedIndexSnapshot::new(
            SchemaIndexId::new(2).unwrap(),
            ordinal,
            "account_all_email".into(),
            "tests::Account::account_all_email".into(),
            true,
            key.clone(),
            None,
        )
    };
    for (indexes, expected) in [
        (vec![partial.clone()], SqlColumnKey::Multiple),
        (vec![unconditional(1)], SqlColumnKey::Unique),
        (vec![partial, unconditional(2)], SqlColumnKey::Unique),
        (Vec::new(), SqlColumnKey::None),
    ] {
        let snapshot = snapshot.clone().with_indexes(indexes);
        let constraints = AcceptedConstraintCatalog::initial(
            snapshot.fields(),
            snapshot.indexes(),
            snapshot.relations(),
        )
        .unwrap();
        let schema = AcceptedSchemaSnapshot::try_new_with_acceptance(
            snapshot.with_constraint_catalog(constraints),
        )
        .unwrap();
        let columns = describe_compact_columns_with_persisted_schema(&schema, &values).unwrap();
        assert_eq!(columns[0].key(), SqlColumnKey::Primary);
        assert_eq!(columns[1].key(), expected);
    }
}

// An accepted record with distinct address.city and city leaves lets the
// compact and verbose projections qualify the same metadata authority.
fn nested_description_fixture() -> (AcceptedSchemaSnapshot, AcceptedValueCatalogHandle) {
    let (snapshot, _) = constraint_description_fixture();
    let profile_id = CompositeTypeId::new(1).unwrap();
    let address_id = CompositeTypeId::new(2).unwrap();
    let text = AcceptedFieldKind::Text { max_len: None };
    let member = |id, name: &str, kind, nullable| {
        AcceptedCompositeField::new(
            CompositeFieldId::new(id).unwrap(),
            name.into(),
            AcceptedCompositeElement::new(kind, nullable),
        )
    };
    let enums = empty_accepted_enum_catalog_for_tests();
    let profile_shape = AcceptedCompositeShape::Record(vec![
        member(
            1,
            "address",
            AcceptedFieldKind::Composite {
                type_id: address_id,
            },
            false,
        ),
        member(2, "city", text.clone(), true),
    ]);
    let address_shape =
        AcceptedCompositeShape::Record(vec![member(1, "city", text.clone(), false)]);
    let composites = AcceptedCompositeCatalog::from_initial_definitions(
        BTreeMap::from([
            (profile_id, ("tests::Profile".into(), profile_shape)),
            (address_id, ("tests::Address".into(), address_shape)),
        ]),
        &enums,
    )
    .unwrap();
    let values = AcceptedValueCatalogHandle::new_for_tests(
        enums,
        composites,
        AcceptedSchemaRevision::INITIAL,
    );
    let leaves = vec![
        PersistedNestedLeafSnapshot::new(
            vec!["address".into(), "city".into()],
            text.clone(),
            false,
        ),
        PersistedNestedLeafSnapshot::new(vec!["city".into()], text, true),
    ];
    let profile = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "profile".into(),
        SchemaFieldSlot::new(1),
        AcceptedFieldKind::Composite {
            type_id: profile_id,
        },
        leaves,
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    );
    let snapshot = snapshot
        .clone()
        .with_fields(vec![snapshot.fields()[0].clone(), profile])
        .with_indexes(Vec::new());
    let constraints = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .unwrap();
    let schema =
        AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraints)).unwrap();

    (schema, values)
}

#[test]
fn descriptive_nested_names_retain_the_complete_relative_path() {
    let (schema, values) = nested_description_fixture();
    let fields = describe_entity_fields_with_persisted_schema(&schema, &values).unwrap();
    let columns = describe_compact_columns_with_persisted_schema(&schema, &values).unwrap();
    assert_eq!(fields[1].name(), "profile");
    for (index, display, canonical) in [
        (2, "├─ address.city", "profile.address.city"),
        (3, "└─ city", "profile.city"),
    ] {
        assert_eq!(fields[index].name(), display);
        assert_eq!(columns[index].name(), canonical);
        assert_eq!(fields[index].slot(), None);
        assert_eq!(fields[index].nullable(), columns[index].nullable());
    }
    assert!(!fields[2].nullable());
    assert!(fields[3].nullable());
}

#[test]
fn descriptive_nested_relations_report_collection_multiplicity() {
    use PersistedRelationPathStepSnapshot::{ListItems, MapValues, OptionalSome, SetItems};
    // Optional/scalar traversal stays single. Any sequence/map fanout is
    // list-like; a path with only set fanout retains the existing Set hint.
    for (steps, expected) in [
        (vec![OptionalSome], EntityRelationCardinality::Single),
        (
            vec![OptionalSome, ListItems],
            EntityRelationCardinality::List,
        ),
        (vec![SetItems], EntityRelationCardinality::Set),
        (vec![MapValues], EntityRelationCardinality::List),
        (vec![SetItems, ListItems], EntityRelationCardinality::List),
        (vec![ListItems, SetItems], EntityRelationCardinality::List),
        (vec![SetItems, SetItems], EntityRelationCardinality::Set),
        (vec![SetItems, MapValues], EntityRelationCardinality::List),
    ] {
        let kind = steps
            .iter()
            .rev()
            .fold(AcceptedFieldKind::Ulid, |kind, step| match step {
                ListItems => AcceptedFieldKind::List(Box::new(kind)),
                SetItems => AcceptedFieldKind::Set(Box::new(kind)),
                MapValues => AcceptedFieldKind::Map {
                    key: Box::new(AcceptedFieldKind::Nat64),
                    value: Box::new(kind),
                },
                OptionalSome => kind,
                _ => unreachable!("fixture contains only collection/optional traversal"),
            });
        let nullable = steps.first() == Some(&OptionalSome);
        let relation = PersistedRelationEdgeSnapshot::new_nested(
            RelationId::new(1).unwrap(),
            "nested_owner".into(),
            "tests::Owner".into(),
            FieldId::new(2),
            steps,
        );
        let schema = relation_description_fixture(kind, nullable, relation);
        let rows = describe_entity_relations_with_persisted_schema(&schema, &|_| {
            Ok(("Owner".into(), "tests::OwnerStore".into()))
        })
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cardinality(), expected);
    }
}

// Keep relation projections rooted in an admitted source shape rather than
// supplying collection steps over an unrelated scalar field.
fn relation_description_fixture(
    kind: AcceptedFieldKind,
    nullable: bool,
    relation: PersistedRelationEdgeSnapshot,
) -> AcceptedSchemaSnapshot {
    let (snapshot, _) = constraint_description_fixture();
    let (storage, codec) = if kind == AcceptedFieldKind::Ulid {
        (
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        )
    } else {
        (FieldStorageDecode::CatalogValue, LeafCodec::Structural)
    };
    let source = PersistedFieldSnapshot::new_initial(
        FieldId::new(2),
        "owner".into(),
        SchemaFieldSlot::new(1),
        kind,
        Vec::new(),
        nullable,
        SchemaInsertDefault::None,
        storage,
        codec,
    );
    let snapshot = snapshot
        .clone()
        .with_fields(vec![snapshot.fields()[0].clone(), source])
        .with_indexes(Vec::new())
        .with_relations(vec![relation]);
    let constraints = AcceptedConstraintCatalog::initial(
        snapshot.fields(),
        snapshot.indexes(),
        snapshot.relations(),
    )
    .unwrap();
    AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraints)).unwrap()
}

#[test]
fn descriptive_direct_relations_preserve_scalar_tuple_list_and_set_hints() {
    for (kind, expected) in [
        (AcceptedFieldKind::Ulid, EntityRelationCardinality::Single),
        (
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Ulid)),
            EntityRelationCardinality::List,
        ),
        (
            AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Ulid)),
            EntityRelationCardinality::Set,
        ),
    ] {
        let relation = PersistedRelationEdgeSnapshot::new_direct(
            RelationId::new(1).unwrap(),
            "owner".into(),
            "tests::Owner".into(),
            vec![FieldId::new(2)],
        );
        let schema = relation_description_fixture(kind, false, relation);
        assert_eq!(
            persisted_relation_cardinality(
                schema.persisted_snapshot(),
                &schema.persisted_snapshot().relations()[0]
            )
            .unwrap(),
            expected
        );
    }
    let (snapshot, _) = constraint_description_fixture();
    let tuple = PersistedRelationEdgeSnapshot::new_direct(
        RelationId::new(1).unwrap(),
        "tuple_owner".into(),
        "tests::Owner".into(),
        vec![FieldId::new(1), FieldId::new(2)],
    );
    assert_eq!(
        persisted_relation_cardinality(&snapshot, &tuple).unwrap(),
        EntityRelationCardinality::Single
    );
}

#[test]
fn compact_extra_contract_is_closed_and_deterministically_ordered() {
    assert_eq!(
        compact_column_extras(true, true, true),
        vec![
            SqlColumnExtra::Identity,
            SqlColumnExtra::Generated,
            SqlColumnExtra::Relation,
        ]
    );
    assert_eq!(
        compact_column_extras(false, true, false),
        vec![SqlColumnExtra::Generated]
    );
    assert!(compact_column_extras(false, false, false).is_empty());
}

#[test]
fn compact_projection_bounds_accept_maximum_and_reject_max_plus_one() {
    assert_eq!(
        compact_column_capacity_from_counts(
            icydb_schema::MAX_FRAGMENT_FIELDS,
            std::iter::repeat_n(
                icydb_schema::MAX_FRAGMENT_FIELDS,
                icydb_schema::MAX_FRAGMENT_FIELDS,
            ),
        )
        .expect("accepted maximum should remain projectable"),
        super::MAX_SQL_COMPACT_COLUMN_ROWS,
    );
    assert!(
        compact_column_capacity_from_counts(
            icydb_schema::MAX_FRAGMENT_FIELDS + 1,
            std::iter::repeat_n(0, icydb_schema::MAX_FRAGMENT_FIELDS + 1),
        )
        .is_err()
    );
    assert!(
        compact_column_capacity_from_counts(1, [icydb_schema::MAX_FRAGMENT_FIELDS + 1],).is_err()
    );

    let valid = SqlColumnSummary::new(
        "value".to_string(),
        "text".to_string(),
        false,
        SqlColumnKey::None,
        SqlColumnDefault::Literal {
            text: "x".repeat(MAX_SCHEMA_VALUE_RENDER_CHARS),
        },
        vec![
            SqlColumnExtra::Identity,
            SqlColumnExtra::Generated,
            SqlColumnExtra::Relation,
        ],
    );
    let valid = valid.expect("the complete admitted compact row should remain valid");
    assert!(
        SqlColumnSummary::new(
            "value".to_string(),
            "text".to_string(),
            false,
            SqlColumnKey::None,
            SqlColumnDefault::Literal {
                text: "x".repeat(MAX_SCHEMA_VALUE_RENDER_CHARS + 1),
            },
            Vec::new(),
        )
        .is_err()
    );
    assert!(
        SqlColumnSummary::new(
            "value".to_string(),
            "text".to_string(),
            false,
            SqlColumnKey::None,
            SqlColumnDefault::Required,
            vec![SqlColumnExtra::Generated; 4],
        )
        .is_err()
    );

    let maximum = SqlDescribeOutput::Compact {
        entity: "AcceptedMaximum".to_string(),
        columns: vec![valid; super::MAX_SQL_COMPACT_COLUMN_ROWS],
    };
    assert!(matches!(
        icydb_schema::wire::encoded_len(&maximum),
        Err(icydb_schema::wire::Error::Limit),
    ));
    assert!(matches!(
        icydb_schema::wire::encode_one(&maximum),
        Err(icydb_schema::wire::Error::Limit),
    ));

    let relation = EntityRelationDescription::new(
        "owner_id".to_string(),
        "entities::Owner".to_string(),
        "Owner".to_string(),
        "stores::Owner".to_string(),
        EntityRelationCardinality::Single,
    );
    assert!(
        SqlShowRelationsOutput::new(
            "Entry".to_string(),
            vec![relation.clone(); icydb_schema::MAX_FRAGMENT_RELATIONS],
        )
        .is_ok()
    );
    assert!(
        SqlShowRelationsOutput::new(
            "Entry".to_string(),
            vec![relation; icydb_schema::MAX_FRAGMENT_RELATIONS + 1],
        )
        .is_err()
    );
}

#[test]
fn reachable_accepted_compact_projection_exceeds_the_public_query_reply_limit() {
    let accepted = reachable_compact_reply_schema();
    let value_catalog = reachable_compact_reply_value_catalog();
    let columns = describe_compact_columns_with_persisted_schema(&accepted, &value_catalog)
        .expect("reachable accepted schema should project compact columns");

    assert_eq!(
        columns.len(),
        1 + REACHABLE_COMPACT_REPLY_TOP_LEVEL_COMPOSITES
            * (1 + REACHABLE_COMPACT_REPLY_COMPOSITE_FIELDS),
    );
    let output = SqlDescribeOutput::Compact {
        entity: accepted.entity_name().to_string(),
        columns,
    };
    let encoded_len = icydb_schema::wire::encoded_len(&output)
        .expect("reachable accepted compact output should admit allocation-free Wire sizing");
    assert!(
        encoded_len > IC_QUERY_REPLY_BYTES,
        "a valid accepted schema must exercise the generated endpoint reply guard",
    );
    assert!(matches!(
        icydb_schema::wire::encode_one(&output),
        Err(icydb_schema::wire::Error::Limit),
    ));
}

#[test]
fn multiple_reachable_accepted_descriptions_exceed_schema_reply_admission() {
    let base = reachable_compact_reply_schema();
    let value_catalog = reachable_compact_reply_value_catalog();
    let mut descriptions = Vec::new();
    for ordinal in 1..=4 {
        let name = format!("ReachableCompactReply{ordinal}");
        let snapshot = PersistedSchemaSnapshot::new(
            base.persisted_snapshot().version(),
            format!("tests::{name}"),
            name,
            FieldId::new(1),
            base.persisted_snapshot().row_layout().clone(),
            base.persisted_snapshot().fields().to_vec(),
        );
        assert!(
            encode_persisted_schema_snapshot(&snapshot).unwrap().len()
                <= MAX_SCHEMA_SNAPSHOT_BYTES as usize
        );
        let accepted = AcceptedSchemaSnapshot::try_new(snapshot).unwrap();
        let fingerprint = crate::db::schema::accepted_schema_cache_fingerprint(&accepted).unwrap();
        descriptions.push(
            super::describe_accepted_entity_with_persisted_schema(
                &accepted,
                &value_catalog,
                &[],
                super::AcceptedEntityDescriptionMetadata::new(
                    None,
                    ordinal,
                    crate::db::schema::accepted_schema_cache_fingerprint_method_version(),
                    fingerprint,
                ),
                |_| Err(crate::error::InternalError::store_invariant()),
            )
            .unwrap(),
        );
    }
    let response: Result<_, ()> = Ok(descriptions);
    // Widen traversal allowances only for this byte-reachability proof.
    // Product admission can refuse nodes/heap before discovering all bytes.
    let sizing = ic_wire::Limits {
        bytes: 64 * 1024 * 1024,
        allocation: 1024 * 1024 * 1024,
        nodes: 8 * icydb_schema::wire::MAX_NODES,
        ..icydb_schema::wire::sizing_limits()
    };
    assert!(ic_wire::native::encoded_frame_len(&response, sizing).unwrap() > IC_QUERY_REPLY_BYTES);
    assert_eq!(
        ic_wire::native::encode_response(
            1,
            &response,
            icydb_schema::wire::limits(IC_QUERY_REPLY_BYTES)
        ),
        Err(ic_wire::Error::Limit),
    );
}

#[test]
fn field_descriptions_preserve_root_and_nested_queryability() {
    let accepted = reachable_compact_reply_schema();
    let value_catalog = reachable_compact_reply_value_catalog();
    let fields = describe_entity_fields_with_persisted_schema(&accepted, &value_catalog)
        .expect("accepted root and leaf metadata should describe");

    assert!(fields[0].queryable());
    let (groups, remainder) =
        fields[1..].as_chunks::<{ 1 + REACHABLE_COMPACT_REPLY_COMPOSITE_FIELDS }>();
    assert!(remainder.is_empty());
    assert_eq!(groups.len(), REACHABLE_COMPACT_REPLY_TOP_LEVEL_COMPOSITES);
    for group in groups {
        assert!(!group[0].queryable(), "record roots remain non-queryable");
        assert!(group[1..].iter().all(EntityFieldDescription::queryable));
    }
}

#[test]
fn nested_nullability_includes_nullable_ancestors() {
    let leaves = vec![
        PersistedNestedLeafSnapshot::new(
            vec!["address".to_string()],
            AcceptedFieldKind::Unit,
            true,
        ),
        PersistedNestedLeafSnapshot::new(
            vec!["address".to_string(), "city".to_string()],
            AcceptedFieldKind::Unit,
            false,
        ),
    ];
    assert!(nested_path_nullable(
        false,
        leaves.as_slice(),
        &["address".to_string(), "city".to_string()],
    ));
    assert!(nested_path_nullable(
        true,
        leaves.as_slice(),
        &["other".to_string()],
    ));
    assert!(!nested_path_nullable(
        false,
        leaves.as_slice(),
        &["other".to_string()],
    ));
}

fn compact_reply_leaf_name(index: usize) -> String {
    let first = u8::try_from(index / 26).expect("bounded leaf prefix fits u8") + b'a';
    let second = u8::try_from(index % 26).expect("bounded leaf suffix fits u8") + b'a';
    String::from_utf8(vec![first, second]).expect("ASCII leaf name should be UTF-8")
}

fn compact_reply_top_level_name(index: usize) -> String {
    let prefix = format!("field_{index:03}_");
    format!("{prefix}{}", "x".repeat(128 - prefix.len()))
}

fn reachable_compact_reply_schema() -> AcceptedSchemaSnapshot {
    let composite_type_id = CompositeTypeId::new(1).expect("one is non-zero");
    let nested_leaves = (0..REACHABLE_COMPACT_REPLY_COMPOSITE_FIELDS)
        .map(|index| {
            PersistedNestedLeafSnapshot::new(
                vec![compact_reply_leaf_name(index)],
                AcceptedFieldKind::Unit,
                false,
            )
        })
        .collect::<Vec<_>>();
    let mut fields = vec![PersistedFieldSnapshot::new_initial(
        FieldId::new(1),
        "id".to_string(),
        SchemaFieldSlot::new(0),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    )];
    let mut layout = vec![(FieldId::new(1), SchemaFieldSlot::new(0))];
    for index in 0..REACHABLE_COMPACT_REPLY_TOP_LEVEL_COMPOSITES {
        let raw_id = u32::try_from(index)
            .expect("bounded top-level index fits u32")
            .checked_add(2)
            .expect("bounded top-level identity has a successor");
        let raw_slot = u16::try_from(index)
            .expect("bounded top-level index fits u16")
            .checked_add(1)
            .expect("bounded top-level slot has a successor");
        let id = FieldId::new(raw_id);
        let slot = SchemaFieldSlot::new(raw_slot);
        fields.push(PersistedFieldSnapshot::new_initial(
            id,
            compact_reply_top_level_name(index),
            slot,
            AcceptedFieldKind::Composite {
                type_id: composite_type_id,
            },
            nested_leaves.clone(),
            false,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Structural,
        ));
        layout.push((id, slot));
    }
    let persisted = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::ReachableCompactReply".to_string(),
        "ReachableCompactReply".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(layout),
        fields,
    );
    let encoded = encode_persisted_schema_snapshot(&persisted)
        .expect("reachable compact-reply schema should fit its persisted payload limit");
    assert_eq!(encoded.len(), 310_865);
    assert!(encoded.len() <= MAX_SCHEMA_SNAPSHOT_BYTES as usize);
    AcceptedSchemaSnapshot::try_new(
        decode_persisted_schema_snapshot(encoded.as_slice())
            .expect("persisted compact-reply schema should decode"),
    )
    .expect("decoded compact-reply schema should satisfy accepted integrity")
}

fn reachable_compact_reply_value_catalog() -> AcceptedValueCatalogHandle {
    let enum_catalog = empty_accepted_enum_catalog_for_tests();
    let composite_type_id = CompositeTypeId::new(1).expect("one is non-zero");
    let composite_fields = (0..REACHABLE_COMPACT_REPLY_COMPOSITE_FIELDS)
        .map(|index| {
            let raw_id = u32::try_from(index)
                .expect("bounded composite index fits u32")
                .checked_add(1)
                .expect("bounded composite identity has a successor");
            AcceptedCompositeField::new(
                CompositeFieldId::new(raw_id).expect("composite field identity is non-zero"),
                compact_reply_leaf_name(index),
                AcceptedCompositeElement::new(AcceptedFieldKind::Unit, false),
            )
        })
        .collect::<Vec<_>>();
    let composite_catalog = AcceptedCompositeCatalog::from_initial_definitions(
        BTreeMap::from([(
            composite_type_id,
            (
                "tests::CompactReplyRecord".to_string(),
                AcceptedCompositeShape::Record(composite_fields),
            ),
        )]),
        &enum_catalog,
    )
    .expect("bounded reusable record composite should admit");
    let encoded = encode_accepted_composite_catalog(&composite_catalog, &enum_catalog)
        .expect("reachable composite authority should fit its persisted payload limit");
    assert!(encoded.len() <= MAX_SCHEMA_SNAPSHOT_BYTES as usize);
    let composite_catalog = decode_accepted_composite_catalog(&encoded, &enum_catalog)
        .expect("persisted composite authority should decode");
    AcceptedValueCatalogHandle::new_for_tests(
        enum_catalog,
        composite_catalog,
        AcceptedSchemaRevision::INITIAL,
    )
}
