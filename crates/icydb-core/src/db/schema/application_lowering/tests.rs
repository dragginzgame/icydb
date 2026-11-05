use std::{borrow::Cow, collections::BTreeMap};

use super::{
    ExistingProposalStore, ProposalStoreTarget, lower_existing_schema_proposal, lower_field_type,
    lower_generated_existing_schema_proposal, lower_initial_field_type,
    lower_initial_schema_proposal, resolve_existing_removal, validate_entity_namespace,
};
use crate::db::{
    data::{
        decode_canonical_value_storage_bytes, encode_input_value_for_candidate_field_contract,
        validate_default_payload_for_accepted_field_contract,
    },
    schema::{
        AcceptedConstraintCatalog, AcceptedConstraintKind, AcceptedConstraintSnapshot,
        AcceptedFieldDecodeContract, AcceptedFieldKind, AcceptedNamedTypeIdentity,
        AcceptedRuleOperation, AcceptedRuleTarget, AcceptedSchemaRevision,
        AcceptedSchemaRevisionBundle, AcceptedSchemaSnapshot, AcceptedSourceBindingCatalog,
        AcceptedStoreCatalogScope, AcceptedValueCatalogHandle, AcceptedValueContract,
        CompiledAcceptedRowConstraints, ConstraintActivationKind, ConstraintOrigin,
        FieldInsertGeneration, PersistedFieldSnapshot, SchemaInsertDefault, ValueAdmissionBudget,
        composite_catalog::AcceptedCompositeShape,
        enum_catalog::{AcceptedEnumVariantBody, ValueAdmissionError, normalize_candidate_value},
    },
};
use crate::value::{EnumTypeId, InputValue, PublicEnumValue, PublicValue, Value};
use icydb_schema::{
    ConstraintFragment, ConstraintSourceKey, DeclaredEntityVersion, EntityFragment,
    EntitySourceKey, EntityStoreAssignment, EnumTypeFragment, EnumVariantFragment,
    ExpectedAcceptedHead, ExpectedSchemaFingerprint, FieldFragment, FieldInsertPolicy,
    FieldSourceKey, FieldType, IndexFragment, IndexKeyFragment, IndexSourceKey, NamedTypeFragment,
    RecordFieldFragment, RecordTypeFragment, ScalarLiteral, ScalarType, SchemaCapability,
    SchemaFragment, SchemaName, SchemaProposal, SchemaRemoval, SchemaSubmissionKey,
    SourceCheckExpr, SourceCheckInstruction, SourceRuleOperation, TargetDatabaseIdentity,
    TargetStoreIdentity, TargetedRuleFragment, TupleElementFragment, TypeSourceKey,
};
fn name(value: &str) -> SchemaName {
    SchemaName::try_new(value).expect("test schema name should admit")
}

fn version_one() -> DeclaredEntityVersion {
    DeclaredEntityVersion::try_new(1).expect("fixture version should admit")
}

#[test]
fn initial_proposal_index_width_matches_the_physical_key_domain() {
    use crate::{MAX_INDEX_FIELDS, error::InternalError};

    for count in [1, MAX_INDEX_FIELDS, MAX_INDEX_FIELDS + 1] {
        for mixed in [false, true] {
            let id = FieldSourceKey::try_new("id").unwrap();
            let value = FieldSourceKey::try_new("value").unwrap();
            let entity = EntityFragment::try_new(
                name("Width"),
                version_one(),
                vec![
                    FieldFragment::new(
                        name("id"),
                        FieldType::Scalar(ScalarType::Nat64),
                        false,
                        FieldInsertPolicy::Required,
                        None,
                    ),
                    FieldFragment::new(
                        name("value"),
                        FieldType::Scalar(ScalarType::Text { max_len: None }),
                        false,
                        FieldInsertPolicy::Required,
                        None,
                    ),
                ],
                vec![id],
                vec![
                    IndexFragment::try_new(
                        name("by_value"),
                        (0..count)
                            .map(|offset| {
                                if mixed && offset % 2 == 0 {
                                    IndexKeyFragment::Lower(value.clone())
                                } else {
                                    IndexKeyFragment::Field(value.clone())
                                }
                            })
                            .collect(),
                        false,
                        None,
                    )
                    .unwrap(),
                ],
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
            let source = entity.source_key().clone();
            let store = TargetStoreIdentity::from_bytes([0x22; 32]);
            let proposal = SchemaProposal::try_compose(
                vec![SchemaCapability::SECONDARY_INDEXES],
                TargetDatabaseIdentity::from_bytes([0x11; 32]),
                SchemaSubmissionKey::try_new("width").unwrap(),
                ExpectedAcceptedHead::Empty,
                vec![SchemaFragment::try_new(vec![entity], Vec::new()).unwrap()],
                vec![EntityStoreAssignment::new(source.clone(), store)],
                Vec::new(),
                None,
            )
            .unwrap();
            let result = lower_initial_schema_proposal(
                &proposal,
                &[ProposalStoreTarget {
                    path: "test::Store",
                    identity: store,
                }],
            );
            if count <= MAX_INDEX_FIELDS {
                let candidates = result.unwrap();
                let bundle = candidates[0].bundle();
                let tag = bundle.source_bindings_for_tests().entity(&source).unwrap();
                let snapshot = &bundle.entity_snapshots()[&tag];
                AcceptedSchemaSnapshot::try_new(snapshot.clone()).unwrap();
                assert_eq!(snapshot.indexes().len(), 1);
                let key = snapshot.indexes()[0].key();
                let arity = key.component_count();
                assert_eq!(arity, count);
            } else {
                let Err(error) = result else {
                    panic!("unsupported index width must reject")
                };
                assert_eq!(
                    error.diagnostic(),
                    InternalError::store_unsupported().diagnostic()
                );
            }
        }
    }
}

#[test]
fn field_type_lowering_uses_the_same_closed_named_identity_authority() {
    let source = TypeSourceKey::try_new("Status").expect("type source should admit");
    let type_id = EnumTypeId::new(7).expect("enum type id should admit");
    let identities = BTreeMap::from([(source.clone(), AcceptedNamedTypeIdentity::Enum(type_id))]);
    let accepted = AcceptedSourceBindingCatalog::default().with_initial_named_types(
        identities.clone(),
        BTreeMap::new(),
        BTreeMap::new(),
    );
    let field_type = FieldType::List(Box::new(FieldType::Named(source)));
    let expected = AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Enum { type_id }));

    assert_eq!(
        lower_initial_field_type(&field_type, &identities)
            .expect("initial named identity should resolve"),
        expected,
    );
    assert_eq!(
        lower_field_type(&field_type, &accepted).expect("accepted named identity should resolve"),
        expected,
    );
}

struct TargetedRuleProposalFixture {
    proposal: SchemaProposal,
    entity_source: EntitySourceKey,
    value_source: FieldSourceKey,
    value_type: TypeSourceKey,
    other_type: TypeSourceKey,
    constraint_source: ConstraintSourceKey,
    store: TargetStoreIdentity,
}

fn targeted_rule_proposal_fixture(
    expected_head: ExpectedAcceptedHead,
    submission_key: &str,
    value_type_name: &str,
    other_type_name: &str,
    include_rule: bool,
    removals: Vec<SchemaRemoval>,
) -> TargetedRuleProposalFixture {
    targeted_rule_proposal_fixture_with_operation(
        expected_head,
        submission_key,
        value_type_name,
        other_type_name,
        "range",
        include_rule.then_some(SourceRuleOperation::NumericRangeInclusive {
            min: ScalarLiteral::Nat(0),
            max: ScalarLiteral::Nat(10),
        }),
        removals,
    )
}

fn targeted_rule_proposal_fixture_with_operation(
    expected_head: ExpectedAcceptedHead,
    submission_key: &str,
    value_type_name: &str,
    other_type_name: &str,
    rule_name: &str,
    operation: Option<SourceRuleOperation>,
    removals: Vec<SchemaRemoval>,
) -> TargetedRuleProposalFixture {
    let entity_source = EntitySourceKey::try_new("Targeted").expect("entity source should admit");
    let id_source = FieldSourceKey::try_new("id").expect("field source should admit");
    let value_source = FieldSourceKey::try_new("value").expect("field source should admit");
    let value_type = TypeSourceKey::try_new(value_type_name).expect("type source should admit");
    let other_type = TypeSourceKey::try_new(other_type_name).expect("type source should admit");
    let rule_source =
        icydb_schema::RuleSourceKey::try_new(rule_name).expect("rule source should admit");
    let constraint_source =
        ConstraintSourceKey::for_targeted_field_rule(&value_source, &value_type, &rule_source);
    let constraints = operation
        .map(|operation| {
            ConstraintFragment::targeted_rule(TargetedRuleFragment::new(
                value_source.clone(),
                value_type.clone(),
                name(rule_name),
                operation,
            ))
        })
        .into_iter()
        .collect();
    let entity = EntityFragment::try_new(
        name("Targeted"),
        version_one(),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("value"),
                FieldType::Named(value_type.clone()),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("other"),
                FieldType::Named(other_type.clone()),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![id_source],
        Vec::new(),
        Vec::new(),
        constraints,
    )
    .expect("targeted entity should admit");
    let fragment = SchemaFragment::try_new(
        vec![entity],
        vec![
            NamedTypeFragment::newtype(name(value_type_name), FieldType::Scalar(ScalarType::Nat8)),
            NamedTypeFragment::newtype(name(other_type_name), FieldType::Scalar(ScalarType::Nat16)),
        ],
    )
    .expect("targeted fragment should admit");
    let store = TargetStoreIdentity::from_bytes([0x64; 32]);
    let proposal = SchemaProposal::try_compose(
        vec![SchemaCapability::ACCEPTED_CHECKS],
        TargetDatabaseIdentity::from_bytes([0x63; 32]),
        SchemaSubmissionKey::try_new(submission_key).expect("submission should admit"),
        expected_head,
        vec![fragment],
        vec![EntityStoreAssignment::new(entity_source.clone(), store)],
        removals,
        None,
    )
    .expect("targeted proposal should compose");
    TargetedRuleProposalFixture {
        proposal,
        entity_source,
        value_source,
        value_type,
        other_type,
        constraint_source,
        store,
    }
}

fn scalar_proposal_fixture(
    expected_head: ExpectedAcceptedHead,
    submission_key: &str,
    score_default: i128,
) -> (SchemaProposal, EntitySourceKey, TargetStoreIdentity) {
    scalar_proposal_fixture_with_names(
        expected_head,
        submission_key,
        score_default,
        "Item",
        "score",
        true,
        Vec::new(),
    )
}

fn nullable_unique_proposal_fixture(
    predicate: Option<SourceCheckExpr>,
) -> (SchemaProposal, TargetStoreIdentity) {
    let entity_source =
        EntitySourceKey::try_new("Account").expect("test entity source should admit");
    let id_source = FieldSourceKey::try_new("id").expect("test id source should admit");
    let email_source = FieldSourceKey::try_new("email").expect("test email source should admit");
    let entity = EntityFragment::try_new(
        name("Account"),
        version_one(),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("email"),
                FieldType::Scalar(ScalarType::Text { max_len: None }),
                true,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![id_source],
        vec![
            IndexFragment::try_new(
                name("account_email"),
                vec![IndexKeyFragment::Field(email_source)],
                true,
                predicate,
            )
            .expect("test index should admit"),
        ],
        Vec::new(),
        Vec::new(),
    )
    .expect("test entity should admit");
    let fragment =
        SchemaFragment::try_new(vec![entity], Vec::new()).expect("test fragment should admit");
    let store = TargetStoreIdentity::from_bytes([0x32; 32]);
    let proposal = SchemaProposal::try_compose(
        vec![SchemaCapability::SECONDARY_INDEXES],
        TargetDatabaseIdentity::from_bytes([0x31; 32]),
        SchemaSubmissionKey::try_new("nullable-unique").expect("test submission key should admit"),
        ExpectedAcceptedHead::Empty,
        vec![fragment],
        vec![EntityStoreAssignment::new(entity_source, store)],
        Vec::new(),
        None,
    )
    .expect("test proposal should compose");
    (proposal, store)
}

fn scalar_proposal_fixture_with_names(
    expected_head: ExpectedAcceptedHead,
    submission_key: &str,
    score_default: i128,
    entity_name: &str,
    score_name: &str,
    include_check: bool,
    removals: Vec<SchemaRemoval>,
) -> (SchemaProposal, EntitySourceKey, TargetStoreIdentity) {
    let entity_source =
        EntitySourceKey::try_new(entity_name).expect("test entity source should admit");
    let id_source = FieldSourceKey::try_new("id").expect("test field source should admit");
    let score_source = FieldSourceKey::try_new(score_name).expect("test field source should admit");
    let check = SourceCheckExpr::try_new(vec![
        SourceCheckInstruction::Field(score_source.clone()),
        SourceCheckInstruction::Literal(ScalarLiteral::Int(0)),
        SourceCheckInstruction::GreaterThanOrEqual,
    ])
    .expect("test check should admit");
    let entity = EntityFragment::try_new(
        name(entity_name),
        version_one(),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name(score_name),
                FieldType::Scalar(ScalarType::Int64),
                false,
                FieldInsertPolicy::Default(ScalarLiteral::Int(score_default)),
                None,
            ),
        ],
        vec![id_source],
        vec![
            IndexFragment::try_new(
                name("score_idx"),
                vec![IndexKeyFragment::Field(score_source)],
                false,
                None,
            )
            .expect("test index should admit"),
        ],
        Vec::new(),
        include_check
            .then(|| ConstraintFragment::check(name("score_non_negative"), check))
            .into_iter()
            .collect(),
    )
    .expect("test entity should admit");
    let fragment =
        SchemaFragment::try_new(vec![entity], Vec::new()).expect("test fragment should admit");
    let store = TargetStoreIdentity::from_bytes([0x22; 32]);
    let proposal = SchemaProposal::try_compose(
        vec![
            SchemaCapability::ACCEPTED_CHECKS,
            SchemaCapability::INSERT_DEFAULTS,
            SchemaCapability::SECONDARY_INDEXES,
        ],
        TargetDatabaseIdentity::from_bytes([0x11; 32]),
        SchemaSubmissionKey::try_new(submission_key).expect("test submission key should admit"),
        expected_head,
        vec![fragment],
        vec![EntityStoreAssignment::new(entity_source.clone(), store)],
        removals,
        None,
    )
    .expect("test proposal should compose");
    (proposal, entity_source, store)
}

#[test]
fn entity_namespace_names_include_unchanged_stores() {
    let make_candidate = |entity_name: &str, path: &'static str| {
        let (proposal, _, store) = scalar_proposal_fixture_with_names(
            ExpectedAcceptedHead::Empty,
            "namespace-initial",
            5,
            entity_name,
            "score",
            false,
            Vec::new(),
        );
        let candidate = lower_initial_schema_proposal(
            &proposal,
            &[ProposalStoreTarget {
                path,
                identity: store,
            }],
        )
        .unwrap()
        .pop()
        .unwrap();
        (proposal, store, candidate)
    };
    let (user_proposal, _, user) = make_candidate("User", "test::Users");
    let (_, other_store, other) = make_candidate("Other", "test::Other");
    let (alias_proposal, _, alias) = make_candidate("user", "test::Other");
    let user_store = TargetStoreIdentity::from_bytes([0x99; 32]);
    let stores = [
        ExistingProposalStore {
            path: "test::Users",
            identity: user_store,
            bundle: user.bundle(),
        },
        ExistingProposalStore {
            path: "test::Other",
            identity: other_store,
            bundle: other.bundle(),
        },
    ];
    let cross_store = SchemaProposal::try_compose(
        user_proposal.capabilities().to_vec(),
        user_proposal.target_database(),
        user_proposal.submission_key().clone(),
        ExpectedAcceptedHead::Empty,
        vec![
            SchemaFragment::try_new(
                vec![
                    user_proposal.fragments()[0].entities()[0].clone(),
                    alias_proposal.fragments()[0].entities()[0].clone(),
                ],
                Vec::new(),
            )
            .unwrap(),
        ],
        vec![
            EntityStoreAssignment::new(EntitySourceKey::try_new("User").unwrap(), user_store),
            EntityStoreAssignment::new(EntitySourceKey::try_new("user").unwrap(), other_store),
        ],
        Vec::new(),
        None,
    )
    .unwrap();
    assert_eq!(
        lower_initial_schema_proposal(
            &cross_store,
            &[
                ProposalStoreTarget {
                    path: "test::Users",
                    identity: user_store
                },
                ProposalStoreTarget {
                    path: "test::Other",
                    identity: other_store
                },
            ]
        )
        .unwrap_err()
        .class(),
        crate::error::ErrorClass::Unsupported,
    );
    for lower in [
        lower_existing_schema_proposal,
        lower_generated_existing_schema_proposal,
    ] {
        assert_eq!(
            lower(&alias_proposal, &stores).unwrap_err().class(),
            crate::error::ErrorClass::Unsupported
        );
    }
    assert!(validate_entity_namespace(std::slice::from_ref(&alias), &stores).is_err());
    // Replacing a store must not count its predecessor's names twice.
    assert!(validate_entity_namespace(std::slice::from_ref(&user), &stores).is_ok());
    assert!(validate_entity_namespace(&[], &stores).is_ok());
}

#[test]
fn initial_scalar_proposal_lowers_source_identity_defaults_indexes_and_checks() {
    let (proposal, entity_source, store) =
        scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "initial-scalar", 5);

    let candidates = lower_initial_schema_proposal(
        &proposal,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("initial scalar proposal should lower");

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].revision(), AcceptedSchemaRevision::INITIAL);
    let bundle = candidates[0].bundle();
    let entity_tag = bundle
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should bind");
    let snapshot = bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("entity snapshot should exist");
    assert_eq!(snapshot.indexes().len(), 1);
    assert!(matches!(
        snapshot
            .fields()
            .iter()
            .find(|field| field.name() == "score")
            .expect("score should exist")
            .insert_default(),
        SchemaInsertDefault::SlotPayload(payload) if !payload.is_empty()
    ));
    assert!(
        snapshot
            .constraint_catalog()
            .constraints()
            .iter()
            .any(
                |constraint| matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
                    && constraint.name() == "score_non_negative"
            )
    );
    assert_eq!(
        bundle
            .source_bindings_for_tests()
            .field_binding_count_for_tests(entity_tag),
        2,
    );
    assert_eq!(
        bundle
            .source_bindings_for_tests()
            .index_binding_count_for_tests(entity_tag),
        1,
    );
    assert_eq!(
        bundle
            .source_bindings_for_tests()
            .constraint_binding_count_for_tests(entity_tag),
        1,
    );
}

#[test]
fn initial_generated_nullable_unique_index_uses_canonical_contract() {
    let (implicit, store) = nullable_unique_proposal_fixture(None);
    let error = lower_initial_schema_proposal(
        &implicit,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect_err("generated implicit nullable uniqueness must reject");
    assert_eq!(error.class(), crate::error::ErrorClass::Unsupported);
    assert_eq!(error.origin(), crate::error::ErrorOrigin::Store);

    let email_source = FieldSourceKey::try_new("email").expect("test email source should admit");
    let guard = SourceCheckExpr::try_new(vec![
        SourceCheckInstruction::Field(email_source),
        SourceCheckInstruction::IsNotNull,
    ])
    .expect("test guard should admit");
    let (explicit, store) = nullable_unique_proposal_fixture(Some(guard));
    let candidates = lower_initial_schema_proposal(
        &explicit,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("generated explicit nullable uniqueness should lower");
    let index = candidates[0]
        .bundle()
        .entity_snapshots()
        .values()
        .next()
        .expect("generated entity should exist")
        .indexes()
        .first()
        .expect("generated index should exist");
    assert_eq!(
        index.predicate(),
        Some(&crate::db::schema::AcceptedIndexPredicate::test_non_null(1))
    );
}

#[test]
fn initial_identity_policy_lowers_all_exact_unsigned_widths_to_accepted_ids() {
    for (index, scalar) in [
        ScalarType::Nat8,
        ScalarType::Nat16,
        ScalarType::Nat32,
        ScalarType::Nat64,
        ScalarType::Nat128,
    ]
    .into_iter()
    .enumerate()
    {
        let entity_name = format!("Identity{index}");
        let entity_source =
            EntitySourceKey::try_new(&entity_name).expect("entity source should admit");
        let id_source = FieldSourceKey::try_new("id").expect("field source should admit");
        let entity = EntityFragment::try_new(
            name(&entity_name),
            version_one(),
            vec![FieldFragment::new(
                name("id"),
                FieldType::Scalar(scalar),
                false,
                FieldInsertPolicy::Generated,
                None,
            )],
            vec![id_source.clone()],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("exact unsigned identity proposal should admit");
        let fragment = SchemaFragment::try_new(vec![entity], Vec::new())
            .expect("identity fragment should admit");
        let discriminator =
            u8::try_from(index).expect("identity width fixture index should fit one byte");
        let store = TargetStoreIdentity::from_bytes(
            [0x70_u8
                .checked_add(discriminator)
                .expect("identity width fixture discriminator should fit"); 32],
        );
        let proposal = SchemaProposal::try_compose(
            vec![SchemaCapability::GENERATED_VALUES],
            TargetDatabaseIdentity::from_bytes([0x61; 32]),
            SchemaSubmissionKey::try_new(format!("identity-{index}"))
                .expect("submission key should admit"),
            ExpectedAcceptedHead::Empty,
            vec![fragment],
            vec![EntityStoreAssignment::new(entity_source.clone(), store)],
            Vec::new(),
            None,
        )
        .expect("identity proposal should compose");

        let candidates = lower_initial_schema_proposal(
            &proposal,
            &[ProposalStoreTarget {
                path: "test::IdentityStore",
                identity: store,
            }],
        )
        .expect("identity proposal should lower through accepted authority");
        let candidate = &candidates[0];
        let entity_tag = candidate
            .bundle()
            .source_bindings_for_tests()
            .entity(&entity_source)
            .expect("entity source should bind to accepted identity");
        let field_id = candidate
            .bundle()
            .source_bindings_for_tests()
            .field(entity_tag, &id_source)
            .expect("field source should bind to accepted identity");
        let field = candidate.bundle().entity_snapshots()[&entity_tag]
            .fields()
            .iter()
            .find(|field| field.id() == field_id)
            .expect("accepted identity field should exist");

        assert_eq!(candidate.store_path(), "test::IdentityStore");
        assert_eq!(
            field.write_policy().insert_generation(),
            Some(FieldInsertGeneration::Identity),
        );
    }
}

#[test]
fn initial_newtype_rules_bind_through_the_accepted_composite_catalog() {
    let entity_source =
        EntitySourceKey::try_new("Compass").expect("test entity source should admit");
    let id_source = FieldSourceKey::try_new("id").expect("test id source should admit");
    let degrees_source =
        FieldSourceKey::try_new("degrees").expect("test degrees source should admit");
    let degrees_type = TypeSourceKey::try_new("Degrees").expect("test type source should admit");
    let expression = SourceCheckExpr::try_new(vec![
        SourceCheckInstruction::Field(degrees_source.clone()),
        SourceCheckInstruction::Literal(ScalarLiteral::Nat(360)),
        SourceCheckInstruction::LessThanOrEqual,
    ])
    .expect("test newtype rule should admit");
    let entity = EntityFragment::try_new(
        name("Compass"),
        version_one(),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("degrees"),
                FieldType::Named(degrees_type),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![id_source],
        Vec::new(),
        Vec::new(),
        vec![ConstraintFragment::check(name("degrees_range"), expression)],
    )
    .expect("test entity should admit");
    let fragment = SchemaFragment::try_new(
        vec![entity],
        vec![NamedTypeFragment::newtype(
            name("Degrees"),
            FieldType::Scalar(ScalarType::Nat16),
        )],
    )
    .expect("test newtype fragment should admit");
    let store = TargetStoreIdentity::from_bytes([0x24; 32]);
    let proposal = SchemaProposal::try_compose(
        vec![
            SchemaCapability::ACCEPTED_CHECKS,
            SchemaCapability::EXACT_COMPOSITE_TYPES,
        ],
        TargetDatabaseIdentity::from_bytes([0x14; 32]),
        SchemaSubmissionKey::try_new("initial-newtype-rule")
            .expect("test submission key should admit"),
        ExpectedAcceptedHead::Empty,
        vec![fragment],
        vec![EntityStoreAssignment::new(entity_source.clone(), store)],
        Vec::new(),
        None,
    )
    .expect("test proposal should compose");

    let candidates = lower_initial_schema_proposal(
        &proposal,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("newtype rule should lower through accepted catalog authority");
    let bundle = candidates[0].bundle();
    let entity_tag = bundle
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should bind");
    let snapshot = bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("entity snapshot should exist");

    assert!(snapshot.constraints().iter().any(|constraint| {
        constraint.name() == "degrees_range"
            && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
    }));
    assert!(matches!(
        snapshot
            .fields()
            .iter()
            .find(|field| field.id()
                == bundle
                    .source_bindings_for_tests()
                    .field(entity_tag, &degrees_source)
                    .expect("degrees field source should bind"))
            .map(PersistedFieldSnapshot::kind),
        Some(crate::db::schema::AcceptedFieldKind::Composite { .. })
    ));
}

#[test]
fn initial_targeted_source_rule_binds_and_persists_accepted_identities() {
    let fixture = targeted_rule_proposal_fixture(
        ExpectedAcceptedHead::Empty,
        "targeted-binding",
        "Value",
        "Other",
        true,
        Vec::new(),
    );

    let candidates = lower_initial_schema_proposal(
        &fixture.proposal,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: fixture.store,
        }],
    )
    .expect("targeted rule should bind into the accepted candidate");
    let bundle = candidates[0].bundle();
    let entity_tag = bundle
        .source_bindings_for_tests()
        .entity(&fixture.entity_source)
        .expect("entity source should bind");
    let root_field_id = bundle
        .source_bindings_for_tests()
        .field(entity_tag, &fixture.value_source)
        .expect("root field source should bind");
    let target_type = bundle
        .source_bindings_for_tests()
        .named_type(&fixture.value_type)
        .expect("target type source should bind");
    let constraint_id = bundle
        .source_bindings_for_tests()
        .constraint(entity_tag, &fixture.constraint_source)
        .expect("constraint source should bind");
    let snapshot = &bundle.entity_snapshots()[&entity_tag];
    let accepted = snapshot
        .constraints()
        .iter()
        .find(|constraint| constraint.id() == constraint_id)
        .expect("accepted targeted rule should persist");
    let AcceptedConstraintKind::TargetedRule { target, operation } = accepted.kind() else {
        panic!("source targeted rule must not lower through a general check");
    };
    assert_eq!(target.root_field_id(), root_field_id);
    assert_eq!(target.target_type(), target_type);
    let AcceptedRuleOperation::NumericRangeInclusive { min, max } = operation.as_ref() else {
        panic!("numeric range operation should remain closed and exact");
    };
    assert_eq!(min.kind(), &crate::db::schema::AcceptedFieldKind::Nat8);
    assert_eq!(max.kind(), &crate::db::schema::AcceptedFieldKind::Nat8);
    assert!(!min.payload().is_empty());
    assert!(!max.payload().is_empty());
    let accepted_schema =
        AcceptedSchemaSnapshot::try_new(snapshot.clone()).expect("snapshot should be accepted");
    let value_catalog = AcceptedValueCatalogHandle::new(
        bundle.enum_catalog().clone(),
        bundle.composite_catalog().clone(),
        AcceptedStoreCatalogScope::new(),
        bundle.revision(),
        bundle
            .semantic_fingerprint()
            .expect("bundle fingerprint should derive"),
    );
    let program = CompiledAcceptedRowConstraints::compile(
        &accepted_schema,
        &value_catalog,
        [0xA3; 16],
        &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
    )
    .expect("N4 should compile the accepted targeted rule");
    program
        .evaluate(
            [0xA3; 16],
            &[
                Some(Cow::Owned(Value::Ulid(crate::types::Ulid::from_u128(1)))),
                Some(Cow::Owned(Value::Nat64(5))),
                Some(Cow::Owned(Value::Nat64(0))),
            ],
        )
        .expect("N5 should admit a compliant targeted value");
    assert!(
        bundle.semantic_fingerprint().is_ok(),
        "accepted target and operation must participate in the bundle fingerprint",
    );
    let repeated = lower_initial_schema_proposal(
        &fixture.proposal,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: fixture.store,
        }],
    )
    .expect("repeated targeted proposal should lower");
    assert_eq!(
        candidates[0].encoded_bundle(),
        repeated[0].encoded_bundle(),
        "accepted target encoding must be canonical",
    );
}

#[test]
fn accepted_target_binding_rejects_retargeting_across_root_fields() {
    let fixture = targeted_rule_proposal_fixture(
        ExpectedAcceptedHead::Empty,
        "targeted-retarget",
        "Value",
        "Other",
        true,
        Vec::new(),
    );
    let candidates = lower_initial_schema_proposal(
        &fixture.proposal,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: fixture.store,
        }],
    )
    .expect("valid targeted proposal should lower");
    let bundle = candidates[0].bundle();
    let entity_tag = bundle
        .source_bindings_for_tests()
        .entity(&fixture.entity_source)
        .expect("entity source should bind");
    let other_type = bundle
        .source_bindings_for_tests()
        .named_type(&fixture.other_type)
        .expect("other type source should bind");
    let mut snapshots = bundle.entity_snapshots().clone();
    let snapshot = snapshots
        .get(&entity_tag)
        .cloned()
        .expect("entity snapshot should exist");
    let constraints = snapshot
        .constraints()
        .iter()
        .map(|constraint| {
            let kind = match constraint.kind() {
                AcceptedConstraintKind::TargetedRule { target, operation } => {
                    AcceptedConstraintKind::TargetedRule {
                        target: AcceptedRuleTarget::new(target.root_field_id(), other_type),
                        operation: operation.clone(),
                    }
                }
                _ => constraint.kind().clone(),
            };
            AcceptedConstraintSnapshot::new(
                constraint.id(),
                constraint.name().to_string(),
                constraint.origin(),
                kind,
            )
        })
        .collect();
    let malformed_catalog = AcceptedConstraintCatalog::from_persisted_parts(
        snapshot.constraint_id_allocator(),
        constraints,
        snapshot.constraint_activations().to_vec(),
    );
    snapshots.insert(
        entity_tag,
        snapshot.with_constraint_catalog(malformed_catalog),
    );

    assert!(
        AcceptedSchemaRevisionBundle::new_with_source_bindings(
            bundle.revision(),
            bundle.store_path(),
            bundle.enum_catalog().clone(),
            bundle.composite_catalog().clone(),
            bundle.source_bindings_for_tests().clone(),
            snapshots,
        )
        .is_err(),
        "accepted target must remain reachable below its bound root field",
    );
}

#[test]
fn existing_targeted_rule_addition_reserves_source_bound_activation() {
    let initial = targeted_rule_proposal_fixture(
        ExpectedAcceptedHead::Empty,
        "targeted-addition-initial",
        "Value",
        "Other",
        false,
        Vec::new(),
    );
    let initial_candidates = lower_initial_schema_proposal(
        &initial.proposal,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: initial.store,
        }],
    )
    .expect("initial proposal without a targeted rule should lower");
    let addition = targeted_rule_proposal_fixture(
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x33; 32]),
        },
        "targeted-addition-existing",
        "Value",
        "Other",
        true,
        Vec::new(),
    );

    let candidate = lower_existing_schema_proposal(
        &addition.proposal,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: initial.store,
            bundle: initial_candidates[0].bundle(),
        }],
    )
    .expect("N6 should lower the targeted rule through accepted activation")
    .pop()
    .expect("targeted addition should produce one candidate");
    let entity_tag = candidate
        .bundle()
        .source_bindings_for_tests()
        .entity(&addition.entity_source)
        .expect("entity source should remain bound");
    let constraint_id = candidate
        .bundle()
        .source_bindings_for_tests()
        .constraint(entity_tag, &addition.constraint_source)
        .expect("targeted source should bind to its reserved activation");
    assert!(
        matches!(
            candidate.bundle().entity_snapshots()[&entity_tag]
                .constraint_catalog()
                .activation(constraint_id)
                .map(crate::db::schema::ConstraintActivationSnapshot::kind),
            Some(ConstraintActivationKind::TargetedRule { .. })
        ),
        "targeted addition must remain pending until historical proof",
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the stable identity, write gate, and direct promotion assertions form one lifecycle"
)]
fn targeted_rule_semantic_edit_stages_and_promotes_under_stable_accepted_identity() {
    let initial = targeted_rule_proposal_fixture(
        ExpectedAcceptedHead::Empty,
        "targeted-edit-initial",
        "Value",
        "Other",
        true,
        Vec::new(),
    );
    let initial_candidate = lower_initial_schema_proposal(
        &initial.proposal,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: initial.store,
        }],
    )
    .expect("initial targeted rule should lower")
    .pop()
    .expect("initial targeted rule should produce one candidate");
    let initial_bundle = initial_candidate.bundle();
    let entity_tag = initial_bundle
        .source_bindings_for_tests()
        .entity(&initial.entity_source)
        .expect("entity source should bind");
    let constraint_id = initial_bundle
        .source_bindings_for_tests()
        .constraint(entity_tag, &initial.constraint_source)
        .expect("targeted source should bind");
    let initial_snapshot = &initial_bundle.entity_snapshots()[&entity_tag];
    let initial_high_water = initial_snapshot.constraint_id_allocator().high_water();

    let edited = targeted_rule_proposal_fixture_with_operation(
        ExpectedAcceptedHead::Exact {
            revision: initial_bundle.revision().get(),
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x34; 32]),
        },
        "targeted-edit-candidate",
        "Value",
        "Other",
        "range",
        Some(SourceRuleOperation::NumericMaximumInclusive {
            value: ScalarLiteral::Nat(8),
        }),
        Vec::new(),
    );
    let candidate = lower_existing_schema_proposal(
        &edited.proposal,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: initial.store,
            bundle: initial_bundle,
        }],
    )
    .expect("same-identity semantic edit should stage through accepted activation")
    .pop()
    .expect("semantic edit should produce one candidate");
    let staged = &candidate.bundle().entity_snapshots()[&entity_tag];

    assert_eq!(
        candidate
            .bundle()
            .source_bindings_for_tests()
            .constraint(entity_tag, &edited.constraint_source),
        Some(constraint_id),
    );
    assert_eq!(
        staged.constraint_id_allocator().high_water(),
        initial_high_water,
        "semantic edits must not allocate a replacement identity",
    );
    assert!(staged.constraints().iter().any(|constraint| {
        constraint.id() == constraint_id
            && matches!(
                constraint.kind(),
                AcceptedConstraintKind::TargetedRule { operation, .. }
                    if matches!(
                        operation.as_ref(),
                        AcceptedRuleOperation::NumericRangeInclusive { .. }
                    )
            )
    }));
    assert!(matches!(
        staged
            .constraint_catalog()
            .activation(constraint_id)
            .map(crate::db::schema::ConstraintActivationSnapshot::kind),
        Some(ConstraintActivationKind::TargetedRule { operation, .. })
            if matches!(
                operation.as_ref(),
                AcceptedRuleOperation::NumericMaximumInclusive { .. }
            )
    ));

    let accepted_schema = AcceptedSchemaSnapshot::try_new(staged.clone())
        .expect("staged replacement should remain accepted row authority");
    let value_catalog = AcceptedValueCatalogHandle::new(
        candidate.bundle().enum_catalog().clone(),
        candidate.bundle().composite_catalog().clone(),
        AcceptedStoreCatalogScope::new(),
        candidate.revision(),
        candidate.root().fingerprint(),
    );
    let program = CompiledAcceptedRowConstraints::compile(
        &accepted_schema,
        &value_catalog,
        [0xA4; 16],
        &crate::db::resource::budget::MaintenanceConstructionBudget::new(),
    )
    .expect("old accepted rule and candidate write gate should compile together");
    let target_field = match staged
        .constraint_catalog()
        .activation(constraint_id)
        .expect("replacement activation should remain staged")
        .kind()
    {
        ConstraintActivationKind::TargetedRule { target, .. } => target.root_field_id(),
        _ => panic!("replacement activation should remain targeted"),
    };
    let target_slot = staged
        .row_layout()
        .slot_for_field(target_field)
        .expect("target root should retain a row slot");
    let mut values = vec![Some(Value::Nat64(0)); staged.row_layout().allocated_slot_count()];
    values[usize::from(target_slot.get())] = Some(Value::Nat64(9));
    assert!(
        program
            .evaluate(
                [0xA4; 16],
                &values
                    .iter()
                    .map(|value| value.as_ref().map(Cow::Borrowed))
                    .collect::<Vec<_>>()
            )
            .is_err(),
        "a row admitted by the old range must still be rejected by the staged maximum",
    );

    let promoted_catalog = staged
        .constraint_catalog()
        .clone()
        .with_directly_validated_activation(constraint_id)
        .expect("bounded proof should promote the replacement");
    assert!(promoted_catalog.activation(constraint_id).is_none());
    assert!(promoted_catalog.constraints().iter().any(|constraint| {
        constraint.id() == constraint_id
            && matches!(
                constraint.kind(),
                AcceptedConstraintKind::TargetedRule { operation, .. }
                    if matches!(
                        operation.as_ref(),
                        AcceptedRuleOperation::NumericMaximumInclusive { .. }
                    )
            )
    }));
}

#[test]
fn targeted_rule_local_name_replacement_is_explicit_remove_and_fresh_add() {
    let initial = targeted_rule_proposal_fixture(
        ExpectedAcceptedHead::Empty,
        "targeted-rename-initial",
        "Value",
        "Other",
        true,
        Vec::new(),
    );
    let initial_candidate = lower_initial_schema_proposal(
        &initial.proposal,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: initial.store,
        }],
    )
    .expect("initial targeted rule should lower")
    .pop()
    .expect("initial targeted rule should produce one candidate");
    let initial_bundle = initial_candidate.bundle();
    let entity_tag = initial_bundle
        .source_bindings_for_tests()
        .entity(&initial.entity_source)
        .expect("entity source should bind");
    let old_id = initial_bundle
        .source_bindings_for_tests()
        .constraint(entity_tag, &initial.constraint_source)
        .expect("old targeted source should bind");
    let replacement = targeted_rule_proposal_fixture_with_operation(
        ExpectedAcceptedHead::Exact {
            revision: initial_bundle.revision().get(),
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x35; 32]),
        },
        "targeted-rename-replacement",
        "Value",
        "Other",
        "cap",
        Some(SourceRuleOperation::NumericMaximumInclusive {
            value: ScalarLiteral::Nat(8),
        }),
        vec![SchemaRemoval::Constraint {
            entity: initial.entity_source.clone(),
            constraint: initial.constraint_source.clone(),
        }],
    );
    let candidate = lower_existing_schema_proposal(
        &replacement.proposal,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: initial.store,
            bundle: initial_bundle,
        }],
    )
    .expect("explicit remove/add should lower")
    .pop()
    .expect("explicit remove/add should produce one candidate");
    let new_id = candidate
        .bundle()
        .source_bindings_for_tests()
        .constraint(entity_tag, &replacement.constraint_source)
        .expect("replacement source should bind");

    assert!(new_id > old_id);
    assert_eq!(
        candidate
            .bundle()
            .source_bindings_for_tests()
            .constraint(entity_tag, &initial.constraint_source),
        None,
    );
    assert!(
        candidate.bundle().entity_snapshots()[&entity_tag]
            .constraint_catalog()
            .activation(new_id)
            .is_some(),
    );
    assert!(
        !candidate.bundle().entity_snapshots()[&entity_tag]
            .constraints()
            .iter()
            .any(|constraint| constraint.id() == old_id)
    );
}

#[test]
fn existing_scalar_default_change_preserves_structural_owner_identity() {
    let (initial, entity_source, store) =
        scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "existing-initial", 5);
    let initial_candidates = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("initial scalar proposal should lower");
    let initial_bundle = initial_candidates[0].bundle();
    let (changed, _, _) = scalar_proposal_fixture(
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
        },
        "existing-default-change",
        7,
    );

    let candidates = lower_existing_schema_proposal(
        &changed,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: store,
            bundle: initial_bundle,
        }],
    )
    .expect("existing future-default proposal should lower");

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].revision().get(), 2);
    let changed_bundle = candidates[0].bundle();
    assert_eq!(
        changed_bundle.source_bindings_for_tests(),
        initial_bundle.source_bindings_for_tests(),
    );
    let entity_tag = changed_bundle
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should remain bound");
    let before = initial_bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("initial entity should exist");
    let after = changed_bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("changed entity should exist");
    assert_eq!(after.row_layout(), before.row_layout());
    assert_eq!(after.indexes(), before.indexes());
    assert_eq!(after.constraint_catalog(), before.constraint_catalog());
    assert_ne!(after.fields(), before.fields());
}

#[test]
fn existing_generated_check_addition_reserves_one_source_bound_activation() {
    let (initial, entity_source, store) = scalar_proposal_fixture_with_names(
        ExpectedAcceptedHead::Empty,
        "check-addition-initial",
        5,
        "Item",
        "score",
        false,
        Vec::new(),
    );
    let initial_candidates = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("initial proposal without a generated check should lower");
    let initial_bundle = initial_candidates[0].bundle();
    let (addition, _, _) = scalar_proposal_fixture(
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
        },
        "add-generated-check",
        5,
    );

    let stores = [ExistingProposalStore {
        path: "test::Store",
        identity: store,
        bundle: initial_bundle,
    }];
    let candidates = lower_existing_schema_proposal(&addition, &stores)
        .expect("generated check addition should lower");
    let generated = lower_generated_existing_schema_proposal(&addition, &stores)
        .expect("sealed generated check addition should lower");
    assert_eq!(generated[0].bundle(), candidates[0].bundle());
    let added_bundle = candidates[0].bundle();
    let entity_tag = added_bundle
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should remain bound");
    let before = initial_bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("initial entity should exist");
    let after = added_bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("entity should survive check addition");
    let source = ConstraintSourceKey::try_new("score_non_negative")
        .expect("test constraint source should admit");
    let added_id = added_bundle
        .source_bindings_for_tests()
        .constraint(entity_tag, &source)
        .expect("new check source should bind");

    assert_eq!(after.version().get(), before.version().get() + 1);
    assert_eq!(
        after.constraint_id_allocator().high_water(),
        before.constraint_id_allocator().high_water() + 1,
    );
    assert!(after.constraint_activations().iter().any(|constraint| {
        constraint.id() == added_id
            && constraint.origin() == ConstraintOrigin::Generated
            && matches!(constraint.kind(), ConstraintActivationKind::Check { .. })
    }));
    let pending_stores = [ExistingProposalStore {
        path: "test::Store",
        identity: store,
        bundle: added_bundle,
    }];
    for removal in [
        SchemaRemoval::Entity(entity_source.clone()),
        SchemaRemoval::Constraint {
            entity: entity_source.clone(),
            constraint: source,
        },
        SchemaRemoval::Field {
            entity: entity_source.clone(),
            field: FieldSourceKey::try_new("score").unwrap(),
        },
        SchemaRemoval::Index {
            entity: entity_source,
            index: icydb_schema::IndexSourceKey::try_new("score_idx").unwrap(),
        },
    ] {
        let error = resolve_existing_removal(&pending_stores, &removal)
            .err()
            .expect("pending validation must fence removals");
        assert_eq!(error.class(), crate::error::ErrorClass::Unsupported);
    }
    assert_eq!(after.fields(), before.fields());
    assert_eq!(after.row_layout(), before.row_layout());
    assert_eq!(after.indexes(), before.indexes());
    assert_eq!(after.relations(), before.relations());
}

#[test]
fn removals_require_one_unambiguous_accepted_entity_source() {
    let (initial, entity_source, store) =
        scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "removal-resolution", 5);
    let candidates = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("initial proposal should lower");
    let stores = [
        ExistingProposalStore {
            path: "test::Store",
            identity: store,
            bundle: candidates[0].bundle(),
        },
        ExistingProposalStore {
            path: "test::Other",
            identity: TargetStoreIdentity::from_bytes([0x23; 32]),
            bundle: candidates[0].bundle(),
        },
    ];
    let removal = SchemaRemoval::Field {
        entity: entity_source,
        field: FieldSourceKey::try_new("score").unwrap(),
    };
    assert!(resolve_existing_removal(&stores[..1], &removal).is_ok());
    for sources in [&stores[..0], &stores[..]] {
        let error = resolve_existing_removal(sources, &removal)
            .err()
            .expect("missing or ambiguous authority must reject");
        assert_eq!(error.class(), crate::error::ErrorClass::Unsupported);
    }
}

#[test]
fn generated_removals_reject_missing_bindings_for_every_member_kind() {
    let (initial, entity, identity) =
        scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "missing-removal-bindings", 5);
    let candidates = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity,
        }],
    )
    .unwrap();
    let stores = [ExistingProposalStore {
        path: "test::Store",
        identity,
        bundle: candidates[0].bundle(),
    }];
    for removal in [
        SchemaRemoval::Field {
            entity: entity.clone(),
            field: FieldSourceKey::try_new("missing").unwrap(),
        },
        SchemaRemoval::Index {
            entity: entity.clone(),
            index: IndexSourceKey::try_new("missing").unwrap(),
        },
        SchemaRemoval::Relation {
            entity: entity.clone(),
            relation: icydb_schema::RelationSourceKey::try_new("missing").unwrap(),
        },
        SchemaRemoval::Constraint {
            entity,
            constraint: ConstraintSourceKey::try_new("missing").unwrap(),
        },
    ] {
        let error = resolve_existing_removal(&stores, &removal)
            .err()
            .expect("unknown source identity must refuse removal");
        assert_eq!(
            error.diagnostic_code(),
            crate::error::InternalError::store_unsupported().diagnostic_code()
        );
    }
}

#[test]
fn existing_generated_check_removal_retires_only_its_source_bound_owner() {
    let (initial, entity_source, store) =
        scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "removal-initial", 5);
    let initial_candidates = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("initial scalar proposal should lower");
    let initial_bundle = initial_candidates[0].bundle();
    let removal = SchemaProposal::try_compose(
        vec![SchemaCapability::ACCEPTED_CHECKS],
        TargetDatabaseIdentity::from_bytes([0x11; 32]),
        SchemaSubmissionKey::try_new("remove-generated-check")
            .expect("test submission key should admit"),
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
        },
        Vec::new(),
        Vec::new(),
        vec![SchemaRemoval::Constraint {
            entity: entity_source.clone(),
            constraint: ConstraintSourceKey::try_new("score_non_negative")
                .expect("test constraint source should admit"),
        }],
        None,
    )
    .expect("exact removal proposal should compose");

    let stores = [ExistingProposalStore {
        path: "test::Store",
        identity: store,
        bundle: initial_bundle,
    }];
    assert!(
        lower_generated_existing_schema_proposal(&removal, &stores).is_err(),
        "sealed generated ingress must reject explicit removals",
    );
    let candidates = lower_existing_schema_proposal(&removal, &stores)
        .expect("explicit generated check removal should lower");

    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].revision().get(), 2);
    let removed_bundle = candidates[0].bundle();
    let entity_tag = initial_bundle
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should bind");
    let before = initial_bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("initial entity should exist");
    let after = removed_bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("entity should survive check removal");
    let removed_id = initial_bundle
        .source_bindings_for_tests()
        .constraint(
            entity_tag,
            &ConstraintSourceKey::try_new("score_non_negative")
                .expect("test constraint source should admit"),
        )
        .expect("initial check source should bind");

    assert_eq!(after.version().get(), before.version().get() + 1);
    assert_eq!(
        after.constraint_id_allocator(),
        before.constraint_id_allocator()
    );
    assert!(
        after
            .constraints()
            .iter()
            .all(|constraint| constraint.id() != removed_id)
    );
    assert_eq!(after.fields(), before.fields());
    assert_eq!(after.row_layout(), before.row_layout());
    assert_eq!(after.indexes(), before.indexes());
    assert_eq!(after.relations(), before.relations());
    assert_eq!(
        removed_bundle
            .source_bindings_for_tests()
            .constraint_binding_count_for_tests(entity_tag),
        0,
    );
    assert_eq!(
        removed_bundle
            .source_bindings_for_tests()
            .field_binding_count_for_tests(entity_tag),
        2,
    );
    assert_eq!(
        removed_bundle
            .source_bindings_for_tests()
            .index_binding_count_for_tests(entity_tag),
        1,
    );
}

#[test]
fn existing_generated_field_removal_rejects_an_index_dependency() {
    let (initial, entity_source, store) =
        scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "physical-removal-initial", 5);
    let initial_candidates = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("initial scalar proposal should lower");
    let removal = SchemaProposal::try_compose(
        Vec::new(),
        TargetDatabaseIdentity::from_bytes([0x11; 32]),
        SchemaSubmissionKey::try_new("remove-generated-field")
            .expect("test submission key should admit"),
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
        },
        Vec::new(),
        Vec::new(),
        vec![SchemaRemoval::Field {
            entity: entity_source,
            field: FieldSourceKey::try_new("score").expect("test field source should admit"),
        }],
        None,
    )
    .expect("exact physical removal proposal should compose");

    assert!(
        lower_existing_schema_proposal(
            &removal,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: store,
                bundle: initial_candidates[0].bundle(),
            }],
        )
        .is_err(),
        "field removal must not discard a dependent accepted index",
    );
}

#[test]
fn existing_generated_index_removal_uses_stable_source_identity() {
    let (initial, entity_source, store) =
        scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "index-removal-initial", 5);
    let initial_candidates = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("initial scalar proposal should lower");
    let initial_bundle = initial_candidates[0].bundle();
    let entity_tag = initial_bundle
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should bind");
    let removed_id = initial_bundle
        .source_bindings_for_tests()
        .index(
            entity_tag,
            &IndexSourceKey::try_new("score_idx").expect("test index source should admit"),
        )
        .expect("index source should bind");
    let removal = SchemaProposal::try_compose(
        Vec::new(),
        TargetDatabaseIdentity::from_bytes([0x11; 32]),
        SchemaSubmissionKey::try_new("remove-generated-index")
            .expect("test submission key should admit"),
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
        },
        Vec::new(),
        Vec::new(),
        vec![SchemaRemoval::Index {
            entity: entity_source,
            index: IndexSourceKey::try_new("score_idx").expect("test index source should admit"),
        }],
        None,
    )
    .expect("exact index removal proposal should compose");

    let candidates = lower_existing_schema_proposal(
        &removal,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: store,
            bundle: initial_bundle,
        }],
    )
    .expect("generated index removal should lower");
    let removed_bundle = candidates[0].bundle();
    let after = removed_bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("entity should survive index removal");

    assert_eq!(after.version().get(), 2);
    assert!(
        after
            .indexes()
            .iter()
            .all(|index| index.schema_id() != removed_id)
    );
    assert_eq!(
        removed_bundle
            .source_bindings_for_tests()
            .index_binding_count_for_tests(entity_tag),
        0,
    );
}

#[test]
fn existing_check_removal_and_default_change_advance_schema_version_once() {
    let (initial, _, store) =
        scalar_proposal_fixture(ExpectedAcceptedHead::Empty, "combined-change-initial", 5);
    let initial_candidates = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("initial scalar proposal should lower");
    let (changed, entity_source, _) = scalar_proposal_fixture_with_names(
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x44; 32]),
        },
        "combined-check-removal-default-change",
        7,
        "Item",
        "score",
        false,
        vec![SchemaRemoval::Constraint {
            entity: EntitySourceKey::try_new("Item").expect("test entity source should admit"),
            constraint: ConstraintSourceKey::try_new("score_non_negative")
                .expect("test constraint source should admit"),
        }],
    );

    let candidates = lower_existing_schema_proposal(
        &changed,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: store,
            bundle: initial_candidates[0].bundle(),
        }],
    )
    .expect("combined metadata change should lower");
    let bundle = candidates[0].bundle();
    let entity_tag = bundle
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("entity source should remain bound");
    let snapshot = bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("entity should survive combined change");
    let before = initial_candidates[0]
        .bundle()
        .entity_snapshots()
        .get(&entity_tag)
        .expect("initial entity should exist");

    assert_eq!(snapshot.version().get(), 2);
    assert_eq!(
        bundle
            .source_bindings_for_tests()
            .constraint_binding_count_for_tests(entity_tag),
        0,
    );
    assert_ne!(snapshot.fields(), before.fields());
    assert!(matches!(
        snapshot
            .fields()
            .iter()
            .find(|field| field.name() == "score")
            .expect("score should exist")
            .insert_default(),
        SchemaInsertDefault::SlotPayload(payload) if !payload.is_empty()
    ));
}

struct NamedTypeKeys {
    status: TypeSourceKey,
    profile: TypeSourceKey,
    score: TypeSourceKey,
    tags: TypeSourceKey,
    roles: TypeSourceKey,
    counters: TypeSourceKey,
    pair: TypeSourceKey,
}

fn named_type_fragments() -> (NamedTypeKeys, TypeSourceKey, Vec<NamedTypeFragment>) {
    let keys = NamedTypeKeys {
        status: TypeSourceKey::try_new("Status").expect("type key should admit"),
        profile: TypeSourceKey::try_new("Profile").expect("type key should admit"),
        score: TypeSourceKey::try_new("Score").expect("type key should admit"),
        tags: TypeSourceKey::try_new("Tags").expect("type key should admit"),
        roles: TypeSourceKey::try_new("Roles").expect("type key should admit"),
        counters: TypeSourceKey::try_new("Counters").expect("type key should admit"),
        pair: TypeSourceKey::try_new("Pair").expect("type key should admit"),
    };
    let active = TypeSourceKey::try_new("Active").expect("variant key should admit");
    let variants = vec![
        EnumVariantFragment::new(name("Active")),
        EnumVariantFragment::with_payload(
            name("Disabled"),
            FieldType::List(Box::new(FieldType::Scalar(ScalarType::Nat16))),
        ),
    ];
    let record_fields = vec![
        RecordFieldFragment::new(
            name("label"),
            FieldType::Scalar(ScalarType::Text { max_len: Some(64) }),
            false,
        ),
        RecordFieldFragment::new(name("status"), FieldType::Named(keys.status.clone()), false),
    ];
    let types = vec![
        NamedTypeFragment::Enum(
            EnumTypeFragment::try_new(name("Status"), variants).expect("enum should admit"),
        ),
        NamedTypeFragment::Record(
            RecordTypeFragment::try_new(name("Profile"), record_fields)
                .expect("record should admit"),
        ),
        NamedTypeFragment::newtype(name("Score"), FieldType::Scalar(ScalarType::Int64)),
        NamedTypeFragment::list(
            name("Tags"),
            FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
        ),
        NamedTypeFragment::set(
            name("Roles"),
            FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
        ),
        NamedTypeFragment::map(
            name("Counters"),
            FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
            FieldType::Scalar(ScalarType::Nat64),
        ),
        NamedTypeFragment::tuple(
            name("Pair"),
            vec![
                TupleElementFragment::new(
                    FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
                    false,
                ),
                TupleElementFragment::new(FieldType::Scalar(ScalarType::Nat64), true),
            ],
        ),
    ];
    (keys, active, types)
}

fn named_holder_entity(
    keys: &NamedTypeKeys,
    active: TypeSourceKey,
) -> (EntitySourceKey, EntityFragment) {
    let entity_source = EntitySourceKey::try_new("Holder").expect("entity key should admit");
    let id_source = FieldSourceKey::try_new("id").expect("field key should admit");
    let mut fields = vec![FieldFragment::new(
        name("id"),
        FieldType::Scalar(ScalarType::Nat64),
        false,
        FieldInsertPolicy::Required,
        None,
    )];
    for (suffix, field_type) in [
        ("profile", keys.profile.clone()),
        ("score", keys.score.clone()),
        ("tags", keys.tags.clone()),
        ("roles", keys.roles.clone()),
        ("counters", keys.counters.clone()),
        ("pair", keys.pair.clone()),
    ] {
        fields.push(FieldFragment::new(
            name(suffix),
            FieldType::Named(field_type),
            false,
            FieldInsertPolicy::Required,
            None,
        ));
    }
    fields.push(FieldFragment::new(
        name("status"),
        FieldType::Named(keys.status.clone()),
        false,
        FieldInsertPolicy::Default(ScalarLiteral::EnumUnit {
            enum_type: keys.status.clone(),
            variant: active,
        }),
        None,
    ));
    let entity = EntityFragment::try_new(
        name("Holder"),
        version_one(),
        fields,
        vec![id_source],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .expect("entity should admit");
    (entity_source, entity)
}

fn named_other_entity(status: &TypeSourceKey) -> (EntitySourceKey, EntityFragment) {
    let source = EntitySourceKey::try_new("Other").expect("entity key should admit");
    let id = FieldSourceKey::try_new("id").expect("field key should admit");
    let entity = EntityFragment::try_new(
        name("Other"),
        version_one(),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("status"),
                FieldType::Named(status.clone()),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![id],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .expect("other entity should admit");
    (source, entity)
}

fn assert_primary_named_bundle(
    bundle: &super::AcceptedSchemaRevisionBundle,
    entity_source: &EntitySourceKey,
    status: &TypeSourceKey,
) {
    let bindings = bundle.source_bindings_for_tests();
    assert_eq!(bindings.named_type_binding_count_for_tests(), 7);
    assert_eq!(bindings.enum_variant_binding_count_for_tests(), 2);
    let status_id = match bindings
        .named_type(status)
        .expect("enum source should bind")
    {
        super::AcceptedNamedTypeIdentity::Enum(type_id) => type_id,
        super::AcceptedNamedTypeIdentity::Composite(_) => {
            panic!("status should bind to the enum catalog")
        }
    };
    assert_eq!(
        bundle
            .enum_catalog()
            .enum_type(status_id)
            .expect("status enum should exist")
            .variant_count(),
        2,
    );
    assert_payload_enum_and_nullable_tuple(bundle, status_id);
    let profile_id = bundle
        .composite_catalog()
        .type_id("Profile")
        .expect("profile composite should exist");
    let super::AcceptedCompositeShape::Record(profile_fields) = bundle
        .composite_catalog()
        .composite_type(profile_id)
        .expect("profile definition should exist")
        .shape()
    else {
        panic!("profile should remain a record")
    };
    let label_source = FieldSourceKey::try_new("label").expect("field source should admit");
    let status_source = FieldSourceKey::try_new("status").expect("field source should admit");
    let label_id = bindings
        .composite_field(profile_id, &label_source)
        .expect("label source should bind");
    let status_field_id = bindings
        .composite_field(profile_id, &status_source)
        .expect("status source should bind");
    assert_eq!(
        bindings.composite_field_binding_count_for_tests(profile_id),
        profile_fields.len(),
    );
    assert_eq!(
        profile_fields
            .iter()
            .find(|field| field.name() == "label")
            .map(super::AcceptedCompositeField::id),
        Some(label_id),
    );
    assert_eq!(
        profile_fields
            .iter()
            .find(|field| field.name() == "status")
            .map(super::AcceptedCompositeField::id),
        Some(status_field_id),
    );
    assert!(matches!(
        profile_fields[1].contract().kind(),
        super::AcceptedFieldKind::Enum { type_id } if *type_id == status_id
    ));
    for type_name in ["Score", "Tags", "Roles", "Counters", "Pair"] {
        assert!(bundle.composite_catalog().type_id(type_name).is_some());
    }
    let entity_tag = bindings
        .entity(entity_source)
        .expect("entity source should bind");
    let snapshot = bundle
        .entity_snapshots()
        .get(&entity_tag)
        .expect("entity snapshot should exist");
    assert!(
        snapshot
            .fields()
            .iter()
            .filter(|field| field.name() != "id")
            .all(|field| {
                field.storage_decode() == super::FieldStorageDecode::CatalogValue
                    && field.leaf_codec() == super::LeafCodec::Structural
            })
    );
    assert!(matches!(
        snapshot
            .fields()
            .iter()
            .find(|field| field.name() == "status")
            .expect("status should exist")
            .insert_default(),
        SchemaInsertDefault::SlotPayload(payload) if !payload.is_empty()
    ));
}

fn assert_payload_enum_and_nullable_tuple(
    bundle: &super::AcceptedSchemaRevisionBundle,
    status_id: super::EnumTypeId,
) {
    let disabled_source = TypeSourceKey::try_new("Disabled").expect("variant source should admit");
    let disabled_id = bundle
        .source_bindings_for_tests()
        .enum_variant(status_id, &disabled_source)
        .expect("payload variant source should bind");
    let disabled = bundle
        .enum_catalog()
        .enum_type(status_id)
        .and_then(|definition| definition.variant(disabled_id))
        .expect("payload variant should exist");
    assert!(matches!(
        disabled.body(),
        AcceptedEnumVariantBody::Payload { contract }
            if matches!(
                contract.kind(),
                super::AcceptedFieldKind::List(item)
                    if matches!(item.as_ref(), super::AcceptedFieldKind::Nat16)
            )
                && contract.storage_decode() == super::FieldStorageDecode::ByKind
    ));

    let pair_id = bundle
        .composite_catalog()
        .type_id("Pair")
        .expect("pair composite should exist");
    let super::AcceptedCompositeShape::Tuple(pair) = bundle
        .composite_catalog()
        .composite_type(pair_id)
        .expect("pair definition should exist")
        .shape()
    else {
        panic!("pair should remain a tuple")
    };
    assert!(!pair[0].nullable());
    assert!(pair[1].nullable());
}

#[test]
// Keep the complete downstream schema shape visible in one regression.
#[expect(clippy::too_many_lines)]
fn initial_lowering_publishes_and_round_trips_collection_named_type_cycles() {
    let field_key = TypeSourceKey::try_new("FieldKey").expect("type source should admit");
    let values = TypeSourceKey::try_new("Values").expect("type source should admit");
    let field_value = TypeSourceKey::try_new("FieldValue").expect("type source should admit");
    let value = TypeSourceKey::try_new("Value").expect("type source should admit");
    let tokens = TypeSourceKey::try_new("Tokens").expect("type source should admit");
    let token_amount = TypeSourceKey::try_new("TokenAmount").expect("type source should admit");
    let tier = TypeSourceKey::try_new("Tier").expect("type source should admit");
    let claim_cost = TypeSourceKey::try_new("ClaimCost").expect("type source should admit");
    let claim_cost_tiers =
        TypeSourceKey::try_new("ClaimCostTiers").expect("type source should admit");
    let policy = TypeSourceKey::try_new("CollectionPolicy").expect("type source should admit");
    let named_types = vec![
        NamedTypeFragment::newtype(
            name("FieldKey"),
            FieldType::Scalar(ScalarType::Text { max_len: Some(64) }),
        ),
        NamedTypeFragment::map(
            name("Values"),
            FieldType::Named(field_key.clone()),
            FieldType::Named(field_value.clone()),
        ),
        NamedTypeFragment::Enum(
            EnumTypeFragment::try_new(
                name("FieldValue"),
                vec![
                    EnumVariantFragment::with_payload(name("One"), FieldType::Named(value.clone())),
                    EnumVariantFragment::with_payload(
                        name("Many"),
                        FieldType::List(Box::new(FieldType::Named(value.clone()))),
                    ),
                ],
            )
            .expect("field-value enum should admit"),
        ),
        NamedTypeFragment::Enum(
            EnumTypeFragment::try_new(
                name("Value"),
                vec![
                    EnumVariantFragment::with_payload(
                        name("Text"),
                        FieldType::Scalar(ScalarType::Text { max_len: Some(128) }),
                    ),
                    EnumVariantFragment::with_payload(
                        name("Record"),
                        FieldType::Named(values.clone()),
                    ),
                ],
            )
            .expect("value enum should admit"),
        ),
        NamedTypeFragment::newtype(name("Tokens"), FieldType::Scalar(ScalarType::Nat64)),
        NamedTypeFragment::newtype(name("TokenAmount"), FieldType::Scalar(ScalarType::Nat64)),
        NamedTypeFragment::newtype(
            name("Tier"),
            FieldType::Scalar(ScalarType::Text { max_len: Some(32) }),
        ),
        NamedTypeFragment::Enum(
            EnumTypeFragment::try_new(
                name("ClaimCost"),
                vec![
                    EnumVariantFragment::new(name("Free")),
                    EnumVariantFragment::with_payload(name("Icp"), FieldType::Named(tokens)),
                    EnumVariantFragment::with_payload(
                        name("Icrc1"),
                        FieldType::Named(token_amount),
                    ),
                ],
            )
            .expect("claim-cost enum should admit"),
        ),
        NamedTypeFragment::map(
            name("ClaimCostTiers"),
            FieldType::Named(tier),
            FieldType::Named(claim_cost.clone()),
        ),
        NamedTypeFragment::Record(
            RecordTypeFragment::try_new(
                name("CollectionPolicy"),
                vec![
                    RecordFieldFragment::new(
                        name("claim_cost_tiers"),
                        FieldType::Named(claim_cost_tiers),
                        false,
                    ),
                    RecordFieldFragment::new(
                        name("fallback"),
                        FieldType::Named(value.clone()),
                        true,
                    ),
                    RecordFieldFragment::new(
                        name("values"),
                        FieldType::Named(values.clone()),
                        false,
                    ),
                ],
            )
            .expect("collection policy should admit"),
        ),
    ];
    let entity_source = EntitySourceKey::try_new("Collection").expect("entity source should admit");
    let id_source = FieldSourceKey::try_new("id").expect("field source should admit");
    let policy_source = FieldSourceKey::try_new("policy").expect("field source should admit");
    let rule_source =
        icydb_schema::RuleSourceKey::try_new("field_key_length").expect("rule source should admit");
    let constraint_source =
        ConstraintSourceKey::for_targeted_field_rule(&policy_source, &field_key, &rule_source);
    let entity = EntityFragment::try_new(
        name("Collection"),
        version_one(),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("policy"),
                FieldType::Named(policy),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![id_source],
        Vec::new(),
        Vec::new(),
        vec![ConstraintFragment::targeted_rule(
            TargetedRuleFragment::new(
                policy_source,
                field_key.clone(),
                name("field_key_length"),
                SourceRuleOperation::LengthRangeInclusive { min: 1, max: 64 },
            ),
        )],
    )
    .expect("collection entity should admit");
    let fragment =
        SchemaFragment::try_new(vec![entity], named_types).expect("cyclic graph should admit");
    let store = TargetStoreIdentity::from_bytes([0x72; 32]);
    let proposal = SchemaProposal::try_compose(
        vec![
            SchemaCapability::ACCEPTED_CHECKS,
            SchemaCapability::EXACT_COMPOSITE_TYPES,
        ],
        TargetDatabaseIdentity::from_bytes([0x71; 32]),
        SchemaSubmissionKey::try_new("collection-cyclic-named-types")
            .expect("submission key should admit"),
        ExpectedAcceptedHead::Empty,
        vec![fragment],
        vec![EntityStoreAssignment::new(entity_source.clone(), store)],
        Vec::new(),
        None,
    )
    .expect("cyclic proposal should compose");

    let candidates = lower_initial_schema_proposal(
        &proposal,
        &[ProposalStoreTarget {
            path: "fixture::Store",
            identity: store,
        }],
    )
    .expect("cyclic named types should lower");
    let candidate = &candidates[0];
    let recovered = super::CandidateSchemaRevision::from_encoded(
        candidate.encoded_bundle().to_vec(),
        candidate.encoded_root().to_vec(),
    )
    .expect("persisted cyclic candidate should recover");
    assert_eq!(recovered.root(), candidate.root());
    assert_eq!(recovered.bundle(), candidate.bundle());
    let bundle = candidate.bundle();
    let bindings = bundle.source_bindings_for_tests();
    let entity_tag = bindings
        .entity(&entity_source)
        .expect("Collection should bind");
    let field_key_identity = bindings
        .named_type(&field_key)
        .expect("FieldKey should bind");
    let targeted_id = bindings
        .constraint(entity_tag, &constraint_source)
        .expect("cyclic targeted rule should bind");
    assert!(matches!(
        bundle.entity_snapshots()[&entity_tag]
            .constraints()
            .iter()
            .find(|constraint| constraint.id() == targeted_id)
            .map(AcceptedConstraintSnapshot::kind),
        Some(AcceptedConstraintKind::TargetedRule { target, .. })
            if target.target_type() == field_key_identity
    ));
    let values_composite_id = match bindings.named_type(&values).expect("Values should bind") {
        AcceptedNamedTypeIdentity::Composite(type_id) => type_id,
        AcceptedNamedTypeIdentity::Enum(_) => panic!("Values should be composite"),
    };
    let field_value_id = match bindings
        .named_type(&field_value)
        .expect("FieldValue should bind")
    {
        AcceptedNamedTypeIdentity::Enum(type_id) => type_id,
        AcceptedNamedTypeIdentity::Composite(_) => panic!("FieldValue should be enum"),
    };
    let value_enum_id = match bindings.named_type(&value).expect("Value should bind") {
        AcceptedNamedTypeIdentity::Enum(type_id) => type_id,
        AcceptedNamedTypeIdentity::Composite(_) => panic!("Value should be enum"),
    };
    let AcceptedCompositeShape::Newtype(values_contract) = bundle
        .composite_catalog()
        .composite_type(values_composite_id)
        .expect("Values should exist")
        .shape()
    else {
        panic!("Values should lower as one map newtype")
    };
    assert!(matches!(
        values_contract.kind(),
        super::AcceptedFieldKind::Map { value, .. }
            if matches!(
                value.as_ref(),
                super::AcceptedFieldKind::Enum { type_id } if *type_id == field_value_id
            )
    ));
    let field_value_definition = bundle
        .enum_catalog()
        .enum_type(field_value_id)
        .expect("FieldValue should exist");
    for variant_name in ["One", "Many"] {
        let variant = field_value_definition
            .variant(
                field_value_definition
                    .variant_id(variant_name)
                    .expect("variant should exist"),
            )
            .expect("variant definition should exist");
        assert!(matches!(
            variant.body(),
            AcceptedEnumVariantBody::Payload { contract }
                if match contract.kind() {
                    super::AcceptedFieldKind::Enum { type_id } => {
                        variant_name == "One" && *type_id == value_enum_id
                    }
                    super::AcceptedFieldKind::List(item) => {
                        variant_name == "Many"
                            && matches!(
                                item.as_ref(),
                                super::AcceptedFieldKind::Enum { type_id }
                                    if *type_id == value_enum_id
                            )
                    }
                    _ => false,
                }
        ));
    }
    let record_variant = bundle
        .enum_catalog()
        .enum_type(value_enum_id)
        .and_then(|definition| definition.variant_id("Record").map(|id| (definition, id)))
        .and_then(|(definition, id)| definition.variant(id))
        .expect("Value::Record should exist");
    assert!(matches!(
        record_variant.body(),
        AcceptedEnumVariantBody::Payload { contract }
            if matches!(
                contract.kind(),
                super::AcceptedFieldKind::Composite { type_id }
                    if *type_id == values_composite_id
            )
    ));

    let claim_cost_id = match bindings
        .named_type(&claim_cost)
        .expect("ClaimCost should bind")
    {
        AcceptedNamedTypeIdentity::Enum(type_id) => type_id,
        AcceptedNamedTypeIdentity::Composite(_) => panic!("ClaimCost should be enum"),
    };
    let claim_cost_definition = bundle
        .enum_catalog()
        .enum_type(claim_cost_id)
        .expect("ClaimCost should exist");
    assert!(matches!(
        claim_cost_definition
            .variant(
                claim_cost_definition
                    .variant_id("Free")
                    .expect("Free should exist")
            )
            .expect("Free definition should exist")
            .body(),
        AcceptedEnumVariantBody::Unit
    ));
    for variant_name in ["Icp", "Icrc1"] {
        assert!(matches!(
            claim_cost_definition
                .variant(
                    claim_cost_definition
                        .variant_id(variant_name)
                        .expect("payload variant should exist")
                )
                .expect("payload variant definition should exist")
                .body(),
            AcceptedEnumVariantBody::Payload { contract }
                if matches!(
                    contract.kind(),
                    super::AcceptedFieldKind::Composite { .. }
                )
        ));
    }

    let entity_tag = bindings
        .entity(&entity_source)
        .expect("collection entity should bind");
    let policy_field = bundle
        .entity_snapshots()
        .get(&entity_tag)
        .and_then(|snapshot| {
            snapshot
                .fields()
                .iter()
                .find(|field| field.name() == "policy")
        })
        .expect("policy field should exist");
    let field_contract = AcceptedFieldDecodeContract::new(
        policy_field.name(),
        policy_field.kind(),
        policy_field.nullable(),
        policy_field.storage_decode(),
        policy_field.leaf_codec(),
    );
    let payload_enum = |variant: &str, payload: InputValue| {
        InputValue::loose_enum(variant)
            .with_enum_payload(payload)
            .expect("an enum input should accept one recursive payload")
    };
    let finite_value = InputValue::map(vec![
        (
            InputValue::from("claim_cost_tiers"),
            InputValue::map(vec![
                (InputValue::from("free"), InputValue::loose_enum("Free")),
                (
                    InputValue::from("gold"),
                    payload_enum("Icp", InputValue::nat64(10)),
                ),
                (
                    InputValue::from("silver"),
                    payload_enum("Icrc1", InputValue::nat64(20)),
                ),
            ]),
        ),
        (InputValue::from("fallback"), InputValue::null()),
        (
            InputValue::from("values"),
            InputValue::map(vec![(
                InputValue::from("root"),
                payload_enum(
                    "One",
                    payload_enum(
                        "Record",
                        InputValue::map(vec![(
                            InputValue::from("nested"),
                            payload_enum(
                                "Many",
                                InputValue::list(vec![payload_enum(
                                    "Text",
                                    InputValue::from("leaf"),
                                )]),
                            ),
                        )]),
                    ),
                ),
            )]),
        ),
    ]);
    let mut budget = ValueAdmissionBudget::standard();
    let encoded = encode_input_value_for_candidate_field_contract(
        bundle.enum_catalog(),
        bundle.composite_catalog(),
        field_contract,
        finite_value,
        &mut budget,
    )
    .expect("finite cyclic-contract value should encode");
    validate_default_payload_for_accepted_field_contract(
        bundle.enum_catalog(),
        bundle.composite_catalog(),
        field_contract,
        &encoded,
    )
    .expect("encoded finite value should validate against accepted catalogs");
    assert!(
        decode_canonical_value_storage_bytes(&encoded).is_ok(),
        "finite cyclic-contract value should use the bounded canonical wire",
    );

    let values_kind = super::AcceptedFieldKind::Composite {
        type_id: values_composite_id,
    };
    let values_contract = AcceptedValueContract::from_candidate_catalogs(
        bundle.enum_catalog(),
        bundle.composite_catalog(),
        &values_kind,
        super::FieldStorageDecode::CatalogValue,
    )
    .expect("Values contract should resolve");
    let mut nested = PublicValue::Enum(
        PublicEnumValue::loose("Text").with_payload(PublicValue::Text("leaf".to_string())),
    );
    for level in 0..=super::MAX_ACCEPTED_RECURSIVE_DEPTH {
        nested = PublicValue::Enum(PublicEnumValue::loose("Record").with_payload(
            PublicValue::Map(vec![(
                PublicValue::Text(format!("level-{level}")),
                PublicValue::Enum(PublicEnumValue::loose("One").with_payload(nested)),
            )]),
        ));
    }
    let mut budget = ValueAdmissionBudget::standard();
    let error = normalize_candidate_value(
        bundle.enum_catalog(),
        bundle.composite_catalog(),
        &values_contract,
        InputValue::from_public(PublicValue::Map(vec![(
            PublicValue::Text("root".to_string()),
            PublicValue::Enum(PublicEnumValue::loose("One").with_payload(nested)),
        )])),
        &mut budget,
    )
    .expect_err("an excessive runtime value depth should reject");
    assert_eq!(error, ValueAdmissionError::DepthExceeded);
}

#[test]
fn nested_leaf_lowering_enforces_snapshot_count_before_expanding_shared_records() {
    use super::{lower_migration_nested_leaves, push_nested_leaves};
    use crate::db::schema::composite_catalog::{
        AcceptedCompositeCatalog, AcceptedCompositeElement, AcceptedCompositeField,
        CompositeFieldId, CompositeTypeId,
    };
    use crate::db::schema::enum_catalog::AcceptedEnumCatalog;
    use std::collections::BTreeSet;

    for levels in [1, 3] {
        let definitions = (1..=levels)
            .map(|level| {
                let kind = if level == levels {
                    AcceptedFieldKind::Nat64
                } else {
                    AcceptedFieldKind::Composite {
                        type_id: CompositeTypeId::new(level + 1).unwrap(),
                    }
                };
                let fields = (0..icydb_schema::MAX_FRAGMENT_FIELDS)
                    .map(|index| {
                        AcceptedCompositeField::new(
                            CompositeFieldId::new(u32::try_from(index + 1).unwrap()).unwrap(),
                            format!("member{index:03}"),
                            AcceptedCompositeElement::new(kind.clone(), false),
                        )
                    })
                    .collect();
                (
                    CompositeTypeId::new(level).unwrap(),
                    (
                        format!("Record{level}"),
                        AcceptedCompositeShape::Record(fields),
                    ),
                )
            })
            .collect();
        let catalog = AcceptedCompositeCatalog::from_initial_definitions(
            definitions,
            &AcceptedEnumCatalog::from_initial_definitions(BTreeMap::new()).unwrap(),
        )
        .unwrap();
        let kind = AcceptedFieldKind::Composite {
            type_id: CompositeTypeId::new(1).unwrap(),
        };
        let result = lower_migration_nested_leaves(&kind, &catalog);
        if levels == 1 {
            let leaves = result.unwrap();
            assert_eq!(leaves.len(), icydb_schema::MAX_FRAGMENT_FIELDS);
            assert!(
                leaves
                    .windows(2)
                    .all(|pair| pair[0].path() < pair[1].path())
            );
        } else {
            assert_eq!(
                result.unwrap_err().class(),
                crate::error::ErrorClass::Unsupported
            );
            // The three-level reused DAG would expand millions of paths.
            // Rejection must occur with only the codec's admitted leaf count retained.
            let mut leaves = Vec::new();
            let error = push_nested_leaves(
                "root",
                &AcceptedCompositeElement::new(kind, false),
                &catalog,
                &mut Vec::new(),
                &mut leaves,
                &mut BTreeSet::new(),
                0,
            )
            .unwrap_err();
            assert_eq!(error.class(), crate::error::ErrorClass::Unsupported);
            assert_eq!(leaves.len(), icydb_schema::MAX_FRAGMENT_FIELDS);
        }
    }
}

#[test]
// Keep both named definitions and the resulting leaf projection together.
#[expect(clippy::too_many_lines)]
fn initial_lowering_cuts_mutual_record_leaf_expansion_at_the_resolved_back_edge() {
    let left = TypeSourceKey::try_new("Left").expect("type source should admit");
    let right = TypeSourceKey::try_new("Right").expect("type source should admit");
    let named_types = vec![
        NamedTypeFragment::Record(
            RecordTypeFragment::try_new(
                name("Left"),
                vec![RecordFieldFragment::new(
                    name("right"),
                    FieldType::Named(right),
                    false,
                )],
            )
            .expect("left record should admit"),
        ),
        NamedTypeFragment::Record(
            RecordTypeFragment::try_new(
                name("Right"),
                vec![RecordFieldFragment::new(
                    name("left"),
                    FieldType::Named(left.clone()),
                    false,
                )],
            )
            .expect("right record should admit"),
        ),
    ];
    let entity_source =
        EntitySourceKey::try_new("CycleHolder").expect("entity source should admit");
    let id_source = FieldSourceKey::try_new("id").expect("field source should admit");
    let entity = EntityFragment::try_new(
        name("CycleHolder"),
        version_one(),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("left"),
                FieldType::Named(left),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![id_source],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .expect("cycle holder should admit");
    let store = TargetStoreIdentity::from_bytes([0x82; 32]);
    let proposal = SchemaProposal::try_compose(
        vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
        TargetDatabaseIdentity::from_bytes([0x81; 32]),
        SchemaSubmissionKey::try_new("mutual-record-cycle").expect("submission key should admit"),
        ExpectedAcceptedHead::Empty,
        vec![
            SchemaFragment::try_new(vec![entity], named_types)
                .expect("resolved record cycle should compose"),
        ],
        vec![EntityStoreAssignment::new(entity_source.clone(), store)],
        Vec::new(),
        None,
    )
    .expect("record-cycle proposal should compose");

    let candidates = lower_initial_schema_proposal(
        &proposal,
        &[ProposalStoreTarget {
            path: "cycle::Store",
            identity: store,
        }],
    )
    .expect("record cycle should lower and survive bundle round-trip");
    let bundle = candidates[0].bundle();
    let entity_tag = bundle
        .source_bindings_for_tests()
        .entity(&entity_source)
        .expect("cycle holder should bind");
    let left_field = bundle
        .entity_snapshots()
        .get(&entity_tag)
        .and_then(|snapshot| {
            snapshot
                .fields()
                .iter()
                .find(|field| field.name() == "left")
        })
        .expect("left field should exist");
    assert_eq!(
        left_field
            .nested_leaves()
            .iter()
            .map(super::PersistedNestedLeafSnapshot::path)
            .collect::<Vec<_>>(),
        [
            ["right".to_string()].as_slice(),
            ["right".to_string(), "left".to_string()].as_slice(),
        ],
    );
}

#[test]
fn initial_lowering_freezes_reachable_named_type_shapes_and_enum_defaults() {
    let (keys, active, named_types) = named_type_fragments();
    let (entity_source, entity) = named_holder_entity(&keys, active);
    let (other_entity_source, other_entity) = named_other_entity(&keys.status);
    let fragment = SchemaFragment::try_new(vec![entity, other_entity], named_types)
        .expect("fragment should admit");
    let store = TargetStoreIdentity::from_bytes([0x22; 32]);
    let other_store = TargetStoreIdentity::from_bytes([0x33; 32]);
    let proposal = SchemaProposal::try_compose(
        vec![
            SchemaCapability::EXACT_COMPOSITE_TYPES,
            SchemaCapability::INSERT_DEFAULTS,
        ],
        TargetDatabaseIdentity::from_bytes([0x11; 32]),
        SchemaSubmissionKey::try_new("named-type-lowering")
            .expect("test submission key should admit"),
        ExpectedAcceptedHead::Empty,
        vec![fragment],
        vec![
            EntityStoreAssignment::new(entity_source.clone(), store),
            EntityStoreAssignment::new(other_entity_source.clone(), other_store),
        ],
        Vec::new(),
        None,
    )
    .expect("test proposal should compose");
    let targets = [
        ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        },
        ProposalStoreTarget {
            path: "test::OtherStore",
            identity: other_store,
        },
    ];
    let candidates =
        lower_initial_schema_proposal(&proposal, &targets).expect("named types should lower");
    let bundle = candidates
        .iter()
        .find(|candidate| candidate.store_path() == "test::Store")
        .expect("primary store candidate should exist")
        .bundle();
    assert_primary_named_bundle(bundle, &entity_source, &keys.status);

    let other_bundle = candidates
        .iter()
        .find(|candidate| candidate.store_path() == "test::OtherStore")
        .expect("other store candidate should exist")
        .bundle();
    assert_eq!(
        other_bundle
            .source_bindings_for_tests()
            .named_type_binding_count_for_tests(),
        1,
        "each store should freeze only its complete reachable type closure"
    );
    assert!(
        other_bundle
            .source_bindings_for_tests()
            .entity(&other_entity_source)
            .is_some()
    );
    assert!(
        other_bundle
            .composite_catalog()
            .type_id("Profile")
            .is_none()
    );
}

#[test]
fn existing_named_type_redeclaration_and_creation_preserve_current_catalogs() {
    let (keys, active, named_types) = named_type_fragments();
    let (entity_source, entity) = named_holder_entity(&keys, active);
    let store = TargetStoreIdentity::from_bytes([0x35; 32]);
    let initial = SchemaProposal::try_compose(
        vec![
            SchemaCapability::EXACT_COMPOSITE_TYPES,
            SchemaCapability::INSERT_DEFAULTS,
        ],
        TargetDatabaseIdentity::from_bytes([0x25; 32]),
        SchemaSubmissionKey::try_new("exact-named-type-initial")
            .expect("initial submission key should admit"),
        ExpectedAcceptedHead::Empty,
        vec![
            SchemaFragment::try_new(vec![entity], named_types)
                .expect("initial named-type fragment should admit"),
        ],
        vec![EntityStoreAssignment::new(entity_source.clone(), store)],
        Vec::new(),
        None,
    )
    .expect("initial named-type proposal should compose");
    let initial_candidates = lower_initial_schema_proposal(
        &initial,
        &[ProposalStoreTarget {
            path: "test::Store",
            identity: store,
        }],
    )
    .expect("initial named-type proposal should lower");

    let (keys, active, named_types) = named_type_fragments();
    let (_, entity) = named_holder_entity(&keys, active);
    let exact = SchemaProposal::try_compose(
        vec![
            SchemaCapability::EXACT_COMPOSITE_TYPES,
            SchemaCapability::INSERT_DEFAULTS,
        ],
        TargetDatabaseIdentity::from_bytes([0x25; 32]),
        SchemaSubmissionKey::try_new("exact-named-type-redeclaration")
            .expect("existing submission key should admit"),
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x45; 32]),
        },
        vec![
            SchemaFragment::try_new(vec![entity], named_types)
                .expect("exact named-type fragment should admit"),
        ],
        vec![EntityStoreAssignment::new(entity_source, store)],
        Vec::new(),
        None,
    )
    .expect("exact named-type proposal should compose");

    let candidates = lower_existing_schema_proposal(
        &exact,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: store,
            bundle: initial_candidates[0].bundle(),
        }],
    )
    .expect("exact current named-type definitions should remain valid");

    assert!(
        candidates.is_empty(),
        "an exact named-type redeclaration must remain a no-op"
    );

    let (keys, _, _) = named_type_fragments();
    let (other_source, other) = named_other_entity(&keys.status);
    let mut entities = exact.fragments()[0].entities().to_vec();
    entities.push(other);
    let mut assignments = exact.assignments().to_vec();
    assignments.push(EntityStoreAssignment::new(other_source.clone(), store));
    let addition = SchemaProposal::try_compose(
        exact.capabilities().to_vec(),
        exact.target_database(),
        SchemaSubmissionKey::try_new("create-named-other").unwrap(),
        exact.expected_head().clone(),
        vec![SchemaFragment::try_new(entities, exact.fragments()[0].types().to_vec()).unwrap()],
        assignments,
        Vec::new(),
        None,
    )
    .unwrap();
    let created = lower_existing_schema_proposal(
        &addition,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: store,
            bundle: initial_candidates[0].bundle(),
        }],
    )
    .unwrap();
    let before = initial_candidates[0].bundle();
    let after = created[0].bundle();
    assert_eq!(after.enum_catalog(), before.enum_catalog());
    assert_eq!(after.composite_catalog(), before.composite_catalog());
    for (tag, snapshot) in before.entity_snapshots() {
        assert_eq!(after.entity_snapshots().get(tag), Some(snapshot));
    }
    assert!(after.source_bindings().entity(&other_source).is_some());
}

fn recursive_type_removal_proposal(
    submission_key: &str,
    removals: Vec<SchemaRemoval>,
) -> SchemaProposal {
    SchemaProposal::try_compose(
        vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
        TargetDatabaseIdentity::from_bytes([0x91; 32]),
        SchemaSubmissionKey::try_new(submission_key)
            .expect("recursive removal submission key should admit"),
        ExpectedAcceptedHead::Exact {
            revision: 1,
            fingerprint: ExpectedSchemaFingerprint::from_bytes([0x92; 32]),
        },
        Vec::new(),
        Vec::new(),
        removals,
        None,
    )
    .expect("recursive removal proposal should compose")
}

fn recursive_holder_entity(
    name_value: &str,
    root_type: &TypeSourceKey,
) -> (EntitySourceKey, EntityFragment) {
    let entity_source = EntitySourceKey::try_new(name_value).expect("entity source should admit");
    let id_source = FieldSourceKey::try_new("id").expect("id field source should admit");
    let entity = EntityFragment::try_new(
        name(name_value),
        version_one(),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("root"),
                FieldType::Named(root_type.clone()),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![id_source],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .expect("recursive holder should admit");
    (entity_source, entity)
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "the regression proves external-reference rejection, partial-cycle rejection, and multi-store component removal together"
)]
fn existing_recursive_named_type_removal_is_closed_and_store_local() {
    let node_type = TypeSourceKey::try_new("Node").expect("node type source should admit");
    let record_type = TypeSourceKey::try_new("Record").expect("record type source should admit");
    let named_types = vec![
        NamedTypeFragment::Enum(
            EnumTypeFragment::try_new(
                name("Node"),
                vec![
                    EnumVariantFragment::new(name("End")),
                    EnumVariantFragment::with_payload(
                        name("Record"),
                        FieldType::Named(record_type.clone()),
                    ),
                ],
            )
            .expect("recursive enum should admit"),
        ),
        NamedTypeFragment::Record(
            RecordTypeFragment::try_new(
                name("Record"),
                vec![RecordFieldFragment::new(
                    name("next"),
                    FieldType::Named(node_type.clone()),
                    false,
                )],
            )
            .expect("recursive record should admit"),
        ),
    ];
    let (left_source, left) = recursive_holder_entity("LeftHolder", &record_type);
    let (right_source, right) = recursive_holder_entity("RightHolder", &record_type);
    let left_store = TargetStoreIdentity::from_bytes([0x93; 32]);
    let right_store = TargetStoreIdentity::from_bytes([0x94; 32]);
    let initial = SchemaProposal::try_compose(
        vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
        TargetDatabaseIdentity::from_bytes([0x91; 32]),
        SchemaSubmissionKey::try_new("recursive-component-initial")
            .expect("initial submission key should admit"),
        ExpectedAcceptedHead::Empty,
        vec![
            SchemaFragment::try_new(vec![left, right], named_types)
                .expect("recursive component fragment should admit"),
        ],
        vec![
            EntityStoreAssignment::new(left_source.clone(), left_store),
            EntityStoreAssignment::new(right_source.clone(), right_store),
        ],
        Vec::new(),
        None,
    )
    .expect("recursive component proposal should compose");
    let targets = [
        ProposalStoreTarget {
            path: "cycle::LeftStore",
            identity: left_store,
        },
        ProposalStoreTarget {
            path: "cycle::RightStore",
            identity: right_store,
        },
    ];
    let initial_candidates =
        lower_initial_schema_proposal(&initial, &targets).expect("cycle should lower");
    let initial_left = initial_candidates
        .iter()
        .find(|candidate| candidate.store_path() == "cycle::LeftStore")
        .expect("left candidate should exist");
    let initial_right = initial_candidates
        .iter()
        .find(|candidate| candidate.store_path() == "cycle::RightStore")
        .expect("right candidate should exist");
    let remove_types = recursive_type_removal_proposal(
        "remove-referenced-cycle",
        vec![
            SchemaRemoval::Type(node_type.clone()),
            SchemaRemoval::Type(record_type.clone()),
        ],
    );
    assert!(
        lower_existing_schema_proposal(
            &remove_types,
            &[
                ExistingProposalStore {
                    path: "cycle::LeftStore",
                    identity: left_store,
                    bundle: initial_left.bundle(),
                },
                ExistingProposalStore {
                    path: "cycle::RightStore",
                    identity: right_store,
                    bundle: initial_right.bundle(),
                },
            ],
        )
        .is_err(),
        "entity fields must keep the recursive component live",
    );

    let remove_left = recursive_type_removal_proposal(
        "remove-left-holder",
        vec![SchemaRemoval::Entity(left_source)],
    );
    let left_without_entity = lower_existing_schema_proposal(
        &remove_left,
        &[
            ExistingProposalStore {
                path: "cycle::LeftStore",
                identity: left_store,
                bundle: initial_left.bundle(),
            },
            ExistingProposalStore {
                path: "cycle::RightStore",
                identity: right_store,
                bundle: initial_right.bundle(),
            },
        ],
    )
    .expect("left entity removal should lower")
    .pop()
    .expect("left entity removal should produce one candidate");
    let remove_right = recursive_type_removal_proposal(
        "remove-right-holder",
        vec![SchemaRemoval::Entity(right_source)],
    );
    let right_without_entity = lower_existing_schema_proposal(
        &remove_right,
        &[
            ExistingProposalStore {
                path: "cycle::LeftStore",
                identity: left_store,
                bundle: left_without_entity.bundle(),
            },
            ExistingProposalStore {
                path: "cycle::RightStore",
                identity: right_store,
                bundle: initial_right.bundle(),
            },
        ],
    )
    .expect("right entity removal should lower")
    .pop()
    .expect("right entity removal should produce one candidate");
    let stores_without_entities = [
        ExistingProposalStore {
            path: "cycle::LeftStore",
            identity: left_store,
            bundle: left_without_entity.bundle(),
        },
        ExistingProposalStore {
            path: "cycle::RightStore",
            identity: right_store,
            bundle: right_without_entity.bundle(),
        },
    ];
    for (submission_key, source) in [
        ("remove-node-only", node_type),
        ("remove-record-only", record_type),
    ] {
        let partial =
            recursive_type_removal_proposal(submission_key, vec![SchemaRemoval::Type(source)]);
        assert!(
            lower_existing_schema_proposal(&partial, &stores_without_entities).is_err(),
            "one retained cycle member must prevent partial removal",
        );
    }

    let removed = lower_existing_schema_proposal(&remove_types, &stores_without_entities)
        .expect("the complete unreferenced cycle should be removed");
    assert_eq!(removed.len(), 2);
    for candidate in removed {
        let bundle = candidate.bundle();
        assert_eq!(
            bundle
                .source_bindings_for_tests()
                .named_type_binding_count_for_tests(),
            0,
        );
        assert!(bundle.enum_catalog().type_id("Node").is_none());
        assert!(bundle.composite_catalog().type_id("Record").is_none());
    }
}
