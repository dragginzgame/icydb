use super::*;
use icydb_schema::{
    ConstraintFragment, DeclaredEntityVersion, EntityStoreAssignment, EnumTypeFragment,
    EnumVariantFragment, ExpectedAcceptedHead, ExpectedSchemaFingerprint, FieldFragment,
    FieldInsertPolicy, FieldManagementPolicy, FieldType, IndexFragment, IndexKeyFragment,
    IndexSourceKey, NamedTypeFragment, RecordFieldFragment, RecordTypeFragment,
    RelationDeleteAction, RelationFragment, RelationPathStepFragment, RelationSourceFragment,
    RelationSourceKey, RuleSourceKey, ScalarLiteral, ScalarType, SchemaCapability, SchemaFragment,
    SchemaMigrationRename, SchemaMigrationTransform, SchemaName, SchemaSubmissionKey,
    SourceCheckExpr, SourceCheckInstruction, SourceRuleOperation, TargetDatabaseIdentity,
    TargetedRuleFragment,
};

use crate::db::schema::{
    lower_initial_schema_proposal, migration_lineage::AcceptedEntitySourceLineage,
};

#[test]
fn constraint_names_resolve_catalog_identity_and_reject_missing_objects() {
    let proposal = complete_rename_proposal(false, ExpectedAcceptedHead::Empty, None);
    let candidate = lower_initial_schema_proposal(
        &proposal,
        &[crate::db::schema::ProposalStoreTarget {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
        }],
    )
    .unwrap()
    .pop()
    .unwrap();
    let entity = candidate
        .bundle()
        .source_bindings()
        .entity(&EntitySourceKey::try_new("Item").unwrap())
        .unwrap();
    let target = &candidate.bundle().entity_snapshots()[&entity];
    for (kind, expected) in [
        (
            AcceptedConstraintKind::Unique {
                index_id: target.indexes()[0].schema_id(),
            },
            target.indexes()[0].name(),
        ),
        (
            AcceptedConstraintKind::Relation {
                relation_id: target.relations()[0].id(),
            },
            target.relations()[0].name(),
        ),
        (AcceptedConstraintKind::PrimaryKey, "stored-name"),
    ] {
        let constraint = AcceptedConstraintSnapshot::new(
            ConstraintId::new(1).unwrap(),
            "stored-name".to_string(),
            ConstraintOrigin::Generated,
            kind,
        );
        assert_eq!(
            resolve_constraint_name(&constraint, target).unwrap(),
            expected
        );
    }
    for kind in [
        AcceptedConstraintKind::Unique {
            index_id: SchemaIndexId::new(u32::MAX).unwrap(),
        },
        AcceptedConstraintKind::Relation {
            relation_id: crate::db::schema::RelationId::new(u32::MAX).unwrap(),
        },
    ] {
        let constraint = AcceptedConstraintSnapshot::new(
            ConstraintId::new(1).unwrap(),
            "stored-name".to_string(),
            ConstraintOrigin::Generated,
            kind,
        );
        assert!(matches!(
            resolve_constraint_name(&constraint, target),
            Err(SchemaMigrationPlanningError::IdentityConflict)
        ));
    }
}

#[test]
fn candidate_failures_preserve_operational_diagnostics() {
    use super::SchemaMigrationPlanningError;
    use crate::{
        db::{
            resource::budget::MaintenanceConstructionBudget,
            resource::construction::ConstructionBudget,
        },
        error::InternalError,
    };
    use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

    let budget = MaintenanceConstructionBudget::with_limit_for_tests(Resource::TemporaryBytes, 0);
    let error = budget.charge(Resource::TemporaryBytes, 1).unwrap_err();
    let diagnostic = error.diagnostic();
    let mapped = SchemaMigrationPlanningError::candidate_failure(
        error,
        SchemaMigrationPlanningError::CandidateMismatch,
    );
    let SchemaMigrationPlanningError::Preparation(error) = mapped else {
        panic!("operational exhaustion must not become schema mismatch");
    };
    assert_eq!(error.diagnostic(), diagnostic);
    assert!(matches!(
        SchemaMigrationPlanningError::candidate_failure(
            InternalError::store_invariant(),
            SchemaMigrationPlanningError::CandidateMismatch,
        ),
        SchemaMigrationPlanningError::CandidateMismatch,
    ));
}

fn field(value: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(value).expect("field should admit")
}

fn r#type(value: &str) -> TypeSourceKey {
    TypeSourceKey::try_new(value).expect("type should admit")
}

fn name(value: &str) -> SchemaName {
    SchemaName::try_new(value).expect("name should admit")
}

fn proposal(
    version: u32,
    entity_name: &str,
    field_name: &str,
    expected_head: ExpectedAcceptedHead,
    migration: Option<SchemaMigrationPlan>,
) -> SchemaProposal {
    let entity_source = EntitySourceKey::try_new(entity_name).expect("entity should admit");
    let entity = EntityFragment::try_new(
        SchemaName::try_new(entity_name).expect("entity name should admit"),
        DeclaredEntityVersion::try_new(version).expect("version should admit"),
        vec![FieldFragment::new(
            SchemaName::try_new(field_name).expect("field name should admit"),
            FieldType::Scalar(icydb_schema::ScalarType::Nat64),
            false,
            FieldInsertPolicy::Required,
            None,
        )],
        vec![field(field_name)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .expect("entity should admit");
    SchemaProposal::try_compose(
        migration
            .is_some()
            .then_some(SchemaCapability::VERSIONED_MIGRATIONS)
            .into_iter()
            .collect(),
        TargetDatabaseIdentity::from_bytes([1; 32]),
        SchemaSubmissionKey::try_new(format!("{entity_name}-{version}"))
            .expect("submission should admit"),
        expected_head,
        vec![SchemaFragment::try_new(vec![entity], Vec::new()).expect("fragment should admit")],
        vec![EntityStoreAssignment::new(
            entity_source,
            TargetStoreIdentity::from_bytes([2; 32]),
        )],
        Vec::new(),
        migration,
    )
    .expect("proposal should admit")
}

fn head() -> ExpectedAcceptedHead {
    ExpectedAcceptedHead::Exact {
        revision: 1,
        fingerprint: ExpectedSchemaFingerprint::from_bytes([3; 32]),
    }
}

fn accepted_fixture() -> (SchemaProposal, CandidateSchemaRevision) {
    let initial = proposal(1, "User", "email", ExpectedAcceptedHead::Empty, None);
    let candidate = lower_initial_schema_proposal(
        &initial,
        &[crate::db::schema::ProposalStoreTarget {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
        }],
    )
    .expect("initial proposal should lower")
    .pop()
    .expect("initial candidate should exist");
    (initial, candidate)
}

fn checked_cast_proposal(
    current: bool,
    expected_head: ExpectedAcceptedHead,
    migration: Option<SchemaMigrationPlan>,
) -> SchemaProposal {
    let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
    let value_name = if current { "value" } else { "old_value" };
    let value_kind = if current {
        ScalarType::Nat8
    } else {
        ScalarType::Int64
    };
    let entity = EntityFragment::try_new(
        name("User"),
        DeclaredEntityVersion::try_new(if current { 2 } else { 1 }).expect("version should admit"),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name(value_name),
                FieldType::Scalar(value_kind),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![field("id")],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .expect("entity should admit");
    SchemaProposal::try_compose(
        migration
            .is_some()
            .then_some(SchemaCapability::VERSIONED_MIGRATIONS)
            .into_iter()
            .collect(),
        TargetDatabaseIdentity::from_bytes([1; 32]),
        SchemaSubmissionKey::try_new(if current { "cast-v2" } else { "cast-v1" })
            .expect("submission should admit"),
        expected_head,
        vec![SchemaFragment::try_new(vec![entity], Vec::new()).expect("fragment should admit")],
        vec![EntityStoreAssignment::new(
            entity_source.clone(),
            TargetStoreIdentity::from_bytes([2; 32]),
        )],
        current
            .then(|| icydb_schema::SchemaRemoval::Field {
                entity: entity_source,
                field: field("old_value"),
            })
            .into_iter()
            .collect(),
        migration,
    )
    .expect("proposal should admit")
}

fn maintained_user_proposal(
    current: bool,
    expected_head: ExpectedAcceptedHead,
    migration: Option<SchemaMigrationPlan>,
) -> SchemaProposal {
    let entity_source = EntitySourceKey::try_new("SqlTestUser").expect("entity should admit");
    let control_source = EntitySourceKey::try_new("Control").expect("entity should admit");
    SchemaProposal::try_compose(
        migration
            .is_some()
            .then_some(SchemaCapability::VERSIONED_MIGRATIONS)
            .into_iter()
            .collect(),
        TargetDatabaseIdentity::from_bytes([1; 32]),
        SchemaSubmissionKey::try_new(if current { "user-v2" } else { "user-v1" })
            .expect("submission should admit"),
        expected_head,
        vec![
            SchemaFragment::try_new(
                vec![maintained_user_entity(current), maintained_control_entity()],
                Vec::new(),
            )
            .expect("fragment should admit"),
        ],
        vec![
            EntityStoreAssignment::new(entity_source, TargetStoreIdentity::from_bytes([2; 32])),
            EntityStoreAssignment::new(control_source, TargetStoreIdentity::from_bytes([2; 32])),
        ],
        Vec::new(),
        migration,
    )
    .expect("proposal should admit")
}

fn maintained_user_entity(current: bool) -> EntityFragment {
    let scalar = |field_name, kind, insert_policy, management| {
        FieldFragment::new(
            name(field_name),
            FieldType::Scalar(kind),
            false,
            insert_policy,
            management,
        )
    };
    EntityFragment::try_new(
        name("SqlTestUser"),
        DeclaredEntityVersion::try_new(if current { 2 } else { 1 }).expect("version should admit"),
        vec![
            scalar("id", ScalarType::Ulid, FieldInsertPolicy::Generated, None),
            scalar(
                "name",
                ScalarType::Text { max_len: None },
                FieldInsertPolicy::Required,
                None,
            ),
            scalar(
                "age",
                if current {
                    ScalarType::Nat16
                } else {
                    ScalarType::Int32
                },
                FieldInsertPolicy::Required,
                None,
            ),
            scalar(
                if current { "score" } else { "rank" },
                ScalarType::Int32,
                FieldInsertPolicy::Required,
                None,
            ),
            scalar(
                "created_at",
                ScalarType::Timestamp,
                FieldInsertPolicy::Required,
                Some(FieldManagementPolicy::CreatedAt),
            ),
            scalar(
                "updated_at",
                ScalarType::Timestamp,
                FieldInsertPolicy::Required,
                Some(FieldManagementPolicy::UpdatedAt),
            ),
        ],
        vec![field("id")],
        vec![
            IndexFragment::try_new(
                name("idx_sql_test_user__name"),
                vec![IndexKeyFragment::Field(field("name"))],
                false,
                None,
            )
            .expect("index should admit"),
        ],
        Vec::new(),
        Vec::new(),
    )
    .expect("entity should admit")
}

fn maintained_control_entity() -> EntityFragment {
    EntityFragment::try_new(
        name("Control"),
        DeclaredEntityVersion::try_new(1).expect("version should admit"),
        vec![FieldFragment::new(
            name("id"),
            FieldType::Scalar(ScalarType::Nat64),
            false,
            FieldInsertPolicy::Generated,
            None,
        )],
        vec![field("id")],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .expect("control should admit")
}

fn closed_transform_proposal(
    current: bool,
    expected_head: ExpectedAcceptedHead,
    migration: Option<SchemaMigrationPlan>,
) -> SchemaProposal {
    let field_fragment = |name_value: &str, kind, nullable| {
        FieldFragment::new(
            name(name_value),
            FieldType::Scalar(kind),
            nullable,
            FieldInsertPolicy::Required,
            None,
        )
    };
    let mut fields = vec![
        field_fragment("id", ScalarType::Nat64, false),
        field_fragment("signed", ScalarType::Int64, false),
        field_fragment("optional", ScalarType::Int64, true),
    ];
    if current {
        fields.extend([
            field_fragment("filled", ScalarType::Nat64, false),
            field_fragment("copied", ScalarType::Int64, false),
            field_fragment("cast", ScalarType::Nat8, false),
            field_fragment("coalesced", ScalarType::Int64, false),
        ]);
    }
    let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
    let entity = EntityFragment::try_new(
        name("User"),
        DeclaredEntityVersion::try_new(if current { 2 } else { 1 }).expect("version should admit"),
        fields,
        vec![field("id")],
        current
            .then(|| {
                IndexFragment::try_new(
                    name("cast_unique"),
                    vec![IndexKeyFragment::Field(field("cast"))],
                    true,
                    None,
                )
                .expect("index should admit")
            })
            .into_iter()
            .collect(),
        Vec::new(),
        Vec::new(),
    )
    .expect("entity should admit");
    SchemaProposal::try_compose(
        migration
            .is_some()
            .then_some(SchemaCapability::VERSIONED_MIGRATIONS)
            .into_iter()
            .collect(),
        TargetDatabaseIdentity::from_bytes([1; 32]),
        SchemaSubmissionKey::try_new(if current { "closed-v2" } else { "closed-v1" })
            .expect("submission should admit"),
        expected_head,
        vec![SchemaFragment::try_new(vec![entity], Vec::new()).expect("fragment should admit")],
        vec![EntityStoreAssignment::new(
            entity_source,
            TargetStoreIdentity::from_bytes([2; 32]),
        )],
        Vec::new(),
        migration,
    )
    .expect("proposal should admit")
}

fn complete_rename_proposal(
    current: bool,
    expected_head: ExpectedAcceptedHead,
    migration: Option<SchemaMigrationPlan>,
) -> SchemaProposal {
    let entity_name = if current { "Entry" } else { "Item" };
    let parent_field = if current { "owner_id" } else { "parent_id" };
    let status_type = if current { "Status" } else { "OldStatus" };
    let status_variant = if current { "Queued" } else { "Pending" };
    let profile_type = if current { "Profile" } else { "OldProfile" };
    let profile_member = if current { "label" } else { "nickname" };
    let score_type = if current { "Score" } else { "OldScore" };
    let relation_name = if current {
        "owner_relation"
    } else {
        "parent_relation"
    };
    let check_name = if current {
        "nonnegative_owner"
    } else {
        "nonnegative_parent"
    };
    let rule_name = if current { "bounded" } else { "range" };
    let check = SourceCheckExpr::try_new(vec![
        SourceCheckInstruction::Field(field(parent_field)),
        SourceCheckInstruction::Literal(ScalarLiteral::Nat(0)),
        SourceCheckInstruction::GreaterThanOrEqual,
    ])
    .expect("check should admit");
    let entity = EntityFragment::try_new(
        name(entity_name),
        DeclaredEntityVersion::try_new(if current { 2 } else { 1 }).expect("version should admit"),
        vec![
            FieldFragment::new(
                name("id"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name(parent_field),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("status"),
                FieldType::Named(r#type(status_type)),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("profile"),
                FieldType::Named(r#type(profile_type)),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                name("score"),
                FieldType::Named(r#type(score_type)),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![field("id")],
        vec![
            IndexFragment::try_new(
                name("parent_lookup"),
                vec![IndexKeyFragment::Field(field(parent_field))],
                false,
                Some(
                    SourceCheckExpr::try_new(vec![
                        SourceCheckInstruction::Field(field("status")),
                        SourceCheckInstruction::Literal(ScalarLiteral::EnumUnit {
                            enum_type: r#type(status_type),
                            variant: r#type(status_variant),
                        }),
                        SourceCheckInstruction::Equal,
                        SourceCheckInstruction::Field(field("id")),
                        SourceCheckInstruction::Field(field(parent_field)),
                        SourceCheckInstruction::Equal,
                        SourceCheckInstruction::And,
                    ])
                    .expect("filtered enum declaration should admit"),
                ),
            )
            .expect("index should admit"),
        ],
        vec![
            RelationFragment::try_new(
                name(relation_name),
                RelationSourceFragment::Nested {
                    root: field("profile"),
                    steps: vec![
                        RelationPathStepFragment::EnterNamed {
                            r#type: r#type(profile_type),
                        },
                        RelationPathStepFragment::RecordMember {
                            record: r#type(profile_type),
                            field: field("owner_id"),
                        },
                    ],
                },
                EntitySourceKey::try_new(entity_name).expect("entity should admit"),
                vec![field("id")],
                RelationDeleteAction::Restrict,
            )
            .expect("relation should admit"),
        ],
        vec![
            ConstraintFragment::check(name(check_name), check),
            ConstraintFragment::targeted_rule(TargetedRuleFragment::new(
                field("score"),
                r#type(score_type),
                name(rule_name),
                SourceRuleOperation::NumericRangeInclusive {
                    min: ScalarLiteral::Nat(0),
                    max: ScalarLiteral::Nat(if current { 8 } else { 10 }),
                },
            )),
        ],
    )
    .expect("entity should admit");
    let fragment = SchemaFragment::try_new(
        vec![entity],
        vec![
            NamedTypeFragment::Enum(
                EnumTypeFragment::try_new(
                    name(status_type),
                    vec![EnumVariantFragment::new(name(status_variant))],
                )
                .expect("enum should admit"),
            ),
            NamedTypeFragment::Record(
                RecordTypeFragment::try_new(
                    name(profile_type),
                    vec![
                        RecordFieldFragment::new(
                            name(profile_member),
                            FieldType::Scalar(ScalarType::Text { max_len: Some(64) }),
                            false,
                        ),
                        RecordFieldFragment::new(
                            name("owner_id"),
                            FieldType::Scalar(ScalarType::Nat64),
                            false,
                        ),
                    ],
                )
                .expect("record should admit"),
            ),
            NamedTypeFragment::newtype(name(score_type), FieldType::Scalar(ScalarType::Nat8)),
        ],
    )
    .expect("fragment should admit");
    let mut capabilities = vec![
        SchemaCapability::ACCEPTED_CHECKS,
        SchemaCapability::EXACT_COMPOSITE_TYPES,
        SchemaCapability::RESTRICTIVE_RELATIONS,
        SchemaCapability::SECONDARY_INDEXES,
    ];
    if migration.is_some() {
        capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
    }
    SchemaProposal::try_compose(
        capabilities,
        TargetDatabaseIdentity::from_bytes([1; 32]),
        SchemaSubmissionKey::try_new(if current {
            "complete-v2"
        } else {
            "complete-v1"
        })
        .expect("submission should admit"),
        expected_head,
        vec![fragment],
        vec![EntityStoreAssignment::new(
            EntitySourceKey::try_new(entity_name).expect("entity should admit"),
            TargetStoreIdentity::from_bytes([2; 32]),
        )],
        Vec::new(),
        migration,
    )
    .expect("proposal should admit")
}

fn complete_rename_plan() -> SchemaMigrationPlan {
    SchemaMigrationPlan::try_new(vec![
        EntityMigration::try_new(
            EntitySourceKey::try_new("Entry").expect("entity should admit"),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            Some(EntitySourceKey::try_new("Item").expect("entity should admit")),
            vec![
                SchemaMigrationRename::Field {
                    from: field("parent_id"),
                    to: field("owner_id"),
                },
                SchemaMigrationRename::NamedType {
                    from: r#type("OldStatus"),
                    to: r#type("Status"),
                },
                SchemaMigrationRename::NamedType {
                    from: r#type("OldProfile"),
                    to: r#type("Profile"),
                },
                SchemaMigrationRename::NamedType {
                    from: r#type("OldScore"),
                    to: r#type("Score"),
                },
                SchemaMigrationRename::EnumVariant {
                    named_type: r#type("OldStatus"),
                    from: r#type("Pending"),
                    to: r#type("Queued"),
                },
                SchemaMigrationRename::RecordField {
                    named_type: r#type("OldProfile"),
                    from: field("nickname"),
                    to: field("label"),
                },
                SchemaMigrationRename::Relation {
                    from: RelationSourceKey::try_new("parent_relation")
                        .expect("relation should admit"),
                    to: RelationSourceKey::try_new("owner_relation")
                        .expect("relation should admit"),
                },
                SchemaMigrationRename::Constraint {
                    from: ConstraintSourceKey::try_new("nonnegative_parent")
                        .expect("constraint should admit"),
                    to: ConstraintSourceKey::try_new("nonnegative_owner")
                        .expect("constraint should admit"),
                },
                SchemaMigrationRename::Rule {
                    named_type: r#type("OldScore"),
                    from: RuleSourceKey::try_new("range").expect("rule should admit"),
                    to: RuleSourceKey::try_new("bounded").expect("rule should admit"),
                },
            ],
            Vec::new(),
        )
        .expect("transition should admit"),
    ])
    .expect("plan should admit")
}

fn shared_type_proposal(
    current: bool,
    expected_head: ExpectedAcceptedHead,
    migration: Option<SchemaMigrationPlan>,
) -> SchemaProposal {
    let type_name = if current { "Value" } else { "OldValue" };
    let version =
        DeclaredEntityVersion::try_new(if current { 2 } else { 1 }).expect("version should admit");
    let mut entities = ["Alpha", "Beta"]
        .into_iter()
        .map(|entity_name| {
            EntityFragment::try_new(
                name(if current {
                    if entity_name == "Alpha" {
                        "NewAlpha"
                    } else {
                        "NewBeta"
                    }
                } else {
                    entity_name
                }),
                version,
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
                        FieldType::Named(r#type(type_name)),
                        false,
                        FieldInsertPolicy::Required,
                        None,
                    ),
                ],
                vec![field("id")],
                Vec::new(),
                vec![
                    RelationFragment::try_new(
                        name("peer"),
                        RelationSourceFragment::Direct {
                            fields: vec![field("id")],
                        },
                        EntitySourceKey::try_new(match (entity_name, current) {
                            ("Alpha", false) => "Beta",
                            ("Alpha", true) => "NewBeta",
                            (_, false) => "Alpha",
                            (_, true) => "NewAlpha",
                        })
                        .unwrap(),
                        vec![field("id")],
                        RelationDeleteAction::Restrict,
                    )
                    .unwrap(),
                ],
                Vec::new(),
            )
            .expect("entity should admit")
        })
        .collect::<Vec<_>>();
    if current {
        entities.push(maintained_control_entity());
    }
    let fragment = SchemaFragment::try_new(
        entities,
        vec![NamedTypeFragment::newtype(
            name(type_name),
            FieldType::Scalar(ScalarType::Nat16),
        )],
    )
    .expect("fragment should admit");
    let mut capabilities = vec![
        SchemaCapability::EXACT_COMPOSITE_TYPES,
        SchemaCapability::RESTRICTIVE_RELATIONS,
    ];
    if migration.is_some() {
        capabilities.push(SchemaCapability::VERSIONED_MIGRATIONS);
    }
    let mut assignments = vec![
        EntityStoreAssignment::new(
            EntitySourceKey::try_new(if current { "NewAlpha" } else { "Alpha" })
                .expect("entity should admit"),
            TargetStoreIdentity::from_bytes([2; 32]),
        ),
        EntityStoreAssignment::new(
            EntitySourceKey::try_new(if current { "NewBeta" } else { "Beta" })
                .expect("entity should admit"),
            TargetStoreIdentity::from_bytes([5; 32]),
        ),
    ];
    if current {
        // Beta needs ordinary reconciliation for creation; Alpha uses its
        // migrated catalog unchanged. Both must appear once in canonical order.
        assignments.push(EntityStoreAssignment::new(
            EntitySourceKey::try_new("Control").unwrap(),
            TargetStoreIdentity::from_bytes([5; 32]),
        ));
    }
    SchemaProposal::try_compose(
        capabilities,
        TargetDatabaseIdentity::from_bytes([1; 32]),
        SchemaSubmissionKey::try_new(if current { "shared-v2" } else { "shared-v1" })
            .expect("submission should admit"),
        expected_head,
        vec![fragment],
        assignments,
        Vec::new(),
        migration,
    )
    .expect("proposal should admit")
}

fn shared_type_plan() -> SchemaMigrationPlan {
    SchemaMigrationPlan::try_new(
        ["Alpha", "Beta"]
            .into_iter()
            .map(|entity_name| {
                EntityMigration::try_new(
                    EntitySourceKey::try_new(if entity_name == "Alpha" {
                        "NewAlpha"
                    } else {
                        "NewBeta"
                    })
                    .unwrap(),
                    DeclaredEntityVersion::try_new(1).expect("version should admit"),
                    Some(EntitySourceKey::try_new(entity_name).unwrap()),
                    vec![SchemaMigrationRename::NamedType {
                        from: r#type("OldValue"),
                        to: r#type("Value"),
                    }],
                    Vec::new(),
                )
                .expect("transition should admit")
            })
            .collect(),
    )
    .expect("plan should admit")
}

#[test]
fn adoption_requires_v1_and_exact_noop_reconciliation() {
    let (initial, candidate) = accepted_fixture();
    let existing = ExistingProposalStore {
        path: "test::Store",
        identity: TargetStoreIdentity::from_bytes([2; 32]),
        bundle: candidate.bundle().catalog(),
    };
    let adopted = plan_entity_source_adoption(
        &proposal(1, "User", "email", head(), None),
        std::slice::from_ref(&existing),
        &AcceptedEntitySourceLineageCatalog::default(),
    )
    .expect("exact v1 adoption should plan");
    assert_eq!(adopted.len(), 1);
    assert_eq!(adopted[0].version().get(), 1);
    assert_eq!(
        adopted[0].digest(),
        initial
            .entity_source_digest(&EntitySourceKey::try_new("User").expect("key should admit"))
            .expect("digest should derive"),
    );

    assert!(matches!(
        plan_entity_source_adoption(
            &proposal(2, "User", "email", head(), None),
            std::slice::from_ref(&existing),
            &AcceptedEntitySourceLineageCatalog::default(),
        )
        .err(),
        Some(SchemaMigrationPlanningError::VersionGap),
    ));
    assert!(matches!(
        plan_entity_source_adoption(
            &proposal(1, "User", "primary_email", head(), None),
            std::slice::from_ref(&existing),
            &AcceptedEntitySourceLineageCatalog::default(),
        )
        .err(),
        Some(SchemaMigrationPlanningError::UnexplainedSchemaDifference),
    ));

    let entity = candidate
        .bundle()
        .source_bindings()
        .entity(&EntitySourceKey::try_new("User").expect("key should admit"))
        .expect("entity should bind");
    let mut prior_publication = AcceptedEntitySourceLineageCatalog::default();
    prior_publication
        .insert(
            TargetStoreIdentity::from_bytes([2; 32]),
            entity,
            AcceptedEntitySourceLineage::unadopted(ExpectedAcceptedHead::Exact {
                revision: 2,
                fingerprint: ExpectedSchemaFingerprint::from_bytes([4; 32]),
            })
            .expect("lineage should admit"),
        )
        .expect("lineage should insert");
    assert!(
        plan_entity_source_adoption(
            &proposal(1, "User", "email", head(), None),
            std::slice::from_ref(&existing),
            &prior_publication,
        )
        .is_ok()
    );

    let mut already_adopted = AcceptedEntitySourceLineageCatalog::default();
    already_adopted
        .insert(
            TargetStoreIdentity::from_bytes([2; 32]),
            entity,
            AcceptedEntitySourceLineage::adopted(
                head(),
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                initial
                    .entity_source_digest(
                        &EntitySourceKey::try_new("User").expect("entity should admit"),
                    )
                    .expect("digest should derive"),
            )
            .expect("lineage should admit"),
        )
        .expect("lineage should insert");
    assert!(matches!(
        plan_entity_source_adoption(
            &proposal(1, "User", "email", head(), None),
            std::slice::from_ref(&existing),
            &already_adopted,
        )
        .err(),
        Some(SchemaMigrationPlanningError::IdentityConflict),
    ));
}

#[test]
fn explicit_entity_and_field_rename_preserve_accepted_ids_in_candidate() {
    let (initial, candidate) = accepted_fixture();
    let accepted_entity = candidate
        .bundle()
        .source_bindings()
        .entity(&EntitySourceKey::try_new("User").expect("key should admit"))
        .expect("entity should bind");
    let accepted_field = candidate
        .bundle()
        .source_bindings()
        .field(accepted_entity, &field("email"))
        .expect("field should bind");
    let transition = EntityMigration::try_new(
        EntitySourceKey::try_new("Account").expect("entity should admit"),
        DeclaredEntityVersion::try_new(1).expect("version should admit"),
        Some(EntitySourceKey::try_new("User").expect("entity should admit")),
        vec![SchemaMigrationRename::Field {
            from: field("email"),
            to: field("primary_email"),
        }],
        Vec::new(),
    )
    .expect("transition should admit");
    let plan = SchemaMigrationPlan::try_new(vec![transition]).expect("plan should admit");
    let migration = proposal(2, "Account", "primary_email", head(), Some(plan));
    let mut lineage = AcceptedEntitySourceLineageCatalog::default();
    lineage
        .insert(
            TargetStoreIdentity::from_bytes([2; 32]),
            accepted_entity,
            AcceptedEntitySourceLineage::adopted(
                head(),
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                initial
                    .entity_source_digest(
                        &EntitySourceKey::try_new("User").expect("key should admit"),
                    )
                    .expect("digest should derive"),
            )
            .expect("lineage should admit"),
        )
        .expect("lineage should insert");
    let planned = plan_schema_migration(
        &migration,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
            bundle: candidate.bundle().catalog(),
        }],
        &lineage,
    )
    .expect("rename should plan");
    let target = planned.candidates()[0].bundle();
    assert_eq!(
        target
            .source_bindings()
            .entity(&EntitySourceKey::try_new("Account").expect("key should admit")),
        Some(accepted_entity),
    );
    assert_eq!(
        target
            .source_bindings()
            .field(accepted_entity, &field("primary_email")),
        Some(accepted_field),
    );
    assert!(
        target
            .source_bindings()
            .entity(&EntitySourceKey::try_new("User").expect("key should admit"))
            .is_none(),
    );
    assert_eq!(planned.lineage()[0].version().get(), 2);
}

#[test]
fn complete_rename_set_preserves_accepted_identities_through_physical_rewrite() {
    let initial = complete_rename_proposal(false, ExpectedAcceptedHead::Empty, None);
    let candidate = lower_initial_schema_proposal(
        &initial,
        &[crate::db::schema::ProposalStoreTarget {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
        }],
    )
    .expect("initial proposal should lower")
    .pop()
    .expect("initial candidate should exist");
    let accepted = candidate.bundle().source_bindings();
    let old_entity = EntitySourceKey::try_new("Item").expect("entity should admit");
    let entity_id = accepted.entity(&old_entity).expect("entity should bind");
    let parent_id = accepted
        .field(entity_id, &field("parent_id"))
        .expect("field should bind");
    let status_id = accepted
        .named_type(&r#type("OldStatus"))
        .expect("enum should bind");
    let profile_id = accepted
        .named_type(&r#type("OldProfile"))
        .expect("record should bind");
    let score_id = accepted
        .named_type(&r#type("OldScore"))
        .expect("newtype should bind");
    let AcceptedNamedTypeIdentity::Enum(status_type_id) = status_id else {
        panic!("status should retain enum identity");
    };
    let AcceptedNamedTypeIdentity::Composite(profile_type_id) = profile_id else {
        panic!("profile should retain composite identity");
    };
    let pending_id = accepted
        .enum_variant(status_type_id, &r#type("Pending"))
        .expect("variant should bind");
    let nickname_id = accepted
        .composite_field(profile_type_id, &field("nickname"))
        .expect("record field should bind");
    let relation_id = accepted
        .relation(
            entity_id,
            &RelationSourceKey::try_new("parent_relation").expect("relation should admit"),
        )
        .expect("relation should bind");
    let check_id = accepted
        .constraint(
            entity_id,
            &ConstraintSourceKey::try_new("nonnegative_parent").expect("constraint should admit"),
        )
        .expect("constraint should bind");
    let old_rule = ConstraintSourceKey::for_targeted_field_rule(
        &field("score"),
        &r#type("OldScore"),
        &RuleSourceKey::try_new("range").expect("rule should admit"),
    );
    let rule_id = accepted
        .constraint(entity_id, &old_rule)
        .expect("rule should bind");
    let index_id = accepted
        .index(
            entity_id,
            &IndexSourceKey::try_new("parent_lookup").expect("index should admit"),
        )
        .expect("index should bind");
    let old_index = candidate.bundle().entity_snapshots()[&entity_id]
        .indexes()
        .iter()
        .find(|index| index.schema_id() == index_id)
        .expect("index should exist");

    let mut lineage = AcceptedEntitySourceLineageCatalog::default();
    lineage
        .insert(
            TargetStoreIdentity::from_bytes([2; 32]),
            entity_id,
            AcceptedEntitySourceLineage::adopted(
                head(),
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                initial
                    .entity_source_digest(&old_entity)
                    .expect("digest should derive"),
            )
            .expect("lineage should admit"),
        )
        .expect("lineage should insert");
    let migration = complete_rename_proposal(true, head(), Some(complete_rename_plan()));
    let planned = plan_schema_migration(
        &migration,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
            bundle: candidate.bundle().catalog(),
        }],
        &lineage,
    )
    .expect("complete rename should plan");
    assert!(planned.requires_physical_validation());
    let target = planned.candidates()[0].bundle();
    assert_eq!(
        candidate
            .bundle()
            .entity_snapshots()
            .get(&entity_id)
            .unwrap()
            .indexes()[0]
            .predicate(),
        target.entity_snapshots().get(&entity_id).unwrap().indexes()[0].predicate(),
        "enum and field rename must retain filtered membership identity",
    );
    let restart = crate::db::schema::enum_catalog::decode_accepted_schema_revision_bundle(
        planned.candidates()[0].encoded_bundle(),
    )
    .unwrap();
    let current = complete_rename_proposal(true, head(), None);
    let reconciliation = crate::db::schema::lower_generated_existing_schema_proposal(
        &current,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
            bundle: restart.catalog(),
        }],
    )
    .expect("current generated declarations reconcile after restart and renames");
    assert!(reconciliation.is_empty());
    let bindings = target.source_bindings();
    let new_entity = EntitySourceKey::try_new("Entry").expect("entity should admit");
    assert_eq!(bindings.entity(&new_entity), Some(entity_id));
    assert_eq!(
        bindings.field(entity_id, &field("owner_id")),
        Some(parent_id)
    );
    assert_eq!(bindings.named_type(&r#type("Status")), Some(status_id));
    assert_eq!(bindings.named_type(&r#type("Profile")), Some(profile_id));
    assert_eq!(bindings.named_type(&r#type("Score")), Some(score_id));
    assert_eq!(
        bindings.enum_variant(status_type_id, &r#type("Queued")),
        Some(pending_id),
    );
    assert_eq!(
        bindings.composite_field(profile_type_id, &field("label")),
        Some(nickname_id),
    );
    assert_eq!(
        bindings.relation(
            entity_id,
            &RelationSourceKey::try_new("owner_relation").expect("relation should admit"),
        ),
        Some(relation_id),
    );
    assert_eq!(
        bindings.constraint(
            entity_id,
            &ConstraintSourceKey::try_new("nonnegative_owner").expect("constraint should admit"),
        ),
        Some(check_id),
    );
    let new_rule = ConstraintSourceKey::for_targeted_field_rule(
        &field("score"),
        &r#type("Score"),
        &RuleSourceKey::try_new("bounded").expect("rule should admit"),
    );
    assert_eq!(bindings.constraint(entity_id, &new_rule), Some(rule_id));
    assert!(bindings.entity(&old_entity).is_none());
    assert!(bindings.field(entity_id, &field("parent_id")).is_none());
    assert!(bindings.named_type(&r#type("OldStatus")).is_none());
    assert!(
        bindings
            .enum_variant(status_type_id, &r#type("Pending"))
            .is_none(),
    );
    assert!(
        bindings
            .composite_field(profile_type_id, &field("nickname"))
            .is_none(),
    );
    assert!(bindings.constraint(entity_id, &old_rule).is_none());
    let target_snapshot = &target.entity_snapshots()[&entity_id];
    let target_index = target_snapshot
        .indexes()
        .iter()
        .find(|index| index.schema_id() == index_id)
        .expect("index should remain");
    assert_ne!(
        target_index.physical_generation(),
        old_index.physical_generation()
    );
    assert_eq!(target_snapshot.relations()[0].id(), relation_id);
    assert_eq!(target_snapshot.relations()[0].name(), "owner_relation");
    assert_eq!(target_snapshot.relations()[0].target_path(), "Entry");
    assert!(
        target_snapshot
            .constraint_catalog()
            .activation(rule_id)
            .is_none(),
        "the physical migration validates the candidate rule before publication",
    );
}

#[test]
fn shared_type_rename_and_creation_preserve_store_order_and_identities() {
    let initial = shared_type_proposal(false, ExpectedAcceptedHead::Empty, None);
    let mut candidates = lower_initial_schema_proposal(
        &initial,
        &[
            crate::db::schema::ProposalStoreTarget {
                path: "test::AlphaStore",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
            },
            crate::db::schema::ProposalStoreTarget {
                path: "test::BetaStore",
                identity: TargetStoreIdentity::from_bytes([5; 32]),
            },
        ],
    )
    .expect("initial proposal should lower");
    candidates.sort_by(|left, right| left.store_path().cmp(right.store_path()));
    let mut lineage = AcceptedEntitySourceLineageCatalog::default();
    let mut accepted = Vec::new();
    for (candidate, entity_name, store) in [
        (
            &candidates[0],
            "Alpha",
            TargetStoreIdentity::from_bytes([2; 32]),
        ),
        (
            &candidates[1],
            "Beta",
            TargetStoreIdentity::from_bytes([5; 32]),
        ),
    ] {
        let entity_source = EntitySourceKey::try_new(entity_name).expect("entity should admit");
        let entity_id = candidate
            .bundle()
            .source_bindings()
            .entity(&entity_source)
            .expect("entity should bind");
        let type_id = candidate
            .bundle()
            .source_bindings()
            .named_type(&r#type("OldValue"))
            .expect("type should bind");
        lineage
            .insert(
                store,
                entity_id,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                    initial
                        .entity_source_digest(&entity_source)
                        .expect("digest should derive"),
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        accepted.push((candidate.store_path(), type_id));
    }
    let migration = shared_type_proposal(true, head(), Some(shared_type_plan()));
    let mut stores = vec![
        ExistingProposalStore {
            path: "test::AlphaStore",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
            bundle: candidates[0].bundle().catalog(),
        },
        ExistingProposalStore {
            path: "test::BetaStore",
            identity: TargetStoreIdentity::from_bytes([5; 32]),
            bundle: candidates[1].bundle().catalog(),
        },
    ];
    stores.reverse();
    let planned =
        plan_schema_migration(&migration, &stores, &lineage).expect("shared rename should plan");
    assert_eq!(
        planned
            .candidates()
            .iter()
            .map(CandidateSchemaRevision::store_path)
            .collect::<Vec<_>>(),
        ["test::AlphaStore", "test::BetaStore"]
    );
    assert_eq!(planned.candidates()[0].bundle().entity_snapshots().len(), 1);
    assert_eq!(planned.candidates()[1].bundle().entity_snapshots().len(), 2);
    for candidate in planned.candidates() {
        let migrated = candidate
            .bundle()
            .entity_snapshots()
            .values()
            .find(|snapshot| snapshot.entity_path().starts_with("New"))
            .unwrap();
        let target = if migrated.entity_path() == "NewAlpha" {
            "NewBeta"
        } else {
            "NewAlpha"
        };
        assert_eq!(migrated.relations()[0].target_path(), target);
        assert!(migrated.constraints().iter().any(|constraint| matches!(
            constraint.kind(), AcceptedConstraintKind::Relation { relation_id }
                if *relation_id == migrated.relations()[0].id()
        )));

        assert_eq!(candidate.bundle().revision().get(), 2);
        let recovered = crate::db::schema::enum_catalog::decode_accepted_schema_revision_bundle(
            candidate.encoded_bundle(),
        )
        .unwrap();
        assert_eq!(&recovered, candidate.bundle());

        let expected = accepted
            .iter()
            .find(|(path, _)| *path == candidate.store_path())
            .map(|(_, identity)| *identity)
            .expect("accepted type should exist");
        assert_eq!(
            candidate
                .bundle()
                .source_bindings()
                .named_type(&r#type("Value")),
            Some(expected),
        );
        assert!(
            candidate
                .bundle()
                .source_bindings()
                .named_type(&r#type("OldValue"))
                .is_none(),
        );
    }
}

#[test]
fn unadopted_lineage_fails_before_candidate_derivation() {
    let (_, candidate) = accepted_fixture();
    let transition = EntityMigration::try_new(
        EntitySourceKey::try_new("User").expect("entity should admit"),
        DeclaredEntityVersion::try_new(1).expect("version should admit"),
        None,
        vec![SchemaMigrationRename::Field {
            from: field("email"),
            to: field("primary_email"),
        }],
        Vec::new(),
    )
    .expect("transition should admit");
    let migration = proposal(
        2,
        "User",
        "primary_email",
        head(),
        Some(SchemaMigrationPlan::try_new(vec![transition]).expect("plan should admit")),
    );
    assert!(matches!(
        plan_schema_migration(
            &migration,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
                bundle: candidate.bundle().catalog(),
            }],
            &AcceptedEntitySourceLineageCatalog::default(),
        )
        .err(),
        Some(SchemaMigrationPlanningError::Unadopted),
    ));
}

#[test]
fn publication_head_does_not_replace_source_version_and_digest_checks() {
    let (initial, candidate) = accepted_fixture();
    let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
    let entity_id = candidate
        .bundle()
        .source_bindings()
        .entity(&entity_source)
        .expect("entity should bind");
    let accepted_digest = initial
        .entity_source_digest(&entity_source)
        .expect("digest should derive");
    let store = TargetStoreIdentity::from_bytes([2; 32]);
    let existing = || ExistingProposalStore {
        path: "test::Store",
        identity: store,
        bundle: candidate.bundle().catalog(),
    };
    let rename = |from_version| {
        SchemaMigrationPlan::try_new(vec![
            EntityMigration::try_new(
                entity_source.clone(),
                DeclaredEntityVersion::try_new(from_version).expect("version should admit"),
                None,
                vec![SchemaMigrationRename::Field {
                    from: field("email"),
                    to: field("primary_email"),
                }],
                Vec::new(),
            )
            .expect("transition should admit"),
        ])
        .expect("plan should admit")
    };

    let mut prior_publication = AcceptedEntitySourceLineageCatalog::default();
    prior_publication
        .insert(
            store,
            entity_id,
            AcceptedEntitySourceLineage::adopted(
                ExpectedAcceptedHead::Exact {
                    revision: 2,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([4; 32]),
                },
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                accepted_digest,
            )
            .expect("lineage should admit"),
        )
        .expect("lineage should insert");
    assert!(
        plan_schema_migration(
            &proposal(2, "User", "primary_email", head(), Some(rename(1))),
            &[existing()],
            &prior_publication,
        )
        .is_ok()
    );

    for (accepted_version, target_version, from_version, expected) in [
        (1, 3, 2, SchemaMigrationPlanningError::VersionGap),
        (2, 2, 1, SchemaMigrationPlanningError::Downgrade),
    ] {
        let mut lineage = AcceptedEntitySourceLineageCatalog::default();
        lineage
            .insert(
                store,
                entity_id,
                AcceptedEntitySourceLineage::adopted(
                    head(),
                    AcceptedEntitySourceVersion::try_new(accepted_version)
                        .expect("version should admit"),
                    accepted_digest,
                )
                .expect("lineage should admit"),
            )
            .expect("lineage should insert");
        let error = plan_schema_migration(
            &proposal(
                target_version,
                "User",
                "primary_email",
                head(),
                Some(rename(from_version)),
            ),
            &[existing()],
            &lineage,
        )
        .err();
        assert!(matches!(
            (error, expected),
            (
                Some(SchemaMigrationPlanningError::VersionGap),
                SchemaMigrationPlanningError::VersionGap
            ) | (
                Some(SchemaMigrationPlanningError::Downgrade),
                SchemaMigrationPlanningError::Downgrade
            )
        ));
    }

    let empty_plan = SchemaMigrationPlan::try_new(vec![
        EntityMigration::try_new(
            entity_source.clone(),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            None,
            vec![SchemaMigrationRename::Field {
                from: field("obsolete_email"),
                to: field("email"),
            }],
            Vec::new(),
        )
        .expect("transition should admit"),
    ])
    .expect("plan should admit");
    let mut lineage = AcceptedEntitySourceLineageCatalog::default();
    lineage
        .insert(
            store,
            entity_id,
            AcceptedEntitySourceLineage::adopted(
                head(),
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                accepted_digest,
            )
            .expect("lineage should admit"),
        )
        .expect("lineage should insert");
    assert!(matches!(
        plan_schema_migration(
            &proposal(2, "User", "email", head(), Some(empty_plan)),
            &[existing()],
            &lineage,
        )
        .err(),
        Some(SchemaMigrationPlanningError::EmptyEntityVersionBump),
    ));
}

#[test]
fn physical_transform_cannot_target_the_primary_key() {
    let (initial, candidate) = accepted_fixture();
    let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
    let entity_id = candidate
        .bundle()
        .source_bindings()
        .entity(&entity_source)
        .expect("entity should bind");
    let transition = EntityMigration::try_new(
        entity_source.clone(),
        DeclaredEntityVersion::try_new(1).expect("version should admit"),
        None,
        Vec::new(),
        vec![SchemaMigrationTransform::Fill {
            to: field("email"),
            literal: ScalarLiteral::Nat(7),
        }],
    )
    .expect("transition should admit");
    let migration = proposal(
        2,
        "User",
        "email",
        head(),
        Some(SchemaMigrationPlan::try_new(vec![transition]).expect("plan should admit")),
    );
    let mut lineage = AcceptedEntitySourceLineageCatalog::default();
    lineage
        .insert(
            TargetStoreIdentity::from_bytes([2; 32]),
            entity_id,
            AcceptedEntitySourceLineage::adopted(
                head(),
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                initial
                    .entity_source_digest(&entity_source)
                    .expect("digest should derive"),
            )
            .expect("lineage should admit"),
        )
        .expect("lineage should insert");
    assert!(matches!(
        plan_schema_migration(
            &migration,
            &[ExistingProposalStore {
                path: "test::Store",
                identity: TargetStoreIdentity::from_bytes([2; 32]),
                bundle: candidate.bundle().catalog(),
            }],
            &lineage,
        )
        .err(),
        Some(SchemaMigrationPlanningError::UnsupportedTransform),
    ));
}

#[test]
fn checked_cast_transform_compiles_to_accepted_ids_and_full_rewrite_layout() {
    let initial = checked_cast_proposal(false, ExpectedAcceptedHead::Empty, None);
    let candidate = lower_initial_schema_proposal(
        &initial,
        &[crate::db::schema::ProposalStoreTarget {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
        }],
    )
    .expect("initial proposal should lower")
    .pop()
    .expect("initial candidate should exist");
    let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
    let entity_id = candidate
        .bundle()
        .source_bindings()
        .entity(&entity_source)
        .expect("entity should bind");
    let old_value = candidate
        .bundle()
        .source_bindings()
        .field(entity_id, &field("old_value"))
        .expect("old field should bind");
    let plan = SchemaMigrationPlan::try_new(vec![
        EntityMigration::try_new(
            entity_source.clone(),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            None,
            Vec::new(),
            vec![SchemaMigrationTransform::CheckedCast {
                from: field("old_value"),
                to: field("value"),
                target: ScalarType::Nat8,
            }],
        )
        .expect("transition should admit"),
    ])
    .expect("plan should admit");
    let migration = checked_cast_proposal(true, head(), Some(plan));
    let mut lineage = AcceptedEntitySourceLineageCatalog::default();
    lineage
        .insert(
            TargetStoreIdentity::from_bytes([2; 32]),
            entity_id,
            AcceptedEntitySourceLineage::adopted(
                head(),
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                initial
                    .entity_source_digest(&entity_source)
                    .expect("digest should derive"),
            )
            .expect("lineage should admit"),
        )
        .expect("lineage should insert");
    let planned = plan_schema_migration(
        &migration,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
            bundle: candidate.bundle().catalog(),
        }],
        &lineage,
    )
    .expect("physical migration should plan");
    assert!(planned.requires_physical_validation());
    assert_eq!(planned.programs().len(), 1);
    let next = planned.candidates()[0]
        .bundle()
        .entity_snapshots()
        .get(&entity_id)
        .expect("candidate entity should exist");
    let value_id = planned.candidates()[0]
        .bundle()
        .source_bindings()
        .field(entity_id, &field("value"))
        .expect("candidate field should bind");
    // Dense field removal may reuse the predecessor ordinal in the
    // candidate catalog. The compiled program keeps the before and after
    // catalog contexts distinct, so numeric equality is not identity
    // aliasing across accepted revisions.
    assert_eq!(old_value, value_id);
    assert!(
        planned.candidates()[0]
            .bundle()
            .source_bindings()
            .field(entity_id, &field("old_value"))
            .is_none()
    );
    assert_eq!(
        next.row_layout().current_version(),
        next.row_layout().history_floor(),
    );
    assert!(
        next.row_layout().current_version()
            > candidate.bundle().entity_snapshots()[&entity_id]
                .row_layout()
                .current_version()
    );
}

#[test]
fn managed_generated_indexed_entity_plans_rename_and_same_field_checked_cast() {
    let initial = maintained_user_proposal(false, ExpectedAcceptedHead::Empty, None);
    let candidate = lower_initial_schema_proposal(
        &initial,
        &[crate::db::schema::ProposalStoreTarget {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
        }],
    )
    .expect("initial proposal should lower")
    .pop()
    .expect("initial candidate should exist");
    let entity_source = EntitySourceKey::try_new("SqlTestUser").expect("entity should admit");
    let entity_id = candidate
        .bundle()
        .source_bindings()
        .entity(&entity_source)
        .expect("entity should bind");
    let plan = SchemaMigrationPlan::try_new(vec![
        EntityMigration::try_new(
            entity_source.clone(),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            None,
            vec![SchemaMigrationRename::Field {
                from: field("rank"),
                to: field("score"),
            }],
            vec![SchemaMigrationTransform::CheckedCast {
                from: field("age"),
                to: field("age"),
                target: ScalarType::Nat16,
            }],
        )
        .expect("transition should admit"),
    ])
    .expect("plan should admit");
    let migration = maintained_user_proposal(true, head(), Some(plan));
    let mut lineage = AcceptedEntitySourceLineageCatalog::default();
    lineage
        .insert(
            TargetStoreIdentity::from_bytes([2; 32]),
            entity_id,
            AcceptedEntitySourceLineage::adopted(
                ExpectedAcceptedHead::Exact {
                    revision: 7,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([9; 32]),
                },
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                initial
                    .entity_source_digest(&entity_source)
                    .expect("digest should derive"),
            )
            .expect("lineage should admit"),
        )
        .expect("lineage should insert");

    let control_source = EntitySourceKey::try_new("Control").expect("entity should admit");
    let control_id = candidate
        .bundle()
        .source_bindings()
        .entity(&control_source)
        .expect("control should bind");
    lineage
        .insert(
            TargetStoreIdentity::from_bytes([2; 32]),
            control_id,
            AcceptedEntitySourceLineage::adopted(
                ExpectedAcceptedHead::Exact {
                    revision: 7,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([9; 32]),
                },
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                initial
                    .entity_source_digest(&control_source)
                    .expect("digest should derive"),
            )
            .expect("lineage should admit"),
        )
        .expect("control lineage should insert");

    let planned = plan_schema_migration(
        &migration,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
            bundle: candidate.bundle().catalog(),
        }],
        &lineage,
    )
    .expect("maintained entity migration should plan");

    assert!(planned.requires_physical_validation());
    assert_eq!(planned.programs().len(), 1);
}

#[test]
fn every_closed_transform_form_compiles_into_one_accepted_id_program() {
    let initial = closed_transform_proposal(false, ExpectedAcceptedHead::Empty, None);
    let candidate = lower_initial_schema_proposal(
        &initial,
        &[crate::db::schema::ProposalStoreTarget {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
        }],
    )
    .expect("initial proposal should lower")
    .pop()
    .expect("initial candidate should exist");
    let entity_source = EntitySourceKey::try_new("User").expect("entity should admit");
    let entity_id = candidate
        .bundle()
        .source_bindings()
        .entity(&entity_source)
        .expect("entity should bind");
    let transition = EntityMigration::try_new(
        entity_source.clone(),
        DeclaredEntityVersion::try_new(1).expect("version should admit"),
        None,
        Vec::new(),
        vec![
            SchemaMigrationTransform::Fill {
                to: field("filled"),
                literal: ScalarLiteral::Nat(9),
            },
            SchemaMigrationTransform::Copy {
                from: field("signed"),
                to: field("copied"),
            },
            SchemaMigrationTransform::CheckedCast {
                from: field("signed"),
                to: field("cast"),
                target: ScalarType::Nat8,
            },
            SchemaMigrationTransform::Coalesce {
                from: field("optional"),
                to: field("coalesced"),
                literal: ScalarLiteral::Int(3),
            },
        ],
    )
    .expect("transition should admit");
    let migration = closed_transform_proposal(
        true,
        head(),
        Some(SchemaMigrationPlan::try_new(vec![transition]).expect("plan should admit")),
    );
    let mut lineage = AcceptedEntitySourceLineageCatalog::default();
    lineage
        .insert(
            TargetStoreIdentity::from_bytes([2; 32]),
            entity_id,
            AcceptedEntitySourceLineage::adopted(
                head(),
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                initial
                    .entity_source_digest(&entity_source)
                    .expect("digest should derive"),
            )
            .expect("lineage should admit"),
        )
        .expect("lineage should insert");
    let planned = plan_schema_migration(
        &migration,
        &[ExistingProposalStore {
            path: "test::Store",
            identity: TargetStoreIdentity::from_bytes([2; 32]),
            bundle: candidate.bundle().catalog(),
        }],
        &lineage,
    )
    .expect("closed transform plan should compile");
    assert_eq!(planned.programs().len(), 1);
    assert_eq!(planned.programs()[0].transform_count(), 4);
    let candidate_entity = &planned.candidates()[0].bundle().entity_snapshots()[&entity_id];
    assert_eq!(candidate_entity.indexes().len(), 1);
    assert!(candidate_entity.indexes()[0].unique());
    assert_ne!(candidate_entity.indexes()[0].physical_generation(), 0);
}

#[test]
fn reconciliation_rejects_incomplete_rekeyed_catalog_before_lowering() {
    let (proposal, candidate) = accepted_fixture();
    let store = ExistingProposalStore {
        path: "test::Store",
        identity: TargetStoreIdentity::from_bytes([2; 32]),
        bundle: candidate.bundle().catalog(),
    };
    let mut working = SchemaDraft::new(&store);
    // The source bindings still name the missing entity. Borrowing unpublished
    // catalog parts must retain the same closure check as bundle construction.
    working.snapshots.clear();
    let stores = BTreeMap::from([(store.path, working)]);
    assert!(matches!(
        reconcile_migration_drafts(&proposal, stores),
        Err(SchemaMigrationPlanningError::RekeyedCatalogInvalid)
    ));
    assert!(!candidate.bundle().entity_snapshots().is_empty());
}
