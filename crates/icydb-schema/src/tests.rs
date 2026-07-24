use crate::{
    Account, Blob, ConstraintFragment, ConstraintSourceKey, Date, Decimal, Duration,
    EntityFragment, EntitySourceKey, EntityStoreAssignment, EnumTypeFragment, EnumVariantFragment,
    ExpectedAcceptedHead, ExpectedSchemaFingerprint, FieldFragment, FieldInsertPolicy,
    FieldSourceKey, FieldType, Float32, Float64, IntBig, MAX_SCHEMA_PROPOSAL_BYTES,
    MAX_SCHEMA_TYPE_DEPTH, MAX_SOURCE_KEY_BYTES, NamedTypeFragment, NatBig,
    ProposalContractVersion, RecordFieldFragment, RecordTypeFragment, RelationDeleteAction,
    RelationFragment, ScalarLiteral, ScalarType, SchemaCapability, SchemaContractError,
    SchemaFragment, SchemaName, SchemaProposal, SchemaRemoval, SchemaSubmissionKey,
    SourceCheckExpr, SourceCheckInstruction, Subaccount, TargetDatabaseIdentity,
    TargetStoreIdentity, Timestamp, TypeSourceKey, Ulid, Unit, decode_schema_fragment,
    decode_schema_proposal, encode_schema_fragment, encode_schema_proposal,
};

fn source<T>(value: &str, constructor: impl FnOnce(String) -> Result<T, SchemaContractError>) -> T {
    constructor(value.to_string()).expect("fixture source key should admit")
}

fn proposal(entity_name: &str, reverse_input: bool) -> SchemaProposal {
    let entity_key = source("entity/user", EntitySourceKey::try_new);
    let id_key = source("field/user/id", FieldSourceKey::try_new);
    let age_key = source("field/user/age", FieldSourceKey::try_new);
    let constraint_key = source(
        "constraint/user/age_nonnegative",
        ConstraintSourceKey::try_new,
    );
    let id = FieldFragment::new(
        id_key.clone(),
        SchemaName::try_new("id").expect("fixture name should admit"),
        FieldType::Scalar(ScalarType::Nat64),
        false,
        FieldInsertPolicy::Required,
        None,
    );
    let age = FieldFragment::new(
        age_key.clone(),
        SchemaName::try_new("age").expect("fixture name should admit"),
        FieldType::Scalar(ScalarType::Int64),
        false,
        FieldInsertPolicy::Required,
        None,
    );
    let expression = SourceCheckExpr::try_new(vec![
        SourceCheckInstruction::Field(age_key),
        SourceCheckInstruction::Literal(ScalarLiteral::Int(0)),
        SourceCheckInstruction::GreaterThanOrEqual,
    ])
    .expect("fixture expression should admit");
    let constraint = ConstraintFragment::new(
        constraint_key,
        SchemaName::try_new("age_nonnegative").expect("fixture name should admit"),
        expression,
    );
    let fields = if reverse_input {
        vec![age, id]
    } else {
        vec![id, age]
    };
    let entity = EntityFragment::try_new(
        entity_key.clone(),
        SchemaName::try_new(entity_name).expect("fixture name should admit"),
        fields,
        vec![id_key],
        vec![],
        vec![],
        vec![constraint],
    )
    .expect("fixture entity should admit");
    let fragment =
        SchemaFragment::try_new(vec![entity], vec![]).expect("fixture fragment should admit");

    SchemaProposal::try_compose(
        vec![
            SchemaCapability::ACCEPTED_CHECKS,
            SchemaCapability::EXACT_COMPOSITE_TYPES,
        ],
        TargetDatabaseIdentity::from_bytes([0x11; 32]),
        SchemaSubmissionKey::try_new("submission/user-v1")
            .expect("fixture submission should admit"),
        ExpectedAcceptedHead::Empty,
        vec![fragment],
        vec![EntityStoreAssignment::new(
            entity_key,
            TargetStoreIdentity::from_bytes([0x22; 32]),
        )],
        vec![],
    )
    .expect("fixture proposal should compose")
}

#[test]
fn source_keys_are_nonempty_bounded_and_canonical() {
    assert_eq!(
        EntitySourceKey::try_new(""),
        Err(SchemaContractError::EmptyIdentity),
    );
    assert!(matches!(
        EntitySourceKey::try_new("x".repeat(MAX_SOURCE_KEY_BYTES + 1)),
        Err(SchemaContractError::IdentityTooLong { .. })
    ));
    assert_eq!(
        EntitySourceKey::try_new("entity user"),
        Err(SchemaContractError::InvalidSourceKey),
    );
    assert!(EntitySourceKey::try_new("module::entity/user-v1").is_ok());
}

#[test]
fn proposal_construction_is_order_independent_and_roundtrips_exactly() {
    let canonical = proposal("users", false);
    let reverse_input = proposal("users", true);
    let canonical_bytes = encode_schema_proposal(&canonical).expect("proposal should encode");
    let reverse_bytes = encode_schema_proposal(&reverse_input).expect("proposal should encode");

    assert_eq!(reverse_bytes, canonical_bytes);
    candid::decode_one::<SchemaProposal>(&canonical_bytes)
        .expect("Candid proposal representation should decode");
    assert_eq!(
        decode_schema_proposal(&canonical_bytes).expect("proposal should decode"),
        canonical,
    );
    assert_eq!(
        reverse_input.digest().expect("proposal should hash"),
        canonical.digest().expect("proposal should hash"),
    );
}

#[test]
fn editable_rename_preserves_source_identity_but_changes_meaning_digest() {
    let before = proposal("users", false);
    let after = proposal("accounts", false);

    assert_eq!(
        before.fragments()[0].entities()[0].source_key(),
        after.fragments()[0].entities()[0].source_key(),
    );
    assert_ne!(
        before.digest().expect("proposal should hash"),
        after.digest().expect("proposal should hash"),
    );
}

#[test]
fn unsupported_capability_fails_before_transport() {
    let error = SchemaProposal::try_compose(
        vec![SchemaCapability::from_raw(u16::MAX)],
        TargetDatabaseIdentity::from_bytes([0x11; 32]),
        SchemaSubmissionKey::try_new("unsupported-capability")
            .expect("fixture submission should admit"),
        ExpectedAcceptedHead::Empty,
        vec![],
        vec![],
        vec![],
    )
    .expect_err("unknown proposal capability must reject");

    assert_eq!(error, SchemaContractError::UnsupportedCapability);
}

#[test]
fn bounded_malformed_and_oversized_proposals_fail_typed() {
    assert_eq!(
        decode_schema_proposal(&[0xFF; 32]),
        Err(SchemaContractError::Decode),
    );
    assert!(matches!(
        decode_schema_proposal(&vec![0; MAX_SCHEMA_PROPOSAL_BYTES + 1]),
        Err(SchemaContractError::EncodedTooLarge { .. })
    ));
}

#[test]
fn expression_stack_shape_is_validated() {
    assert_eq!(
        SourceCheckExpr::try_new(vec![SourceCheckInstruction::Equal]),
        Err(SchemaContractError::InvalidExpression),
    );
    assert_eq!(
        SourceCheckExpr::try_new(vec![
            SourceCheckInstruction::Literal(ScalarLiteral::Bool(true)),
            SourceCheckInstruction::Literal(ScalarLiteral::Bool(false)),
        ]),
        Err(SchemaContractError::InvalidExpression),
    );
}

#[test]
fn contract_version_is_current_and_nonzero() {
    assert_eq!(ProposalContractVersion::CURRENT.get(), 1);
}

#[test]
fn every_defined_entity_requires_exact_store_routing() {
    let base = proposal("users", false);
    let error = SchemaProposal::try_compose(
        base.capabilities().to_vec(),
        base.target_database(),
        SchemaSubmissionKey::try_new("missing-routing").expect("submission key should admit"),
        ExpectedAcceptedHead::Empty,
        base.fragments().to_vec(),
        Vec::new(),
        Vec::new(),
    )
    .expect_err("defined entity without routing must reject");

    assert_eq!(error, SchemaContractError::MissingEntityStoreAssignment);
}

#[test]
fn removals_conflict_only_with_the_exact_defined_source_key() {
    let base = proposal("users", false);
    let entity = base.fragments()[0].entities()[0].source_key().clone();
    let unrelated = source("constraint/user/retired", ConstraintSourceKey::try_new);
    let unrelated_removal = SchemaRemoval::Constraint {
        entity: entity.clone(),
        constraint: unrelated,
    };
    assert!(
        SchemaProposal::try_compose(
            base.capabilities().to_vec(),
            base.target_database(),
            SchemaSubmissionKey::try_new("unrelated-removal").expect("submission key should admit"),
            ExpectedAcceptedHead::Empty,
            base.fragments().to_vec(),
            base.assignments().to_vec(),
            vec![unrelated_removal],
        )
        .is_ok(),
        "a definition for the entity must not collide with an unrelated accepted removal",
    );

    let defined_constraint = base.fragments()[0].entities()[0].constraints()[0]
        .source_key()
        .clone();
    let error = SchemaProposal::try_compose(
        base.capabilities().to_vec(),
        base.target_database(),
        SchemaSubmissionKey::try_new("defined-removal").expect("submission key should admit"),
        ExpectedAcceptedHead::Empty,
        base.fragments().to_vec(),
        base.assignments().to_vec(),
        vec![SchemaRemoval::Constraint {
            entity,
            constraint: defined_constraint,
        }],
    )
    .expect_err("defining and removing the same constraint must reject");
    assert_eq!(error, SchemaContractError::DefinitionRemovalConflict);
}

#[test]
fn entity_local_references_and_editable_names_are_closed() {
    let entity_key = source("entity/local", EntitySourceKey::try_new);
    let id_key = source("field/local/id", FieldSourceKey::try_new);
    let missing_key = source("field/local/missing", FieldSourceKey::try_new);
    let id = FieldFragment::new(
        id_key.clone(),
        SchemaName::try_new("id").expect("name should admit"),
        FieldType::Scalar(ScalarType::Nat64),
        false,
        FieldInsertPolicy::Required,
        None,
    );
    let expression = SourceCheckExpr::try_new(vec![
        SourceCheckInstruction::Field(missing_key),
        SourceCheckInstruction::Literal(ScalarLiteral::Nat(0)),
        SourceCheckInstruction::GreaterThanOrEqual,
    ])
    .expect("expression stack should admit");
    let constraint = ConstraintFragment::new(
        source("constraint/local/check", ConstraintSourceKey::try_new),
        SchemaName::try_new("local_check").expect("name should admit"),
        expression,
    );
    assert_eq!(
        EntityFragment::try_new(
            entity_key.clone(),
            SchemaName::try_new("local").expect("name should admit"),
            vec![id.clone()],
            vec![id_key.clone()],
            Vec::new(),
            Vec::new(),
            vec![constraint],
        ),
        Err(SchemaContractError::InvalidLocalReference),
    );

    let duplicate_name = FieldFragment::new(
        source("field/local/other", FieldSourceKey::try_new),
        SchemaName::try_new("id").expect("name should admit"),
        FieldType::Scalar(ScalarType::Text { max_len: None }),
        false,
        FieldInsertPolicy::Required,
        None,
    );
    assert_eq!(
        EntityFragment::try_new(
            entity_key,
            SchemaName::try_new("local").expect("name should admit"),
            vec![id, duplicate_name],
            vec![id_key],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ),
        Err(SchemaContractError::DuplicateEditableName),
    );
}

#[test]
fn named_collection_fragments_roundtrip_canonically() {
    let fragment = SchemaFragment::try_new(
        Vec::new(),
        vec![NamedTypeFragment::Tuple {
            source_key: source("type/point", TypeSourceKey::try_new),
            name: SchemaName::try_new("Point").expect("name should admit"),
            members: vec![
                FieldType::Scalar(ScalarType::Int64),
                FieldType::Scalar(ScalarType::Int64),
            ],
        }],
    )
    .expect("tuple fragment should admit");
    let bytes = encode_schema_fragment(&fragment).expect("fragment should encode");

    assert_eq!(
        decode_schema_fragment(&bytes).expect("fragment should decode"),
        fragment,
    );
}

fn entity_with_field(
    field_type: FieldType,
    insert_policy: FieldInsertPolicy,
) -> Result<EntityFragment, SchemaContractError> {
    let entity = source("entity/field_holder", EntitySourceKey::try_new);
    let field = source("field/field_holder/value", FieldSourceKey::try_new);
    EntityFragment::try_new(
        entity,
        SchemaName::try_new("FieldHolder").expect("name should admit"),
        vec![FieldFragment::new(
            field.clone(),
            SchemaName::try_new("value").expect("name should admit"),
            field_type,
            false,
            insert_policy,
            None,
        )],
        vec![field],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
}

fn entity_with_default(
    field_type: FieldType,
    literal: ScalarLiteral,
) -> Result<EntityFragment, SchemaContractError> {
    entity_with_field(field_type, FieldInsertPolicy::Default(literal))
}

fn record_with_field(field_type: FieldType) -> Result<RecordTypeFragment, SchemaContractError> {
    RecordTypeFragment::try_new(
        source("type/record_holder", TypeSourceKey::try_new),
        SchemaName::try_new("RecordHolder").expect("name should admit"),
        vec![RecordFieldFragment::new(
            source("field/record_holder/value", FieldSourceKey::try_new),
            SchemaName::try_new("value").expect("name should admit"),
            field_type,
            false,
        )],
    )
}

#[test]
fn exact_scalar_field_types_roundtrip_without_width_or_bound_loss() {
    let types = vec![
        ScalarType::Account,
        ScalarType::Blob { max_len: None },
        ScalarType::Blob { max_len: Some(41) },
        ScalarType::Bool,
        ScalarType::Date,
        ScalarType::Decimal { scale: 7 },
        ScalarType::Duration,
        ScalarType::Float32,
        ScalarType::Float64,
        ScalarType::Int8,
        ScalarType::Int16,
        ScalarType::Int32,
        ScalarType::Int64,
        ScalarType::Int128,
        ScalarType::IntBig { max_bytes: 257 },
        ScalarType::Principal,
        ScalarType::Subaccount,
        ScalarType::Text { max_len: None },
        ScalarType::Text { max_len: Some(43) },
        ScalarType::Timestamp,
        ScalarType::Nat8,
        ScalarType::Nat16,
        ScalarType::Nat32,
        ScalarType::Nat64,
        ScalarType::Nat128,
        ScalarType::NatBig { max_bytes: 263 },
        ScalarType::Ulid,
        ScalarType::Unit,
    ];
    let bytes = candid::encode_one(&types).expect("exact scalar types should encode");

    assert_eq!(
        candid::decode_one::<Vec<ScalarType>>(&bytes).expect("exact scalar types should decode"),
        types,
    );
    assert_ne!(
        candid::encode_one(ScalarType::Int8).expect("int8 should encode"),
        candid::encode_one(ScalarType::Int64).expect("int64 should encode"),
    );
    assert_ne!(
        candid::encode_one(ScalarType::Text { max_len: Some(8) })
            .expect("bounded text should encode"),
        candid::encode_one(ScalarType::Text { max_len: Some(9) })
            .expect("distinct bounded text should encode"),
    );
}

#[test]
fn exact_scalar_defaults_reject_values_outside_the_declared_contract() {
    assert!(
        entity_with_default(FieldType::Scalar(ScalarType::Int8), ScalarLiteral::Int(127)).is_ok()
    );
    assert_eq!(
        entity_with_default(FieldType::Scalar(ScalarType::Int8), ScalarLiteral::Int(128)),
        Err(SchemaContractError::LiteralTypeMismatch),
    );
    assert_eq!(
        entity_with_default(
            FieldType::Scalar(ScalarType::Int64),
            ScalarLiteral::Int(i128::from(i64::MAX) + 1),
        ),
        Err(SchemaContractError::LiteralTypeMismatch),
    );
    assert_eq!(
        entity_with_default(
            FieldType::Scalar(ScalarType::Nat64),
            ScalarLiteral::Nat(u128::from(u64::MAX) + 1),
        ),
        Err(SchemaContractError::LiteralTypeMismatch),
    );
    assert!(
        entity_with_default(
            FieldType::Scalar(ScalarType::Text { max_len: Some(1) }),
            ScalarLiteral::Text("é".to_string()),
        )
        .is_ok(),
    );
    assert_eq!(
        entity_with_default(
            FieldType::Scalar(ScalarType::Text { max_len: Some(1) }),
            ScalarLiteral::Text("éx".to_string()),
        ),
        Err(SchemaContractError::LiteralTypeMismatch),
    );
    assert_eq!(
        entity_with_default(
            FieldType::Scalar(ScalarType::Blob { max_len: Some(1) }),
            ScalarLiteral::Blob(Blob::try_new(vec![1, 2]).expect("literal should admit")),
        ),
        Err(SchemaContractError::LiteralTypeMismatch),
    );
    let signed_big = IntBig::from(128_i64);
    let signed_len = u32::try_from(signed_big.to_leb128().len()).expect("fixture length fits");
    assert!(
        entity_with_default(
            FieldType::Scalar(ScalarType::IntBig {
                max_bytes: signed_len,
            }),
            ScalarLiteral::IntBig(signed_big.clone()),
        )
        .is_ok()
    );
    assert_eq!(
        entity_with_default(
            FieldType::Scalar(ScalarType::IntBig {
                max_bytes: signed_len - 1,
            }),
            ScalarLiteral::IntBig(signed_big),
        ),
        Err(SchemaContractError::LiteralTypeMismatch),
    );
    assert_eq!(
        entity_with_default(
            FieldType::Scalar(ScalarType::Decimal { scale: 1 }),
            ScalarLiteral::Decimal(Decimal::from_i128_with_scale(i128::MAX, 0)),
        ),
        Err(SchemaContractError::LiteralTypeMismatch),
    );
}

#[test]
fn invalid_exact_scalar_field_shapes_reject_before_composition() {
    assert_eq!(
        entity_with_field(
            FieldType::Scalar(ScalarType::Decimal {
                scale: Decimal::max_supported_scale() + 1,
            }),
            FieldInsertPolicy::Required,
        ),
        Err(SchemaContractError::InvalidFieldType),
    );
    assert_eq!(
        record_with_field(FieldType::Scalar(ScalarType::NatBig { max_bytes: 0 })),
        Err(SchemaContractError::InvalidFieldType),
    );
}

#[test]
fn one_entity_cannot_duplicate_a_managed_timestamp_policy() {
    let entity = source("entity/audit", EntitySourceKey::try_new);
    let id = source("field/audit/id", FieldSourceKey::try_new);
    let created_a = source("field/audit/created_a", FieldSourceKey::try_new);
    let created_b = source("field/audit/created_b", FieldSourceKey::try_new);
    let managed = |source_key, name| {
        FieldFragment::new(
            source_key,
            SchemaName::try_new(name).expect("name should admit"),
            FieldType::Scalar(ScalarType::Timestamp),
            false,
            FieldInsertPolicy::Required,
            Some(crate::FieldManagementPolicy::CreatedAt),
        )
    };

    assert_eq!(
        EntityFragment::try_new(
            entity,
            SchemaName::try_new("Audit").expect("name should admit"),
            vec![
                FieldFragment::new(
                    id.clone(),
                    SchemaName::try_new("id").expect("name should admit"),
                    FieldType::Scalar(ScalarType::Nat64),
                    false,
                    FieldInsertPolicy::Required,
                    None,
                ),
                managed(created_a, "created_a"),
                managed(created_b, "created_b"),
            ],
            vec![id],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        ),
        Err(SchemaContractError::InvalidFieldPolicy),
    );
}

fn compose_schema(
    types: Vec<NamedTypeFragment>,
    entities: Vec<EntityFragment>,
    expected_head: ExpectedAcceptedHead,
    removals: Vec<SchemaRemoval>,
) -> Result<SchemaProposal, SchemaContractError> {
    let assignments = entities
        .iter()
        .map(|entity| {
            EntityStoreAssignment::new(
                entity.source_key().clone(),
                TargetStoreIdentity::from_bytes([0x33; 32]),
            )
        })
        .collect();
    SchemaProposal::try_compose(
        vec![SchemaCapability::EXACT_COMPOSITE_TYPES],
        TargetDatabaseIdentity::from_bytes([0x31; 32]),
        SchemaSubmissionKey::try_new("type-composition").expect("submission key should admit"),
        expected_head,
        vec![SchemaFragment::try_new(entities, types)?],
        assignments,
        removals,
    )
}

fn compose_types(
    types: Vec<NamedTypeFragment>,
    expected_head: ExpectedAcceptedHead,
    removals: Vec<SchemaRemoval>,
) -> Result<SchemaProposal, SchemaContractError> {
    compose_schema(types, Vec::new(), expected_head, removals)
}

#[test]
fn source_keyed_enum_defaults_validate_against_local_type_and_variant() {
    let enum_key = source("type/status", TypeSourceKey::try_new);
    let active_key = source("variant/status/active", TypeSourceKey::try_new);
    let enum_type = NamedTypeFragment::Enum(
        EnumTypeFragment::try_new(
            enum_key.clone(),
            SchemaName::try_new("Status").expect("name should admit"),
            vec![EnumVariantFragment::new(
                active_key.clone(),
                SchemaName::try_new("Active").expect("name should admit"),
            )],
        )
        .expect("enum should admit"),
    );
    let holder = |default_enum, default_variant| {
        entity_with_default(
            FieldType::Named(enum_key.clone()),
            ScalarLiteral::EnumUnit {
                enum_type: default_enum,
                variant: default_variant,
            },
        )
        .expect("entity shape should admit before proposal closure")
    };

    assert!(
        compose_schema(
            vec![enum_type.clone()],
            vec![holder(enum_key.clone(), active_key)],
            ExpectedAcceptedHead::Empty,
            Vec::new(),
        )
        .is_ok(),
    );
    assert_eq!(
        compose_schema(
            vec![enum_type.clone()],
            vec![holder(
                enum_key.clone(),
                source("variant/status/missing", TypeSourceKey::try_new),
            )],
            ExpectedAcceptedHead::Empty,
            Vec::new(),
        ),
        Err(SchemaContractError::InvalidEnumLiteral),
    );
    assert_eq!(
        compose_schema(
            vec![enum_type],
            vec![holder(
                source("type/other_status", TypeSourceKey::try_new),
                source("variant/status/active", TypeSourceKey::try_new),
            )],
            ExpectedAcceptedHead::Empty,
            Vec::new(),
        ),
        Err(SchemaContractError::LiteralTypeMismatch),
    );
}

#[test]
fn named_type_graphs_reject_cycles_and_excessive_depth() {
    let a = source("type/a", TypeSourceKey::try_new);
    let b = source("type/b", TypeSourceKey::try_new);
    assert_eq!(
        compose_types(
            vec![
                NamedTypeFragment::Newtype {
                    source_key: a.clone(),
                    name: SchemaName::try_new("A").expect("name should admit"),
                    inner: FieldType::Named(b.clone()),
                },
                NamedTypeFragment::Newtype {
                    source_key: b,
                    name: SchemaName::try_new("B").expect("name should admit"),
                    inner: FieldType::Named(a),
                },
            ],
            ExpectedAcceptedHead::Empty,
            Vec::new(),
        ),
        Err(SchemaContractError::InvalidNamedTypeGraph),
    );

    let mut deep = Vec::new();
    for ordinal in 0..=MAX_SCHEMA_TYPE_DEPTH {
        deep.push(NamedTypeFragment::Newtype {
            source_key: source(&format!("type/deep/{ordinal:03}"), TypeSourceKey::try_new),
            name: SchemaName::try_new(format!("Deep{ordinal:03}")).expect("name should admit"),
            inner: if ordinal == MAX_SCHEMA_TYPE_DEPTH {
                FieldType::Scalar(ScalarType::Unit)
            } else {
                FieldType::Named(source(
                    &format!("type/deep/{:03}", ordinal + 1),
                    TypeSourceKey::try_new,
                ))
            },
        });
    }
    assert_eq!(
        compose_types(deep, ExpectedAcceptedHead::Empty, Vec::new()),
        Err(SchemaContractError::InvalidNamedTypeGraph),
    );
}

#[test]
fn unresolved_expected_head_references_are_deferred_but_never_removed() {
    let external = source("type/external", TypeSourceKey::try_new);
    let local = NamedTypeFragment::Newtype {
        source_key: source("type/local", TypeSourceKey::try_new),
        name: SchemaName::try_new("Local").expect("name should admit"),
        inner: FieldType::Named(external.clone()),
    };
    assert_eq!(
        compose_types(vec![local.clone()], ExpectedAcceptedHead::Empty, Vec::new(),),
        Err(SchemaContractError::InvalidLocalReference),
    );
    let expected = ExpectedAcceptedHead::Exact {
        revision: 7,
        fingerprint: ExpectedSchemaFingerprint::from_bytes([0x32; 32]),
    };
    assert!(compose_types(vec![local.clone()], expected.clone(), Vec::new()).is_ok());
    assert_eq!(
        compose_types(vec![local], expected, vec![SchemaRemoval::Type(external)],),
        Err(SchemaContractError::RemovedReference),
    );
}

fn relation_entities(
    source_kind: ScalarType,
    target_kind: ScalarType,
    include_target: bool,
) -> (
    Vec<SchemaFragment>,
    Vec<EntityStoreAssignment>,
    EntitySourceKey,
    FieldSourceKey,
) {
    let target_entity = source("entity/relation_target", EntitySourceKey::try_new);
    let target_id = source("field/relation_target/id", FieldSourceKey::try_new);
    let source_entity = source("entity/relation_source", EntitySourceKey::try_new);
    let source_id = source("field/relation_source/id", FieldSourceKey::try_new);
    let source_target = source("field/relation_source/target", FieldSourceKey::try_new);
    let relation = RelationFragment::try_new(
        source("relation/source/target", crate::RelationSourceKey::try_new),
        SchemaName::try_new("target").expect("name should admit"),
        vec![source_target.clone()],
        target_entity.clone(),
        vec![target_id.clone()],
        RelationDeleteAction::Restrict,
    )
    .expect("relation should admit");
    let source_definition = EntityFragment::try_new(
        source_entity.clone(),
        SchemaName::try_new("RelationSource").expect("name should admit"),
        vec![
            FieldFragment::new(
                source_id.clone(),
                SchemaName::try_new("id").expect("name should admit"),
                FieldType::Scalar(ScalarType::Nat64),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
            FieldFragment::new(
                source_target,
                SchemaName::try_new("target").expect("name should admit"),
                FieldType::Scalar(source_kind),
                false,
                FieldInsertPolicy::Required,
                None,
            ),
        ],
        vec![source_id],
        Vec::new(),
        vec![relation],
        Vec::new(),
    )
    .expect("source entity should admit");
    let mut fragments = vec![
        SchemaFragment::try_new(vec![source_definition], Vec::new())
            .expect("source fragment should admit"),
    ];
    let mut assignments = vec![EntityStoreAssignment::new(
        source_entity,
        TargetStoreIdentity::from_bytes([0x41; 32]),
    )];
    if include_target {
        let target_definition = EntityFragment::try_new(
            target_entity.clone(),
            SchemaName::try_new("RelationTarget").expect("name should admit"),
            vec![FieldFragment::new(
                target_id.clone(),
                SchemaName::try_new("id").expect("name should admit"),
                FieldType::Scalar(target_kind),
                false,
                FieldInsertPolicy::Required,
                None,
            )],
            vec![target_id.clone()],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .expect("target entity should admit");
        fragments.push(
            SchemaFragment::try_new(vec![target_definition], Vec::new())
                .expect("target fragment should admit"),
        );
        assignments.push(EntityStoreAssignment::new(
            target_entity.clone(),
            TargetStoreIdentity::from_bytes([0x42; 32]),
        ));
    }
    (fragments, assignments, target_entity, target_id)
}

#[test]
fn cross_fragment_relations_validate_exact_field_contracts() {
    let compose = |source_kind, target_kind| {
        let (fragments, assignments, _, _) = relation_entities(source_kind, target_kind, true);
        SchemaProposal::try_compose(
            vec![SchemaCapability::RESTRICTIVE_RELATIONS],
            TargetDatabaseIdentity::from_bytes([0x43; 32]),
            SchemaSubmissionKey::try_new("relation-closure").expect("submission key should admit"),
            ExpectedAcceptedHead::Empty,
            fragments,
            assignments,
            Vec::new(),
        )
    };
    assert!(compose(ScalarType::Nat64, ScalarType::Nat64).is_ok());
    assert_eq!(
        compose(ScalarType::Nat64, ScalarType::Nat32),
        Err(SchemaContractError::RelationTypeMismatch),
    );
}

#[test]
fn external_relation_targets_require_an_expected_head_and_cannot_be_removed() {
    let (fragments, assignments, target_entity, target_field) =
        relation_entities(ScalarType::Nat64, ScalarType::Nat64, false);
    let compose = |expected_head, removals| {
        SchemaProposal::try_compose(
            vec![SchemaCapability::RESTRICTIVE_RELATIONS],
            TargetDatabaseIdentity::from_bytes([0x44; 32]),
            SchemaSubmissionKey::try_new("external-relation").expect("submission key should admit"),
            expected_head,
            fragments.clone(),
            assignments.clone(),
            removals,
        )
    };
    assert_eq!(
        compose(ExpectedAcceptedHead::Empty, Vec::new()),
        Err(SchemaContractError::InvalidLocalReference),
    );
    let expected = ExpectedAcceptedHead::Exact {
        revision: 8,
        fingerprint: ExpectedSchemaFingerprint::from_bytes([0x45; 32]),
    };
    assert!(compose(expected.clone(), Vec::new()).is_ok());
    assert_eq!(
        compose(
            expected,
            vec![SchemaRemoval::Field {
                entity: target_entity,
                field: target_field,
            }],
        ),
        Err(SchemaContractError::RemovedReference),
    );
}

#[test]
fn proposal_literals_preserve_every_canonical_scalar_atom() {
    let principal = crate::Principal::from_slice(&[1, 2, 3]);
    let enum_type = source("type/literal_status", TypeSourceKey::try_new);
    let enum_variant = source("variant/literal_status/active", TypeSourceKey::try_new);
    let literals = vec![
        ScalarLiteral::Account(Account::new(principal, None::<Subaccount>)),
        ScalarLiteral::Blob(Blob::try_new(vec![1, 2, 3]).expect("blob should admit")),
        ScalarLiteral::Bool(true),
        ScalarLiteral::Date(Date::try_new(2026, 7, 24).expect("date should admit")),
        ScalarLiteral::Decimal(Decimal::try_new(125, 2).expect("decimal should admit")),
        ScalarLiteral::Duration(Duration::from_millis(9)),
        ScalarLiteral::EnumUnit {
            enum_type,
            variant: enum_variant,
        },
        ScalarLiteral::Float32(Float32::try_new(1.25).expect("float should admit")),
        ScalarLiteral::Float64(Float64::try_new(-2.5).expect("float should admit")),
        ScalarLiteral::Int(-7),
        ScalarLiteral::IntBig(IntBig::from(-8_i64)),
        ScalarLiteral::Nat(10),
        ScalarLiteral::NatBig(NatBig::from(11_u64)),
        ScalarLiteral::Principal(principal),
        ScalarLiteral::Subaccount(Subaccount::from_array([12; 32])),
        ScalarLiteral::Text("canonical".to_string()),
        ScalarLiteral::Timestamp(Timestamp::from_millis(13)),
        ScalarLiteral::Ulid(Ulid::from_u128(14)),
        ScalarLiteral::Unit(Unit),
    ];
    let bytes = candid::encode_one(&literals).expect("literal vector should encode");
    let decoded =
        candid::decode_one::<Vec<ScalarLiteral>>(&bytes).expect("literal vector should decode");

    assert_eq!(decoded, literals);
}

#[test]
fn proposal_digest_has_a_fixed_current_form_vector() {
    assert_eq!(
        proposal("users", false)
            .digest()
            .expect("proposal should hash")
            .to_bytes(),
        [
            87, 73, 73, 76, 194, 31, 148, 65, 65, 83, 195, 129, 223, 124, 113, 177, 81, 212, 17,
            116, 143, 135, 39, 104, 58, 246, 116, 7, 199, 64, 255, 124,
        ],
    );
}
