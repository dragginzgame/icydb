use super::*;

use crate::{
    db::schema::{FieldStorageDecode, LeafCodec, ScalarCodec},
    db::write_context::MutationMode,
    db::{
        commit::CommitSchemaFingerprint,
        data::AcceptedFieldWriteProvenance,
        schema::{
            AcceptedCompositeCatalog, AcceptedConstraintCatalog, AcceptedConstraintKind,
            AcceptedNamedTypeIdentity, AcceptedRuleOperation, AcceptedRuleTarget,
            AcceptedSchemaFingerprint, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
            AcceptedSourceBindingCatalog, AcceptedValueCatalogHandle, ConstraintActivationKind,
            ConstraintActivationSnapshot, ConstraintActivationState, ConstraintId,
            ConstraintOrigin, FieldId, PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot,
            SchemaFieldSlot, SchemaIndexId, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
            TestEnumDefinition, TestEnumVariant, build_accepted_enum_catalog_for_tests,
            composite_catalog::{
                AcceptedCompositeElement, AcceptedCompositeField, AcceptedCompositeShape,
                CompositeFieldId, CompositeTypeId,
            },
            empty_accepted_enum_catalog_for_tests,
        },
    },
    error::{ConstraintDiagnosticKind, ConstraintValuePathComponent},
    value::{InputValue, Value},
};
use icydb_schema::{Decimal, IntBig, NatBig, ScalarLiteral};
use std::collections::BTreeMap;

const FINGERPRINT: CommitSchemaFingerprint = [7; 16];

#[test]
fn exact_multiple_of_covers_every_runtime_integer_width_and_decimal() {
    let exact_cases = vec![
        (Value::Int64(-10), Value::Int64(5), Value::Int64(-11)),
        (Value::Int128(10), Value::Int128(5), Value::Int128(11)),
        (
            Value::IntBig(IntBig::from(-10_i64)),
            Value::IntBig(IntBig::from(5_i64)),
            Value::IntBig(IntBig::from(-11_i64)),
        ),
        (Value::Nat64(10), Value::Nat64(5), Value::Nat64(11)),
        (Value::Nat128(10), Value::Nat128(5), Value::Nat128(11)),
        (
            Value::NatBig(NatBig::from(10_u64)),
            Value::NatBig(NatBig::from(5_u64)),
            Value::NatBig(NatBig::from(11_u64)),
        ),
        (
            Value::Decimal(Decimal::new(100, 2)),
            Value::Decimal(Decimal::new(25, 2)),
            Value::Decimal(Decimal::new(101, 2)),
        ),
    ];
    for (multiple, divisor, remainder) in exact_cases {
        assert_eq!(
            super::compile::exact_numeric_is_multiple(&multiple, &divisor),
            Some(true),
        );
        assert_eq!(
            super::compile::exact_numeric_is_multiple(&remainder, &divisor),
            Some(false),
        );
    }
    assert_eq!(
        super::compile::exact_numeric_is_multiple(&Value::Nat64(10), &Value::Nat64(0)),
        None,
    );
    assert_eq!(
        super::compile::exact_numeric_is_multiple(&Value::Int64(i64::MIN), &Value::Int64(-1)),
        Some(true),
    );
    assert_eq!(
        super::compile::exact_numeric_is_multiple(&Value::Int128(i128::MIN), &Value::Int128(-1)),
        Some(true),
    );
    assert_eq!(
        super::compile::exact_numeric_is_multiple(
            &Value::Float64(crate::types::Float64::try_new(10.0).expect("finite float")),
            &Value::Float64(crate::types::Float64::try_new(5.0).expect("finite float")),
        ),
        None,
    );
}

#[test]
fn exact_multiple_of_kind_set_excludes_only_numeric_floats() {
    for kind in [
        AcceptedFieldKind::Decimal { scale: 2 },
        AcceptedFieldKind::Int8,
        AcceptedFieldKind::Int16,
        AcceptedFieldKind::Int32,
        AcceptedFieldKind::Int64,
        AcceptedFieldKind::Int128,
        AcceptedFieldKind::IntBig { max_bytes: 32 },
        AcceptedFieldKind::Nat8,
        AcceptedFieldKind::Nat16,
        AcceptedFieldKind::Nat32,
        AcceptedFieldKind::Nat64,
        AcceptedFieldKind::Nat128,
        AcceptedFieldKind::NatBig { max_bytes: 32 },
    ] {
        assert!(crate::db::schema::accepted_rule_exact_numeric_kind_is_supported(&kind));
    }
    for kind in [AcceptedFieldKind::Float32, AcceptedFieldKind::Float64] {
        assert!(!crate::db::schema::accepted_rule_exact_numeric_kind_is_supported(&kind));
    }
}

fn field(
    id: u32,
    slot: u16,
    name: &str,
    kind: AcceptedFieldKind,
    nullable: bool,
    leaf_codec: LeafCodec,
) -> PersistedFieldSnapshot {
    PersistedFieldSnapshot::new_initial(
        FieldId::new(id),
        name.to_string(),
        SchemaFieldSlot::new(slot),
        kind,
        Vec::new(),
        nullable,
        SchemaInsertDefault::None,
        if matches!(leaf_codec, LeafCodec::Structural) {
            FieldStorageDecode::CatalogValue
        } else {
            FieldStorageDecode::ByKind
        },
        leaf_codec,
    )
}

fn snapshot() -> PersistedSchemaSnapshot {
    let fields = vec![
        field(
            1,
            0,
            "id",
            AcceptedFieldKind::Ulid,
            false,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        ),
        field(
            2,
            1,
            "score",
            AcceptedFieldKind::Int64,
            false,
            LeafCodec::Scalar(ScalarCodec::Int64),
        ),
        field(
            3,
            2,
            "nickname",
            AcceptedFieldKind::Text { max_len: Some(64) },
            true,
            LeafCodec::Scalar(ScalarCodec::Text),
        ),
        field(
            4,
            3,
            "tags",
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Text { max_len: Some(16) })),
            false,
            LeafCodec::Structural,
        ),
        field(
            5,
            4,
            "payload",
            AcceptedFieldKind::Blob { max_len: Some(64) },
            false,
            LeafCodec::Scalar(ScalarCodec::Blob),
        ),
    ];
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::CheckedEntity".to_string(),
        "CheckedEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    )
}

fn value_catalog() -> AcceptedValueCatalogHandle {
    let enum_catalog = empty_accepted_enum_catalog_for_tests();
    AcceptedValueCatalogHandle::new_for_tests(
        enum_catalog,
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    )
}

fn accepted_with_check(
    input: CheckExprV1Input,
) -> (
    AcceptedSchemaSnapshot,
    AcceptedValueCatalogHandle,
    ConstraintId,
) {
    let snapshot = snapshot();
    let value_catalog = value_catalog();
    let expression = bind_check_expr_v1(
        input,
        &snapshot,
        value_catalog.enum_catalog(),
        value_catalog.composite_catalog(),
    )
    .expect("test check expression should bind");
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check(
            "score_policy".to_string(),
            ConstraintOrigin::Generated,
            expression,
        )
        .expect("test check constraint should allocate");
    let constraint_id = catalog
        .constraints()
        .iter()
        .find_map(|constraint| {
            matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
                .then_some(constraint.id())
        })
        .expect("test check constraint should be present");
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(catalog))
        .expect("test check schema should be accepted");

    (accepted, value_catalog, constraint_id)
}

fn values(score: i64, nickname: Value, tags: Vec<Value>) -> Vec<Option<Value>> {
    vec![
        Some(Value::Ulid(crate::types::Ulid::from_u128(1))),
        Some(Value::Int64(score)),
        Some(nickname),
        Some(Value::List(tags)),
        Some(Value::Blob(Vec::new())),
    ]
}

struct TargetedRuleFixture {
    accepted: AcceptedSchemaSnapshot,
    value_catalog: AcceptedValueCatalogHandle,
    program: CompiledAcceptedRowConstraints,
    values: Vec<Option<Value>>,
    degree_type: CompositeTypeId,
    node_type: CompositeTypeId,
    pair_type: CompositeTypeId,
    label_type: CompositeTypeId,
    bag_type: CompositeTypeId,
    blob_type: CompositeTypeId,
    wrapper_type: CompositeTypeId,
    choice_type: crate::db::schema::enum_catalog::EnumTypeId,
    choice_payload_variant: crate::db::schema::enum_catalog::EnumVariantId,
}

impl TargetedRuleFixture {
    fn node(degree: u64, next: Vec<Value>) -> Value {
        Value::Map(vec![
            (Value::Text("a_next".to_string()), Value::List(next)),
            (Value::Text("z_degree".to_string()), Value::Nat64(degree)),
        ])
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "one accepted fixture keeps every structural edge and its stable identities together"
)]
fn targeted_rule_fixture() -> TargetedRuleFixture {
    let degree_type = CompositeTypeId::new(1).expect("degree type ID should be non-zero");
    let node_type = CompositeTypeId::new(2).expect("node type ID should be non-zero");
    let pair_type = CompositeTypeId::new(3).expect("pair type ID should be non-zero");
    let label_type = CompositeTypeId::new(4).expect("label type ID should be non-zero");
    let bag_type = CompositeTypeId::new(5).expect("bag type ID should be non-zero");
    let blob_type = CompositeTypeId::new(6).expect("blob type ID should be non-zero");
    let wrapper_type = CompositeTypeId::new(7).expect("wrapper type ID should be non-zero");
    let enum_catalog = build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
        "tests::Choice",
        vec![
            TestEnumVariant::unit("None"),
            TestEnumVariant::payload(
                "Some",
                AcceptedFieldKind::Composite {
                    type_id: degree_type,
                },
                FieldStorageDecode::CatalogValue,
            ),
        ],
    )])
    .expect("enum fixture should build");
    let choice_type = enum_catalog
        .type_id("tests::Choice")
        .expect("choice type should bind");
    let choice_payload_variant = enum_catalog
        .enum_type(choice_type)
        .and_then(|definition| definition.variant_id("Some"))
        .expect("payload variant should bind");
    let composite_catalog = AcceptedCompositeCatalog::from_initial_definitions(
        BTreeMap::from([
            (
                degree_type,
                (
                    "tests::Degrees".to_string(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        AcceptedFieldKind::Nat8,
                        false,
                    )),
                ),
            ),
            (
                node_type,
                (
                    "tests::Node".to_string(),
                    AcceptedCompositeShape::Record(vec![
                        AcceptedCompositeField::new(
                            CompositeFieldId::new(20).expect("member ID should be non-zero"),
                            "a_next".to_string(),
                            AcceptedCompositeElement::new(
                                AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Composite {
                                    type_id: node_type,
                                })),
                                false,
                            ),
                        ),
                        AcceptedCompositeField::new(
                            CompositeFieldId::new(10).expect("member ID should be non-zero"),
                            "z_degree".to_string(),
                            AcceptedCompositeElement::new(
                                AcceptedFieldKind::Composite {
                                    type_id: degree_type,
                                },
                                false,
                            ),
                        ),
                    ]),
                ),
            ),
            (
                pair_type,
                (
                    "tests::Pair".to_string(),
                    AcceptedCompositeShape::Tuple(vec![
                        AcceptedCompositeElement::new(
                            AcceptedFieldKind::Composite {
                                type_id: degree_type,
                            },
                            false,
                        ),
                        AcceptedCompositeElement::new(
                            AcceptedFieldKind::Composite {
                                type_id: degree_type,
                            },
                            true,
                        ),
                    ]),
                ),
            ),
            (
                label_type,
                (
                    "tests::Label".to_string(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        AcceptedFieldKind::Text { max_len: None },
                        false,
                    )),
                ),
            ),
            (
                bag_type,
                (
                    "tests::Bag".to_string(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat8)),
                        false,
                    )),
                ),
            ),
            (
                blob_type,
                (
                    "tests::Payload".to_string(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        AcceptedFieldKind::Blob { max_len: None },
                        false,
                    )),
                ),
            ),
            (
                wrapper_type,
                (
                    "tests::DegreeWrapper".to_string(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        AcceptedFieldKind::Composite {
                            type_id: degree_type,
                        },
                        false,
                    )),
                ),
            ),
        ]),
        &enum_catalog,
    )
    .expect("composite fixture should build");
    let fields = vec![
        field(
            1,
            0,
            "id",
            AcceptedFieldKind::Ulid,
            false,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        ),
        field(
            2,
            1,
            "node",
            AcceptedFieldKind::Composite { type_id: node_type },
            false,
            LeafCodec::Structural,
        ),
        field(
            3,
            2,
            "pair",
            AcceptedFieldKind::Composite { type_id: pair_type },
            false,
            LeafCodec::Structural,
        ),
        field(
            4,
            3,
            "choice",
            AcceptedFieldKind::Enum {
                type_id: choice_type,
            },
            false,
            LeafCodec::Structural,
        ),
        field(
            5,
            4,
            "degrees",
            AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Composite {
                type_id: degree_type,
            })),
            false,
            LeafCodec::Structural,
        ),
        field(
            6,
            5,
            "degree_set",
            AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Composite {
                type_id: degree_type,
            })),
            false,
            LeafCodec::Structural,
        ),
        field(
            7,
            6,
            "degree_map",
            AcceptedFieldKind::Map {
                key: Box::new(AcceptedFieldKind::Composite {
                    type_id: degree_type,
                }),
                value: Box::new(AcceptedFieldKind::Composite {
                    type_id: degree_type,
                }),
            },
            false,
            LeafCodec::Structural,
        ),
        field(
            8,
            7,
            "label",
            AcceptedFieldKind::Composite {
                type_id: label_type,
            },
            true,
            LeafCodec::Structural,
        ),
        field(
            9,
            8,
            "bag",
            AcceptedFieldKind::Composite { type_id: bag_type },
            false,
            LeafCodec::Structural,
        ),
        field(
            10,
            9,
            "payload",
            AcceptedFieldKind::Composite { type_id: blob_type },
            false,
            LeafCodec::Structural,
        ),
        field(
            11,
            10,
            "degree_wrapper",
            AcceptedFieldKind::Composite {
                type_id: wrapper_type,
            },
            false,
            LeafCodec::Structural,
        ),
    ];
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::Targeted".to_string(),
        "Targeted".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    let bindings = AcceptedSourceBindingCatalog::initial(
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    );
    let numeric_literal = |value| {
        bind_source_rule_literal(
            &ScalarLiteral::Nat(value),
            AcceptedFieldKind::Nat8,
            &bindings,
            &enum_catalog,
            &composite_catalog,
        )
        .expect("numeric targeted-rule literal should bind")
    };
    let numeric_operation = || AcceptedRuleOperation::NumericRangeInclusive {
        min: numeric_literal(0),
        max: numeric_literal(10),
    };
    let mut catalog = snapshot.constraint_catalog().clone();
    for root_field_id in 2..=7 {
        catalog = catalog
            .with_added_targeted_rule(
                format!("degree_rule_{root_field_id}"),
                ConstraintOrigin::Generated,
                AcceptedRuleTarget::new(
                    FieldId::new(root_field_id),
                    AcceptedNamedTypeIdentity::Composite(degree_type),
                ),
                numeric_operation(),
            )
            .expect("numeric targeted rule should allocate");
    }
    catalog = catalog
        .with_added_targeted_rule(
            "wrapped_degree_rule".to_string(),
            ConstraintOrigin::Generated,
            AcceptedRuleTarget::new(
                FieldId::new(11),
                AcceptedNamedTypeIdentity::Composite(degree_type),
            ),
            AcceptedRuleOperation::NumericMaximumInclusive {
                value: numeric_literal(10),
            },
        )
        .expect("wrapped numeric maximum should allocate");
    catalog = catalog
        .with_added_targeted_rule(
            "wrapped_degree_minimum".to_string(),
            ConstraintOrigin::Generated,
            AcceptedRuleTarget::new(
                FieldId::new(11),
                AcceptedNamedTypeIdentity::Composite(degree_type),
            ),
            AcceptedRuleOperation::NumericMinimumInclusive {
                value: numeric_literal(3),
            },
        )
        .expect("wrapped numeric minimum should allocate");
    catalog = catalog
        .with_added_targeted_rule(
            "wrapped_degree_multiple".to_string(),
            ConstraintOrigin::Generated,
            AcceptedRuleTarget::new(
                FieldId::new(11),
                AcceptedNamedTypeIdentity::Composite(degree_type),
            ),
            AcceptedRuleOperation::MultipleOf {
                divisor: numeric_literal(5),
            },
        )
        .expect("wrapped exact multiple should allocate");
    for (root_field_id, target_type, operation, name) in [
        (
            8,
            label_type,
            AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 3 },
            "label_length",
        ),
        (
            9,
            bag_type,
            AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 2 },
            "bag_length",
        ),
        (
            10,
            blob_type,
            AcceptedRuleOperation::LengthRangeInclusive { min: 1, max: 2 },
            "payload_length",
        ),
    ] {
        catalog = catalog
            .with_added_targeted_rule(
                name.to_string(),
                ConstraintOrigin::Generated,
                AcceptedRuleTarget::new(
                    FieldId::new(root_field_id),
                    AcceptedNamedTypeIdentity::Composite(target_type),
                ),
                operation,
            )
            .expect("length targeted rule should allocate");
    }
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(catalog))
        .expect("targeted schema should be accepted");
    let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
        enum_catalog,
        composite_catalog,
        AcceptedSchemaRevision::INITIAL,
    );
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &value_catalog, FINGERPRINT)
        .expect("targeted program should compile");
    let values = vec![
        Some(Value::Ulid(crate::types::Ulid::from_u128(1))),
        Some(Value::Map(vec![
            (Value::Text("a_next".to_string()), Value::List(Vec::new())),
            (Value::Text("z_degree".to_string()), Value::Nat64(5)),
        ])),
        Some(Value::List(vec![Value::Nat64(5), Value::Null])),
        Some(Value::Enum(crate::value::ValueEnum::test_payload(
            choice_type.get(),
            choice_payload_variant.get(),
            Value::Nat64(5),
        ))),
        Some(Value::List(vec![Value::Nat64(5)])),
        Some(Value::List(vec![Value::Nat64(5)])),
        Some(Value::Map(vec![(Value::Nat64(5), Value::Nat64(5))])),
        Some(Value::Text("é".to_string())),
        Some(Value::List(vec![Value::Nat64(1)])),
        Some(Value::Blob(vec![1, 2])),
        Some(Value::Nat64(5)),
    ];
    TargetedRuleFixture {
        accepted,
        value_catalog,
        program,
        values,
        degree_type,
        node_type,
        pair_type,
        label_type,
        bag_type,
        blob_type,
        wrapper_type,
        choice_type,
        choice_payload_variant,
    }
}

fn targeted_path(error: AcceptedRowConstraintEvaluationError) -> Vec<AcceptedTargetPathComponent> {
    let AcceptedRowConstraintEvaluationError::TargetedRuleViolation { path, .. } = error else {
        panic!("expected one targeted-rule violation");
    };
    path.components().to_vec()
}

#[test]
fn integrity_uses_the_same_targeted_artifact_and_first_occurrence() {
    let fixture = targeted_rule_fixture();
    let mut values = fixture.values.clone();
    values[10] = Some(Value::Nat64(11));
    let mutation_path = targeted_path(
        fixture
            .program
            .evaluate(FINGERPRINT, &values)
            .expect_err("mutation evaluation should reject the nested value"),
    );
    let integrity_violations = (0..fixture.program.integrity_constraint_count())
        .filter_map(|ordinal| {
            fixture
                .program
                .evaluate_integrity_constraint(ordinal, FINGERPRINT, &values)
                .err()
        })
        .collect::<Vec<_>>();

    assert_eq!(fixture.program.integrity_constraint_count(), 11);
    assert_eq!(integrity_violations.len(), 1);
    assert_eq!(
        targeted_path(
            integrity_violations
                .into_iter()
                .next()
                .expect("one targeted integrity violation should remain"),
        ),
        mutation_path,
    );
}

#[test]
fn pending_targeted_activation_uses_the_same_compiled_artifact_as_write_admission() {
    let fixture = targeted_rule_fixture();
    let snapshot = fixture.accepted.persisted_snapshot();
    let pending = snapshot
        .constraints()
        .iter()
        .find(|constraint| constraint.name() == "wrapped_degree_rule")
        .cloned()
        .expect("targeted constraint should exist");
    let AcceptedConstraintKind::TargetedRule { target, operation } = pending.kind().clone() else {
        panic!("selected constraint should remain targeted");
    };
    let activation = ConstraintActivationSnapshot::new(
        pending.id(),
        pending.name().to_string(),
        pending.origin(),
        ConstraintActivationKind::TargetedRule { target, operation },
        ConstraintActivationState::Validating,
        AcceptedSchemaFingerprint::new([0xC7; 32]),
        15,
    );
    let catalog = AcceptedConstraintCatalog::from_persisted_parts(
        snapshot.constraint_id_allocator(),
        snapshot
            .constraints()
            .iter()
            .filter(|constraint| constraint.id() != pending.id())
            .cloned()
            .collect(),
        vec![activation],
    );
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.clone().with_constraint_catalog(catalog))
            .expect("targeted activation should remain structurally accepted");
    let activation_program = CompiledAcceptedRowConstraints::compile_targeted_rule_activation(
        &accepted,
        &fixture.value_catalog,
        FINGERPRINT,
        pending.id(),
    )
    .expect("targeted activation should compile");
    let write_program =
        CompiledAcceptedRowConstraints::compile(&accepted, &fixture.value_catalog, FINGERPRINT)
            .expect("pending targeted write gate should compile");
    let mut values = fixture.values.clone();
    values[10] = Some(Value::Nat64(11));

    assert_eq!(
        targeted_path(
            activation_program
                .evaluate(FINGERPRINT, &values)
                .expect_err("historical activation should reject the nested value"),
        ),
        targeted_path(
            write_program
                .evaluate(FINGERPRINT, &values)
                .expect_err("pending new-write gate should reject the same value"),
        ),
    );
}

#[test]
fn targeted_rules_walk_every_structural_edge_with_typed_deterministic_paths() {
    let fixture = targeted_rule_fixture();

    let mut values = fixture.values.clone();
    values[10] = Some(Value::Nat64(11));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &values)
                .expect_err("nested newtype target should violate"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(11)),
            AcceptedTargetPathComponent::Newtype {
                composite_type_id: fixture.wrapper_type,
            },
        ],
    );

    let mut values = fixture.values.clone();
    values[2] = Some(Value::List(vec![Value::Nat64(11), Value::Null]));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &values)
                .expect_err("tuple target should violate"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(3)),
            AcceptedTargetPathComponent::TupleElement {
                composite_type_id: fixture.pair_type,
                ordinal: 0,
            },
        ],
    );

    let mut values = fixture.values.clone();
    values[3] = Some(Value::Enum(crate::value::ValueEnum::test_payload(
        fixture.choice_type.get(),
        fixture.choice_payload_variant.get(),
        Value::Nat64(11),
    )));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &values)
                .expect_err("enum payload target should violate"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(4)),
            AcceptedTargetPathComponent::EnumVariant {
                enum_type_id: fixture.choice_type,
                variant_id: fixture.choice_payload_variant,
            },
        ],
    );

    let mut values = fixture.values.clone();
    values[4] = Some(Value::List(vec![Value::Nat64(11)]));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &values)
                .expect_err("list target should violate"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(5)),
            AcceptedTargetPathComponent::ListElement { index: 0 },
        ],
    );

    let mut values = fixture.values.clone();
    values[5] = Some(Value::List(vec![Value::Nat64(11)]));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &values)
                .expect_err("set target should violate"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(6)),
            AcceptedTargetPathComponent::SetElement { index: 0 },
        ],
    );

    let mut values = fixture.values.clone();
    values[6] = Some(Value::Map(vec![(Value::Nat64(11), Value::Nat64(12))]));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &values)
                .expect_err("map key should be visited before its value"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(7)),
            AcceptedTargetPathComponent::MapEntryKey { index: 0 },
        ],
    );
}

#[test]
fn targeted_rules_terminate_on_finite_values_of_a_cyclic_schema_graph() {
    let fixture = targeted_rule_fixture();
    let mut identity_order = fixture.values.clone();
    identity_order[1] = Some(TargetedRuleFixture::node(
        11,
        vec![TargetedRuleFixture::node(12, Vec::new())],
    ));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &identity_order)
                .expect_err("record members should visit stable member identity order"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(2)),
            AcceptedTargetPathComponent::RecordMember {
                composite_type_id: fixture.node_type,
                member_id: CompositeFieldId::new(10).expect("member ID should be non-zero"),
            },
        ],
    );

    let mut values = fixture.values.clone();
    let nested = TargetedRuleFixture::node(11, Vec::new());
    values[1] = Some(TargetedRuleFixture::node(5, vec![nested]));

    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &values)
                .expect_err("nested recursive occurrence should violate"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(2)),
            AcceptedTargetPathComponent::RecordMember {
                composite_type_id: fixture.node_type,
                member_id: CompositeFieldId::new(20).expect("member ID should be non-zero"),
            },
            AcceptedTargetPathComponent::ListElement { index: 0 },
            AcceptedTargetPathComponent::RecordMember {
                composite_type_id: fixture.node_type,
                member_id: CompositeFieldId::new(10).expect("member ID should be non-zero"),
            },
        ],
    );
}

#[test]
fn targeted_rules_share_exact_numeric_and_length_semantics_and_skip_null() {
    let fixture = targeted_rule_fixture();
    fixture
        .program
        .evaluate(FINGERPRINT, &fixture.values)
        .expect("compliant targeted values should pass mutation admission");

    let mut unicode = fixture.values.clone();
    unicode[7] = Some(Value::Text("éééé".to_string()));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &unicode)
                .expect_err("text length should use Unicode scalar count"),
        ),
        vec![AcceptedTargetPathComponent::RootField(FieldId::new(8))],
    );

    let mut null = fixture.values.clone();
    null[7] = Some(Value::Null);
    fixture
        .program
        .evaluate(FINGERPRINT, &null)
        .expect("nullable targeted values should pass vacuously");

    let mut minimum = fixture.values.clone();
    minimum[10] = Some(Value::Nat64(2));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &minimum)
                .expect_err("inclusive numeric minimum should reject a smaller value"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(11)),
            AcceptedTargetPathComponent::Newtype {
                composite_type_id: fixture.wrapper_type,
            },
        ],
    );

    let mut maximum = fixture.values.clone();
    maximum[10] = Some(Value::Nat64(11));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &maximum)
                .expect_err("inclusive numeric maximum should reject a larger value"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(11)),
            AcceptedTargetPathComponent::Newtype {
                composite_type_id: fixture.wrapper_type,
            },
        ],
    );

    let mut multiple = fixture.values.clone();
    multiple[10] = Some(Value::Nat64(7));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &multiple)
                .expect_err("exact multiple-of should reject a remainder"),
        ),
        vec![
            AcceptedTargetPathComponent::RootField(FieldId::new(11)),
            AcceptedTargetPathComponent::Newtype {
                composite_type_id: fixture.wrapper_type,
            },
        ],
    );

    let mut cardinality = fixture.values.clone();
    cardinality[8] = Some(Value::List(Vec::new()));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &cardinality)
                .expect_err("empty nominal collection should violate its own length minimum"),
        ),
        vec![AcceptedTargetPathComponent::RootField(FieldId::new(9))],
    );

    let mut octets = fixture.values.clone();
    octets[9] = Some(Value::Blob(vec![1, 2, 3]));
    assert_eq!(
        targeted_path(
            fixture
                .program
                .evaluate(FINGERPRINT, &octets)
                .expect_err("blob length should use octets"),
        ),
        vec![AcceptedTargetPathComponent::RootField(FieldId::new(10))],
    );

    assert_ne!(fixture.degree_type, fixture.label_type);
    assert_ne!(fixture.bag_type, fixture.blob_type);
}

#[test]
fn targeted_rules_share_stable_constraint_id_order() {
    let fixture = targeted_rule_fixture();
    let mut earlier_not_null = fixture.values.clone();
    earlier_not_null[1] = Some(TargetedRuleFixture::node(12, Vec::new()));
    earlier_not_null[8] = Some(Value::Null);
    let bag_not_null = fixture
        .accepted
        .persisted_snapshot()
        .constraints()
        .iter()
        .find(|constraint| {
            matches!(
                constraint.kind(),
                AcceptedConstraintKind::NotNull { field_id }
                    if *field_id == FieldId::new(9)
            )
        })
        .expect("bag not-null identity should exist");
    assert_eq!(
        fixture.program.evaluate(FINGERPRINT, &earlier_not_null),
        Err(AcceptedRowConstraintEvaluationError::Violation {
            constraint_id: bag_not_null.id(),
            constraint_name: bag_not_null.name().to_string(),
            kind: AcceptedRowConstraintViolationKind::NotNull,
            field_paths: vec!["bag".to_string()],
        }),
        "an earlier accepted constraint ID must win across families",
    );

    let late_catalog = fixture
        .accepted
        .persisted_snapshot()
        .constraint_catalog()
        .clone()
        .with_added_check(
            "late_false".to_string(),
            ConstraintOrigin::Generated,
            AcceptedCheckExprV1::False,
        )
        .expect("late check should allocate");
    let late_check_id = late_catalog
        .constraints()
        .last()
        .expect("late check should be retained")
        .id();
    let late_schema = AcceptedSchemaSnapshot::try_new(
        fixture
            .accepted
            .persisted_snapshot()
            .clone()
            .with_constraint_catalog(late_catalog),
    )
    .expect("late-check schema should close");
    let late_program =
        CompiledAcceptedRowConstraints::compile(&late_schema, &fixture.value_catalog, FINGERPRINT)
            .expect("late-check program should compile");
    let mut targeted_first = fixture.values.clone();
    targeted_first[1] = Some(TargetedRuleFixture::node(12, Vec::new()));
    let error = late_program
        .evaluate(FINGERPRINT, &targeted_first)
        .expect_err("the earlier targeted rule should reject before the late check");
    let AcceptedRowConstraintEvaluationError::TargetedRuleViolation {
        constraint_id,
        constraint_name,
        field_path,
        path,
    } = error
    else {
        panic!("expected the targeted rule to win stable accepted-ID ordering");
    };
    assert!(constraint_id < late_check_id);
    assert_eq!(constraint_name, "degree_rule_2");
    assert_eq!(field_path, "node");
    assert_eq!(
        path.components(),
        &[
            AcceptedTargetPathComponent::RootField(FieldId::new(2)),
            AcceptedTargetPathComponent::RecordMember {
                composite_type_id: fixture.node_type,
                member_id: CompositeFieldId::new(10).expect("member ID should be non-zero"),
            },
        ],
    );
}

#[test]
fn targeted_rule_write_diagnostic_preserves_the_typed_occurrence_path() {
    let fixture = targeted_rule_fixture();
    let mut values = fixture.values.clone();
    values[1] = Some(TargetedRuleFixture::node(12, Vec::new()));
    let evaluation_error = fixture
        .program
        .evaluate(FINGERPRINT, &values)
        .expect_err("invalid targeted value should reject");
    let write_error =
        accepted_row_constraint_write_error("tests::Targeted", Some(vec![4, 2]), evaluation_error);
    let diagnostic = write_error
        .constraint_diagnostic()
        .expect("targeted write failure should retain one public diagnostic");
    assert_eq!(
        diagnostic.constraint_kind(),
        ConstraintDiagnosticKind::TargetedRule
    );
    assert_eq!(diagnostic.field_paths(), &["node".to_string()]);
    assert_eq!(diagnostic.primary_key(), Some([4, 2].as_slice()));
    assert_eq!(
        diagnostic
            .value_path()
            .expect("targeted write diagnostic should retain its typed path")
            .components(),
        &[
            ConstraintValuePathComponent::RootField { field_id: 2 },
            ConstraintValuePathComponent::RecordMember {
                composite_type_id: fixture.node_type.get(),
                member_id: 10,
            },
        ],
    );
}

#[test]
fn targeted_rule_evaluation_reports_each_resource_and_shape_boundary() {
    let fixture = targeted_rule_fixture();
    let limits = |depth, nodes, operations, path| {
        TargetedEvaluationLimits::for_tests(depth, nodes, operations, path)
    };
    assert_eq!(
        fixture.program.evaluate_targeted_rules_with_limits(
            FINGERPRINT,
            &fixture.values,
            limits(64, 0, 64, 64),
        ),
        Err(AcceptedRowConstraintEvaluationError::ValueNodeBudgetExceeded),
    );
    assert_eq!(
        fixture.program.evaluate_targeted_rules_with_limits(
            FINGERPRINT,
            &fixture.values,
            limits(1, 64, 64, 64),
        ),
        Err(AcceptedRowConstraintEvaluationError::ValueDepthExceeded),
    );
    assert_eq!(
        fixture.program.evaluate_targeted_rules_with_limits(
            FINGERPRINT,
            &fixture.values,
            limits(64, 64, 0, 64),
        ),
        Err(AcceptedRowConstraintEvaluationError::OperationBudgetExceeded),
    );
    assert_eq!(
        fixture.program.evaluate_targeted_rules_with_limits(
            FINGERPRINT,
            &fixture.values,
            limits(64, 64, 64, 1),
        ),
        Err(AcceptedRowConstraintEvaluationError::PathBudgetExceeded),
    );

    let mut malformed = fixture.values.clone();
    malformed[1] = Some(Value::List(Vec::new()));
    assert_eq!(
        fixture.program.evaluate_targeted_rules_with_limits(
            FINGERPRINT,
            &malformed,
            limits(64, 64, 64, 64),
        ),
        Err(AcceptedRowConstraintEvaluationError::RuntimeValueMismatch),
    );
}

#[test]
fn binder_lowers_ranges_and_canonicalizes_commutative_children() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let score_nonnegative = CheckExprV1Input::Compare {
        left: CheckValueExprV1Input::Field("score".to_string()),
        op: AcceptedCheckCompareOpV1::Gte,
        right: CheckValueExprV1Input::Literal(InputValue::Int64(0)),
    };
    let expression = bind_check_expr_v1(
        CheckExprV1Input::And(vec![
            CheckExprV1Input::Between {
                value: CheckValueExprV1Input::Field("score".to_string()),
                lower: InputValue::Int64(0),
                upper: InputValue::Int64(100),
            },
            score_nonnegative.clone(),
            score_nonnegative,
        ]),
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .expect("bounded range should bind");

    assert_eq!(expression.dependencies(), vec![FieldId::new(2)]);
    let AcceptedCheckExprV1::And(children) = expression else {
        panic!("range and duplicate comparison should canonicalize to one AND");
    };
    assert_eq!(children.len(), 2);
    assert!(children[0].canonical_key() < children[1].canonical_key());
}

#[test]
fn binder_covers_boolean_null_and_octet_length_nodes() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let expression = bind_check_expr_v1(
        CheckExprV1Input::And(vec![
            CheckExprV1Input::Or(vec![CheckExprV1Input::True, CheckExprV1Input::False]),
            CheckExprV1Input::Not(Box::new(CheckExprV1Input::False)),
            CheckExprV1Input::IsNull(CheckValueExprV1Input::Field("nickname".to_string())),
            CheckExprV1Input::IsNotNull(CheckValueExprV1Input::Field("score".to_string())),
            CheckExprV1Input::Compare {
                left: CheckValueExprV1Input::OctetLength("payload".to_string()),
                op: AcceptedCheckCompareOpV1::Lte,
                right: CheckValueExprV1Input::Literal(InputValue::Nat64(64)),
            },
        ]),
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .expect("supported boolean and length nodes should bind");

    assert_eq!(
        expression.dependencies(),
        vec![FieldId::new(2), FieldId::new(3), FieldId::new(5)]
    );
}

#[test]
fn accepted_check_renderer_uses_current_field_names_and_canonical_literals() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let numeric = bind_check_expr_v1(
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field("score".to_string()),
            op: AcceptedCheckCompareOpV1::Gte,
            right: CheckValueExprV1Input::Literal(InputValue::Int64(0)),
        },
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .expect("numeric check should bind");
    let text = bind_check_expr_v1(
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field("nickname".to_string()),
            op: AcceptedCheckCompareOpV1::Ne,
            right: CheckValueExprV1Input::Literal(InputValue::Text("O'Reilly".to_string())),
        },
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .expect("text check should bind");

    assert_eq!(
        render_accepted_check_expr_sql(&numeric, &snapshot, &catalog)
            .expect("accepted numeric check should render"),
        "score >= 0",
    );
    assert_eq!(
        render_accepted_check_expr_sql(&text, &snapshot, &catalog)
            .expect("accepted text check should render"),
        "nickname != 'O''Reilly'",
    );
}

#[test]
fn compiled_checks_apply_sql_three_valued_semantics_and_stable_violation_identity() {
    let (accepted, catalog, constraint_id) = accepted_with_check(CheckExprV1Input::And(vec![
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field("score".to_string()),
            op: AcceptedCheckCompareOpV1::Gte,
            right: CheckValueExprV1Input::Literal(InputValue::Int64(0)),
        },
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field("nickname".to_string()),
            op: AcceptedCheckCompareOpV1::Ne,
            right: CheckValueExprV1Input::Literal(InputValue::Text("blocked".to_string())),
        },
    ]));
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &catalog, FINGERPRINT)
        .expect("accepted checks should compile");

    program
        .evaluate(FINGERPRINT, &values(1, Value::Null, Vec::new()))
        .expect("UNKNOWN nickname comparison should satisfy CHECK");
    let error = program
        .evaluate(
            FINGERPRINT,
            &values(-1, Value::Text("allowed".to_string()), Vec::new()),
        )
        .expect_err("false score comparison should reject");
    assert_eq!(
        error,
        AcceptedRowConstraintEvaluationError::Violation {
            constraint_id,
            constraint_name: "score_policy".to_string(),
            kind: AcceptedRowConstraintViolationKind::Check,
            field_paths: vec!["score".to_string(), "nickname".to_string()],
        }
    );
}

#[test]
fn compiled_checks_include_pending_check_activation_gates() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let expression = bind_check_expr_v1(
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field("score".to_string()),
            op: AcceptedCheckCompareOpV1::Gte,
            right: CheckValueExprV1Input::Literal(InputValue::Int64(0)),
        },
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .expect("activation expression should bind");
    let constraint_catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check_activation(
            "pending_score_policy".to_string(),
            ConstraintOrigin::Generated,
            expression,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            2,
        )
        .expect("activation should reserve identity");
    let activation_id = constraint_catalog.activations()[0].id();
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraint_catalog))
            .expect("activation snapshot should close");
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &catalog, FINGERPRINT)
        .expect("pending gate should compile");

    assert_eq!(
        program.evaluate(FINGERPRINT, &values(-1, Value::Null, Vec::new()),),
        Err(AcceptedRowConstraintEvaluationError::Violation {
            constraint_id: activation_id,
            constraint_name: "pending_score_policy".to_string(),
            kind: AcceptedRowConstraintViolationKind::Check,
            field_paths: vec!["score".to_string()],
        }),
    );
}

#[test]
fn integrity_check_program_excludes_pending_activation_semantics() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let expression = bind_check_expr_v1(
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field("score".to_string()),
            op: AcceptedCheckCompareOpV1::Gte,
            right: CheckValueExprV1Input::Literal(InputValue::Int64(0)),
        },
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .expect("activation expression should bind");
    let constraint_catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check_activation(
            "pending_score_policy".to_string(),
            ConstraintOrigin::Generated,
            expression,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            2,
        )
        .expect("activation should reserve identity");
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraint_catalog))
            .expect("activation snapshot should close");
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &catalog, FINGERPRINT)
        .expect("shared write/integrity program should compile");

    assert_eq!(program.integrity_constraint_count(), 0);
}

#[test]
fn integrity_check_program_evaluates_each_validated_check_by_stable_ordinal() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let score = bind_check_expr_v1(
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field("score".to_string()),
            op: AcceptedCheckCompareOpV1::Gte,
            right: CheckValueExprV1Input::Literal(InputValue::Int64(0)),
        },
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .expect("score check should bind");
    let nickname = bind_check_expr_v1(
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Field("nickname".to_string()),
            op: AcceptedCheckCompareOpV1::Ne,
            right: CheckValueExprV1Input::Literal(InputValue::Text("blocked".to_string())),
        },
        &snapshot,
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .expect("nickname check should bind");
    let constraint_catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check(
            "score_policy".to_string(),
            ConstraintOrigin::Generated,
            score,
        )
        .expect("score check should allocate")
        .with_added_check(
            "nickname_policy".to_string(),
            ConstraintOrigin::Generated,
            nickname,
        )
        .expect("nickname check should allocate");
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraint_catalog))
            .expect("validated checks should close");
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &catalog, FINGERPRINT)
        .expect("shared write/integrity program should compile");
    let row = values(-1, Value::Text("blocked".to_string()), Vec::new());

    assert_eq!(program.integrity_constraint_count(), 2);
    for ordinal in 0..program.integrity_constraint_count() {
        assert!(matches!(
            program.evaluate_integrity_constraint(ordinal, FINGERPRINT, row.as_slice()),
            Err(AcceptedRowConstraintEvaluationError::Violation { .. }),
        ));
    }
}

#[test]
fn compiled_row_constraints_include_pending_not_null_activation_gates() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    let nullable_field = snapshot
        .fields()
        .iter()
        .find(|field| field.name() == "nickname")
        .expect("nullable test field should exist");
    let constraint_catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_not_null_activation(
            nullable_field,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
            3,
        )
        .expect("not-null activation should reserve identity");
    let activation_id = constraint_catalog.activations()[0].id();
    let activation_name = constraint_catalog.activations()[0].name().to_string();
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(constraint_catalog))
            .expect("not-null activation snapshot should close");
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &catalog, FINGERPRINT)
        .expect("pending not-null gate should compile");

    assert_eq!(program.required_slots(), &[0, 1, 2, 3, 4]);
    assert_eq!(
        program.evaluate(FINGERPRINT, &values(1, Value::Null, Vec::new())),
        Err(AcceptedRowConstraintEvaluationError::Violation {
            constraint_id: activation_id,
            constraint_name: activation_name,
            kind: AcceptedRowConstraintViolationKind::NotNull,
            field_paths: vec!["nickname".to_string()],
        }),
    );
    program
        .evaluate(
            FINGERPRINT,
            &values(1, Value::Text("Ada".to_string()), Vec::new()),
        )
        .expect("non-null final value should pass the pending gate");
}

#[test]
fn compiled_row_constraints_include_accepted_not_null_identity_before_encoding() {
    let snapshot = snapshot();
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot.clone())
        .expect("accepted not-null constraints should close");
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &value_catalog(), FINGERPRINT)
        .expect("accepted not-null constraints should compile");
    let score_constraint = snapshot
        .constraints()
        .iter()
        .find(|constraint| {
            matches!(
                constraint.kind(),
                AcceptedConstraintKind::NotNull { field_id }
                    if *field_id == FieldId::new(2)
            )
        })
        .expect("score not-null identity should exist");

    assert_eq!(
        program.evaluate_accepted_not_null_before_encoding(FINGERPRINT, 1),
        Err(AcceptedRowConstraintEvaluationError::Violation {
            constraint_id: score_constraint.id(),
            constraint_name: score_constraint.name().to_string(),
            kind: AcceptedRowConstraintViolationKind::NotNull,
            field_paths: vec!["score".to_string()],
        }),
    );
    program
        .evaluate(
            FINGERPRINT,
            &values(1, Value::Null, vec![Value::Text("tag".to_string())]),
        )
        .expect("non-null accepted fields and nullable nickname should pass");
}

#[test]
fn compiled_unique_activation_blocks_inserts_and_dependency_changes_only() {
    let snapshot = snapshot();
    let candidate = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).expect("test schema index identity should be non-zero"),
        1,
        "unique_score".to_string(),
        "tests::CheckedEntity::unique_score".to_string(),
        true,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            FieldId::new(2),
            SchemaFieldSlot::new(1),
            vec!["score".to_string()],
            AcceptedFieldKind::Int64,
            false,
        )]),
        None,
    )
    .clone_with_schema_identity(
        SchemaIndexId::new(1).expect("test schema index identity should be non-zero"),
        1,
        9,
    );
    let snapshot = snapshot
        .with_added_unique_activation(candidate, AcceptedSchemaFingerprint::new([0xA5; 32]), 9)
        .expect("unique activation should close");
    let activation_id = snapshot.constraint_activations()[0].id();
    let accepted =
        AcceptedSchemaSnapshot::try_new(snapshot).expect("unique activation snapshot should close");
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &value_catalog(), FINGERPRINT)
        .expect("unique write barrier should compile");
    let mut provenance = vec![Some(AcceptedFieldWriteProvenance::Preserved); 5];

    assert!(
        !program.is_empty(),
        "an activation gate makes the compiled constraint authority non-empty",
    );
    let insert_barrier = program
        .unique_activation_write_blocker(MutationMode::Insert, &provenance)
        .expect("insert barrier should evaluate")
        .expect("insert should be blocked");
    assert_eq!(insert_barrier.constraint_id(), activation_id);
    assert!(!insert_barrier.constraint_name().is_empty());
    assert_eq!(insert_barrier.field_paths(), &["score".to_string()]);
    assert!(
        program
            .unique_activation_write_blocker(MutationMode::Update, &provenance)
            .expect("unrelated update barrier should evaluate")
            .is_none(),
    );
    provenance[2] = Some(AcceptedFieldWriteProvenance::Authored);
    assert!(
        program
            .unique_activation_write_blocker(MutationMode::Update, &provenance)
            .expect("unrelated authored field should evaluate")
            .is_none(),
    );
    provenance[1] = Some(AcceptedFieldWriteProvenance::Authored);
    assert_eq!(
        program
            .unique_activation_write_blocker(MutationMode::Update, &provenance)
            .expect("dependency barrier should evaluate")
            .map(super::compile::CompiledUniqueWriteBarrier::constraint_id),
        Some(activation_id),
    );
}

#[test]
fn length_and_cardinality_use_one_prebound_slot_set() {
    let (accepted, catalog, _) = accepted_with_check(CheckExprV1Input::And(vec![
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::CharLength("nickname".to_string()),
            op: AcceptedCheckCompareOpV1::Lte,
            right: CheckValueExprV1Input::Literal(InputValue::Nat64(4)),
        },
        CheckExprV1Input::Compare {
            left: CheckValueExprV1Input::Cardinality("tags".to_string()),
            op: AcceptedCheckCompareOpV1::Lte,
            right: CheckValueExprV1Input::Literal(InputValue::Nat64(2)),
        },
    ]));
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &catalog, FINGERPRINT)
        .expect("accepted checks should compile");

    assert_eq!(program.required_slots(), &[0, 1, 2, 3, 4]);
    program
        .evaluate(
            FINGERPRINT,
            &values(
                10,
                Value::Text("éé".to_string()),
                vec![Value::Text("a".to_string()), Value::Text("b".to_string())],
            ),
        )
        .expect("valid length and cardinality should pass");
}

#[test]
fn compiled_checks_reject_stale_fingerprint_and_missing_required_slot() {
    let (accepted, catalog, _) = accepted_with_check(CheckExprV1Input::Compare {
        left: CheckValueExprV1Input::Field("score".to_string()),
        op: AcceptedCheckCompareOpV1::Gte,
        right: CheckValueExprV1Input::Literal(InputValue::Int64(0)),
    });
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &catalog, FINGERPRINT)
        .expect("accepted checks should compile");

    assert_eq!(
        program.evaluate([8; 16], &values(1, Value::Null, Vec::new())),
        Err(AcceptedRowConstraintEvaluationError::FingerprintMismatch)
    );
    assert_eq!(
        program.evaluate(FINGERPRINT, &[None]),
        Err(AcceptedRowConstraintEvaluationError::MissingSlot)
    );
}

#[test]
fn binder_rejects_empty_or_oversized_membership() {
    let snapshot = snapshot();
    let catalog = value_catalog();
    assert_eq!(
        bind_check_expr_v1(
            CheckExprV1Input::EnumIn {
                field: "score".to_string(),
                members: Vec::new(),
            },
            &snapshot,
            catalog.enum_catalog(),
            catalog.composite_catalog(),
        ),
        Err(AcceptedCheckExprV1Error::MembershipEmpty)
    );
    assert_eq!(
        bind_check_expr_v1(
            CheckExprV1Input::EnumIn {
                field: "score".to_string(),
                members: vec![InputValue::Int64(1); MAX_CHECK_EXPR_V1_MEMBERSHIP_ITEMS + 1],
            },
            &snapshot,
            catalog.enum_catalog(),
            catalog.composite_catalog(),
        ),
        Err(AcceptedCheckExprV1Error::MembershipTooWide)
    );
}

#[test]
fn local_validation_defers_composite_meaning_but_exact_validation_rejects_non_newtypes() {
    let mut fields = snapshot().fields().to_vec();
    let type_id = CompositeTypeId::new(1).expect("test composite type ID should be non-zero");
    fields.push(field(
        6,
        5,
        "details",
        AcceptedFieldKind::Composite { type_id },
        true,
        LeafCodec::Structural,
    ));
    let expression = AcceptedCheckExprV1::IsNull(AcceptedCheckValueExprV1::Field(FieldId::new(6)));

    assert!(
        expression
            .validate_snapshot_local(fields.as_slice())
            .is_ok()
    );
    let enum_catalog = empty_accepted_enum_catalog_for_tests();
    let composite_catalog = AcceptedCompositeCatalog::from_initial_definitions(
        BTreeMap::from([(
            type_id,
            (
                "tests::Details".to_string(),
                AcceptedCompositeShape::Tuple(vec![AcceptedCompositeElement::new(
                    AcceptedFieldKind::Nat64,
                    false,
                )]),
            ),
        )]),
        &enum_catalog,
    )
    .expect("tuple composite fixture should build");
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::CheckedEntity".to_string(),
        "CheckedEntity".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    assert_eq!(
        expression.validate(&snapshot, &composite_catalog),
        Err(AcceptedCheckExprV1Error::UnsupportedFieldKind)
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one compiled-program fixture proves numeric, decimal, and length nominal-newtype semantics together"
)]
fn accepted_checks_resolve_nominal_newtype_values_through_catalog_authority() {
    let type_id = CompositeTypeId::new(1).expect("test composite type ID should be non-zero");
    let label_type_id = CompositeTypeId::new(2).expect("test composite type ID should be non-zero");
    let amount_type_id =
        CompositeTypeId::new(3).expect("test composite type ID should be non-zero");
    let enum_catalog = empty_accepted_enum_catalog_for_tests();
    let composite_catalog = AcceptedCompositeCatalog::from_initial_definitions(
        BTreeMap::from([
            (
                type_id,
                (
                    "tests::Degrees".to_string(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        AcceptedFieldKind::Nat16,
                        false,
                    )),
                ),
            ),
            (
                label_type_id,
                (
                    "tests::Label".to_string(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        AcceptedFieldKind::Text { max_len: None },
                        false,
                    )),
                ),
            ),
            (
                amount_type_id,
                (
                    "tests::Amount".to_string(),
                    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(
                        AcceptedFieldKind::Decimal { scale: 8 },
                        false,
                    )),
                ),
            ),
        ]),
        &enum_catalog,
    )
    .expect("newtype composite fixture should build");
    let fields = vec![
        field(
            1,
            0,
            "id",
            AcceptedFieldKind::Ulid,
            false,
            LeafCodec::Scalar(ScalarCodec::Ulid),
        ),
        field(
            2,
            1,
            "degrees",
            AcceptedFieldKind::Composite { type_id },
            false,
            LeafCodec::Structural,
        ),
        field(
            3,
            2,
            "label",
            AcceptedFieldKind::Composite {
                type_id: label_type_id,
            },
            false,
            LeafCodec::Structural,
        ),
        field(
            4,
            3,
            "amount",
            AcceptedFieldKind::Composite {
                type_id: amount_type_id,
            },
            false,
            LeafCodec::Structural,
        ),
    ];
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::Compass".to_string(),
        "Compass".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    let expression = bind_check_expr_v1(
        CheckExprV1Input::And(vec![
            CheckExprV1Input::Compare {
                left: CheckValueExprV1Input::Field("degrees".to_string()),
                op: AcceptedCheckCompareOpV1::Lte,
                right: CheckValueExprV1Input::Literal(InputValue::Nat64(360)),
            },
            CheckExprV1Input::Compare {
                left: CheckValueExprV1Input::CharLength("label".to_string()),
                op: AcceptedCheckCompareOpV1::Gte,
                right: CheckValueExprV1Input::Literal(InputValue::Nat64(2)),
            },
            CheckExprV1Input::Compare {
                left: CheckValueExprV1Input::Field("amount".to_string()),
                op: AcceptedCheckCompareOpV1::Gte,
                right: CheckValueExprV1Input::Literal(InputValue::Decimal(
                    Decimal::from_i128_with_scale(0, 8),
                )),
            },
        ]),
        &snapshot,
        &enum_catalog,
        &composite_catalog,
    )
    .expect("newtype scalar check should bind through accepted catalog authority");
    let catalog = snapshot
        .constraint_catalog()
        .clone()
        .with_added_check(
            "degrees_range".to_string(),
            ConstraintOrigin::Generated,
            expression,
        )
        .expect("newtype check constraint should allocate");
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot.with_constraint_catalog(catalog))
        .expect("catalog-bound newtype check schema should be structurally accepted");
    let value_catalog = AcceptedValueCatalogHandle::new_for_tests(
        enum_catalog,
        composite_catalog,
        AcceptedSchemaRevision::INITIAL,
    );
    let program = CompiledAcceptedRowConstraints::compile(&accepted, &value_catalog, FINGERPRINT)
        .expect("newtype check should compile once through accepted authority");

    program
        .evaluate(
            FINGERPRINT,
            &[
                Some(Value::Ulid(crate::types::Ulid::from_u128(1))),
                Some(Value::Nat64(360)),
                Some(Value::Text("ok".to_string())),
                Some(Value::Decimal(Decimal::from_i128_with_scale(1, 8))),
            ],
        )
        .expect("inclusive newtype bound should pass");
    assert!(matches!(
        program.evaluate(
            FINGERPRINT,
            &[
                Some(Value::Ulid(crate::types::Ulid::from_u128(1))),
                Some(Value::Nat64(361)),
                Some(Value::Text("ok".to_string())),
                Some(Value::Decimal(Decimal::from_i128_with_scale(1, 8))),
            ],
        ),
        Err(AcceptedRowConstraintEvaluationError::Violation { .. })
    ));
    assert!(matches!(
        program.evaluate(
            FINGERPRINT,
            &[
                Some(Value::Ulid(crate::types::Ulid::from_u128(1))),
                Some(Value::Nat64(360)),
                Some(Value::Text("x".to_string())),
                Some(Value::Decimal(Decimal::from_i128_with_scale(1, 8))),
            ],
        ),
        Err(AcceptedRowConstraintEvaluationError::Violation { .. })
    ));
    assert!(matches!(
        program.evaluate(
            FINGERPRINT,
            &[
                Some(Value::Ulid(crate::types::Ulid::from_u128(1))),
                Some(Value::Nat64(360)),
                Some(Value::Text("ok".to_string())),
                Some(Value::Decimal(Decimal::from_i128_with_scale(-1, 8))),
            ],
        ),
        Err(AcceptedRowConstraintEvaluationError::Violation { .. })
    ));
}
