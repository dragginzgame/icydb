use super::*;

use crate::{
    db::{
        commit::CommitSchemaFingerprint,
        data::AcceptedFieldWriteProvenance,
        schema::{
            AcceptedCompositeCatalog, AcceptedConstraintKind, AcceptedSchemaFingerprint,
            AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle,
            ConstraintId, ConstraintOrigin, FieldId, PersistedFieldSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
            PersistedSchemaSnapshot, SchemaFieldSlot, SchemaIndexId, SchemaInsertDefault,
            SchemaRowLayout, SchemaVersion, composite_catalog::CompositeTypeId,
            enum_catalog::build_initial_accepted_enum_catalog,
        },
    },
    model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
    sanitize::SanitizeWriteMode,
    value::{InputValue, Value},
};

const FINGERPRINT: CommitSchemaFingerprint = [7; 16];

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
    let enum_catalog =
        build_initial_accepted_enum_catalog(&[]).expect("empty enum catalog should build");
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
        None,
        Some(Value::Int64(score)),
        Some(nickname),
        Some(Value::List(tags)),
    ]
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
    let program =
        CompiledAcceptedRowConstraints::compile_validated_checks(&accepted, &catalog, FINGERPRINT)
            .expect("validated-only integrity program should compile");

    assert!(program.is_empty());
    program
        .evaluate(FINGERPRINT, &values(-1, Value::Null, Vec::new()))
        .expect("pending activation violations are not accepted-state corruption");
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

    assert_eq!(program.required_slots(), &[2]);
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
        .unique_activation_write_blocker(SanitizeWriteMode::Insert, &provenance)
        .expect("insert barrier should evaluate")
        .expect("insert should be blocked");
    assert_eq!(insert_barrier.constraint_id(), activation_id);
    assert!(!insert_barrier.constraint_name().is_empty());
    assert_eq!(insert_barrier.field_paths(), &["score".to_string()]);
    assert!(
        program
            .unique_activation_write_blocker(SanitizeWriteMode::Update, &provenance)
            .expect("unrelated update barrier should evaluate")
            .is_none(),
    );
    provenance[2] = Some(AcceptedFieldWriteProvenance::Authored);
    assert!(
        program
            .unique_activation_write_blocker(SanitizeWriteMode::Update, &provenance)
            .expect("unrelated authored field should evaluate")
            .is_none(),
    );
    provenance[1] = Some(AcceptedFieldWriteProvenance::Authored);
    assert_eq!(
        program
            .unique_activation_write_blocker(SanitizeWriteMode::Update, &provenance)
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

    assert_eq!(program.required_slots(), &[2, 3]);
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
fn local_validation_rejects_relations_and_composites_even_for_null_checks() {
    let mut fields = snapshot().fields().to_vec();
    fields.push(field(
        6,
        5,
        "details",
        AcceptedFieldKind::Composite {
            type_id: CompositeTypeId::new(1).expect("test composite type ID should be non-zero"),
        },
        true,
        LeafCodec::Structural,
    ));
    let expression = AcceptedCheckExprV1::IsNull(AcceptedCheckValueExprV1::Field(FieldId::new(6)));

    assert_eq!(
        expression.validate_snapshot_local(fields.as_slice()),
        Err(AcceptedCheckExprV1Error::UnsupportedFieldKind)
    );
}
