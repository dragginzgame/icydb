mod grouped_attachment;
mod normalization_admission;
mod primary_key_names;
mod projection_construction;
mod projection_identity;
mod residual_borrowing;
mod scalar_accounting;
mod scalar_compilation;

use icydb_schema::ScalarKind;
use std::sync::Arc;

use crate::{
    db::{
        predicate::normalize_enum_literals,
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
        query::{plan::FieldSlot, predicate::validate_predicate},
        schema::{
            AcceptedFieldKind, AcceptedSchemaRevision, AcceptedSchemaSnapshot,
            AcceptedValueCatalogHandle, FieldId, FieldStorageDecode, FieldType, LeafCodec,
            PersistedFieldSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot,
            ScalarCodec, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
            TestEnumDefinition, TestEnumVariant, ValidateError,
            build_accepted_enum_catalog_for_tests,
            build_record_newtype_composite_catalog_for_tests,
            empty_accepted_enum_catalog_for_tests, field_type_from_persisted_kind,
        },
    },
    value::Value,
};

use super::SchemaInfo;

fn resolve_group_field(
    schema: &super::SchemaInfo,
    label: &str,
) -> Option<crate::db::query::plan::GroupField> {
    crate::db::query::preparation::with_preparation_work(|work| {
        crate::db::query::plan::GroupField::resolve_with_schema(schema, label, work)
    })
    .unwrap()
}

#[test]
fn inference_nested_rejection_labels_use_current_preparation_budget() {
    use crate::db::{
        QueryError, RequestExecutionRoot,
        query::{
            plan::expr::{Expr, FieldPath, infer_expr_type},
            preparation::PreparationWork,
        },
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
        DiagnosticFactTag, QueryFieldRole,
    };

    let schema = newtype_query_schema();
    let expr = Expr::FieldPath(FieldPath::new("profile", vec!["missing".into()]));
    for limit in [0, 16_000_000] {
        let root = RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::TemporaryBytes, limit),
        );
        let error = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            infer_expr_type(&expr, &schema, work)
        })
        .unwrap_err()
        .attach_query_field(QueryFieldRole::Projection);
        if limit == 0 {
            assert!(matches!(error, QueryError::Execute(_)));
            assert!(error.diagnostic_facts().contains(&(
                DiagnosticFactTag::BudgetResource,
                Resource::TemporaryBytes.raw()
            )));
        } else {
            assert_eq!(
                error.query_field_context(),
                Some((QueryFieldRole::Projection, "profile.missing"))
            );
        }
    }
}

#[test]
fn binding_preflight_query_operand_is_not_a_stored_field_value() {
    use crate::{db::schema::enum_catalog::ValueAdmissionBudget, value::InputValue};
    let schema = newtype_query_schema();
    let input = Value::Int64(-1);
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "id",
        CompareOp::Gte,
        input,
        CoercionId::NumericWiden,
    ));
    let normalized = crate::db::query::preparation::with_preparation_work(|work| {
        normalize_enum_literals(&schema, &predicate, work)
    })
    .expect("query normalization admits the comparison operand");
    validate_predicate(&schema, &normalized)
        .expect("a negative comparison boundary is valid for a Nat64 field");
    assert_eq!(normalized, predicate);
    assert!(
        schema
            .accepted_field_contract("id")
            .expect("accepted Nat64 field")
            .normalize_input_to_runtime(
                InputValue::int64(-1),
                &mut ValueAdmissionBudget::standard()
            )
            .is_err()
    );
}

fn newtype_query_schema() -> SchemaInfo {
    let enums = empty_accepted_enum_catalog_for_tests();
    let (composites, record_type, name_type, _) = build_record_newtype_composite_catalog_for_tests(
        "tests::Profile".to_string(),
        "name".to_string(),
        "tests::Name".to_string(),
        AcceptedFieldKind::Text { max_len: Some(64) },
        &enums,
    )
    .expect("newtype query fixture catalog should build");
    let name_kind = AcceptedFieldKind::Composite { type_id: name_type };
    let fields = vec![
        scalar_field(1, 0, "id", AcceptedFieldKind::Nat64, ScalarCodec::Nat64),
        composite_field(2, 1, "name", name_kind.clone(), Vec::new()),
        composite_field(
            3,
            2,
            "profile",
            AcceptedFieldKind::Composite {
                type_id: record_type,
            },
            vec![PersistedNestedLeafSnapshot::new(
                vec!["name".to_string()],
                name_kind.clone(),
                false,
            )],
        ),
        composite_field(
            4,
            3,
            "aliases",
            AcceptedFieldKind::Set(Box::new(name_kind)),
            Vec::new(),
        ),
    ];
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::Token".to_string(),
        "Token".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    let accepted = AcceptedSchemaSnapshot::new(snapshot);
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        enums,
        composites,
        AcceptedSchemaRevision::INITIAL,
    );

    SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog)
}

fn enum_newtype_query_schema(collection: bool) -> SchemaInfo {
    enum_newtype_query_schema_with_variants(
        collection,
        vec![
            TestEnumVariant::unit("Active"),
            TestEnumVariant::unit("Paused"),
        ],
    )
}

fn enum_newtype_query_schema_with_variants(
    collection: bool,
    variants: Vec<TestEnumVariant>,
) -> SchemaInfo {
    let enums =
        build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new("tests::Stage", variants)])
            .expect("newtype enum fixture catalog should build");
    let enum_type = enums
        .type_id("tests::Stage")
        .expect("newtype enum fixture type should exist");
    let enum_kind = AcceptedFieldKind::Enum { type_id: enum_type };
    let newtype_kind = if collection {
        AcceptedFieldKind::Set(Box::new(enum_kind))
    } else {
        enum_kind
    };
    let (composites, _, stage_type, _) = build_record_newtype_composite_catalog_for_tests(
        "tests::StageRecord".to_string(),
        "stage".to_string(),
        "tests::StageNewtype".to_string(),
        newtype_kind,
        &enums,
    )
    .expect("newtype enum composite catalog should build");
    let fields = vec![
        scalar_field(1, 0, "id", AcceptedFieldKind::Nat64, ScalarCodec::Nat64),
        composite_field(
            2,
            1,
            "stage",
            AcceptedFieldKind::Composite {
                type_id: stage_type,
            },
            Vec::new(),
        ),
    ];
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::EnumToken".to_string(),
        "EnumToken".to_string(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    let accepted = AcceptedSchemaSnapshot::new(snapshot);
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        enums,
        composites,
        AcceptedSchemaRevision::INITIAL,
    );

    SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog)
}

fn scalar_field(
    id: u32,
    slot: u16,
    name: &str,
    kind: AcceptedFieldKind,
    codec: ScalarCodec,
) -> PersistedFieldSnapshot {
    PersistedFieldSnapshot::new_initial(
        FieldId::new(id),
        name.to_string(),
        SchemaFieldSlot::new(slot),
        kind,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(codec),
    )
}

fn composite_field(
    id: u32,
    slot: u16,
    name: &str,
    kind: AcceptedFieldKind,
    nested_leaves: Vec<PersistedNestedLeafSnapshot>,
) -> PersistedFieldSnapshot {
    PersistedFieldSnapshot::new_initial(
        FieldId::new(id),
        name.to_string(),
        SchemaFieldSlot::new(slot),
        kind,
        nested_leaves,
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::CatalogValue,
        LeafCodec::Structural,
    )
}

#[test]
fn accepted_newtype_fields_use_their_admitted_value_shape_for_query_planning() {
    let schema = newtype_query_schema();
    let text = Value::Text("Copper".to_string());
    let predicates = [
        Predicate::eq("name".to_string(), text.clone()),
        Predicate::Compare(ComparePredicate::with_coercion(
            "name",
            CompareOp::StartsWith,
            Value::Text("Cop".to_string()),
            CoercionId::Strict,
        )),
        Predicate::TextContainsCi {
            field: "name".to_string(),
            value: Value::Text("opp".to_string()),
        },
    ];

    assert_eq!(
        schema.field("name"),
        Some(&FieldType::Scalar(ScalarKind::Text))
    );
    for predicate in predicates {
        validate_predicate(&schema, &predicate)
            .expect("text newtype should retain underlying predicate support");
    }
    assert_eq!(
        crate::db::query::preparation::with_preparation_work(|work| schema
            .canonicalize_filter_literal("name", &text, work)
            .expect("literal preparation")
            .map(std::borrow::Cow::into_owned)),
        Some(text)
    );
    #[cfg(feature = "sql")]
    assert!(schema.sql_capabilities("name").is_some_and(|capabilities| {
        capabilities.orderable()
            && capabilities.groupable()
            && capabilities.aggregate_input().count()
            && capabilities.aggregate_input().extrema()
    }));
    assert!(matches!(
        FieldSlot::resolve_with_schema(&schema, "name")
            .and_then(|slot| slot.accepted_kind().cloned()),
        Some(AcceptedFieldKind::Text { max_len: Some(64) })
    ));
}

#[test]
fn borrowed_group_labels_match_retained_key_eligibility() {
    use crate::db::query::plan::GroupField;

    let schema = newtype_query_schema();
    for label in [
        "id",
        "name",
        "profile",
        "aliases",
        "profile.name",
        "profile.missing",
        "profile..name",
        ".name",
        "profile.",
        "missing",
        "",
        "名",
    ] {
        assert_eq!(
            GroupField::accepted_kind_for_label(&schema, label).is_some(),
            resolve_group_field(&schema, label).is_some(),
            "{label}"
        );
    }
}

#[test]
fn retained_field_slots_share_detached_accepted_labels_and_query_kinds() {
    use crate::db::query::plan::FieldSlot;

    let schema = newtype_query_schema();
    let cloned_schema = schema.clone();
    let mut retained = Vec::new();
    for field in ["id", "name", "profile", "aliases"] {
        let slot = FieldSlot::resolve_with_schema(&schema, field).unwrap();
        let cloned_slot = slot.clone();
        let resolved_again = FieldSlot::resolve_with_schema(&cloned_schema, field).unwrap();
        let accepted_kind = schema.accepted_query_field_kind(field).unwrap();
        let (accepted_label, _) = super::schema_field_entry(&schema.fields, field).unwrap();
        // Sharing is the ownership contract: neither resolving a key nor
        // cloning its plan/view may recursively copy this metadata tree.
        for candidate in [&slot, &cloned_slot, &resolved_again] {
            assert!(std::ptr::eq(
                candidate.accepted_kind().unwrap(),
                accepted_kind
            ));
            assert_eq!(candidate.index(), schema.field_slot_index(field).unwrap());
            assert_eq!(candidate.field(), field);
            assert!(std::ptr::eq(candidate.field(), accepted_label.as_ref()));
        }
        retained.push(slot);
    }
    assert!(FieldSlot::resolve_with_schema(&schema, "missing").is_none());
    drop(cloned_schema);
    drop(schema);
    assert_eq!(retained[1].field(), "name");
    assert!(matches!(
        retained[0].accepted_kind(),
        Some(AcceptedFieldKind::Nat64)
    ));
    assert!(matches!(
        retained[1].accepted_kind(),
        Some(AcceptedFieldKind::Text { max_len: Some(64) })
    ));
    assert!(matches!(
        retained[2].accepted_kind(),
        Some(AcceptedFieldKind::Composite { .. })
    ));
    assert!(
        matches!(retained[3].accepted_kind(), Some(AcceptedFieldKind::Set(inner))
        if matches!(inner.as_ref(), AcceptedFieldKind::Text { max_len: Some(64) }))
    );
}

#[test]
fn retained_group_rebinding_selects_current_metadata_without_mutating_old_slots() {
    use crate::db::query::plan::{FieldSlot, GroupFieldSet};

    let mut current = newtype_query_schema();
    let old_slot = FieldSlot::resolve_with_schema(&current, "name").unwrap();
    let retained = GroupFieldSet::Direct(vec![old_slot.clone()]);
    // A second schema view has the same field identity but a different kind.
    // Retained metadata is immutable; only explicit rebinding selects it.
    let (label, field) = current
        .fields
        .iter_mut()
        .find(|(name, _)| name.as_ref() == "name")
        .unwrap();
    *label = Arc::from("name");
    field.query_kind = Arc::new(AcceptedFieldKind::Bool);
    field.ty = field_type_from_persisted_kind(&field.query_kind);
    let rebound = crate::db::query::preparation::with_preparation_work(|work| {
        retained.resolve_with_schema(&current, work)
    })
    .unwrap()
    .unwrap();
    let rebound_slot = &rebound.as_direct().unwrap()[0];
    assert!(!std::ptr::eq(old_slot.field(), rebound_slot.field()));
    let (current_label, _) = super::schema_field_entry(&current.fields, "name").unwrap();
    assert!(std::ptr::eq(rebound_slot.field(), current_label.as_ref()));
    assert_eq!(
        old_slot, *rebound_slot,
        "field identity is independent of metadata ownership"
    );
    assert!(matches!(
        old_slot.accepted_kind(),
        Some(AcceptedFieldKind::Text { max_len: Some(64) })
    ));
    assert!(matches!(
        rebound_slot.accepted_kind(),
        Some(AcceptedFieldKind::Bool)
    ));
    assert!(std::ptr::eq(
        rebound_slot.accepted_kind().unwrap(),
        current.accepted_query_field_kind("name").unwrap()
    ));
}

#[test]
fn retained_slot_accounting_includes_shared_label_and_kind_allocations() {
    use crate::{db::query::plan::FieldSlot, retained::RetainedBytes};

    let schema = newtype_query_schema();
    let slot = FieldSlot::resolve_with_schema(&schema, "aliases").unwrap();
    let kind_bytes = RetainedBytes::measure(slot.accepted_kind().unwrap(), usize::MAX).unwrap();
    let label_bytes =
        (2 * size_of::<usize>() + slot.field().len()).next_multiple_of(align_of::<usize>());
    let expected = size_of::<FieldSlot>() + label_bytes + 2 * size_of::<usize>() + kind_bytes;
    assert_eq!(RetainedBytes::measure(&slot, expected), Some(expected));
    assert_eq!(RetainedBytes::measure(&slot, expected - 1), None);
    // Independent residents remain conservatively charged for shared data.
    for resident in [slot.clone(), slot] {
        assert_eq!(
            RetainedBytes::measure(&resident, usize::MAX),
            Some(expected)
        );
    }
}

#[test]
fn borrowed_group_validation_matches_current_resolution_without_rebinding() {
    use crate::db::query::plan::{FieldSlot, GroupField, GroupFieldRef};

    let mut schema = newtype_query_schema();
    let direct = resolve_group_field(&schema, "name").unwrap();
    let profile = schema
        .fields
        .iter()
        .position(|(name, _)| name.as_ref() == "profile")
        .unwrap();
    let path = |label: &str, root: &str, segments: &[&str], slot| {
        GroupField::scalar_path_for_test(
            label,
            root,
            segments
                .iter()
                .map(|segment| (*segment).to_string())
                .collect(),
            slot,
            &AcceptedFieldKind::Int32,
        )
    };
    let candidates = [
        direct.clone(),
        GroupField::Direct(FieldSlot::from_test_accepted_kind(
            999,
            "name",
            AcceptedFieldKind::Bool,
        )),
        GroupField::Direct(FieldSlot::from_test_accepted_kind(
            2,
            "profile.name",
            AcceptedFieldKind::Bool,
        )),
        path("profile.name", "profile", &["name"], 2),
        path("profile.name", "profile", &["name"], 999),
        path("profile.name", "missing", &["name"], 2),
        path("profile.name", "profile", &["missing"], 2),
        path("profile.name", "profile", &["na", "me"], 2),
        path("profile", "profile", &["name"], 2),
        path("profile..name", "profile", &["", "name"], 2),
        path("missing.name", "missing", &["name"], 2),
    ];
    // The maintained resolver is the reference: eligibility can change
    // with current authority, but this check must not refresh retained kinds.
    for kind in [
        AcceptedFieldKind::Int64,
        AcceptedFieldKind::Text { max_len: Some(9) },
        AcceptedFieldKind::Float64,
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Int64)),
    ] {
        schema.fields[profile].1.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
            vec!["name".into()],
            kind,
            false,
        )];
        for candidate in &candidates {
            let borrowed = GroupFieldRef::PathAware(candidate);
            let expected =
                resolve_group_field(&schema, candidate.field()).is_some_and(|resolved| {
                    borrowed.root_slot() == resolved.root_slot()
                        && borrowed.same_identity(GroupFieldRef::PathAware(&resolved))
                });
            assert_eq!(
                borrowed.matches_schema_identity(&schema),
                expected,
                "{candidate:?}"
            );
        }
    }
    let slot = direct.as_direct().unwrap();
    assert!(GroupFieldRef::Direct(slot).matches_schema_identity(&schema));
    assert!(GroupFieldRef::Direct(slot).same_identity(GroupFieldRef::PathAware(&direct)));

    schema.fields[profile].1.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
        vec!["name".into()],
        AcceptedFieldKind::Int64,
        false,
    )];
    let retained = path("profile.name", "profile", &["name"], 2);
    assert!(GroupFieldRef::PathAware(&retained).matches_schema_identity(&schema));
    schema.fields[profile].1.slot = 9;
    assert!(!GroupFieldRef::PathAware(&retained).matches_schema_identity(&schema));
}

#[test]
fn grouped_expression_resolution_borrows_current_direct_and_nested_authority() {
    use crate::db::query::plan::{
        GroupField,
        expr::{Expr, FieldPath},
    };

    let mut schema = newtype_query_schema();
    for field in ["id", "name", "aliases", "profile"] {
        let expr = Expr::Field(field.into());
        let kind = GroupField::accepted_kind_for_expr(&schema, &expr)
            .expect("direct resolution does not impose nested-key eligibility");
        assert!(std::ptr::eq(
            kind,
            schema.accepted_query_field_kind(field).unwrap()
        ));
        let retained = resolve_group_field(&schema, field).unwrap();
        assert_eq!(retained.as_direct().unwrap().accepted_kind(), Some(kind));
    }

    // Vary the accepted nested-leaf projection, not generated model metadata.
    // Reusing the expression must always observe the supplied schema view.
    let expr = Expr::FieldPath(FieldPath::new("profile", vec!["name".into()]));
    let profile = schema
        .fields
        .iter()
        .position(|(name, _)| name.as_ref() == "profile")
        .unwrap();
    for kind in [
        AcceptedFieldKind::Int64,
        AcceptedFieldKind::Text { max_len: Some(9) },
    ] {
        schema.fields[profile].1.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
            vec!["name".into()],
            kind,
            false,
        )];
        let borrowed = GroupField::accepted_kind_for_expr(&schema, &expr).unwrap();
        assert!(std::ptr::eq(
            borrowed,
            schema.fields[profile].1.nested_leaves[0].kind()
        ));
        let retained = resolve_group_field(&schema, "profile.name").unwrap();
        assert_eq!(retained.projection_expr(), expr);
        assert_eq!(retained.root_slot(), 2);
        assert_eq!(
            GroupField::accepted_kind_for_expr(&schema, &retained.projection_expr()),
            Some(borrowed)
        );
        #[cfg(feature = "sql")]
        {
            let mut value = Value::Nat64(7);
            crate::db::query::preparation::with_preparation_work(|work| {
                crate::db::query::plan::canonicalize_grouped_having_numeric_literal_for_expr(
                    &schema, &expr, &mut value, work,
                )
            })
            .unwrap();
            assert_eq!(
                value,
                if matches!(borrowed, AcceptedFieldKind::Int64) {
                    Value::Int64(7)
                } else {
                    Value::Nat64(7)
                }
            );
        }
    }
}

#[test]
fn grouped_expression_resolution_keeps_nested_eligibility_and_spelling() {
    use crate::db::query::plan::{
        GroupField,
        expr::{Expr, FieldPath},
    };

    let mut schema = newtype_query_schema();
    let profile = schema
        .fields
        .iter()
        .position(|(name, _)| name.as_ref() == "profile")
        .unwrap();
    for kind in [
        AcceptedFieldKind::Text { max_len: None },
        AcceptedFieldKind::Float64,
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Int64)),
        schema.fields[profile].1.nested_leaves[0].kind().clone(),
    ] {
        schema.fields[profile].1.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
            vec!["name".into()],
            kind,
            false,
        )];
        for expr in [
            Expr::Field("profile.name".into()),
            Expr::FieldPath(FieldPath::new("profile", vec!["name".into()])),
            Expr::FieldPath(FieldPath::new("profile.name", vec![String::new()])),
            Expr::FieldPath(FieldPath::new("profile", vec![".name".into()])),
            Expr::FieldPath(FieldPath::new("profile", vec!["name.missing".into()])),
            Expr::FieldPath(FieldPath::new("missing", vec!["name".into()])),
        ] {
            let label = match &expr {
                Expr::Field(field) => field.as_str().to_string(),
                Expr::FieldPath(path) => path.path_spec().dotted_label(),
                _ => unreachable!(),
            };
            assert_eq!(
                GroupField::accepted_kind_for_expr(&schema, &expr).is_some(),
                resolve_group_field(&schema, &label).is_some(),
                "{label}",
            );
        }
        let semantics = crate::db::schema::classify_accepted_field_kind(
            schema.fields[profile].1.nested_leaves[0].kind(),
        );
        assert_eq!(
            GroupField::accepted_kind_for_expr(&schema, &Expr::Field("profile.name".into()))
                .is_some(),
            semantics.is_scalar()
                && semantics.is_sql_comparable()
                && semantics.supports_stable_group_key(),
        );
    }
    assert!(GroupField::accepted_kind_for_expr(&schema, &Expr::Literal(Value::Null)).is_none());
    assert!(GroupField::accepted_kind_for_expr(&schema, &Expr::Field("missing".into())).is_none());
}

#[test]
fn query_projection_recurses_but_keeps_structural_composites_closed() {
    let schema = newtype_query_schema();

    assert_eq!(
        schema.field("aliases"),
        Some(&FieldType::Set(Box::new(FieldType::Scalar(
            ScalarKind::Text
        ))))
    );
    assert_eq!(
        schema
            .nested_fields("profile")
            .unwrap()
            .field_type(&std::iter::once("name")),
        Some(FieldType::Scalar(ScalarKind::Text))
    );
    assert!(matches!(
        schema.field("profile"),
        Some(FieldType::Composite)
    ));
    assert!(matches!(
        validate_predicate(
            &schema,
            &Predicate::eq("profile".to_string(), Value::Text("Copper".to_string())),
        ),
        Err(ValidateError::NonQueryableFieldType { field }) if field == "profile"
    ));
}

#[test]
fn nested_query_membership_preserves_leaf_and_projection_contracts() {
    use crate::db::query::{
        plan::expr::{Expr, ExprType, FieldPath, infer_expr_type},
        preparation::with_preparation_work,
    };

    let schema = newtype_query_schema();
    assert!(schema.nested_fields("missing").is_none());
    assert!(schema.nested_fields("id").unwrap().is_empty());
    assert!(!schema.nested_fields("profile").unwrap().is_empty());

    for (name, exists) in [
        ("id", true),
        ("name", true),
        ("aliases", true),
        ("profile", true),
        ("profile.name", true),
        ("missing", false),
        ("missing.name", false),
        ("id.name", false),
        ("profile.missing", false),
        ("profile.nam", false),
        ("profile.name.extra", false),
        ("profile..name", false),
        ("profile.", false),
        (".name", false),
        ("", false),
    ] {
        assert_eq!(schema.has_query_field(name), exists, "{name}");
        assert_eq!(
            schema.accepted_query_field_type(name).is_some(),
            exists,
            "{name}"
        );
    }

    // Membership borrows the stored newtype kind; only type consumers project it.
    assert!(matches!(
        schema.accepted_nested_query_field_kind("profile", &std::iter::once("name")),
        Some(AcceptedFieldKind::Composite { .. })
    ));
    assert_eq!(
        schema.accepted_query_field_type("profile.name").as_deref(),
        Some(&FieldType::Scalar(ScalarKind::Text))
    );
    let expression = Expr::FieldPath(FieldPath::new("profile", vec!["name".into()]));
    assert_eq!(
        with_preparation_work(|work| infer_expr_type(&expression, &schema, work)).unwrap(),
        ExprType::Text
    );
}

#[test]
fn filter_enum_label_copies_use_the_current_request_budget() {
    use crate::db::{
        RequestExecutionRoot,
        query::preparation::PreparationWork,
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
    };

    for collection in [false, true] {
        let schema = enum_newtype_query_schema(collection);
        let input = Value::Text("Active".into());
        for limit in [5, 6] {
            let root = RequestExecutionRoot::new_for_tests(
                HardExecutionBudget::uniform_for_tests(
                    16_000_000,
                    HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                )
                .with_limit_for_tests(Resource::TemporaryBytes, limit),
            );
            let result =
                PreparationWork::run(&root.scope(), DiagnosticExecutionLane::PublicRead, |work| {
                    if collection {
                        schema.canonicalize_filter_collection_element("stage", &input, work)
                    } else {
                        schema.canonicalize_filter_literal("stage", &input, work)
                    }
                });
            if limit == 6 {
                assert!(matches!(
                    result.expect("label copy fits").as_deref(),
                    Some(Value::Enum(_))
                ));
            } else {
                let error = result.expect_err("reject before copying the enum label");
                assert!(error.diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw(),
                )));
            }
            assert_eq!(root.observed(Resource::TemporaryBytes), 6);
        }
    }
}

#[test]
fn retained_group_path_construction_admits_exact_work_and_copy_limits() {
    use crate::db::{
        RequestExecutionRoot,
        query::{plan::GroupField, preparation::PreparationWork},
        resource::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane, DiagnosticFactTag,
    };

    let mut schema = newtype_query_schema();
    let (_, profile) = schema
        .fields
        .iter_mut()
        .find(|(name, _)| name.as_ref() == "profile")
        .unwrap();
    profile.nested_leaves = vec![PersistedNestedLeafSnapshot::new(
        vec!["name".into()],
        AcceptedFieldKind::Int64,
        false,
    )];
    let path = "profile.name";
    let bytes = (size_of::<String>() + path.len() + "profile".len() + "name".len()) as u64;
    let steps = (4 + 2 * path.len() + "profile".len() + "name".len()) as u64;
    for lane in [
        DiagnosticExecutionLane::PublicRead,
        DiagnosticExecutionLane::TrustedRead,
    ] {
        for (resource, exact) in [
            (Resource::TemporaryBytes, bytes),
            (Resource::PredicateExpressionSteps, steps),
        ] {
            for limit in [exact - 1, exact] {
                let root = RequestExecutionRoot::new_for_tests(
                    HardExecutionBudget::uniform_for_tests(
                        16_000_000,
                        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
                    )
                    .with_limit_for_tests(resource, limit),
                );
                let result = PreparationWork::run(&root.scope(), lane, |work| {
                    GroupField::resolve_with_schema(&schema, path, work)
                });
                if limit == exact {
                    assert_eq!(result.unwrap().unwrap().field(), path);
                } else {
                    let error = result.unwrap_err();
                    assert!(
                        error
                            .diagnostic_facts()
                            .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                    );
                }
                assert_eq!(root.observed(resource), exact);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }
}

#[test]
fn enum_newtype_literals_normalize_through_the_nominal_admission_contract() {
    let schema = enum_newtype_query_schema(false);
    let predicate = Predicate::eq("stage".to_string(), Value::Text("Active".to_string()));

    let normalized = crate::db::query::preparation::with_preparation_work(|work| {
        normalize_enum_literals(&schema, &predicate, work)
    })
    .expect("loose enum literal should normalize through its newtype contract");

    assert!(matches!(
        &normalized,
        Predicate::Compare(compare) if matches!(compare.value(), Value::Enum(_))
    ));
    validate_predicate(&schema, &normalized)
        .expect("normalized enum newtype predicate should validate");

    let collection_schema = enum_newtype_query_schema(true);
    let contains = Predicate::Compare(ComparePredicate::with_coercion(
        "stage",
        CompareOp::Contains,
        Value::Text("Active".to_string()),
        CoercionId::CollectionElement,
    ));
    let normalized_contains = crate::db::query::preparation::with_preparation_work(|work| {
        normalize_enum_literals(&collection_schema, &contains, work)
    })
    .expect("collection newtype enum literal should normalize");
    assert!(matches!(
        &normalized_contains,
        Predicate::Compare(compare) if matches!(compare.value(), Value::Enum(_))
    ));
    validate_predicate(&collection_schema, &normalized_contains)
        .expect("normalized collection newtype predicate should validate");
}

#[test]
fn typed_filter_enum_identity_is_admitted_against_the_accepted_field_or_element() {
    use crate::{
        db::{FilterExpr, query::intent::StructuralQuery},
        value::InputValue,
    };
    for collection in [false, true] {
        let schema = enum_newtype_query_schema(collection);
        for (path, succeeds) in [("tests::Stage", true), ("tests::Other", false)] {
            let value = InputValue::enum_value("Active", Some(path));
            let filter = if collection {
                FilterExpr::contains("stage", value)
            } else {
                FilterExpr::eq("stage", value)
            };
            let result = crate::db::query::preparation::with_preparation_work(|work| {
                StructuralQuery::new(crate::db::MissingRowPolicy::Ignore)
                    .filter_for_schema(&schema, &filter, work)
            });
            assert_eq!(result.is_ok(), succeeds);
        }
    }
}
