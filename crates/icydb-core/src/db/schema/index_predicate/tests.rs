//! Current accepted filtered predicates preserve catalog facts through persistence.

use std::borrow::Cow;

use crate::{
    db::{
        predicate::{
            CoercionId, CompareOp, ComparePredicate, Predicate, PredicateProgram,
            parse_sql_predicate,
        },
        schema::{
            AcceptedCheckCompareOpV1, AcceptedCheckExprV1, AcceptedCheckValueExprV1,
            AcceptedCompositeCatalog, AcceptedFieldKind as K, AcceptedIndexPredicate as P,
            AcceptedSchemaRevision, AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId,
            FieldStorageDecode, PersistedFieldSnapshot, PersistedIndexFieldPathSnapshot,
            PersistedIndexKeySnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot,
            SchemaFieldSlot, SchemaIndexId, SchemaInfo, SchemaInsertDefault, SchemaRowLayout,
            SchemaVersion, check::bind_index_predicate_literal, decode_persisted_schema_snapshot,
            empty_accepted_enum_catalog_for_tests, encode_persisted_schema_snapshot,
        },
    },
    types::{Decimal, Ulid},
    value::{InputValue, Value},
};

fn catalog() -> AcceptedValueCatalogHandle {
    AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    )
}

fn snapshot(kind: K) -> PersistedSchemaSnapshot {
    let fields = [("id", K::Ulid), ("left", kind.clone()), ("right", kind)]
        .into_iter()
        .enumerate()
        .map(|(slot, (name, kind))| {
            let leaf = kind.leaf_codec_for_storage(FieldStorageDecode::ByKind);
            PersistedFieldSnapshot::new_initial(
                FieldId::new(u32::try_from(slot).unwrap() + 1),
                name.into(),
                SchemaFieldSlot::new(u16::try_from(slot).unwrap()),
                kind,
                vec![],
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                leaf,
            )
        })
        .collect::<Vec<_>>();
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::Filtered".into(),
        "Filtered".into(),
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

fn with_predicate(base: &PersistedSchemaSnapshot, predicate: P) -> PersistedSchemaSnapshot {
    let field = &base.fields()[1];
    let index = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).unwrap(),
        1,
        "filtered".into(),
        "tests::Filtered::filtered".into(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
            field.id(),
            field.slot(),
            vec![field.name().into()],
            field.kind().clone(),
            false,
        )]),
        Some(predicate),
    );
    PersistedSchemaSnapshot::new_with_indexes(
        base.version(),
        base.entity_path().into(),
        base.entity_name().into(),
        base.primary_key_field_ids().to_vec(),
        base.row_layout().clone(),
        base.fields().to_vec(),
        vec![index],
    )
}

fn evaluate(snapshot: &PersistedSchemaSnapshot, value: &Value) -> bool {
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot.clone()).unwrap();
    let catalog = catalog();
    let schema = SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog.clone());
    let predicate = snapshot.indexes()[0]
        .predicate()
        .unwrap()
        .to_predicate(snapshot.fields(), &catalog)
        .unwrap();
    let program = PredicateProgram::compile_with_schema_info(&schema, &predicate);
    program.eval_with_slot_value_cow_reader(&mut |slot| (slot == 1).then_some(Cow::Borrowed(value)))
}

#[test]
fn generated_literals_preserve_typed_membership_after_codec() {
    for (kind, value) in [
        (K::Nat64, Value::Nat64(3)),
        (K::Int128, Value::Int128(3)),
        (K::Decimal { scale: 0 }, Value::Decimal(Decimal::from(3))),
        (K::Ulid, Value::Ulid(Ulid::MIN)),
    ] {
        let base = snapshot(kind.clone());
        let literal = bind_index_predicate_literal(
            InputValue::from(value.clone()),
            kind,
            catalog().enum_catalog(),
            catalog().composite_catalog(),
        )
        .unwrap();
        let generated = P::from_check(
            &AcceptedCheckExprV1::Compare {
                left: AcceptedCheckValueExprV1::Field(FieldId::new(2)),
                op: AcceptedCheckCompareOpV1::Eq,
                right: AcceptedCheckValueExprV1::Literal(literal),
            },
            base.fields(),
            catalog().composite_catalog(),
        )
        .unwrap();
        let snapshot = with_predicate(&base, generated.clone());
        let decoded =
            decode_persisted_schema_snapshot(&encode_persisted_schema_snapshot(&snapshot).unwrap())
                .unwrap();
        assert_eq!(decoded.indexes()[0].predicate(), Some(&generated));
        assert!(evaluate(&decoded, &value), "{value:?}");
        assert!(!evaluate(&decoded, &Value::Null));
    }
}

#[test]
fn frontend_numeric_admission_preserves_strict_types_and_widened_operands() {
    let base = snapshot(K::Nat64);
    for (sql, value, expected) in [
        ("left = 3", Value::Nat64(3), true),
        ("left IN (3, 4, NULL)", Value::Nat64(4), true),
        ("left > -1", Value::Nat64(0), true),
        ("left > -1", Value::Null, false),
        ("left NOT IN (3, NULL)", Value::Nat64(4), true),
    ] {
        let predicate = P::bind(&parse_sql_predicate(sql).unwrap(), &base, &catalog()).unwrap();
        assert_eq!(
            evaluate(&with_predicate(&base, predicate), &value),
            expected,
            "{sql}"
        );
    }
    let base = snapshot(K::Int8);
    let predicate = P::bind(
        &parse_sql_predicate("left < 1000").unwrap(),
        &base,
        &catalog(),
    )
    .unwrap();
    assert!(evaluate(
        &with_predicate(&base, predicate),
        &Value::Int64(127)
    ));
}

#[test]
fn accepted_identity_survives_unrelated_chained_and_swapped_renames() {
    let base = snapshot(K::Text { max_len: None });
    let predicate = P::bind(
        &parse_sql_predicate("left = right").unwrap(),
        &base,
        &catalog(),
    )
    .unwrap();
    let original = predicate.canonical_bytes().unwrap();
    for names in [
        ["other_id", "left", "right"],
        ["id", "right", "display"],
        ["id", "right", "left"],
    ] {
        let fields = base
            .fields()
            .iter()
            .zip(names)
            .map(|(field, name)| field.clone_with_name(name.into()))
            .collect::<Vec<_>>();
        let projected = predicate.to_predicate(&fields, &catalog()).unwrap();
        let renamed = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().into(),
            base.entity_name().into(),
            base.primary_key_field_ids().to_vec(),
            base.row_layout().clone(),
            fields,
        );
        assert_eq!(
            P::bind(&projected, &renamed, &catalog())
                .unwrap()
                .canonical_bytes()
                .unwrap(),
            original
        );
        assert_ne!(
            predicate.render_sql(renamed.fields(), &catalog()).unwrap(),
            "left = left"
        );
    }
    for sql in [
        "left = right",
        "right=left",
        "(left = right)",
        "left = right AND left = right",
    ] {
        assert_eq!(
            P::bind(&parse_sql_predicate(sql).unwrap(), &base, &catalog()).unwrap(),
            predicate
        );
    }
}

#[test]
fn frontend_rejects_bad_roots_before_simplification_and_preserves_casefold_policy() {
    let base = snapshot(K::Text { max_len: None });
    for sql in [
        "missing IS NOT NULL",
        "left.path IS NOT NULL",
        "left = 'a' AND left = 'b' AND missing IS NULL",
    ] {
        assert!(
            P::bind(&parse_sql_predicate(sql).unwrap(), &base, &catalog()).is_err(),
            "{sql}"
        );
    }
    for (sql, value, expected) in [
        ("left LIKE 'Al%'", "alice", false),
        ("left ILIKE 'Al%'", "alice", true),
        ("left ILIKE 'Al%'", "bob", false),
    ] {
        let predicate = P::bind(&parse_sql_predicate(sql).unwrap(), &base, &catalog()).unwrap();
        let decoded = decode_persisted_schema_snapshot(
            &encode_persisted_schema_snapshot(&with_predicate(&base, predicate)).unwrap(),
        )
        .unwrap();
        assert_eq!(evaluate(&decoded, &Value::Text(value.into())), expected);
    }
    let compare = Predicate::Compare(ComparePredicate::with_coercion(
        "left",
        CompareOp::Eq,
        Value::Text("AL".into()),
        CoercionId::TextCasefold,
    ));
    let bound = P::bind(&compare, &base, &catalog()).unwrap();
    assert!(evaluate(
        &with_predicate(&base, bound),
        &Value::Text("al".into())
    ));
}

#[test]
fn accepted_predicate_codec_rejects_excess_depth_and_noncanonical_children() {
    let base = snapshot(K::Text { max_len: None });
    let mut predicate = P::test_non_null(2);
    for _ in 0..crate::db::sql_shared::MAX_SQL_EXPR_DEPTH {
        predicate = P::Not(Box::new(predicate));
    }
    assert!(predicate.canonical_bytes().is_err());
    let value = Value::Text("a".repeat(5_000));
    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
        "left",
        CompareOp::Eq,
        value.clone(),
        CoercionId::Strict,
    ));
    let bound = P::bind(&predicate, &base, &catalog()).unwrap();
    let encoded = encode_persisted_schema_snapshot(&with_predicate(&base, bound)).unwrap();
    assert!(evaluate(
        &decode_persisted_schema_snapshot(&encoded).unwrap(),
        &value
    ));
    let duplicate = P::And(vec![P::test_non_null(2), P::test_non_null(2)]);
    assert!(encode_persisted_schema_snapshot(&with_predicate(&base, duplicate)).is_err());
}

#[test]
fn enum_literal_identity_uses_variant_ids_through_renames() {
    use crate::db::schema::{
        TestEnumDefinition, TestEnumVariant, build_accepted_enum_catalog_for_tests,
    };
    let enums = build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
        "tests::State",
        vec![
            TestEnumVariant::unit("Queued"),
            TestEnumVariant::unit("Done"),
        ],
    )])
    .unwrap();
    let type_id = enums.type_id("tests::State").unwrap();
    let variant_id = enums
        .enum_type(type_id)
        .unwrap()
        .variant_id("Queued")
        .unwrap();
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        enums.clone(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let base = snapshot(K::Enum { type_id });
    let bound = P::bind(
        &parse_sql_predicate("left = 'Queued'").unwrap(),
        &base,
        &catalog,
    )
    .unwrap();
    let renamed = enums
        .with_renamed_variant(type_id, variant_id, "Waiting".into())
        .unwrap();
    let renamed_catalog = AcceptedValueCatalogHandle::new_for_tests(
        renamed,
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    assert_eq!(
        bound.render_sql(base.fields(), &renamed_catalog).unwrap(),
        "left = 'Waiting'"
    );
    assert_eq!(
        P::bind(
            &parse_sql_predicate("left = 'Waiting'").unwrap(),
            &base,
            &renamed_catalog
        )
        .unwrap(),
        bound
    );
    assert_eq!(
        bound.to_predicate(base.fields(), &catalog).unwrap(),
        bound.to_predicate(base.fields(), &renamed_catalog).unwrap()
    );
}

#[test]
fn generated_field_comparisons_keep_accepted_capabilities_after_codec() {
    for (kind, value) in [
        (K::Nat64, Value::Nat64(3)),
        (
            K::Date,
            Value::Date(crate::types::Date::try_new(2024, 1, 1).unwrap()),
        ),
        (K::U256, Value::U256(crate::types::U256::ONE)),
    ] {
        let base = snapshot(kind);
        let catalog = catalog();
        let generated = P::from_check(
            &AcceptedCheckExprV1::Compare {
                left: AcceptedCheckValueExprV1::Field(FieldId::new(2)),
                op: AcceptedCheckCompareOpV1::Eq,
                right: AcceptedCheckValueExprV1::Field(FieldId::new(3)),
            },
            base.fields(),
            catalog.composite_catalog(),
        )
        .unwrap();
        let decoded = decode_persisted_schema_snapshot(
            &encode_persisted_schema_snapshot(&with_predicate(&base, generated.clone())).unwrap(),
        )
        .unwrap();
        let accepted = AcceptedSchemaSnapshot::try_new(decoded.clone()).unwrap();
        let schema = SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog.clone());
        generated
            .validate_semantics(decoded.fields(), &catalog, &schema)
            .unwrap();
        let executable = decoded.indexes()[0]
            .predicate()
            .unwrap()
            .to_predicate(decoded.fields(), &catalog)
            .unwrap();
        let program = PredicateProgram::compile_with_schema_info(&schema, &executable);
        assert!(program.eval_with_slot_value_cow_reader(&mut |slot| {
            (slot == 1 || slot == 2).then_some(Cow::Borrowed(&value))
        }));
        assert!(!program.eval_with_slot_value_cow_reader(&mut |_| None));
        for (op, right) in [(CompareOp::Contains, 3), (CompareOp::Eq, 1)] {
            let invalid = P::CompareFields {
                left: FieldId::new(2),
                op,
                right: FieldId::new(right),
                coercion: CoercionId::Strict,
            };
            assert!(
                invalid
                    .validate_semantics(decoded.fields(), &catalog, &schema)
                    .is_err()
            );
        }
    }
}

#[test]
fn accepted_predicate_validation_retains_literal_admission_bounds() {
    let base = snapshot(K::Text { max_len: Some(3) });
    let catalog = catalog();
    let literal = bind_index_predicate_literal(
        InputValue::from(Value::Text("oversized".into())),
        K::Text { max_len: None },
        catalog.enum_catalog(),
        catalog.composite_catalog(),
    )
    .unwrap();
    let predicate = P::Compare {
        field: FieldId::new(2),
        op: CompareOp::Eq,
        coercion: CoercionId::Strict,
        values: vec![Some(literal)],
    };
    let accepted = AcceptedSchemaSnapshot::try_new(base.clone()).unwrap();
    let schema = SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog.clone());
    assert!(
        predicate
            .validate_semantics(base.fields(), &catalog, &schema)
            .is_err()
    );
}
