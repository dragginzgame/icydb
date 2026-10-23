//! Prepared text literals retain the general predicate contract on every reader.

use super::*;
use crate::db::{
    data::{
        AcceptedStructuralRowAuthority, RawRow, StructuralRowContract, StructuralSlotReader,
        canonical_row_from_runtime_value_source_with_accepted_contract,
    },
    predicate::CompareFieldsPredicate,
    schema::{
        AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
        AcceptedSchemaSnapshot, AcceptedValueCatalogHandle, FieldId, FieldStorageDecode,
        PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot, SchemaInsertDefault,
        SchemaRowLayout, SchemaVersion, empty_accepted_enum_catalog_for_tests,
    },
};
use std::rc::Rc;

fn text_schema(storage: FieldStorageDecode) -> (SchemaInfo, StructuralRowContract) {
    let fields = [
        ("id", AcceptedFieldKind::Nat64),
        ("left", AcceptedFieldKind::Text { max_len: None }),
        ("right", AcceptedFieldKind::Text { max_len: None }),
    ]
    .into_iter()
    .enumerate()
    .map(|(slot, (name, kind))| {
        let storage = if slot == 0 {
            FieldStorageDecode::ByKind
        } else {
            storage
        };
        let leaf = kind.leaf_codec_for_storage(storage);
        PersistedFieldSnapshot::new_initial(
            FieldId::new(u32::try_from(slot).unwrap() + 1),
            name.into(),
            SchemaFieldSlot::new(u16::try_from(slot).unwrap()),
            kind,
            vec![],
            slot != 0,
            SchemaInsertDefault::None,
            storage,
            leaf,
        )
    })
    .collect::<Vec<_>>();
    let accepted = Rc::new(
        AcceptedSchemaSnapshot::try_new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "tests::TextRow".into(),
            "TextRow".into(),
            FieldId::new(1),
            SchemaRowLayout::initial(
                fields
                    .iter()
                    .map(|field| (field.id(), field.slot()))
                    .collect(),
            ),
            fields,
        ))
        .unwrap(),
    );
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    let contract = AcceptedStructuralRowAuthority::from_candidate_snapshot(
        "tests::TextRow",
        accepted.clone(),
        catalog.clone(),
    )
    .unwrap()
    .into_row_contract();
    (
        SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog),
        contract,
    )
}

fn assert_readers(
    programs: &[PredicateProgram; 2],
    contract: &StructuralRowContract,
    left: Value,
    right: Value,
    expected: bool,
) {
    let values = [Value::Nat64(1), left, right];
    let row = canonical_row_from_runtime_value_source_with_accepted_contract(contract, |slot| {
        Ok(Cow::Borrowed(&values[slot]))
    })
    .unwrap()
    .into_raw_row();
    for program in programs {
        for owned in [false, true] {
            assert_eq!(
                program.eval_with_slot_value_cow_reader(&mut |slot| {
                    values.get(slot).map(|value| {
                        if owned {
                            Cow::Owned(value.clone())
                        } else {
                            Cow::Borrowed(value)
                        }
                    })
                }),
                expected,
                "values={values:?}, program={program:?}"
            );
        }
        let reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, contract).unwrap();
        assert_eq!(
            program.eval_with_structural_slot_reader(&reader).unwrap(),
            expected,
            "values={values:?}, program={program:?}"
        );
    }
}

#[test]
fn casefold_compare_literals_preserve_generic_results_across_readers() {
    let literals = [
        Value::Text("ALPHA".into()),
        Value::Text("ΣΟΣ".into()),
        Value::Text("İ".into()),
        Value::Text(String::new()),
        Value::Null,
        Value::Nat64(7),
        Value::List(vec![]),
        Value::List(vec![
            Value::Text("ALPHA".into()),
            Value::Null,
            Value::Nat64(7),
        ]),
        Value::List(vec![
            Value::Text("BETA".into()),
            Value::Text("İ".into()),
            Value::Text("BETA".into()),
        ]),
        Value::List(vec![Value::Null, Value::Nat64(7)]),
    ];
    for storage in [FieldStorageDecode::ByKind, FieldStorageDecode::CatalogValue] {
        let (schema, contract) = text_schema(storage);
        for coercion in [CoercionId::Strict, CoercionId::TextCasefold] {
            for op in [
                CompareOp::Eq,
                CompareOp::Ne,
                CompareOp::Lt,
                CompareOp::Lte,
                CompareOp::Gt,
                CompareOp::Gte,
                CompareOp::In,
                CompareOp::NotIn,
                CompareOp::StartsWith,
                CompareOp::EndsWith,
                CompareOp::Contains,
            ] {
                for literal in &literals {
                    let predicate = Predicate::Compare(ComparePredicate::with_coercion(
                        "left",
                        op,
                        literal.clone(),
                        coercion,
                    ));
                    let programs = [
                        PredicateProgram::compile_with_schema_info(&schema, &predicate),
                        PredicateProgram::compile_with_row_contract(&contract, &predicate),
                    ];
                    // Public predicates keep their original payload. Only the
                    // executable program owns preparation of runtime constants.
                    if let Value::Text(text) = literal {
                        for program in &programs {
                            let ExecutablePredicate::Compare(compare) = program.executable() else {
                                panic!("fixture is one compare");
                            };
                            let expected = if coercion == CoercionId::TextCasefold {
                                Value::Text(casefold_text(text))
                            } else {
                                literal.clone()
                            };
                            assert_eq!(compare.right_literal(), Some(&expected));
                        }
                    }
                    for actual in [
                        "Alpha",
                        "BETA",
                        "ΣΟΣ",
                        "σος",
                        "İ",
                        "I",
                        "",
                        "unmatched",
                        "é🙂",
                    ]
                    .into_iter()
                    .map(|text| Value::Text(text.into()))
                    .chain([Value::Null])
                    {
                        let expected =
                            eval_compare_values(&actual, op, literal, &CoercionSpec::new(coercion));
                        assert_readers(&programs, &contract, actual, Value::Null, expected);
                    }
                }
            }
        }
    }
}

#[test]
fn casefold_contains_literals_preserve_generic_results_across_readers() {
    for storage in [FieldStorageDecode::ByKind, FieldStorageDecode::CatalogValue] {
        let (schema, contract) = text_schema(storage);
        for literal in [
            Value::Text("ALPHA".into()),
            Value::Text("İ".into()),
            Value::Text(String::new()),
            Value::Null,
            Value::Nat64(7),
        ] {
            let predicate = Predicate::TextContainsCi {
                field: "left".into(),
                value: literal.clone(),
            };
            let programs = [
                PredicateProgram::compile_with_schema_info(&schema, &predicate),
                PredicateProgram::compile_with_row_contract(&contract, &predicate),
            ];
            for actual in ["preAlphaPost", "İ", "I", "", "other"]
                .into_iter()
                .map(|text| Value::Text(text.into()))
                .chain([Value::Null])
            {
                let expected = actual
                    .text_contains(&literal, TextMode::Ci)
                    .unwrap_or(false);
                assert_readers(&programs, &contract, actual, Value::Null, expected);
            }
        }
    }
}

#[test]
fn casefold_field_pairs_still_fold_both_row_values() {
    let (schema, contract) = text_schema(FieldStorageDecode::ByKind);
    for op in [
        CompareOp::Eq,
        CompareOp::Ne,
        CompareOp::Lt,
        CompareOp::Lte,
        CompareOp::Gt,
        CompareOp::Gte,
        CompareOp::StartsWith,
        CompareOp::EndsWith,
    ] {
        let predicate = Predicate::CompareFields(CompareFieldsPredicate::with_coercion(
            "left",
            op,
            "right",
            CoercionId::TextCasefold,
        ));
        let programs = [
            PredicateProgram::compile_with_schema_info(&schema, &predicate),
            PredicateProgram::compile_with_row_contract(&contract, &predicate),
        ];
        for left in [
            Value::Text("AlPhA".into()),
            Value::Text("İ".into()),
            Value::Null,
        ] {
            for right in [
                Value::Text("ALPHA".into()),
                Value::Text("İ".into()),
                Value::Null,
            ] {
                let expected = eval_compare_values(
                    &left,
                    op,
                    &right,
                    &CoercionSpec::new(CoercionId::TextCasefold),
                );
                assert_readers(&programs, &contract, left.clone(), right, expected);
            }
        }
    }
}

#[test]
fn casefold_missing_slots_and_corrupt_rows_keep_their_existing_outcomes() {
    for storage in [FieldStorageDecode::ByKind, FieldStorageDecode::CatalogValue] {
        let (schema, contract) = text_schema(storage);
        let values = [Value::Nat64(1), Value::Text("valid".into()), Value::Null];
        let row =
            canonical_row_from_runtime_value_source_with_accepted_contract(&contract, |slot| {
                Ok(Cow::Borrowed(&values[slot]))
            })
            .unwrap()
            .into_raw_row();
        // Break only the known text payload, preserving the row envelope and
        // directory so the error still belongs to the requested field read.
        let mut bytes = row.as_bytes().to_vec();
        let start = bytes
            .windows(5)
            .position(|bytes| bytes == b"valid")
            .unwrap();
        bytes[start] = u8::MAX;
        let row = RawRow::try_new(bytes).unwrap();
        let reader =
            StructuralSlotReader::from_raw_row_with_borrowed_contract(&row, &contract).unwrap();
        let expected = reader.required_value_by_contract(1).unwrap_err();
        for field in ["missing", "left"] {
            let predicate = Predicate::Compare(ComparePredicate::with_coercion(
                field,
                CompareOp::Eq,
                Value::Text("VALID".into()),
                CoercionId::TextCasefold,
            ));
            for program in [
                PredicateProgram::compile_with_schema_info(&schema, &predicate),
                PredicateProgram::compile_with_row_contract(&contract, &predicate),
            ] {
                let result = program.eval_with_structural_slot_reader(&reader);
                if field == "missing" {
                    assert!(!result.unwrap());
                    assert!(!program.eval_with_slot_value_cow_reader(&mut |_| {
                        panic!("an unresolved field must not request a row slot");
                    }));
                } else {
                    let error = result.unwrap_err();
                    assert_eq!(error.diagnostic_code(), expected.diagnostic_code());
                    assert_eq!(error.class(), expected.class());
                }
            }
        }
    }
}
