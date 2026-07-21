use super::*;
use crate::{
    db::schema::{AcceptedFieldKind, composite_catalog::CompositeTypeId},
    error::{ErrorClass, ErrorOrigin},
};

struct TestType {
    id: u32,
    path: &'static str,
    codec: u8,
    shape: TestShape,
}

enum TestShape {
    Record(Vec<TestField>),
    Newtype(TestElement),
    Unknown(u8),
}

struct TestField {
    name: &'static str,
    element: TestElement,
}

struct TestElement {
    kind: AcceptedFieldKind,
    nullable: bool,
}

fn empty_enum_catalog() -> AcceptedEnumCatalog {
    crate::db::schema::enum_catalog::build_initial_accepted_enum_catalog_from_kinds_for_tests(&[])
        .expect("empty enum catalog should build")
}

fn scalar_element() -> TestElement {
    TestElement {
        kind: AcceptedFieldKind::Nat64,
        nullable: false,
    }
}

fn newtype(id: u32, path: &'static str, kind: AcceptedFieldKind) -> TestType {
    TestType {
        id,
        path,
        codec: CODEC_STRUCTURAL_V1,
        shape: TestShape::Newtype(TestElement {
            kind,
            nullable: false,
        }),
    }
}

fn encode_test_wire(version: u16, types: &[TestType]) -> Vec<u8> {
    let mut writer = CatalogWriter::new();
    writer.push_bytes(ACCEPTED_COMPOSITE_CATALOG_MAGIC);
    writer.push_u16(version);
    writer
        .push_len(types.len())
        .expect("test type count should fit");
    for definition in types {
        writer.push_u32(definition.id);
        writer
            .push_string(definition.path)
            .expect("test type path should fit");
        writer.push_u8(definition.codec);
        match &definition.shape {
            TestShape::Record(fields) => {
                writer.push_u8(SHAPE_RECORD);
                writer
                    .push_len(fields.len())
                    .expect("test record field count should fit");
                for field in fields {
                    writer
                        .push_string(field.name)
                        .expect("test field name should fit");
                    encode_test_element(&mut writer, &field.element);
                }
            }
            TestShape::Newtype(inner) => {
                writer.push_u8(SHAPE_NEWTYPE);
                encode_test_element(&mut writer, inner);
            }
            TestShape::Unknown(tag) => writer.push_u8(*tag),
        }
    }
    writer.finish().expect("test wire should stay bounded")
}

fn encode_test_element(writer: &mut CatalogWriter, element: &TestElement) {
    writer.push_u8(u8::from(element.nullable));
    encode_value_kind(writer, &element.kind, 0).expect("test field kind should encode");
}

#[test]
fn accepted_composite_catalog_empty_wire_vector_is_frozen() {
    let catalog = AcceptedCompositeCatalog::empty();

    assert_eq!(
        encode_accepted_composite_catalog(&catalog, &empty_enum_catalog())
            .expect("empty catalog should encode"),
        [
            0x49, 0x43, 0x59, 0x44, 0x42, 0x43, 0x4d, 0x50, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        ],
    );
}

#[test]
fn accepted_composite_catalog_decode_rejects_future_codec_version() {
    let encoded = encode_test_wire(ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION + 1, &[]);
    let error = decode_accepted_composite_catalog(&encoded, &empty_enum_catalog())
        .expect_err("future composite catalog codec must fail closed");

    assert_eq!(error.class(), ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(error.origin(), ErrorOrigin::Serialize);
}

#[test]
fn accepted_composite_catalog_decode_rejects_invalid_identity_and_tags() {
    let zero = encode_test_wire(
        ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION,
        &[newtype(0, "codec::Zero", AcceptedFieldKind::Nat64)],
    );
    let unsorted = encode_test_wire(
        ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION,
        &[
            newtype(2, "codec::Second", AcceptedFieldKind::Nat64),
            newtype(1, "codec::First", AcceptedFieldKind::Nat64),
        ],
    );
    let unknown_codec = encode_test_wire(
        ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION,
        &[TestType {
            id: 1,
            path: "codec::UnknownCodec",
            codec: 0xff,
            shape: TestShape::Newtype(scalar_element()),
        }],
    );
    let unknown_shape = encode_test_wire(
        ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION,
        &[TestType {
            id: 1,
            path: "codec::UnknownShape",
            codec: CODEC_STRUCTURAL_V1,
            shape: TestShape::Unknown(0xff),
        }],
    );

    for encoded in [zero, unsorted, unknown_codec, unknown_shape] {
        let error = decode_accepted_composite_catalog(&encoded, &empty_enum_catalog())
            .expect_err("invalid composite catalog identity or tag must reject");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
    }
}

#[test]
fn accepted_composite_catalog_decode_rejects_duplicate_or_unsorted_record_names() {
    let duplicate_paths = encode_test_wire(
        ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION,
        &[
            newtype(1, "codec::Same", AcceptedFieldKind::Nat64),
            newtype(2, "codec::Same", AcceptedFieldKind::Nat64),
        ],
    );
    let duplicate_fields = encode_test_wire(
        ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION,
        &[TestType {
            id: 1,
            path: "codec::DuplicateFields",
            codec: CODEC_STRUCTURAL_V1,
            shape: TestShape::Record(vec![
                TestField {
                    name: "same",
                    element: scalar_element(),
                },
                TestField {
                    name: "same",
                    element: scalar_element(),
                },
            ]),
        }],
    );
    let unsorted_fields = encode_test_wire(
        ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION,
        &[TestType {
            id: 1,
            path: "codec::UnsortedFields",
            codec: CODEC_STRUCTURAL_V1,
            shape: TestShape::Record(vec![
                TestField {
                    name: "zeta",
                    element: scalar_element(),
                },
                TestField {
                    name: "alpha",
                    element: scalar_element(),
                },
            ]),
        }],
    );

    for encoded in [duplicate_paths, duplicate_fields, unsorted_fields] {
        assert_eq!(
            decode_accepted_composite_catalog(&encoded, &empty_enum_catalog())
                .expect_err("noncanonical composite names must reject")
                .class(),
            ErrorClass::Corruption,
        );
    }
}

#[test]
fn accepted_composite_catalog_decode_rejects_unknown_and_recursive_references() {
    let unknown_type_id = CompositeTypeId::new(2).expect("two is non-zero");
    let unknown = encode_test_wire(
        ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION,
        &[newtype(
            1,
            "codec::UnknownRef",
            AcceptedFieldKind::Composite {
                type_id: unknown_type_id,
            },
        )],
    );
    let recursive_type_id = CompositeTypeId::new(1).expect("one is non-zero");
    let recursive = encode_test_wire(
        ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION,
        &[newtype(
            1,
            "codec::Recursive",
            AcceptedFieldKind::Composite {
                type_id: recursive_type_id,
            },
        )],
    );

    for encoded in [unknown, recursive] {
        assert_eq!(
            decode_accepted_composite_catalog(&encoded, &empty_enum_catalog())
                .expect_err("unknown or recursive composite references must reject")
                .class(),
            ErrorClass::Corruption,
        );
    }
}

#[test]
fn accepted_composite_catalog_decode_rejects_trailing_and_oversized_input() {
    let mut trailing = encode_test_wire(ACCEPTED_COMPOSITE_CATALOG_CODEC_VERSION, &[]);
    trailing.push(0xff);
    let oversized = vec![0_u8; MAX_ACCEPTED_COMPOSITE_CATALOG_BYTES + 1];

    for encoded in [trailing, oversized] {
        let error = decode_accepted_composite_catalog(&encoded, &empty_enum_catalog())
            .expect_err("noncanonical composite catalog envelope must reject");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
    }
}
