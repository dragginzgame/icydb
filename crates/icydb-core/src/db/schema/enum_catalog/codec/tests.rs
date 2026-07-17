use super::*;
use crate::{
    db::schema::enum_catalog::build_initial_accepted_enum_catalog_from_kinds,
    error::{ErrorClass, ErrorOrigin},
    model::field::{EnumVariantModel, FieldKind},
};

static PAYLOAD_KIND: FieldKind = FieldKind::Nat64;
static ALPHA_VARIANTS: [EnumVariantModel; 2] = [
    EnumVariantModel::new("Zulu", None, FieldStorageDecode::ByKind),
    EnumVariantModel::new("Alpha", Some(&PAYLOAD_KIND), FieldStorageDecode::ByKind),
];
static ALPHA_KIND: FieldKind = FieldKind::Enum {
    path: "codec::Alpha",
    variants: &ALPHA_VARIANTS,
};
static ALPHA_REORDERED_VARIANTS: [EnumVariantModel; 2] = [
    EnumVariantModel::new("Alpha", Some(&PAYLOAD_KIND), FieldStorageDecode::ByKind),
    EnumVariantModel::new("Zulu", None, FieldStorageDecode::ByKind),
];
static ALPHA_REORDERED_KIND: FieldKind = FieldKind::Enum {
    path: "codec::Alpha",
    variants: &ALPHA_REORDERED_VARIANTS,
};

struct TestType<'a> {
    id: u32,
    path: &'a str,
    variants: Vec<TestVariant<'a>>,
}

struct TestVariant<'a> {
    id: u32,
    name: &'a str,
    body: TestVariantBody,
}

enum TestVariantBody {
    Unit,
    Payload(TestKind),
}

enum TestKind {
    Nat64,
    Enum(u32),
    List(Box<Self>),
}

fn unit_type(id: u32, path: &str) -> TestType<'_> {
    TestType {
        id,
        path,
        variants: vec![TestVariant {
            id: 1,
            name: "Unit",
            body: TestVariantBody::Unit,
        }],
    }
}

fn reference_type(id: u32, path: &str, referenced_type_id: u32) -> TestType<'_> {
    TestType {
        id,
        path,
        variants: vec![TestVariant {
            id: 1,
            name: "Ref",
            body: TestVariantBody::Payload(TestKind::Enum(referenced_type_id)),
        }],
    }
}

fn encode_test_wire(version: u16, types: &[TestType<'_>]) -> Vec<u8> {
    let mut writer = CatalogWriter::new();
    writer.push_bytes(ACCEPTED_ENUM_CATALOG_MAGIC);
    writer.push_u16(version);
    writer
        .push_len(types.len())
        .expect("test type count should fit");
    for definition in types {
        writer.push_u32(definition.id);
        writer
            .push_string(definition.path)
            .expect("test path should fit");
        writer.push_u8(ORDERING_EQUALITY_ONLY);
        writer
            .push_len(definition.variants.len())
            .expect("test variant count should fit");
        for variant in &definition.variants {
            writer.push_u32(variant.id);
            writer
                .push_string(variant.name)
                .expect("test variant name should fit");
            match &variant.body {
                TestVariantBody::Unit => writer.push_u8(VARIANT_BODY_UNIT),
                TestVariantBody::Payload(kind) => {
                    writer.push_u8(VARIANT_BODY_PAYLOAD);
                    writer.push_u8(STORAGE_DECODE_BY_KIND);
                    encode_test_kind(&mut writer, kind);
                }
            }
        }
    }
    writer.finish().expect("test wire should stay bounded")
}

fn encode_test_kind(writer: &mut CatalogWriter, kind: &TestKind) {
    match kind {
        TestKind::Nat64 => writer.push_u8(KIND_NAT64),
        TestKind::Enum(type_id) => {
            writer.push_u8(KIND_ENUM);
            writer.push_u32(*type_id);
        }
        TestKind::List(inner) => {
            writer.push_u8(KIND_LIST);
            encode_test_kind(writer, inner);
        }
    }
}

#[test]
fn accepted_enum_catalog_empty_wire_vector_is_frozen() {
    let catalog =
        build_initial_accepted_enum_catalog_from_kinds(&[]).expect("empty catalog should build");

    assert_eq!(
        encode_accepted_enum_catalog(&catalog).expect("empty catalog should encode"),
        [
            0x49, 0x43, 0x59, 0x44, 0x42, 0x45, 0x4e, 0x43, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        ],
    );
}

#[test]
fn accepted_enum_catalog_codec_round_trips_canonical_catalog() {
    let catalog = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("catalog should build");
    let encoded = encode_accepted_enum_catalog(&catalog).expect("catalog should encode");
    let decoded = decode_accepted_enum_catalog(&encoded).expect("catalog should decode");

    assert_eq!(decoded, catalog);
}

#[test]
fn accepted_enum_catalog_encoding_erases_proposal_and_declaration_order() {
    let first = build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_KIND])
        .expect("first catalog should build");
    let second =
        build_initial_accepted_enum_catalog_from_kinds(&[ALPHA_REORDERED_KIND, ALPHA_KIND])
            .expect("reordered catalog should build");

    assert_eq!(
        encode_accepted_enum_catalog(&first).expect("first catalog should encode"),
        encode_accepted_enum_catalog(&second).expect("second catalog should encode"),
    );
}

#[test]
fn accepted_enum_catalog_codec_round_trips_empty_catalog() {
    let catalog =
        build_initial_accepted_enum_catalog_from_kinds(&[]).expect("empty catalog should build");
    let encoded = encode_accepted_enum_catalog(&catalog).expect("empty catalog should encode");

    assert_eq!(
        decode_accepted_enum_catalog(&encoded).expect("empty catalog should decode"),
        catalog,
    );
}

#[test]
fn accepted_enum_catalog_decode_rejects_future_codec_version() {
    let encoded = encode_test_wire(ACCEPTED_ENUM_CATALOG_CODEC_VERSION + 1, &[]);
    let error =
        decode_accepted_enum_catalog(&encoded).expect_err("future catalog codec must fail closed");

    assert_eq!(error.class(), ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(error.origin(), ErrorOrigin::Serialize);
}

#[test]
fn accepted_enum_catalog_decode_rejects_zero_and_unsorted_type_ids() {
    let zero = encode_test_wire(
        ACCEPTED_ENUM_CATALOG_CODEC_VERSION,
        &[unit_type(0, "codec::Zero")],
    );
    let unsorted = encode_test_wire(
        ACCEPTED_ENUM_CATALOG_CODEC_VERSION,
        &[unit_type(2, "codec::Second"), unit_type(1, "codec::First")],
    );

    for encoded in [zero, unsorted] {
        let error = decode_accepted_enum_catalog(&encoded)
            .expect_err("invalid type identity order must reject");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
    }
}

#[test]
fn accepted_enum_catalog_decode_rejects_duplicate_paths_and_variant_names() {
    let duplicate_paths = encode_test_wire(
        ACCEPTED_ENUM_CATALOG_CODEC_VERSION,
        &[unit_type(1, "codec::Same"), unit_type(2, "codec::Same")],
    );
    let duplicate_variant_names = encode_test_wire(
        ACCEPTED_ENUM_CATALOG_CODEC_VERSION,
        &[TestType {
            id: 1,
            path: "codec::Variants",
            variants: vec![
                TestVariant {
                    id: 1,
                    name: "Unit",
                    body: TestVariantBody::Unit,
                },
                TestVariant {
                    id: 2,
                    name: "Unit",
                    body: TestVariantBody::Unit,
                },
            ],
        }],
    );

    for encoded in [duplicate_paths, duplicate_variant_names] {
        assert_eq!(
            decode_accepted_enum_catalog(&encoded)
                .expect_err("duplicate catalog names must reject")
                .class(),
            ErrorClass::Corruption,
        );
    }
}

#[test]
fn accepted_enum_catalog_decode_rejects_unknown_reference() {
    let encoded = encode_test_wire(
        ACCEPTED_ENUM_CATALOG_CODEC_VERSION,
        &[reference_type(1, "codec::UnknownRef", 2)],
    );

    assert_eq!(
        decode_accepted_enum_catalog(&encoded)
            .expect_err("unknown enum reference must reject")
            .class(),
        ErrorClass::Corruption,
    );
}

#[test]
fn accepted_enum_catalog_decode_rejects_mutual_recursion() {
    let encoded = encode_test_wire(
        ACCEPTED_ENUM_CATALOG_CODEC_VERSION,
        &[
            reference_type(1, "codec::Left", 2),
            reference_type(2, "codec::Right", 1),
        ],
    );

    assert_eq!(
        decode_accepted_enum_catalog(&encoded)
            .expect_err("recursive enum graph must reject")
            .class(),
        ErrorClass::Corruption,
    );
}

#[test]
fn accepted_enum_catalog_decode_rejects_excessive_value_kind_depth() {
    let mut kind = TestKind::Nat64;
    for _ in 0..=MAX_ENUM_CONTRACT_DEPTH {
        kind = TestKind::List(Box::new(kind));
    }
    let encoded = encode_test_wire(
        ACCEPTED_ENUM_CATALOG_CODEC_VERSION,
        &[TestType {
            id: 1,
            path: "codec::Deep",
            variants: vec![TestVariant {
                id: 1,
                name: "Deep",
                body: TestVariantBody::Payload(kind),
            }],
        }],
    );

    assert_eq!(
        decode_accepted_enum_catalog(&encoded)
            .expect_err("excessive contract nesting must reject")
            .class(),
        ErrorClass::Corruption,
    );
}

#[test]
fn accepted_enum_catalog_decode_rejects_trailing_and_oversized_input() {
    let mut trailing = encode_test_wire(ACCEPTED_ENUM_CATALOG_CODEC_VERSION, &[]);
    trailing.push(0xff);
    let oversized = vec![0_u8; MAX_ACCEPTED_ENUM_CATALOG_BYTES + 1];

    for encoded in [trailing, oversized] {
        let error = decode_accepted_enum_catalog(&encoded)
            .expect_err("noncanonical catalog envelope must reject");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Store);
    }
}

#[test]
fn accepted_enum_catalog_writer_stops_at_encoded_size_bound() {
    let mut writer = CatalogWriter::new();
    writer.push_bytes(&vec![0_u8; MAX_ACCEPTED_ENUM_CATALOG_BYTES]);
    writer.push_u8(1);

    let error = writer
        .finish()
        .expect_err("writer must reject growth beyond the catalog bound");

    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(error.origin(), ErrorOrigin::Store);
}
