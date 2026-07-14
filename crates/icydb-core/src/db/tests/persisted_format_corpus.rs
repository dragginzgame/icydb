//! Deterministic malformed-byte corpus for DB-owned persisted decode envelopes.

use crate::{
    db::{
        codec::{ROW_FORMAT_VERSION_CURRENT, decode_row_payload_bytes},
        commit::{COMMIT_MARKER_FORMAT_VERSION_CURRENT, validate_commit_marker_envelope_for_tests},
        cursor::{
            ContinuationSignature, ContinuationToken, CursorBoundary, GroupedContinuationToken,
        },
        data::{
            decode_canonical_value_storage_bytes, decode_structural_field_by_kind_bytes,
            decode_structural_value_storage_bytes, validate_structural_field_by_kind_bytes,
            validate_structural_value_storage_bytes,
        },
        database_format::{DATABASE_BOOT_RECORD_BYTES, crc32c, validate_current_boot_record},
        direction::Direction,
        index::{IndexEntryValue, IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey},
        journal::{JournalBatch, JournalSequence, decode_journal_batch, encode_journal_batch},
        key_taxonomy::{PrimaryKeyComponent, PrimaryKeyValue},
        schema::{
            AcceptedSchemaRevision, CandidateSchemaRevision, decode_persisted_schema_snapshot,
            empty_accepted_schema_candidate_for_tests,
            validate_accepted_enum_catalog_format_for_tests,
            validate_accepted_schema_bundle_format_for_tests,
            validate_raw_schema_snapshot_format_for_tests,
        },
    },
    model::field::FieldKind,
    types::EntityTag,
};
use ic_stable_structures::{Memory, Storable, VectorMemory};
use std::borrow::Cow;

const VALUE_TAG_TEXT: u8 = 0x12;
const VALUE_TAG_LIST: u8 = 0x20;
const CANONICAL_ENUM_TAG: u8 = 0x84;
static TEXT_FIELD_KIND: FieldKind = FieldKind::Text { max_len: None };
static TEXT_LIST_FIELD_KIND: FieldKind = FieldKind::List(&TEXT_FIELD_KIND);

fn assert_err<T, E>(label: &str, result: Result<T, E>) {
    assert!(result.is_err(), "{label} should fail closed");
}

fn row_envelope(magic: [u8; 2], version: u8, declared_len: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(7 + payload.len());
    bytes.extend_from_slice(&magic);
    bytes.push(version);
    bytes.extend_from_slice(&declared_len.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn valid_raw_index_key() -> RawIndexStoreKey {
    let component = vec![0x42];
    IndexKey::new_from_components_with_primary_key_value(
        &IndexId::new(EntityTag::new(0x1902), 1),
        IndexKeyKind::User,
        std::slice::from_ref(&component),
        &PrimaryKeyValue::from(PrimaryKeyComponent::Nat64(7)),
    )
    .expect("test index key should build")
    .to_raw()
    .expect("test index key should encode")
}

fn unchecked_raw_index_key(bytes: Vec<u8>) -> RawIndexStoreKey {
    <RawIndexStoreKey as Storable>::from_bytes(Cow::Owned(bytes))
}

fn value_storage_len_prefixed(tag: u8, declared_len: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + payload.len());
    bytes.push(tag);
    bytes.extend_from_slice(&declared_len.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

fn memory_with_prefix(bytes: &[u8]) -> VectorMemory {
    let memory = VectorMemory::default();
    assert!(memory.grow(1) >= 0, "test memory should grow");
    memory.write(0, bytes);
    memory
}

fn database_boot_record(version: u16, state: u8) -> [u8; DATABASE_BOOT_RECORD_BYTES] {
    let mut bytes = [0_u8; DATABASE_BOOT_RECORD_BYTES];
    bytes[..8].copy_from_slice(b"ICYDBBOT");
    bytes[8..10].copy_from_slice(&version.to_be_bytes());
    bytes[10] = state;
    let checksum_offset = DATABASE_BOOT_RECORD_BYTES - size_of::<u32>();
    let checksum = crc32c(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    bytes
}

fn raw_schema_snapshot_envelope(version: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(25 + payload.len());
    bytes.extend_from_slice(b"ICYDBSCH");
    bytes.push(version);
    bytes.extend_from_slice(&[0; 16]);
    bytes.extend_from_slice(payload);
    bytes
}

fn accepted_enum_catalog_envelope(version: u16) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(14);
    bytes.extend_from_slice(b"ICYDBENC");
    bytes.extend_from_slice(&version.to_be_bytes());
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes
}

fn commit_marker_envelope(version: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(5 + payload.len());
    bytes.push(version);
    bytes.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("test marker payload length should fit")
            .to_le_bytes(),
    );
    bytes.extend_from_slice(payload);
    bytes
}

fn canonical_enum_envelope(
    type_id: u32,
    variant_id: u32,
    body_tag: u8,
    declared_len: u32,
    payload: &[u8],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(14 + payload.len());
    bytes.push(CANONICAL_ENUM_TAG);
    bytes.extend_from_slice(&type_id.to_be_bytes());
    bytes.extend_from_slice(&variant_id.to_be_bytes());
    bytes.push(body_tag);
    bytes.extend_from_slice(&declared_len.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

#[test]
fn database_boot_record_malformed_corpus_fails_closed() {
    assert_err(
        "missing database boot record",
        validate_current_boot_record(&VectorMemory::default()),
    );

    let mut corrupt_magic = database_boot_record(1, 1);
    corrupt_magic[0] = b'X';
    let mut corrupt_checksum = database_boot_record(1, 1);
    corrupt_checksum[DATABASE_BOOT_RECORD_BYTES - 1] ^= 0xff;
    let cases = [
        ("pre-0.200 database version", database_boot_record(0, 1)),
        ("future database version", database_boot_record(2, 1)),
        ("unknown database boot state", database_boot_record(1, 0xff)),
        ("corrupt database boot magic", corrupt_magic),
        ("corrupt database boot checksum", corrupt_checksum),
    ];

    for (label, bytes) in cases {
        assert_err(
            label,
            validate_current_boot_record(&memory_with_prefix(&bytes)),
        );
    }
}

#[test]
fn persisted_row_envelope_malformed_corpus_fails_closed() {
    let cases = [
        ("empty row envelope", Vec::new()),
        ("truncated row magic", vec![b'I']),
        (
            "bad row magic",
            row_envelope(*b"XX", ROW_FORMAT_VERSION_CURRENT, 0, &[]),
        ),
        (
            "future row version",
            row_envelope(*b"IR", ROW_FORMAT_VERSION_CURRENT.saturating_add(1), 0, &[]),
        ),
        (
            "pre-0.200 row version",
            row_envelope(*b"IR", ROW_FORMAT_VERSION_CURRENT.saturating_sub(1), 0, &[]),
        ),
        (
            "declared row payload too long",
            row_envelope(*b"IR", ROW_FORMAT_VERSION_CURRENT, 4, &[0xAA]),
        ),
        (
            "trailing row payload bytes",
            row_envelope(*b"IR", ROW_FORMAT_VERSION_CURRENT, 0, &[0xAA]),
        ),
        (
            "huge declared row payload without bytes",
            row_envelope(*b"IR", ROW_FORMAT_VERSION_CURRENT, u32::MAX, &[]),
        ),
    ];

    for (label, bytes) in cases {
        assert_err(label, decode_row_payload_bytes(&bytes));
    }
}

#[test]
fn persisted_schema_snapshot_malformed_corpus_fails_closed() {
    let cases = [
        ("empty schema snapshot", Vec::new()),
        ("truncated candid schema snapshot", vec![0x44]),
        (
            "nonsense schema snapshot bytes",
            vec![0xDE, 0xAD, 0xBE, 0xEF],
        ),
    ];

    for (label, bytes) in cases {
        assert_err(label, decode_persisted_schema_snapshot(&bytes));
    }
}

#[test]
fn raw_schema_snapshot_envelope_malformed_corpus_fails_closed() {
    let cases = [
        ("empty raw schema snapshot", Vec::new()),
        (
            "headerless raw schema snapshot",
            vec![0x44, 0x49, 0x44, 0x4c],
        ),
        (
            "pre-0.200 raw schema version",
            raw_schema_snapshot_envelope(0, &[0x44, 0x49, 0x44, 0x4c]),
        ),
        (
            "future raw schema version",
            raw_schema_snapshot_envelope(2, &[0x44, 0x49, 0x44, 0x4c]),
        ),
        (
            "current raw schema with corrupt payload",
            raw_schema_snapshot_envelope(1, &[0xDE, 0xAD, 0xBE, 0xEF]),
        ),
    ];

    for (label, bytes) in cases {
        assert_err(label, validate_raw_schema_snapshot_format_for_tests(bytes));
    }
}

#[test]
fn accepted_enum_catalog_malformed_corpus_fails_closed() {
    let mut corrupt_magic = accepted_enum_catalog_envelope(1);
    corrupt_magic[0] = b'X';
    let mut trailing = accepted_enum_catalog_envelope(1);
    trailing.push(0xff);
    let cases = [
        ("empty accepted enum catalog", Vec::new()),
        ("truncated accepted enum catalog", b"ICYDBENC".to_vec()),
        (
            "pre-0.200 accepted enum catalog version",
            accepted_enum_catalog_envelope(0),
        ),
        (
            "future accepted enum catalog version",
            accepted_enum_catalog_envelope(2),
        ),
        ("corrupt accepted enum catalog magic", corrupt_magic),
        ("trailing accepted enum catalog bytes", trailing),
    ];

    for (label, bytes) in cases {
        assert_err(
            label,
            validate_accepted_enum_catalog_format_for_tests(&bytes),
        );
    }
}

#[test]
fn accepted_schema_publication_malformed_corpus_fails_closed() {
    let candidate = empty_accepted_schema_candidate_for_tests(
        "format::Corpus",
        AcceptedSchemaRevision::INITIAL,
    );

    let mut old_bundle = candidate.encoded_bundle().to_vec();
    old_bundle[8..10].copy_from_slice(&0_u16.to_be_bytes());
    let mut future_bundle = candidate.encoded_bundle().to_vec();
    future_bundle[8..10].copy_from_slice(&2_u16.to_be_bytes());
    let mut corrupt_bundle_magic = candidate.encoded_bundle().to_vec();
    corrupt_bundle_magic[0] = b'X';
    let bundle_cases = [
        ("empty accepted schema bundle", Vec::new()),
        ("truncated accepted schema bundle", b"ICYDBASB".to_vec()),
        ("pre-0.200 accepted schema bundle version", old_bundle),
        ("future accepted schema bundle version", future_bundle),
        ("corrupt accepted schema bundle magic", corrupt_bundle_magic),
    ];
    for (label, bytes) in bundle_cases {
        assert_err(
            label,
            validate_accepted_schema_bundle_format_for_tests(&bytes),
        );
    }

    let mut old_root = candidate.encoded_root().to_vec();
    old_root[8..10].copy_from_slice(&0_u16.to_be_bytes());
    let checksum_offset = old_root.len() - size_of::<u32>();
    let checksum = crc32c(&old_root[..checksum_offset]);
    old_root[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    let mut future_root = candidate.encoded_root().to_vec();
    future_root[8..10].copy_from_slice(&2_u16.to_be_bytes());
    let checksum_offset = future_root.len() - size_of::<u32>();
    let checksum = crc32c(&future_root[..checksum_offset]);
    future_root[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    let mut corrupt_root = candidate.encoded_root().to_vec();
    let checksum_byte = corrupt_root.len() - 1;
    corrupt_root[checksum_byte] ^= 0xff;
    let root_cases = [
        ("empty accepted schema root", Vec::new()),
        ("truncated accepted schema root", b"ICYDBASR".to_vec()),
        ("pre-0.200 accepted schema root version", old_root),
        ("future accepted schema root version", future_root),
        ("corrupt accepted schema root checksum", corrupt_root),
    ];
    for (label, bytes) in root_cases {
        assert_err(
            label,
            CandidateSchemaRevision::from_encoded(candidate.encoded_bundle().to_vec(), bytes),
        );
    }
}

#[test]
fn journal_batch_malformed_corpus_fails_closed() {
    let batch = JournalBatch::new([0x11; 16], [0x22; 16], JournalSequence::new(1), Vec::new())
        .expect("empty test journal batch should be valid");
    let current = encode_journal_batch(&batch).expect("test journal batch should encode");
    let mut old_version = current.clone();
    old_version[4] = current[4].saturating_sub(1);
    let mut future_version = current.clone();
    future_version[4] = current[4].saturating_add(1);
    let mut corrupt_magic = current.clone();
    corrupt_magic[0] = b'X';
    let mut truncated = current.clone();
    truncated.pop();
    let mut trailing = current;
    trailing.push(0xff);
    let cases = [
        ("empty journal batch", Vec::new()),
        ("pre-0.200 journal batch version", old_version),
        ("future journal batch version", future_version),
        ("corrupt journal batch magic", corrupt_magic),
        ("truncated journal batch", truncated),
        ("trailing journal batch bytes", trailing),
    ];

    for (label, bytes) in cases {
        assert_err(label, decode_journal_batch(&bytes));
    }
}

#[test]
fn commit_marker_envelope_malformed_corpus_fails_closed() {
    let mut wrong_length = commit_marker_envelope(COMMIT_MARKER_FORMAT_VERSION_CURRENT, &[0xaa]);
    wrong_length[1..5].copy_from_slice(&2_u32.to_le_bytes());
    let cases = [
        (
            "truncated current commit marker",
            vec![COMMIT_MARKER_FORMAT_VERSION_CURRENT],
        ),
        (
            "pre-0.200 commit marker version",
            commit_marker_envelope(COMMIT_MARKER_FORMAT_VERSION_CURRENT.saturating_sub(1), &[]),
        ),
        (
            "future commit marker version",
            commit_marker_envelope(COMMIT_MARKER_FORMAT_VERSION_CURRENT.saturating_add(1), &[]),
        ),
        (
            "current commit marker with corrupt payload",
            commit_marker_envelope(COMMIT_MARKER_FORMAT_VERSION_CURRENT, &[]),
        ),
        (
            "current commit marker with corrupt nonempty payload",
            commit_marker_envelope(COMMIT_MARKER_FORMAT_VERSION_CURRENT, &[0xaa]),
        ),
        ("commit marker length mismatch", wrong_length),
    ];

    for (label, bytes) in cases {
        assert_err(label, validate_commit_marker_envelope_for_tests(&bytes));
    }
}

#[test]
fn continuation_token_malformed_corpus_fails_closed() {
    let scalar = ContinuationToken::new_with_direction(
        ContinuationSignature::from_bytes([0x33; 32]),
        CursorBoundary { slots: Vec::new() },
        Direction::Asc,
        0,
    )
    .encode()
    .expect("current scalar token should encode");
    let grouped = GroupedContinuationToken::new_with_direction(
        ContinuationSignature::from_bytes([0x44; 32]),
        Vec::new(),
        Direction::Asc,
        0,
    )
    .encode()
    .expect("current grouped token should encode");

    let mut old_scalar = scalar.clone();
    old_scalar[0] = scalar[0].saturating_sub(1);
    let mut future_scalar = scalar.clone();
    future_scalar[0] = scalar[0].saturating_add(1);
    let mut truncated_scalar = scalar;
    truncated_scalar.pop();
    assert_err(
        "pre-0.200 scalar token",
        ContinuationToken::decode(&old_scalar),
    );
    assert_err(
        "future scalar token",
        ContinuationToken::decode(&future_scalar),
    );
    assert_err(
        "truncated scalar token",
        ContinuationToken::decode(&truncated_scalar),
    );

    let mut old_grouped = grouped.clone();
    old_grouped[0] = grouped[0].saturating_sub(1);
    let mut future_grouped = grouped.clone();
    future_grouped[0] = grouped[0].saturating_add(1);
    let mut truncated_grouped = grouped;
    truncated_grouped.pop();
    assert_err(
        "pre-0.200 grouped token",
        GroupedContinuationToken::decode(&old_grouped),
    );
    assert_err(
        "future grouped token",
        GroupedContinuationToken::decode(&future_grouped),
    );
    assert_err(
        "truncated grouped token",
        GroupedContinuationToken::decode(&truncated_grouped),
    );
}

#[test]
fn persisted_index_envelope_malformed_corpus_fails_closed() {
    let valid_key = valid_raw_index_key();
    let mut unknown_kind = valid_key.as_bytes().to_vec();
    unknown_kind[0] = 0xFF;
    let mut truncated = valid_key.as_bytes().to_vec();
    truncated.pop();
    let mut trailing = valid_key.as_bytes().to_vec();
    trailing.push(0x42);

    let key_cases = [
        ("empty index key", Vec::new()),
        ("unknown index key kind", unknown_kind),
        ("truncated index key", truncated),
        ("trailing index key bytes", trailing),
    ];
    for (label, bytes) in key_cases {
        let raw = unchecked_raw_index_key(bytes);
        assert_err(label, IndexKey::try_from_raw(&raw));
    }

    let entry_cases = [
        ("empty index entry", Vec::new()),
        ("invalid index entry witness", vec![0xFF]),
        ("oversized index entry", vec![0; 2]),
    ];
    for (label, bytes) in entry_cases {
        let entry = IndexEntryValue::from_bytes(Cow::Owned(bytes));
        assert_err(label, entry.validate());
    }
}

#[test]
fn structural_value_storage_malformed_corpus_fails_closed() {
    let cases = [
        ("empty structural value", Vec::new()),
        ("unknown structural value tag", vec![0xFF]),
        (
            "truncated structural text",
            value_storage_len_prefixed(VALUE_TAG_TEXT, 4, b"a"),
        ),
        (
            "huge declared structural list without items",
            value_storage_len_prefixed(VALUE_TAG_LIST, u32::MAX, &[]),
        ),
        (
            "trailing structural bytes",
            value_storage_len_prefixed(VALUE_TAG_LIST, 0, &[0x00]),
        ),
    ];

    for (label, bytes) in cases {
        assert_err(label, validate_structural_value_storage_bytes(&bytes));
        assert_err(label, decode_structural_value_storage_bytes(&bytes));
    }
}

#[test]
fn canonical_enum_value_malformed_corpus_fails_closed() {
    let cases = [
        ("truncated canonical enum", vec![CANONICAL_ENUM_TAG]),
        (
            "zero canonical enum type ID",
            canonical_enum_envelope(0, 1, 0, 0, &[]),
        ),
        (
            "zero canonical enum variant ID",
            canonical_enum_envelope(1, 0, 0, 0, &[]),
        ),
        (
            "unknown canonical enum body tag",
            canonical_enum_envelope(1, 1, 0xff, 0, &[]),
        ),
        (
            "unit canonical enum with payload",
            canonical_enum_envelope(1, 1, 0, 1, &[0xaa]),
        ),
        (
            "payload canonical enum without payload",
            canonical_enum_envelope(1, 1, 1, 0, &[]),
        ),
        (
            "canonical enum payload length mismatch",
            canonical_enum_envelope(1, 1, 1, 4, &[0xaa]),
        ),
    ];

    for (label, bytes) in cases {
        assert_err(label, decode_canonical_value_storage_bytes(&bytes));
    }
}

#[test]
fn structural_field_by_kind_malformed_corpus_fails_closed() {
    let cases = [
        (
            "truncated by-kind text",
            FieldKind::Text { max_len: None },
            value_storage_len_prefixed(VALUE_TAG_TEXT, 4, b"a"),
        ),
        ("empty by-kind nat64", FieldKind::Nat64, Vec::new()),
        (
            "huge declared by-kind text list without items",
            TEXT_LIST_FIELD_KIND,
            value_storage_len_prefixed(VALUE_TAG_LIST, u32::MAX, &[]),
        ),
    ];

    for (label, kind, bytes) in cases {
        assert_err(label, validate_structural_field_by_kind_bytes(&bytes, kind));
        assert_err(label, decode_structural_field_by_kind_bytes(&bytes, kind));
    }
}
