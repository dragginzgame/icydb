use super::*;

const ENTITY_64: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const ENTITY_64_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const FIELD_64_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FIELD_64_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const FIELD_64_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const FIELD_64_D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

#[test]
fn index_name_max_len_matches_limits() {
    let entity = EntityName::try_from_str(ENTITY_64).unwrap();
    let fields = [FIELD_64_A, FIELD_64_B, FIELD_64_C, FIELD_64_D];

    assert_eq!(entity.as_str().len(), MAX_ENTITY_NAME_LEN);
    for field in &fields {
        assert_eq!(field.len(), MAX_INDEX_FIELD_NAME_LEN);
    }
    assert_eq!(fields.len(), MAX_INDEX_FIELDS);

    let name = IndexName::try_from_parts(&entity, &fields).unwrap();
    assert_eq!(name.as_bytes().len(), MAX_INDEX_NAME_LEN);
}

#[test]
fn index_name_max_size_roundtrip_and_ordering() {
    let entity_a = EntityName::try_from_str(ENTITY_64).unwrap();
    let entity_b = EntityName::try_from_str(ENTITY_64_B).unwrap();

    let fields_a = [FIELD_64_A, FIELD_64_A, FIELD_64_A, FIELD_64_A];
    let fields_b = [FIELD_64_B, FIELD_64_B, FIELD_64_B, FIELD_64_B];

    let idx_a = IndexName::try_from_parts(&entity_a, &fields_a).unwrap();
    let idx_b = IndexName::try_from_parts(&entity_b, &fields_b).unwrap();

    let decoded = IndexName::from_bytes(&idx_a.to_bytes()).unwrap();
    assert_eq!(idx_a, decoded);

    assert_eq!(idx_a.cmp(&idx_b), idx_a.to_bytes().cmp(&idx_b.to_bytes()));
}

#[test]
fn rejects_too_many_index_fields() {
    let entity = EntityName::try_from_str("entity").unwrap();
    let fields = ["a", "b", "c", "d", "e"];

    let err = IndexName::try_from_parts(&entity, &fields).unwrap_err();
    assert!(matches!(err, IndexNameError::TooManyFields { .. }));
}

#[test]
fn rejects_index_field_over_len() {
    let entity = EntityName::try_from_str("entity").unwrap();
    let long_field = "a".repeat(MAX_INDEX_FIELD_NAME_LEN + 1);

    let err = IndexName::try_from_parts(&entity, &[long_field.as_str()]).unwrap_err();
    assert!(matches!(err, IndexNameError::FieldTooLong { .. }));
}

#[test]
fn entity_try_from_str_roundtrip() {
    let e = EntityName::try_from_str("user").unwrap();
    assert_eq!(e.len(), 4);
    assert_eq!(e.as_str(), "user");
}

#[test]
fn entity_rejects_empty() {
    let err = EntityName::try_from_str("").unwrap_err();
    assert!(matches!(err, EntityNameError::Empty));
}

#[test]
fn entity_rejects_len_over_max() {
    let s = "a".repeat(MAX_ENTITY_NAME_LEN + 1);
    let err = EntityName::try_from_str(&s).unwrap_err();
    assert!(matches!(err, EntityNameError::TooLong { .. }));
}

#[test]
fn entity_rejects_non_ascii() {
    let err = EntityName::try_from_str("usér").unwrap_err();
    assert!(matches!(err, EntityNameError::NonAscii));
}

#[test]
fn entity_storage_roundtrip() {
    let e = EntityName::try_from_str("entity_name").unwrap();
    let bytes = e.to_bytes();
    let decoded = EntityName::from_bytes(&bytes).unwrap();
    assert_eq!(e, decoded);
}

#[test]
fn entity_max_storable_is_ascii_utf8() {
    let max = EntityName::max_storable();
    assert_eq!(max.len(), MAX_ENTITY_NAME_LEN);
    assert!(max.as_str().is_ascii());
}

#[test]
fn entity_rejects_invalid_size() {
    let buf = vec![0u8; EntityName::STORED_SIZE_USIZE - 1];
    assert!(matches!(
        EntityName::from_bytes(&buf),
        Err(IdentityDecodeError::InvalidSize)
    ));
}

#[test]
fn entity_rejects_len_over_max_from_bytes() {
    let mut buf = [0u8; EntityName::STORED_SIZE_USIZE];
    buf[0] = (MAX_ENTITY_NAME_LEN as u8).saturating_add(1);
    assert!(matches!(
        EntityName::from_bytes(&buf),
        Err(IdentityDecodeError::InvalidLength)
    ));
}

#[test]
fn entity_rejects_non_ascii_from_bytes() {
    let mut buf = [0u8; EntityName::STORED_SIZE_USIZE];
    buf[0] = 1;
    buf[1] = 0xFF;
    assert!(matches!(
        EntityName::from_bytes(&buf),
        Err(IdentityDecodeError::NonAscii)
    ));
}

#[test]
fn entity_rejects_non_zero_padding() {
    let e = EntityName::try_from_str("user").unwrap();
    let mut bytes = e.to_bytes();
    bytes[1 + e.len()] = b'x';

    assert!(matches!(
        EntityName::from_bytes(&bytes),
        Err(IdentityDecodeError::NonZeroPadding)
    ));
}

#[test]
fn entity_ordering_matches_bytes() {
    let a = EntityName::try_from_str("abc").unwrap();
    let b = EntityName::try_from_str("abd").unwrap();
    let c = EntityName::try_from_str("abcx").unwrap();

    assert_eq!(a.cmp(&b), a.to_bytes().cmp(&b.to_bytes()));
    assert_eq!(a.cmp(&c), a.to_bytes().cmp(&c.to_bytes()));
}

#[test]
fn entity_ordering_is_not_lexicographic() {
    let z = EntityName::try_from_str("z").unwrap();
    let aa = EntityName::try_from_str("aa").unwrap();

    assert_eq!(z.cmp(&aa), Ordering::Less);
    assert_eq!(z.to_bytes().cmp(&aa.to_bytes()), Ordering::Less);
    assert_eq!(z.as_str().cmp(aa.as_str()), Ordering::Greater);
}

#[test]
fn index_single_field_format() {
    let entity = EntityName::try_from_str("user").unwrap();
    let idx = IndexName::try_from_parts(&entity, &["email"]).unwrap();

    assert_eq!(idx.as_str(), "user|email");
}

#[test]
fn index_field_order_is_preserved() {
    let entity = EntityName::try_from_str("user").unwrap();
    let idx = IndexName::try_from_parts(&entity, &["a", "b", "c"]).unwrap();

    assert_eq!(idx.as_str(), "user|a|b|c");
}

#[test]
fn index_storage_roundtrip() {
    let entity = EntityName::try_from_str("user").unwrap();
    let idx = IndexName::try_from_parts(&entity, &["a", "b"]).unwrap();

    let bytes = idx.to_bytes();
    let decoded = IndexName::from_bytes(&bytes).unwrap();
    assert_eq!(idx, decoded);
}

#[test]
fn index_max_storable_is_ascii_utf8() {
    let max = IndexName::max_storable();
    assert_eq!(max.as_bytes().len(), MAX_INDEX_NAME_LEN);
    assert!(max.as_str().is_ascii());
}

#[test]
fn index_rejects_non_ascii_from_bytes() {
    let mut buf = [0u8; IndexName::STORED_SIZE_USIZE];
    buf[..2].copy_from_slice(&1u16.to_be_bytes());
    buf[2] = 0xFF;

    assert!(matches!(
        IndexName::from_bytes(&buf),
        Err(IdentityDecodeError::NonAscii)
    ));
}

// ------------------------------------------------------------------
// FUZZING (deterministic)
// ------------------------------------------------------------------

fn gen_ascii(seed: u64, max_len: usize) -> String {
    let len = (seed as usize % max_len).max(1);
    let mut out = String::with_capacity(len);

    let mut x = seed;
    for _ in 0..len {
        x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let c = b'a' + (x % 26) as u8;
        out.push(c as char);
    }

    out
}

#[test]
fn fuzz_entity_name_roundtrip_and_ordering() {
    let mut prev: Option<EntityName> = None;

    for i in 1..=1_000u64 {
        let s = gen_ascii(i, MAX_ENTITY_NAME_LEN);
        let e = EntityName::try_from_str(&s).unwrap();

        let bytes = e.to_bytes();
        let decoded = EntityName::from_bytes(&bytes).unwrap();
        assert_eq!(e, decoded);

        if let Some(p) = prev {
            assert_eq!(p.cmp(&e), p.to_bytes().cmp(&e.to_bytes()));
        }

        prev = Some(e);
    }
}

#[test]
fn fuzz_index_name_roundtrip_and_ordering() {
    let entity = EntityName::try_from_str("entity").unwrap();
    let mut prev: Option<IndexName> = None;

    for i in 1..=1_000u64 {
        let field_count = (i as usize % MAX_INDEX_FIELDS).max(1);

        let mut fields = Vec::with_capacity(field_count);
        for f in 0..field_count {
            let s = gen_ascii(i * 31 + f as u64, MAX_INDEX_FIELD_NAME_LEN);
            fields.push(s);
        }

        let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
        let idx = IndexName::try_from_parts(&entity, &field_refs).unwrap();

        let bytes = idx.to_bytes();
        let decoded = IndexName::from_bytes(&bytes).unwrap();
        assert_eq!(idx, decoded);

        if let Some(p) = prev {
            assert_eq!(p.cmp(&idx), p.to_bytes().cmp(&idx.to_bytes()));
        }

        prev = Some(idx);
    }
}
