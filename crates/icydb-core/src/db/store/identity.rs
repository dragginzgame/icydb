#![allow(clippy::cast_possible_truncation)]

use crate::MAX_INDEX_FIELDS;
use std::{
    cmp::Ordering,
    fmt::{self, Display},
};

///
/// Constants
///

pub const MAX_ENTITY_NAME_LEN: usize = 64;
pub const MAX_INDEX_FIELD_NAME_LEN: usize = 64;
pub const MAX_INDEX_NAME_LEN: usize =
    MAX_ENTITY_NAME_LEN + (MAX_INDEX_FIELDS * (MAX_INDEX_FIELD_NAME_LEN + 1));

///
/// EntityName
///

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct EntityName {
    pub len: u8,
    pub bytes: [u8; MAX_ENTITY_NAME_LEN],
}

impl EntityName {
    pub const STORED_SIZE: u32 = 1 + MAX_ENTITY_NAME_LEN as u32;
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE as usize;

    #[must_use]
    pub const fn from_static(name: &'static str) -> Self {
        let bytes = name.as_bytes();
        let len = bytes.len();

        assert!(
            len > 0 && len <= MAX_ENTITY_NAME_LEN,
            "entity name length out of bounds"
        );

        let mut out = [0u8; MAX_ENTITY_NAME_LEN];
        let mut i = 0;
        while i < len {
            let b = bytes[i];
            assert!(b.is_ascii(), "entity name must be ASCII");
            out[i] = b;
            i += 1;
        }

        Self {
            len: len as u8,
            bytes: out,
        }
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len()]
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        // Safe because we enforce ASCII
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::STORED_SIZE_USIZE] {
        let mut out = [0u8; Self::STORED_SIZE_USIZE];
        out[0] = self.len;
        out[1..].copy_from_slice(&self.bytes);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != Self::STORED_SIZE_USIZE {
            return Err("corrupted EntityName: invalid size");
        }

        let len = bytes[0] as usize;
        if len == 0 || len > MAX_ENTITY_NAME_LEN {
            return Err("corrupted EntityName: invalid length");
        }
        if !bytes[1..=len].is_ascii() {
            return Err("corrupted EntityName: invalid encoding");
        }
        if bytes[1 + len..].iter().any(|&b| b != 0) {
            return Err("corrupted EntityName: non-zero padding");
        }

        let mut name = [0u8; MAX_ENTITY_NAME_LEN];
        name.copy_from_slice(&bytes[1..]);

        Ok(Self {
            len: len as u8,
            bytes: name,
        })
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        Self::from_bytes(bytes)
    }

    #[must_use]
    pub const fn max_storable() -> Self {
        Self {
            len: MAX_ENTITY_NAME_LEN as u8,
            bytes: [b'z'; MAX_ENTITY_NAME_LEN],
        }
    }
}

impl TryFrom<&[u8]> for EntityName {
    type Error = &'static str;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl Ord for EntityName {
    fn cmp(&self, other: &Self) -> Ordering {
        self.len
            .cmp(&other.len)
            .then_with(|| self.bytes.cmp(&other.bytes))
    }
}

impl PartialOrd for EntityName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for EntityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for EntityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EntityName({})", self.as_str())
    }
}

///
/// IndexName
///

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct IndexName {
    pub len: u16,
    pub bytes: [u8; MAX_INDEX_NAME_LEN],
}

impl IndexName {
    pub const STORED_SIZE: u32 = 2 + MAX_INDEX_NAME_LEN as u32;
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE as usize;

    #[must_use]
    pub fn from_parts(entity: &EntityName, fields: &[&str]) -> Self {
        assert!(
            fields.len() <= MAX_INDEX_FIELDS,
            "index has too many fields"
        );

        let mut out = [0u8; MAX_INDEX_NAME_LEN];
        let mut len = 0usize;

        Self::push_ascii(&mut out, &mut len, entity.as_bytes());

        for field in fields {
            assert!(
                field.len() <= MAX_INDEX_FIELD_NAME_LEN,
                "index field name too long"
            );
            Self::push_ascii(&mut out, &mut len, b"|");
            Self::push_ascii(&mut out, &mut len, field.as_bytes());
        }

        Self {
            len: len as u16,
            bytes: out,
        }
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    #[must_use]
    pub fn to_bytes(self) -> [u8; Self::STORED_SIZE_USIZE] {
        let mut out = [0u8; Self::STORED_SIZE_USIZE];
        out[..2].copy_from_slice(&self.len.to_be_bytes());
        out[2..].copy_from_slice(&self.bytes);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != Self::STORED_SIZE_USIZE {
            return Err("corrupted IndexName: invalid size");
        }

        let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
        if len == 0 || len > MAX_INDEX_NAME_LEN {
            return Err("corrupted IndexName: invalid length");
        }
        if !bytes[2..2 + len].is_ascii() {
            return Err("corrupted IndexName: invalid encoding");
        }
        if bytes[2 + len..].iter().any(|&b| b != 0) {
            return Err("corrupted IndexName: non-zero padding");
        }

        let mut name = [0u8; MAX_INDEX_NAME_LEN];
        name.copy_from_slice(&bytes[2..]);

        Ok(Self {
            len: len as u16,
            bytes: name,
        })
    }

    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        Self::from_bytes(bytes)
    }

    fn push_ascii(out: &mut [u8; MAX_INDEX_NAME_LEN], len: &mut usize, bytes: &[u8]) {
        assert!(bytes.is_ascii(), "index name must be ASCII");
        assert!(
            *len + bytes.len() <= MAX_INDEX_NAME_LEN,
            "index name too long"
        );

        out[*len..*len + bytes.len()].copy_from_slice(bytes);
        *len += bytes.len();
    }

    #[must_use]
    pub const fn max_storable() -> Self {
        Self {
            len: MAX_INDEX_NAME_LEN as u16,
            bytes: [b'z'; MAX_INDEX_NAME_LEN],
        }
    }
}

impl TryFrom<&[u8]> for IndexName {
    type Error = &'static str;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl fmt::Debug for IndexName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IndexName({})", self.as_str())
    }
}

impl Display for IndexName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Ord for IndexName {
    fn cmp(&self, other: &Self) -> Ordering {
        self.len
            .cmp(&other.len)
            .then_with(|| self.bytes.cmp(&other.bytes))
    }
}

impl PartialOrd for IndexName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;

    const ENTITY_64: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const ENTITY_64_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const FIELD_64_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const FIELD_64_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const FIELD_64_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const FIELD_64_D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

    #[test]
    fn index_name_max_len_matches_limits() {
        let entity = EntityName::from_static(ENTITY_64);
        let fields = [FIELD_64_A, FIELD_64_B, FIELD_64_C, FIELD_64_D];

        assert_eq!(entity.as_str().len(), MAX_ENTITY_NAME_LEN);
        for field in &fields {
            assert_eq!(field.len(), MAX_INDEX_FIELD_NAME_LEN);
        }
        assert_eq!(fields.len(), MAX_INDEX_FIELDS);

        let name = IndexName::from_parts(&entity, &fields);

        assert_eq!(name.as_bytes().len(), MAX_INDEX_NAME_LEN);
    }

    #[test]
    fn index_name_max_size_roundtrip_and_ordering() {
        let entity_a = EntityName::from_static(ENTITY_64);
        let entity_b = EntityName::from_static(ENTITY_64_B);
        let fields_a = [FIELD_64_A, FIELD_64_A, FIELD_64_A, FIELD_64_A];
        let fields_b = [FIELD_64_B, FIELD_64_B, FIELD_64_B, FIELD_64_B];

        let idx_a = IndexName::from_parts(&entity_a, &fields_a);
        let idx_b = IndexName::from_parts(&entity_b, &fields_b);

        assert_eq!(idx_a.as_bytes().len(), MAX_INDEX_NAME_LEN);
        assert_eq!(idx_b.as_bytes().len(), MAX_INDEX_NAME_LEN);

        let decoded = IndexName::from_bytes(&idx_a.to_bytes()).unwrap();
        assert_eq!(idx_a, decoded);

        assert_eq!(idx_a.cmp(&idx_b), idx_a.to_bytes().cmp(&idx_b.to_bytes()));
    }

    #[test]
    #[should_panic(expected = "index has too many fields")]
    fn rejects_too_many_index_fields() {
        let entity = EntityName::from_static("entity");
        let fields = ["a", "b", "c", "d", "e"];
        let _ = IndexName::from_parts(&entity, &fields);
    }

    #[test]
    #[should_panic(expected = "index field name too long")]
    fn rejects_index_field_over_len() {
        let entity = EntityName::from_static("entity");
        let long_field = "a".repeat(MAX_INDEX_FIELD_NAME_LEN + 1);
        let fields = [long_field.as_str()];
        let _ = IndexName::from_parts(&entity, &fields);
    }

    #[test]
    fn entity_from_static_roundtrip() {
        let e = EntityName::from_static("user");
        assert_eq!(e.len(), 4);
        assert_eq!(e.as_str(), "user");
    }

    #[test]
    #[should_panic(expected = "entity name length out of bounds")]
    fn entity_rejects_empty() {
        let _ = EntityName::from_static("");
    }

    #[test]
    #[should_panic(expected = "entity name must be ASCII")]
    fn entity_rejects_non_ascii() {
        let _ = EntityName::from_static("usér");
    }

    #[test]
    fn entity_storage_roundtrip() {
        let e = EntityName::from_static("entity_name");
        let bytes = e.to_bytes();
        let decoded = EntityName::from_bytes(&bytes).unwrap();
        assert_eq!(e, decoded);
    }

    #[test]
    fn entity_rejects_invalid_size() {
        let buf = vec![0u8; EntityName::STORED_SIZE_USIZE - 1];
        assert!(EntityName::from_bytes(&buf).is_err());
    }

    #[test]
    fn entity_rejects_invalid_size_oversized() {
        let buf = vec![0u8; EntityName::STORED_SIZE_USIZE + 1];
        assert!(EntityName::from_bytes(&buf).is_err());
    }

    #[test]
    fn entity_rejects_len_over_max() {
        let mut buf = [0u8; EntityName::STORED_SIZE_USIZE];
        buf[0] = (MAX_ENTITY_NAME_LEN as u8).saturating_add(1);
        assert!(EntityName::from_bytes(&buf).is_err());
    }

    #[test]
    fn entity_rejects_non_ascii_from_bytes() {
        let mut buf = [0u8; EntityName::STORED_SIZE_USIZE];
        buf[0] = 1;
        buf[1] = 0xFF;
        assert!(EntityName::from_bytes(&buf).is_err());
    }

    #[test]
    fn entity_rejects_non_zero_padding() {
        let e = EntityName::from_static("user");
        let mut bytes = e.to_bytes();
        bytes[1 + e.len()] = b'x';
        assert!(EntityName::from_bytes(&bytes).is_err());
    }

    #[test]
    fn entity_ordering_matches_bytes() {
        let a = EntityName::from_static("abc");
        let b = EntityName::from_static("abd");
        let c = EntityName::from_static("abcx");

        assert_eq!(a.cmp(&b), a.to_bytes().cmp(&b.to_bytes()));
        assert_eq!(a.cmp(&c), a.to_bytes().cmp(&c.to_bytes()));
    }

    #[test]
    fn entity_ordering_b_vs_aa() {
        let b = EntityName::from_static("b");
        let aa = EntityName::from_static("aa");
        assert_eq!(b.cmp(&aa), b.to_bytes().cmp(&aa.to_bytes()));
    }

    #[test]
    fn entity_ordering_prefix_matches_bytes() {
        let a = EntityName::from_static("a");
        let aa = EntityName::from_static("aa");
        assert_eq!(a.cmp(&aa), a.to_bytes().cmp(&aa.to_bytes()));
    }

    #[test]
    fn index_single_field_format() {
        let entity = EntityName::from_static("user");
        let idx = IndexName::from_parts(&entity, &["email"]);

        assert_eq!(idx.as_str(), "user|email");
    }

    #[test]
    fn index_field_order_is_preserved() {
        let entity = EntityName::from_static("user");
        let idx = IndexName::from_parts(&entity, &["a", "b", "c"]);

        assert_eq!(idx.as_str(), "user|a|b|c");
    }

    #[test]
    fn index_storage_roundtrip() {
        let entity = EntityName::from_static("user");
        let idx = IndexName::from_parts(&entity, &["a", "b"]);

        let bytes = idx.to_bytes();
        let decoded = IndexName::from_bytes(&bytes).unwrap();

        assert_eq!(idx, decoded);
    }

    #[test]
    fn index_rejects_zero_len() {
        let mut buf = [0u8; IndexName::STORED_SIZE_USIZE];
        buf[0] = 0;
        assert!(IndexName::from_bytes(&buf).is_err());
    }

    #[test]
    fn index_rejects_invalid_size_oversized() {
        let buf = vec![0u8; IndexName::STORED_SIZE_USIZE + 1];
        assert!(IndexName::from_bytes(&buf).is_err());
    }

    #[test]
    fn index_rejects_len_over_max() {
        let mut buf = [0u8; IndexName::STORED_SIZE_USIZE];
        let len = (MAX_INDEX_NAME_LEN as u16).saturating_add(1);
        buf[..2].copy_from_slice(&len.to_be_bytes());
        assert!(IndexName::from_bytes(&buf).is_err());
    }

    #[test]
    fn index_rejects_non_ascii_from_bytes() {
        let mut buf = [0u8; IndexName::STORED_SIZE_USIZE];
        buf[..2].copy_from_slice(&1u16.to_be_bytes());
        buf[2] = 0xFF;
        assert!(IndexName::from_bytes(&buf).is_err());
    }

    #[test]
    fn index_rejects_non_zero_padding() {
        let entity = EntityName::from_static("user");
        let idx = IndexName::from_parts(&entity, &["a"]);
        let mut bytes = idx.to_bytes();
        bytes[2 + idx.len as usize] = b'x';
        assert!(IndexName::from_bytes(&bytes).is_err());
    }

    #[test]
    fn index_ordering_matches_bytes() {
        let entity = EntityName::from_static("user");

        let a = IndexName::from_parts(&entity, &["a"]);
        let ab = IndexName::from_parts(&entity, &["a", "b"]);
        let b = IndexName::from_parts(&entity, &["b"]);

        assert_eq!(a.cmp(&ab), a.to_bytes().cmp(&ab.to_bytes()));
        assert_eq!(ab.cmp(&b), ab.to_bytes().cmp(&b.to_bytes()));
    }

    #[test]
    fn index_ordering_prefix_matches_bytes() {
        let entity = EntityName::from_static("user");
        let a = IndexName::from_parts(&entity, &["a"]);
        let ab = IndexName::from_parts(&entity, &["a", "b"]);
        assert_eq!(a.cmp(&ab), a.to_bytes().cmp(&ab.to_bytes()));
    }

    #[test]
    fn max_storable_orders_last() {
        let entity = EntityName::from_static("zz");
        let max = EntityName::max_storable();

        assert!(entity < max);
    }

    ///
    /// FUZZING
    ///

    /// Simple deterministic ASCII generator
    fn gen_ascii(seed: u64, max_len: usize) -> String {
        let len = (seed as usize % max_len).max(1);
        let mut out = String::with_capacity(len);

        let mut x = seed;
        for _ in 0..len {
            // printable ASCII range [a–z]
            x = x.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let c = b'a' + (x % 26) as u8;
            out.push(c as char);
        }

        out
    }

    #[test]
    fn fuzz_entity_name_roundtrip_and_ordering() {
        const RUNS: u64 = 1_000;

        let mut prev: Option<EntityName> = None;

        for i in 1..=RUNS {
            let s = gen_ascii(i, MAX_ENTITY_NAME_LEN);
            let e = EntityName::from_static(Box::leak(s.clone().into_boxed_str()));

            // Round-trip
            let bytes = e.to_bytes();
            let decoded = EntityName::from_bytes(&bytes).unwrap();
            assert_eq!(e, decoded);

            // Ordering vs bytes
            if let Some(p) = prev {
                let ord_entity = p.cmp(&e);
                let ord_bytes = p.to_bytes().cmp(&e.to_bytes());
                assert_eq!(ord_entity, ord_bytes);
            }

            prev = Some(e);
        }
    }

    #[test]
    fn fuzz_index_name_roundtrip_and_ordering() {
        const RUNS: u64 = 1_000;

        let entity = EntityName::from_static("entity");
        let mut prev: Option<IndexName> = None;

        for i in 1..=RUNS {
            let field_count = (i as usize % MAX_INDEX_FIELDS).max(1);

            let mut field_strings = Vec::with_capacity(field_count);
            let mut fields = Vec::with_capacity(field_count);
            let mut string_parts = Vec::with_capacity(field_count + 1);

            string_parts.push(entity.as_str().to_owned());

            for f in 0..field_count {
                let s = gen_ascii(i * 31 + f as u64, MAX_INDEX_FIELD_NAME_LEN);
                string_parts.push(s.clone());
                field_strings.push(s);
            }

            for s in &field_strings {
                fields.push(s.as_str());
            }

            let idx = IndexName::from_parts(&entity, &fields);
            let expected = string_parts.join("|");

            // Structural correctness
            assert_eq!(idx.as_str(), expected);

            // Round-trip
            let bytes = idx.to_bytes();
            let decoded = IndexName::from_bytes(&bytes).unwrap();
            assert_eq!(idx, decoded);

            // Ordering vs bytes
            if let Some(p_idx) = prev {
                let ord_idx = p_idx.cmp(&idx);
                let ord_bytes = p_idx.to_bytes().cmp(&idx.to_bytes());
                assert_eq!(ord_idx, ord_bytes);
            }

            prev = Some(idx);
        }
    }
}
