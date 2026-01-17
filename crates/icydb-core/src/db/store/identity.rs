#![allow(clippy::cast_possible_truncation)]

use crate::MAX_INDEX_FIELDS;
use std::{
    cmp::Ordering,
    fmt::{self, Display},
};

///
/// Constants
///

pub const MAX_ENTITY_NAME_LEN: usize = 48;
pub const MAX_INDEX_FIELD_NAME_LEN: usize = 48;
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

    pub const fn len(&self) -> usize {
        self.len as usize
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len()]
    }

    pub fn as_str(&self) -> &str {
        // Safe because we enforce ASCII
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    pub fn to_bytes(self) -> [u8; Self::STORED_SIZE_USIZE] {
        let mut out = [0u8; Self::STORED_SIZE_USIZE];
        out[0] = self.len;
        out[1..].copy_from_slice(&self.bytes);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != Self::STORED_SIZE_USIZE {
            return Err("invalid EntityName size");
        }

        let len = bytes[0] as usize;
        if len == 0 || len > MAX_ENTITY_NAME_LEN {
            return Err("invalid EntityName length");
        }

        let mut name = [0u8; MAX_ENTITY_NAME_LEN];
        name.copy_from_slice(&bytes[1..]);

        Ok(Self {
            len: len as u8,
            bytes: name,
        })
    }

    #[must_use]
    pub const fn max_storable() -> Self {
        Self {
            len: MAX_ENTITY_NAME_LEN as u8,
            bytes: [b'z'; MAX_ENTITY_NAME_LEN],
        }
    }
}

impl Ord for EntityName {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
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
    pub len: u8,
    pub bytes: [u8; MAX_INDEX_NAME_LEN],
}

impl IndexName {
    pub const STORED_SIZE: u32 = 1 + MAX_INDEX_NAME_LEN as u32;
    pub const STORED_SIZE_USIZE: usize = Self::STORED_SIZE as usize;

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
            len: len as u8,
            bytes: out,
        }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.as_bytes()) }
    }

    pub fn to_bytes(self) -> [u8; Self::STORED_SIZE_USIZE] {
        let mut out = [0u8; Self::STORED_SIZE_USIZE];
        out[0] = self.len;
        out[1..].copy_from_slice(&self.bytes);
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != Self::STORED_SIZE_USIZE {
            return Err("invalid IndexName size");
        }

        let len = bytes[0] as usize;
        if len == 0 || len > MAX_INDEX_NAME_LEN {
            return Err("invalid IndexName length");
        }

        let mut name = [0u8; MAX_INDEX_NAME_LEN];
        name.copy_from_slice(&bytes[1..]);

        Ok(Self {
            len: len as u8,
            bytes: name,
        })
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
            len: MAX_INDEX_NAME_LEN as u8,
            bytes: [b'z'; MAX_INDEX_NAME_LEN],
        }
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
        self.as_bytes().cmp(other.as_bytes())
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

    const ENTITY_48: &str = "0123456789abcdef0123456789abcdef0123456789abcdef";
    const FIELD_48_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const FIELD_48_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const FIELD_48_C: &str = "cccccccccccccccccccccccccccccccccccccccccccccccc";
    const FIELD_48_D: &str = "dddddddddddddddddddddddddddddddddddddddddddddddd";

    #[test]
    fn index_name_max_len_matches_limits() {
        let entity = EntityName::from_static(ENTITY_48);
        let fields = [FIELD_48_A, FIELD_48_B, FIELD_48_C, FIELD_48_D];

        assert_eq!(entity.as_str().len(), MAX_ENTITY_NAME_LEN);
        for field in &fields {
            assert_eq!(field.len(), MAX_INDEX_FIELD_NAME_LEN);
        }
        assert_eq!(fields.len(), MAX_INDEX_FIELDS);

        let name = IndexName::from_parts(&entity, &fields);

        assert_eq!(name.as_bytes().len(), MAX_INDEX_NAME_LEN);
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
}
