#![allow(clippy::cast_possible_truncation)]

use candid::CandidType;
use canic_memory::impl_storable_bounded;
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use std::{
    cmp::Ordering,
    fmt::{self, Display},
};

///
/// Constants
///

pub const MAX_ENTITY_NAME_LEN: usize = 64;
pub const MAX_INDEX_NAME_LEN: usize = 200;

///
/// EntityName
///

#[derive(CandidType, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct EntityName {
    len: u8,
    #[serde(with = "BigArray")]
    bytes: [u8; MAX_ENTITY_NAME_LEN],
}

impl EntityName {
    pub const STORABLE_MAX_SIZE: u32 = (MAX_ENTITY_NAME_LEN as u32) + 8;

    #[must_use]
    pub const fn from_static(name: &'static str) -> Self {
        let bytes = name.as_bytes();
        let len = bytes.len();

        assert!(
            !(len == 0 || len > MAX_ENTITY_NAME_LEN),
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
        std::str::from_utf8(self.as_bytes()).unwrap_or("")
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
        write!(f, "{}", self.as_str())
    }
}

impl fmt::Debug for EntityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EntityName({})", self.as_str())
    }
}

impl_storable_bounded!(EntityName, EntityName::STORABLE_MAX_SIZE, false);

///
/// IndexName
///

#[derive(CandidType, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct IndexName {
    len: u8,
    #[serde(with = "BigArray")]
    bytes: [u8; MAX_INDEX_NAME_LEN],
}

impl IndexName {
    pub const STORABLE_MAX_SIZE: u32 = (MAX_INDEX_NAME_LEN as u32) + 8;

    #[must_use]
    pub fn from_parts(entity: &EntityName, fields: &[&str]) -> Self {
        let mut out = [0u8; MAX_INDEX_NAME_LEN];
        let mut len = 0usize;

        Self::push_ascii(&mut out, &mut len, entity.as_bytes());
        for field in fields {
            Self::push_ascii(&mut out, &mut len, b"|");
            Self::push_ascii(&mut out, &mut len, field.as_bytes());
        }

        Self {
            len: len as u8,
            bytes: out,
        }
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(self.as_bytes()).unwrap_or("")
    }

    #[must_use]
    pub const fn max_storable() -> Self {
        Self {
            len: MAX_INDEX_NAME_LEN as u8,
            bytes: [b'z'; MAX_INDEX_NAME_LEN],
        }
    }

    fn push_ascii(out: &mut [u8; MAX_INDEX_NAME_LEN], len: &mut usize, bytes: &[u8]) {
        assert!(bytes.is_ascii(), "index name must be ASCII");
        assert!(
            *len + bytes.len() <= MAX_INDEX_NAME_LEN,
            "index name too long"
        );

        let mut i = 0;
        while i < bytes.len() {
            out[*len + i] = bytes[i];
            i += 1;
        }
        *len += bytes.len();
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

impl_storable_bounded!(IndexName, IndexName::STORABLE_MAX_SIZE, false);
