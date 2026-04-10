//! Module: index::entry
//! Responsibility: index-entry payload encode/decode and structural validation.
//! Does not own: commit ordering or unique-policy decisions.
//! Boundary: commit/index-store consume raw entries after prevalidation.

use crate::{
    db::{
        data::{StorageKey, StorageKeyEncodeError},
        index::RawIndexKey,
    },
    error::InternalError,
    traits::Storable,
    value::Value,
};
use canic_cdk::structures::storable::Bound;
use std::{borrow::Cow, collections::BTreeSet};
use thiserror::Error as ThisError;

///
/// Constants
///

const INDEX_ENTRY_LEN_BYTES: usize = 4;
const INDEX_ENTRY_MEMBER_WITNESS_BYTES: usize = 1;
const INDEX_ENTRY_MEMBER_SIZE_USIZE: usize =
    StorageKey::STORED_SIZE_USIZE + INDEX_ENTRY_MEMBER_WITNESS_BYTES;
const INDEX_ENTRY_WITNESS_PRESENT: u8 = 0;
const INDEX_ENTRY_WITNESS_MISSING: u8 = 1;
pub(crate) const MAX_INDEX_ENTRY_KEYS: usize = 65_535;

#[expect(clippy::cast_possible_truncation)]
pub(crate) const MAX_INDEX_ENTRY_BYTES: u32 =
    (INDEX_ENTRY_LEN_BYTES + (MAX_INDEX_ENTRY_KEYS * INDEX_ENTRY_MEMBER_SIZE_USIZE)) as u32;

///
/// IndexEntryCorruption
///

#[derive(Debug, ThisError)]
pub(crate) enum IndexEntryCorruption {
    #[error("index entry exceeds max size")]
    TooLarge { len: usize },

    #[error("index entry missing key count")]
    MissingLength,

    #[error("index entry key count exceeds limit")]
    TooManyKeys { count: usize },

    #[error("index entry length does not match key count")]
    LengthMismatch,

    #[error("index entry contains invalid key bytes")]
    InvalidKey,

    #[error("index entry contains invalid existence witness")]
    InvalidWitness,

    #[error("index entry contains duplicate key")]
    DuplicateKey,

    #[error("index entry contains zero keys")]
    EmptyEntry,

    #[error("unique index entry contains {keys} keys")]
    NonUniqueEntry { keys: usize },

    #[error("index entry missing expected entity key: {entity_key:?} (index {index_key:?})")]
    MissingKey {
        index_key: Box<RawIndexKey>,
        entity_key: Value,
    },
}

impl IndexEntryCorruption {
    #[must_use]
    pub(crate) fn missing_key(index_key: RawIndexKey, entity_key: StorageKey) -> Self {
        Self::MissingKey {
            index_key: Box::new(index_key),
            entity_key: entity_key.as_value(),
        }
    }
}

///
/// IndexEntryEncodeError
///

#[derive(Debug, ThisError)]
pub(crate) enum IndexEntryEncodeError {
    #[error("index entry exceeds max keys: {keys} (limit {MAX_INDEX_ENTRY_KEYS})")]
    TooManyKeys { keys: usize },

    #[cfg(test)]
    #[error("index entry contains duplicate key")]
    DuplicateKey,

    #[error("index entry key encoding failed: {0}")]
    KeyEncoding(#[from] StorageKeyEncodeError),
}

impl IndexEntryEncodeError {
    // Lift one commit-time index-entry encode failure into internal taxonomy.
    pub(crate) fn into_commit_internal_error(
        self,
        entity_path: &str,
        fields: &str,
    ) -> InternalError {
        match self {
            Self::TooManyKeys { keys } => {
                InternalError::index_entry_exceeds_max_keys(entity_path, fields, keys)
            }
            #[cfg(test)]
            Self::DuplicateKey => {
                InternalError::index_entry_duplicate_keys_unexpected(entity_path, fields)
            }
            Self::KeyEncoding(err) => {
                InternalError::index_entry_key_encoding_failed(entity_path, fields, err)
            }
        }
    }
}

///
/// IndexEntry
///

#[derive(Clone, Debug)]
pub(crate) struct IndexEntry {
    ids: BTreeSet<StorageKey>,
}

///
/// IndexEntryExistenceWitness
///
/// Narrow storage-owned row-existence witness carried per raw index-entry
/// membership. `Present` is the normal encoded state; `Missing` exists so the
/// stale single-component prototype can preserve the secondary entry while
/// still exposing one explicit storage-level missing-row witness.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IndexEntryExistenceWitness {
    Present,
    Missing,
}

impl IndexEntryExistenceWitness {
    const fn to_stored_byte(self) -> u8 {
        match self {
            Self::Present => INDEX_ENTRY_WITNESS_PRESENT,
            Self::Missing => INDEX_ENTRY_WITNESS_MISSING,
        }
    }

    const fn try_from_stored_byte(byte: u8) -> Result<Self, IndexEntryCorruption> {
        match byte {
            INDEX_ENTRY_WITNESS_PRESENT => Ok(Self::Present),
            INDEX_ENTRY_WITNESS_MISSING => Ok(Self::Missing),
            _ => Err(IndexEntryCorruption::InvalidWitness),
        }
    }
}

///
/// IndexEntryMembership
///
/// One decoded raw index-entry membership plus its storage-owned existence
/// witness.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexEntryMembership {
    storage_key: StorageKey,
    existence_witness: IndexEntryExistenceWitness,
}

impl IndexEntryMembership {
    const fn new(storage_key: StorageKey, existence_witness: IndexEntryExistenceWitness) -> Self {
        Self {
            storage_key,
            existence_witness,
        }
    }

    #[must_use]
    pub(in crate::db) const fn storage_key(self) -> StorageKey {
        self.storage_key
    }

    #[must_use]
    pub(in crate::db) const fn existence_witness(self) -> IndexEntryExistenceWitness {
        self.existence_witness
    }
}

impl IndexEntry {
    #[must_use]
    pub(crate) fn new(id: StorageKey) -> Self {
        let mut ids = BTreeSet::new();
        ids.insert(id);
        Self { ids }
    }

    pub(crate) fn insert(&mut self, id: StorageKey) {
        self.ids.insert(id);
    }

    pub(crate) fn remove(&mut self, id: StorageKey) {
        self.ids.remove(&id);
    }

    #[must_use]
    pub(crate) fn contains(&self, id: StorageKey) -> bool {
        self.ids.contains(&id)
    }

    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.ids.len()
    }

    pub(crate) fn iter_ids(&self) -> impl Iterator<Item = StorageKey> + '_ {
        self.ids.iter().copied()
    }
}

///
/// RawIndexEntry
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RawIndexEntry(Vec<u8>);

///
/// RawIndexEntryKeyIter
///
/// RawIndexEntryKeyIter validates and streams one raw index-entry membership
/// set without first materializing a temporary `Vec<StorageKey>`.
///

pub(in crate::db) struct RawIndexEntryKeyIter<'a> {
    inner: RawIndexEntryMembershipIter<'a>,
}

///
/// RawIndexEntryMembershipIter
///
/// RawIndexEntryMembershipIter validates and streams one raw index-entry
/// membership set, including the storage-owned existence witness for each
/// decoded storage key.
///

pub(in crate::db) struct RawIndexEntryMembershipIter<'a> {
    bytes: &'a [u8],
    declared_count: usize,
    offset: usize,
    remaining: usize,
    seen: BTreeSet<StorageKey>,
}

impl RawIndexEntry {
    pub(crate) fn try_from_entry(entry: &IndexEntry) -> Result<Self, IndexEntryEncodeError> {
        // `IndexEntry` already owns the canonical sorted-unique membership set,
        // so commit-time re-encoding can stream it directly without rebuilding
        // another temporary vector or duplicate-check set.
        let count = entry.ids.len();
        if count > MAX_INDEX_ENTRY_KEYS {
            return Err(IndexEntryEncodeError::TooManyKeys { keys: count });
        }

        let mut out =
            Vec::with_capacity(INDEX_ENTRY_LEN_BYTES + count * INDEX_ENTRY_MEMBER_SIZE_USIZE);

        let count_u32 =
            u32::try_from(count).map_err(|_| IndexEntryEncodeError::TooManyKeys { keys: count })?;
        out.extend_from_slice(&count_u32.to_be_bytes());

        for id in &entry.ids {
            out.extend_from_slice(&id.to_bytes()?);
            out.push(IndexEntryExistenceWitness::Present.to_stored_byte());
        }

        Ok(Self(out))
    }

    pub(crate) fn try_decode(&self) -> Result<IndexEntry, IndexEntryCorruption> {
        let storage_keys = self.decode_keys_checked()?;
        let mut ids = BTreeSet::new();

        for key in storage_keys {
            ids.insert(key);
        }

        if ids.is_empty() {
            return Err(IndexEntryCorruption::EmptyEntry);
        }

        Ok(IndexEntry { ids })
    }

    #[cfg(test)]
    pub(crate) fn try_from_keys<I>(keys: I) -> Result<Self, IndexEntryEncodeError>
    where
        I: IntoIterator<Item = StorageKey>,
    {
        // Phase 1: collect and bound-check key cardinality.
        let keys: Vec<StorageKey> = keys.into_iter().collect();
        let count = keys.len();

        if count > MAX_INDEX_ENTRY_KEYS {
            return Err(IndexEntryEncodeError::TooManyKeys { keys: count });
        }

        // Enforce encode/decode symmetry: duplicates are rejected at construction,
        // not deferred to decode-time corruption validation.
        let mut unique = BTreeSet::new();
        for key in &keys {
            if !unique.insert(*key) {
                return Err(IndexEntryEncodeError::DuplicateKey);
            }
        }

        // Phase 2: encode canonical length-prefixed payload.
        let mut out =
            Vec::with_capacity(INDEX_ENTRY_LEN_BYTES + count * INDEX_ENTRY_MEMBER_SIZE_USIZE);

        let count_u32 =
            u32::try_from(count).map_err(|_| IndexEntryEncodeError::TooManyKeys { keys: count })?;
        out.extend_from_slice(&count_u32.to_be_bytes());

        for sk in keys {
            out.extend_from_slice(&sk.to_bytes()?);
            out.push(IndexEntryExistenceWitness::Present.to_stored_byte());
        }

        Ok(Self(out))
    }

    pub(crate) fn decode_keys(&self) -> Result<Vec<StorageKey>, IndexEntryCorruption> {
        let mut keys = Vec::new();
        let mut iter = self.iter_keys()?;
        keys.reserve(iter.declared_count());

        for key in &mut iter {
            keys.push(key?);
        }

        Ok(keys)
    }

    // Decode one single-key entry without allocating the full membership
    // vector when the frame declares exactly one storage key.
    pub(crate) fn decode_single_key(&self) -> Result<Option<StorageKey>, IndexEntryCorruption> {
        Ok(self
            .decode_single_membership()?
            .map(IndexEntryMembership::storage_key))
    }

    // Decode one single-key entry plus its storage-owned existence witness
    // without allocating the full membership vector when the frame declares
    // exactly one storage key.
    pub(in crate::db) fn decode_single_membership(
        &self,
    ) -> Result<Option<IndexEntryMembership>, IndexEntryCorruption> {
        let count = self.validate_frame()?;
        if count != 1 {
            return Ok(None);
        }

        let bytes = self.0.as_slice();
        let membership = decode_membership_at_offset(bytes, INDEX_ENTRY_LEN_BYTES)?;

        Ok(Some(membership))
    }

    // Stream the validated storage-key membership set without first allocating
    // a temporary vector for multi-key scan callers.
    pub(in crate::db) fn iter_keys(
        &self,
    ) -> Result<RawIndexEntryKeyIter<'_>, IndexEntryCorruption> {
        Ok(RawIndexEntryKeyIter {
            inner: self.iter_memberships()?,
        })
    }

    // Stream the validated storage-key membership set, including the
    // storage-owned existence witness for each member.
    pub(in crate::db) fn iter_memberships(
        &self,
    ) -> Result<RawIndexEntryMembershipIter<'_>, IndexEntryCorruption> {
        let count = self.validate_frame()?;

        Ok(RawIndexEntryMembershipIter::new(self.0.as_slice(), count))
    }

    // Mark one encoded membership as missing while preserving the containing
    // secondary entry itself.
    pub(in crate::db) fn mark_key_missing(
        &mut self,
        storage_key: StorageKey,
    ) -> Result<bool, IndexEntryCorruption> {
        let count = self.validate_frame()?;
        let mut offset = INDEX_ENTRY_LEN_BYTES;

        for _ in 0..count {
            let membership = decode_membership_at_offset(self.0.as_slice(), offset)?;
            if membership.storage_key() == storage_key {
                let witness_offset = offset + StorageKey::STORED_SIZE_USIZE;
                self.0[witness_offset] = IndexEntryExistenceWitness::Missing.to_stored_byte();

                return Ok(true);
            }

            offset += INDEX_ENTRY_MEMBER_SIZE_USIZE;
        }

        Ok(false)
    }

    // Decode the canonical storage-key payload while validating the raw entry
    // shape and duplicate-key invariants in the same pass.
    fn decode_keys_checked(&self) -> Result<Vec<StorageKey>, IndexEntryCorruption> {
        self.decode_keys()
    }

    /// Validate the raw index entry structure without binding to an entity.
    pub(crate) fn validate(&self) -> Result<(), IndexEntryCorruption> {
        self.decode_keys_checked().map(|_| ())
    }

    // Validate the raw index-entry frame and return the declared key count for
    // the checked decode pass.
    fn validate_frame(&self) -> Result<usize, IndexEntryCorruption> {
        let bytes = self.0.as_slice();

        // Phase 1: frame-level checks (size, header, declared count).
        if bytes.len() > MAX_INDEX_ENTRY_BYTES as usize {
            return Err(IndexEntryCorruption::TooLarge { len: bytes.len() });
        }
        if bytes.len() < INDEX_ENTRY_LEN_BYTES {
            return Err(IndexEntryCorruption::MissingLength);
        }

        let mut len_buf = [0u8; INDEX_ENTRY_LEN_BYTES];
        len_buf.copy_from_slice(&bytes[..INDEX_ENTRY_LEN_BYTES]);
        let count = u32::from_be_bytes(len_buf) as usize;

        if count == 0 {
            return Err(IndexEntryCorruption::EmptyEntry);
        }
        if count > MAX_INDEX_ENTRY_KEYS {
            return Err(IndexEntryCorruption::TooManyKeys { count });
        }

        let expected = INDEX_ENTRY_LEN_BYTES
            + count
                .checked_mul(INDEX_ENTRY_MEMBER_SIZE_USIZE)
                .ok_or(IndexEntryCorruption::LengthMismatch)?;

        if bytes.len() != expected {
            return Err(IndexEntryCorruption::LengthMismatch);
        }

        Ok(count)
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub(crate) const fn len(&self) -> usize {
        self.0.len()
    }
}

impl<'a> RawIndexEntryMembershipIter<'a> {
    // Build one validated storage-key iterator over one canonical raw entry.
    const fn new(bytes: &'a [u8], declared_count: usize) -> Self {
        Self {
            bytes,
            declared_count,
            offset: INDEX_ENTRY_LEN_BYTES,
            remaining: declared_count,
            seen: BTreeSet::new(),
        }
    }

    #[must_use]
    pub(in crate::db) const fn declared_count(&self) -> usize {
        self.declared_count
    }
}

impl RawIndexEntryKeyIter<'_> {
    #[must_use]
    pub(in crate::db) const fn declared_count(&self) -> usize {
        self.inner.declared_count()
    }
}

impl Iterator for RawIndexEntryKeyIter<'_> {
    type Item = Result<StorageKey, IndexEntryCorruption>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|membership| membership.map(IndexEntryMembership::storage_key))
    }
}

impl Iterator for RawIndexEntryMembershipIter<'_> {
    type Item = Result<IndexEntryMembership, IndexEntryCorruption>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let membership = match decode_membership_at_offset(self.bytes, self.offset) {
            Ok(membership) => membership,
            Err(err) => {
                self.remaining = 0;
                return Some(Err(err));
            }
        };

        self.offset += INDEX_ENTRY_MEMBER_SIZE_USIZE;
        self.remaining -= 1;

        if !self.seen.insert(membership.storage_key()) {
            self.remaining = 0;
            return Some(Err(IndexEntryCorruption::DuplicateKey));
        }

        Some(Ok(membership))
    }
}

// Decode one fixed-width stored key segment from one validated index-entry
// payload offset.
fn decode_membership_at_offset(
    bytes: &[u8],
    offset: usize,
) -> Result<IndexEntryMembership, IndexEntryCorruption> {
    let key = decode_stored_key_at_offset(bytes, offset)?;
    let witness = bytes
        .get(offset + StorageKey::STORED_SIZE_USIZE)
        .copied()
        .ok_or(IndexEntryCorruption::InvalidWitness)?;

    Ok(IndexEntryMembership::new(
        key,
        IndexEntryExistenceWitness::try_from_stored_byte(witness)?,
    ))
}

fn decode_stored_key_at_offset(
    bytes: &[u8],
    offset: usize,
) -> Result<StorageKey, IndexEntryCorruption> {
    let end = offset + StorageKey::STORED_SIZE_USIZE;
    let key_bytes: &[u8; StorageKey::STORED_SIZE_USIZE] = (&bytes[offset..end])
        .try_into()
        .map_err(|_| IndexEntryCorruption::InvalidKey)?;

    StorageKey::try_from_stored_bytes(key_bytes).map_err(|_| IndexEntryCorruption::InvalidKey)
}

impl TryFrom<&IndexEntry> for RawIndexEntry {
    type Error = IndexEntryEncodeError;

    fn try_from(entry: &IndexEntry) -> Result<Self, Self::Error> {
        Self::try_from_entry(entry)
    }
}

impl Storable for RawIndexEntry {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_INDEX_ENTRY_BYTES,
        is_fixed_size: false,
    };
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        IndexEntryCorruption, IndexEntryEncodeError, IndexEntryExistenceWitness,
        MAX_INDEX_ENTRY_BYTES, MAX_INDEX_ENTRY_KEYS, RawIndexEntry,
    };
    use crate::{
        db::data::StorageKey,
        error::{ErrorClass, ErrorOrigin},
        traits::Storable,
    };
    use std::borrow::Cow;

    fn duplicate_membership_bytes(key: StorageKey) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u32.to_be_bytes());
        bytes.extend_from_slice(&key.to_bytes().expect("encode"));
        bytes.push(IndexEntryExistenceWitness::Present.to_stored_byte());
        bytes.extend_from_slice(&key.to_bytes().expect("encode"));
        bytes.push(IndexEntryExistenceWitness::Present.to_stored_byte());

        bytes
    }

    #[test]
    fn raw_index_entry_round_trip() {
        let keys = vec![StorageKey::Int(1), StorageKey::Uint(2)];

        let raw = RawIndexEntry::try_from_keys(keys.clone()).expect("encode index entry");
        let decoded = raw.decode_keys().expect("decode index entry");

        assert_eq!(decoded.len(), keys.len());
        assert!(decoded.contains(&StorageKey::Int(1)));
        assert!(decoded.contains(&StorageKey::Uint(2)));
    }

    #[test]
    fn raw_index_entry_decode_single_key_recovers_single_member_without_vector_allocation() {
        let raw = RawIndexEntry::try_from_keys([StorageKey::Int(9)]).expect("encode index entry");

        assert_eq!(
            raw.decode_single_key().expect("decode single key"),
            Some(StorageKey::Int(9))
        );
    }

    #[test]
    fn raw_index_entry_decode_single_key_rejects_multi_key_entries() {
        let raw = RawIndexEntry::try_from_keys([StorageKey::Int(1), StorageKey::Uint(2)])
            .expect("encode index entry");

        assert_eq!(
            raw.decode_single_key().expect("decode multi-key entry"),
            None
        );
    }

    #[test]
    fn raw_index_entry_iter_keys_streams_multi_key_entries_without_vector_staging() {
        let raw = RawIndexEntry::try_from_keys([StorageKey::Int(3), StorageKey::Uint(4)])
            .expect("encode index entry");
        let mut iter = raw.iter_keys().expect("build key iterator");

        assert_eq!(iter.declared_count(), 2);
        assert_eq!(
            iter.next().expect("first key").expect("decode key"),
            StorageKey::Int(3)
        );
        assert_eq!(
            iter.next().expect("second key").expect("decode key"),
            StorageKey::Uint(4)
        );
        assert!(
            iter.next().is_none(),
            "iterator should exhaust after two keys"
        );
    }

    #[test]
    fn raw_index_entry_decode_single_membership_recovers_present_witness() {
        let raw = RawIndexEntry::try_from_keys([StorageKey::Int(9)]).expect("encode index entry");
        let membership = raw
            .decode_single_membership()
            .expect("decode single membership")
            .expect("single-key entry should decode");

        assert_eq!(membership.storage_key(), StorageKey::Int(9));
        assert_eq!(
            membership.existence_witness(),
            IndexEntryExistenceWitness::Present
        );
    }

    #[test]
    fn raw_index_entry_mark_key_missing_preserves_entry_and_flips_witness() {
        let mut raw =
            RawIndexEntry::try_from_keys([StorageKey::Int(9)]).expect("encode index entry");

        assert!(
            raw.mark_key_missing(StorageKey::Int(9))
                .expect("mark key missing"),
            "encoded membership should be found",
        );

        let membership = raw
            .decode_single_membership()
            .expect("decode single membership")
            .expect("single-key entry should decode");
        assert_eq!(membership.storage_key(), StorageKey::Int(9));
        assert_eq!(
            membership.existence_witness(),
            IndexEntryExistenceWitness::Missing
        );
    }

    #[test]
    fn raw_index_entry_roundtrip_via_bytes() {
        let keys = vec![StorageKey::Int(9), StorageKey::Uint(10)];

        let raw = RawIndexEntry::try_from_keys(keys.clone()).expect("encode index entry");
        let encoded = Storable::to_bytes(&raw);
        let raw = RawIndexEntry::from_bytes(encoded);
        let decoded = raw.decode_keys().expect("decode index entry");

        assert_eq!(decoded.len(), keys.len());
        assert!(decoded.contains(&StorageKey::Int(9)));
        assert!(decoded.contains(&StorageKey::Uint(10)));
    }

    #[test]
    fn raw_index_entry_rejects_empty() {
        let bytes = vec![0, 0, 0, 0];
        let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
        assert!(matches!(
            raw.decode_keys(),
            Err(IndexEntryCorruption::EmptyEntry)
        ));
    }

    #[test]
    fn raw_index_entry_rejects_truncated_payload() {
        let key = StorageKey::Int(1);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(&key.to_bytes().expect("encode"));
        bytes.truncate(bytes.len() - 1);

        let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
        assert!(matches!(
            raw.decode_keys(),
            Err(IndexEntryCorruption::LengthMismatch)
        ));
    }

    #[test]
    fn raw_index_entry_rejects_oversized_payload() {
        let bytes = vec![0u8; MAX_INDEX_ENTRY_BYTES as usize + 1];
        let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
        assert!(matches!(
            raw.decode_keys(),
            Err(IndexEntryCorruption::TooLarge { .. })
        ));
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn raw_index_entry_rejects_corrupted_length_field() {
        let count = (MAX_INDEX_ENTRY_KEYS + 1) as u32;
        let raw = RawIndexEntry::from_bytes(Cow::Owned(count.to_be_bytes().to_vec()));
        assert!(matches!(
            raw.decode_keys(),
            Err(IndexEntryCorruption::TooManyKeys { .. })
        ));
    }

    #[test]
    fn raw_index_entry_rejects_duplicate_keys() {
        let key = StorageKey::Int(1);
        let raw = RawIndexEntry::from_bytes(Cow::Owned(duplicate_membership_bytes(key)));
        assert!(matches!(
            raw.decode_keys(),
            Err(IndexEntryCorruption::DuplicateKey)
        ));
    }

    #[test]
    fn raw_index_entry_iter_keys_rejects_duplicate_keys() {
        let key = StorageKey::Int(1);
        let raw = RawIndexEntry::from_bytes(Cow::Owned(duplicate_membership_bytes(key)));
        let mut iter = raw.iter_keys().expect("build key iterator");

        assert_eq!(iter.next().expect("first item").expect("decode key"), key);
        assert!(matches!(
            iter.next(),
            Some(Err(IndexEntryCorruption::DuplicateKey))
        ));
        assert!(
            iter.next().is_none(),
            "iterator should stop after duplicate corruption"
        );
    }

    #[test]
    fn raw_index_entry_try_from_keys_rejects_duplicate_keys() {
        let key = StorageKey::Int(7);
        let err = RawIndexEntry::try_from_keys([key, key]).expect_err(
            "encoding should reject duplicate keys instead of deferring to decode validation",
        );

        assert!(matches!(err, IndexEntryEncodeError::DuplicateKey));
    }

    #[test]
    fn index_entry_encode_error_owns_commit_internal_mapping() {
        let err = IndexEntryEncodeError::DuplicateKey
            .into_commit_internal_error("tests::Entity", "email");

        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
    }

    #[test]
    #[expect(clippy::cast_possible_truncation)]
    fn raw_index_entry_decode_fuzz_does_not_panic() {
        const RUNS: u64 = 1_000;
        const MAX_LEN: usize = 256;

        let mut seed = 0xA5A5_5A5A_u64;
        for _ in 0..RUNS {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let len = (seed as usize) % MAX_LEN;

            let mut bytes = vec![0u8; len];
            for byte in &mut bytes {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                *byte = (seed >> 24) as u8;
            }

            let raw = RawIndexEntry::from_bytes(Cow::Owned(bytes));
            let _ = raw.decode_keys();
        }
    }
}
