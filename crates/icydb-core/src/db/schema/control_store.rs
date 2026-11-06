//! Module: db::schema::control_store
//! Responsibility: one compact physical tree for database-wide schema metadata.
//! Does not own: record codecs, retention, accepted authority or publication.
//! Boundary: fixed record namespaces -> one current control-memory allocation.

use crate::{
    db::{
        commit::{MAX_COMMIT_BYTES, commit_memory_handle, current_commit_memory_allocation},
        database_format::crc32c,
        schema::application_store::MAX_SCHEMA_APPLICATION_RECORDS,
    },
    error::InternalError,
};
#[cfg(test)]
use ic_storage::Memory;
use ic_storage::adapter::{BTreeMapRegion, Storable, storable::Bound};
use ic_storage::{DefaultMemoryImpl, RuntimeMemory};
use std::{borrow::Cow, marker::PhantomData, rc::Rc};

pub(super) type ControlMemory = Rc<RuntimeMemory<DefaultMemoryImpl>>;
pub(super) const CONTROL_START_PAGE: u64 = MAX_COMMIT_BYTES as u64 / 65_536 + 1;
const CONTROL_END_PAGE: u64 = 4_194_304;
const HEADER_KEY: [u8; 33] = [0; 33];
// This persisted application header identifies IcyDB's namespace contract,
// independently of Storage's tree framing. Retain its exact bytes
// and validation for same-contract reopen.
const HEADER_MAGIC: &[u8; 8] = b"ICYSCMAP";
const HEADER_VERSION: u8 = 1;
const HEADER_BYTES: usize = 13;
// One header, application receipts, store checkpoints, lineage and migration.
const MAX_RECORDS: u64 =
    1 + MAX_SCHEMA_APPLICATION_RECORDS + icydb_schema::MAX_SCHEMA_ASSIGNMENTS as u64 + 2;

#[derive(Clone, Copy)]
pub(super) enum ControlRecordFamily {
    Application = 1,
    Checkpoint = 2,
}

pub(super) struct SchemaControlBytes(pub(super) Vec<u8>);

impl Storable for SchemaControlBytes {
    fn to_bytes(&self) -> Result<Cow<'_, [u8]>, ic_storage::storable::CodecError> {
        Ok(Cow::Borrowed(&self.0))
    }
    fn into_bytes(self) -> Result<Vec<u8>, ic_storage::storable::CodecError> {
        Ok(self.0)
    }
    fn from_bytes(bytes: Cow<'_, [u8]>) -> Result<Self, ic_storage::storable::CodecError> {
        Self::check_bounds(bytes.as_ref())?;
        Ok(Self(bytes.into_owned()))
    }
    // Codec admission caps records; the storage maximum must not inflate pages.
    const BOUND: Bound = Bound::Unbounded;
}

/// A fixed namespace view, not an independent allocator or cached authority.
pub(super) struct SchemaControlMap<K> {
    map: BTreeMapRegion<[u8; 33], SchemaControlBytes, DefaultMemoryImpl>,
    family: ControlRecordFamily,
    key: PhantomData<K>,
}

impl<K: Copy + From<[u8; 32]> + Into<[u8; 32]>> SchemaControlMap<K> {
    pub(super) fn open(
        memory: ControlMemory,
        family: ControlRecordFamily,
    ) -> Result<Self, InternalError> {
        if let Some(map) = Self::open_existing(memory.clone(), family)? {
            return Ok(map);
        }
        let mut map = BTreeMapRegion::<[u8; 33], SchemaControlBytes>::new(
            memory,
            CONTROL_START_PAGE..CONTROL_END_PAGE,
        );
        map.initialize().map_err(InternalError::from)?;
        let mut header = Vec::with_capacity(HEADER_BYTES);
        header.extend_from_slice(HEADER_MAGIC);
        header.push(HEADER_VERSION);
        header.extend_from_slice(&crc32c(&header).to_le_bytes());
        map.insert(HEADER_KEY, SchemaControlBytes(header))
            .map_err(InternalError::from)?;
        Ok(Self {
            map,
            family,
            key: PhantomData,
        })
    }

    pub(super) fn open_existing(
        memory: ControlMemory,
        family: ControlRecordFamily,
    ) -> Result<Option<Self>, InternalError> {
        let map = BTreeMapRegion::<[u8; 33], SchemaControlBytes>::new(
            memory,
            CONTROL_START_PAGE..CONTROL_END_PAGE,
        );
        if map.allocated_pages() == 0 {
            return Ok(None);
        }
        // Storage validates its own map frame before decoding application metadata.
        let header = map
            .get(&HEADER_KEY)
            .map_err(InternalError::from)?
            .ok_or_else(InternalError::store_corruption)?;
        if header.0.len() != HEADER_BYTES
            || &header.0[..8] != HEADER_MAGIC
            || header.0[8] != HEADER_VERSION
            || header.0[9..] != crc32c(&header.0[..9]).to_le_bytes()
            || map.len().map_err(InternalError::from)? > MAX_RECORDS
        {
            return Err(InternalError::store_corruption());
        }
        // Namespace zero contains only the header; no other family is admitted.
        let mut first_family = [0; 33];
        first_family[0] = 1;
        let mut after_families = [0; 33];
        after_families[0] = 3;
        if map
            .keys_range(HEADER_KEY..first_family)
            .map_err(InternalError::from)?
            .any(|key| key != HEADER_KEY)
            || map
                .keys_range(after_families..)
                .map_err(InternalError::from)?
                .next()
                .is_some()
        {
            return Err(InternalError::store_corruption());
        }
        Ok(Some(Self {
            map,
            family,
            key: PhantomData,
        }))
    }

    fn storage_key(&self, key: K) -> [u8; 33] {
        let mut encoded = [0; 33];
        encoded[0] = self.family as u8;
        encoded[1..].copy_from_slice(&key.into());
        encoded
    }

    const fn range(&self) -> std::ops::RangeInclusive<[u8; 33]> {
        let mut start = [0; 33];
        start[0] = self.family as u8;
        let mut end = [u8::MAX; 33];
        end[0] = self.family as u8;
        start..=end
    }

    pub(super) fn get(&self, key: &K) -> Result<Option<SchemaControlBytes>, InternalError> {
        self.map
            .get(&self.storage_key(*key))
            .map_err(InternalError::from)
    }
    pub(super) fn insert(
        &mut self,
        key: K,
        value: SchemaControlBytes,
    ) -> Result<(), InternalError> {
        self.map
            .insert(self.storage_key(key), value)
            .map_err(InternalError::from)?;

        Ok(())
    }
    pub(super) fn remove(&mut self, key: &K) -> Result<Option<SchemaControlBytes>, InternalError> {
        self.map
            .remove(&self.storage_key(*key))
            .map_err(InternalError::from)
    }
    pub(super) fn len(&self) -> Result<u64, InternalError> {
        Ok(self
            .map
            .keys_range(self.range())
            .map_err(InternalError::from)?
            .fold(0, |count, _| count + 1))
    }
    pub(super) fn keys(&self) -> Result<impl Iterator<Item = K>, InternalError> {
        Ok(self
            .map
            .keys_range(self.range())
            .map_err(InternalError::from)?
            .map(|stored| {
                let mut key = [0; 32];
                key.copy_from_slice(&stored[1..]);
                K::from(key)
            }))
    }
}

pub(super) fn control_memory() -> Result<ControlMemory, InternalError> {
    commit_memory_handle(current_commit_memory_allocation()?)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::db) use helper_tests::corrupt_schema_control_header_for_tests;
