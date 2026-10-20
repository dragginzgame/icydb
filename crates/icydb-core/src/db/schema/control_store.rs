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
use ic_memory::Memory;
use ic_memory::{DefaultMemoryImpl, RuntimeMemory};
use ic_storage::adapter::{BTreeMapRegion, Storable, storable::Bound};
use std::{borrow::Cow, marker::PhantomData, rc::Rc};

pub(super) type ControlMemory = Rc<RuntimeMemory<DefaultMemoryImpl>>;
pub(super) const CONTROL_START_PAGE: u64 = MAX_COMMIT_BYTES as u64 / 65_536 + 1;
const CONTROL_END_PAGE: u64 = 4_194_304;
const HEADER_KEY: [u8; 33] = [0; 33];
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
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }
    fn into_bytes(self) -> Vec<u8> {
        self.0
    }
    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
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
        map.initialize();
        let mut header = Vec::with_capacity(HEADER_BYTES);
        header.extend_from_slice(HEADER_MAGIC);
        header.push(HEADER_VERSION);
        header.extend_from_slice(&crc32c(&header).to_le_bytes());
        map.insert(HEADER_KEY, SchemaControlBytes(header));
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
        // Reject malformed framing before the upstream infallible tree loader.
        let mut header = [0; 4];
        map.read(0, &mut header);
        if header != *b"BTR\x02" {
            return Err(InternalError::store_corruption());
        }
        map.read(52, &mut header);
        if header != *b"BTA\x01" {
            return Err(InternalError::store_corruption());
        }
        let header = map
            .get(&HEADER_KEY)
            .ok_or_else(InternalError::store_corruption)?;
        if header.0.len() != HEADER_BYTES
            || &header.0[..8] != HEADER_MAGIC
            || header.0[8] != HEADER_VERSION
            || header.0[9..] != crc32c(&header.0[..9]).to_le_bytes()
            || map.len() > MAX_RECORDS
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
            .any(|key| key != HEADER_KEY)
            || map.keys_range(after_families..).next().is_some()
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

    pub(super) fn get(&self, key: &K) -> Option<SchemaControlBytes> {
        self.map.get(&self.storage_key(*key))
    }
    pub(super) fn insert(&mut self, key: K, value: SchemaControlBytes) {
        self.map.insert(self.storage_key(key), value);
    }
    pub(super) fn remove(&mut self, key: &K) -> Option<SchemaControlBytes> {
        self.map.remove(&self.storage_key(*key))
    }
    pub(super) fn len(&self) -> u64 {
        self.map
            .keys_range(self.range())
            .fold(0, |count, _| count + 1)
    }
    pub(super) fn keys(&self) -> impl Iterator<Item = K> {
        self.map.keys_range(self.range()).map(|stored| {
            let mut key = [0; 32];
            key.copy_from_slice(&stored[1..]);
            K::from(key)
        })
    }

    #[cfg(test)]
    pub(super) fn corrupt_header_for_tests(&mut self) {
        self.map.insert(HEADER_KEY, SchemaControlBytes(vec![0xff]));
    }
}

pub(super) fn control_memory() -> Result<ControlMemory, InternalError> {
    commit_memory_handle(current_commit_memory_allocation()?)
}

#[cfg(test)]
pub(in crate::db) fn corrupt_schema_control_header_for_tests() -> Result<(), InternalError> {
    let mut map =
        SchemaControlMap::<[u8; 32]>::open(control_memory()?, ControlRecordFamily::Checkpoint)?;
    map.corrupt_header_for_tests();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{test_memory, test_memory_owner};

    #[test]
    fn namespace_views_share_growth_without_stale_roots_or_cross_family_reads() {
        let owner = test_memory_owner(242);
        let memory = Rc::new(owner.open());
        memory.as_ref().grow(1).unwrap();
        memory.write(0, b"retained frame");
        let mut application =
            SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application)
                .unwrap();
        let mut checkpoint =
            SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Checkpoint)
                .unwrap();
        // Interleave enough writes to split the root through both retained views.
        for key in 0..40_u8 {
            application.insert([key; 32], SchemaControlBytes(vec![key; 40]));
            checkpoint.insert([key; 32], SchemaControlBytes(vec![key + 1; 40]));
        }
        assert_eq!(application.len(), 40);
        assert_eq!(checkpoint.len(), 40);
        assert_eq!(application.keys().count(), 40);
        for key in 0..40_u8 {
            assert_eq!(application.get(&[key; 32]).unwrap().0, vec![key; 40]);
            assert_eq!(checkpoint.get(&[key; 32]).unwrap().0, vec![key + 1; 40]);
            application.remove(&[key; 32]).unwrap();
        }
        assert_eq!(application.len(), 0);
        assert_eq!(checkpoint.len(), 40);
        assert!(memory.size() <= CONTROL_START_PAGE + 3);
        let mut frame = [0; 14];
        memory.read(0, &mut frame);
        assert_eq!(&frame, b"retained frame");
        let reopened =
            SchemaControlMap::<[u8; 32]>::open_existing(memory, ControlRecordFamily::Checkpoint)
                .unwrap()
                .unwrap();
        assert_eq!(reopened.len(), 40);
        assert_eq!(reopened.get(&[3; 32]).unwrap().0, vec![4; 40]);
        drop(application);
        drop(checkpoint);
        drop(reopened);
        // Every region view retains the same exclusive lease; dropping the
        // last view must release it before the admitted allocation reopens.
        let reopened = SchemaControlMap::<[u8; 32]>::open_existing(
            Rc::new(owner.open()),
            ControlRecordFamily::Checkpoint,
        )
        .unwrap()
        .unwrap();
        assert_eq!(reopened.len(), 40);
        assert_eq!(reopened.get(&[3; 32]).unwrap().0, vec![4; 40]);
    }

    #[test]
    fn absent_control_inspection_preserves_marker_region_without_allocating() {
        let backing = Rc::new(test_memory(241));
        assert_eq!(backing.as_ref().grow(1), Ok(0));
        backing.write(0, &[19]);
        let memory = backing.clone();
        for family in [
            ControlRecordFamily::Application,
            ControlRecordFamily::Checkpoint,
        ] {
            assert!(
                SchemaControlMap::<[u8; 32]>::open_existing(memory.clone(), family)
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(backing.size(), 1);
        let mut byte = [0];
        backing.read(0, &mut byte);
        assert_eq!(byte, [19]);
        assert_eq!(CONTROL_START_PAGE, 257);
    }

    #[test]
    fn malformed_control_headers_and_unknown_namespaces_fail_without_reinitializing() {
        for (offset, byte) in [(0, b'X'), (3, 0), (52, b'X'), (55, 0)] {
            let memory = Rc::new(test_memory(243));
            SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application)
                .unwrap();
            let offset = CONTROL_START_PAGE * 65_536 + offset;
            memory.write(offset, &[byte]);
            assert!(
                SchemaControlMap::<[u8; 32]>::open(
                    memory.clone(),
                    ControlRecordFamily::Application
                )
                .is_err()
            );
            let mut observed = [0];
            memory.read(offset, &mut observed);
            assert_eq!(observed, [byte]);
        }
        for tag in [0, 3] {
            let memory = Rc::new(test_memory(243));
            let mut view = SchemaControlMap::<[u8; 32]>::open(
                memory.clone(),
                ControlRecordFamily::Application,
            )
            .unwrap();
            let mut key = [0; 33];
            key[0] = tag;
            key[32] = 1;
            view.map.insert(key, SchemaControlBytes(vec![0]));
            assert!(
                SchemaControlMap::<[u8; 32]>::open_existing(
                    memory,
                    ControlRecordFamily::Application
                )
                .is_err()
            );
        }
        let memory = Rc::new(test_memory(243));
        let mut view =
            SchemaControlMap::<[u8; 32]>::open(memory.clone(), ControlRecordFamily::Application)
                .unwrap();
        view.corrupt_header_for_tests();
        assert!(
            SchemaControlMap::<[u8; 32]>::open_existing(memory, ControlRecordFamily::Application)
                .is_err()
        );
    }
}
