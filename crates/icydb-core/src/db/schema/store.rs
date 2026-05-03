//! Module: db::schema::store
//! Responsibility: stable BTreeMap-backed schema metadata persistence.
//! Does not own: reconciliation policy, typed snapshot encoding, or generated proposal construction.
//! Boundary: provides the third per-store stable memory alongside row and index stores.

use crate::{
    db::schema::{
        PersistedSchemaSnapshot, SchemaVersion, decode_persisted_schema_snapshot,
        encode_persisted_schema_snapshot,
    },
    error::InternalError,
    traits::Storable,
    types::EntityTag,
};
use canic_cdk::structures::{BTreeMap, DefaultMemoryImpl, memory::VirtualMemory, storable::Bound};
use std::borrow::Cow;

const SCHEMA_KEY_BYTES_USIZE: usize = 12;
const SCHEMA_KEY_BYTES: u32 = 12;
const MAX_SCHEMA_SNAPSHOT_BYTES: u32 = 512 * 1024;

///
/// RawSchemaKey
///
/// Stable key for one persisted schema snapshot entry.
/// It combines the entity tag and schema version so reconciliation can load
/// concrete versions without depending on generated entity names.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RawSchemaKey([u8; SCHEMA_KEY_BYTES_USIZE]);

#[allow(
    dead_code,
    reason = "raw schema keys are populated by upcoming startup reconciliation"
)]
impl RawSchemaKey {
    /// Build the raw persisted key for one entity schema version.
    #[must_use]
    fn from_entity_version(entity: EntityTag, version: SchemaVersion) -> Self {
        let mut out = [0u8; SCHEMA_KEY_BYTES_USIZE];
        out[..size_of::<u64>()].copy_from_slice(&entity.value().to_be_bytes());
        out[size_of::<u64>()..].copy_from_slice(&version.get().to_be_bytes());

        Self(out)
    }

    /// Return the entity tag encoded in this schema key.
    #[must_use]
    fn entity_tag(self) -> EntityTag {
        let mut bytes = [0u8; size_of::<u64>()];
        bytes.copy_from_slice(&self.0[..size_of::<u64>()]);

        EntityTag::new(u64::from_be_bytes(bytes))
    }

    /// Return the schema version encoded in this schema key.
    #[must_use]
    fn version(self) -> u32 {
        let mut bytes = [0u8; size_of::<u32>()];
        bytes.copy_from_slice(&self.0[size_of::<u64>()..]);

        u32::from_be_bytes(bytes)
    }
}

impl Storable for RawSchemaKey {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(&self.0)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        debug_assert_eq!(
            bytes.len(),
            SCHEMA_KEY_BYTES_USIZE,
            "RawSchemaKey::from_bytes received unexpected byte length",
        );

        if bytes.len() != SCHEMA_KEY_BYTES_USIZE {
            return Self([0u8; SCHEMA_KEY_BYTES_USIZE]);
        }

        let mut out = [0u8; SCHEMA_KEY_BYTES_USIZE];
        out.copy_from_slice(bytes.as_ref());
        Self(out)
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0.to_vec()
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: SCHEMA_KEY_BYTES,
        is_fixed_size: true,
    };
}

///
/// RawSchemaSnapshot
///
/// Raw persisted schema snapshot payload.
/// This wrapper stores the encoded `PersistedSchemaSnapshot` payload while
/// keeping the stable-memory value boundary independent from the typed schema
/// DTOs used by reconciliation.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawSchemaSnapshot(Vec<u8>);

impl RawSchemaSnapshot {
    /// Encode one typed persisted-schema snapshot into a raw store payload.
    fn from_persisted_snapshot(snapshot: &PersistedSchemaSnapshot) -> Result<Self, InternalError> {
        encode_persisted_schema_snapshot(snapshot).map(Self)
    }

    /// Build one raw schema snapshot from already-encoded bytes.
    #[must_use]
    #[cfg(test)]
    const fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Borrow the encoded schema snapshot payload.
    #[must_use]
    const fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Consume the snapshot into its encoded payload bytes.
    #[must_use]
    #[cfg(test)]
    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Decode this raw store payload into a typed persisted-schema snapshot.
    fn decode_persisted_snapshot(&self) -> Result<PersistedSchemaSnapshot, InternalError> {
        decode_persisted_schema_snapshot(self.as_bytes())
    }
}

impl Storable for RawSchemaSnapshot {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self.as_bytes())
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        Self(bytes.into_owned())
    }

    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    const BOUND: Bound = Bound::Bounded {
        max_size: MAX_SCHEMA_SNAPSHOT_BYTES,
        is_fixed_size: false,
    };
}

///
/// SchemaStore
///
/// Thin persistence wrapper over one stable schema metadata BTreeMap.
/// Startup reconciliation writes and validates encoded schema snapshots here
/// before row/index operations proceed.
///

pub struct SchemaStore {
    map: BTreeMap<RawSchemaKey, RawSchemaSnapshot, VirtualMemory<DefaultMemoryImpl>>,
}

impl SchemaStore {
    /// Initialize the schema store with the provided backing memory.
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            map: BTreeMap::init(memory),
        }
    }

    /// Insert or replace one typed persisted schema snapshot.
    pub(in crate::db) fn insert_persisted_snapshot(
        &mut self,
        entity: EntityTag,
        snapshot: &PersistedSchemaSnapshot,
    ) -> Result<(), InternalError> {
        let key = RawSchemaKey::from_entity_version(entity, snapshot.version());
        let raw_snapshot = RawSchemaSnapshot::from_persisted_snapshot(snapshot)?;
        let _ = self.insert_raw_snapshot(key, raw_snapshot);

        Ok(())
    }

    /// Load and decode one typed persisted schema snapshot.
    pub(in crate::db) fn get_persisted_snapshot(
        &self,
        entity: EntityTag,
        version: SchemaVersion,
    ) -> Result<Option<PersistedSchemaSnapshot>, InternalError> {
        let key = RawSchemaKey::from_entity_version(entity, version);
        self.get_raw_snapshot(&key)
            .map(|snapshot| snapshot.decode_persisted_snapshot())
            .transpose()
    }

    /// Insert or replace one raw schema snapshot.
    fn insert_raw_snapshot(
        &mut self,
        key: RawSchemaKey,
        snapshot: RawSchemaSnapshot,
    ) -> Option<RawSchemaSnapshot> {
        self.map.insert(key, snapshot)
    }

    /// Load one raw schema snapshot by key.
    #[must_use]
    fn get_raw_snapshot(&self, key: &RawSchemaKey) -> Option<RawSchemaSnapshot> {
        self.map.get(key)
    }

    /// Return whether one schema snapshot key is present.
    #[must_use]
    #[cfg(test)]
    fn contains_raw_snapshot(&self, key: &RawSchemaKey) -> bool {
        self.map.contains_key(key)
    }

    /// Return the number of schema snapshot entries in this store.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn len(&self) -> u64 {
        self.map.len()
    }

    /// Return whether this schema store currently has no persisted snapshots.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Clear all schema metadata entries from the store.
    #[cfg(test)]
    pub(in crate::db) fn clear(&mut self) {
        self.map.clear();
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{RawSchemaKey, RawSchemaSnapshot, SchemaStore};
    use crate::{
        db::schema::{
            FieldId, PersistedFieldKind, PersistedFieldSnapshot, PersistedSchemaSnapshot,
            SchemaFieldDefault, SchemaFieldSlot, SchemaRowLayout, SchemaVersion,
        },
        model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
        testing::test_memory,
        traits::Storable,
        types::EntityTag,
    };
    use std::borrow::Cow;

    #[test]
    fn raw_schema_key_round_trips_entity_and_version() {
        let key = RawSchemaKey::from_entity_version(EntityTag::new(0x0102_0304_0506_0708), {
            SchemaVersion::initial()
        });
        let encoded = key.to_bytes().into_owned();
        let decoded = RawSchemaKey::from_bytes(Cow::Owned(encoded));

        assert_eq!(decoded.entity_tag(), EntityTag::new(0x0102_0304_0506_0708));
        assert_eq!(decoded.version(), SchemaVersion::initial().get());
    }

    #[test]
    fn raw_schema_snapshot_round_trips_payload_bytes() {
        let snapshot = RawSchemaSnapshot::from_bytes(vec![1, 2, 3, 5, 8]);
        let encoded = snapshot.to_bytes().into_owned();
        let decoded = <RawSchemaSnapshot as Storable>::from_bytes(Cow::Owned(encoded));

        assert_eq!(decoded.as_bytes(), &[1, 2, 3, 5, 8]);
        assert_eq!(decoded.into_bytes(), vec![1, 2, 3, 5, 8]);
    }

    #[test]
    fn schema_store_persists_raw_snapshots_by_entity_version_key() {
        let mut store = SchemaStore::init(test_memory(251));
        let key = RawSchemaKey::from_entity_version(EntityTag::new(17), SchemaVersion::initial());

        assert!(store.is_empty());
        assert!(!store.contains_raw_snapshot(&key));

        store.insert_raw_snapshot(key, RawSchemaSnapshot::from_bytes(vec![9, 4, 6]));

        assert_eq!(store.len(), 1);
        assert!(store.contains_raw_snapshot(&key));
        assert_eq!(
            store
                .get_raw_snapshot(&key)
                .expect("schema snapshot should be present")
                .as_bytes(),
            &[9, 4, 6],
        );

        store.clear();
        assert!(store.is_empty());
    }

    #[test]
    fn raw_schema_snapshot_encodes_and_decodes_typed_snapshot() {
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "entities::Encoded".to_string(),
            "Encoded".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(1)),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::Scalar(ScalarCodec::Ulid),
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "payload".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Map {
                        key: Box::new(PersistedFieldKind::Text { max_len: None }),
                        value: Box::new(PersistedFieldKind::List(Box::new(
                            PersistedFieldKind::Uint,
                        ))),
                    },
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
            ],
        );

        let raw = RawSchemaSnapshot::from_persisted_snapshot(&snapshot)
            .expect("schema snapshot should encode");
        let decoded = raw
            .decode_persisted_snapshot()
            .expect("schema snapshot should decode");

        assert_eq!(decoded, snapshot);
    }
}
