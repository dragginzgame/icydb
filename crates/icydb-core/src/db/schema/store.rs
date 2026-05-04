//! Module: db::schema::store
//! Responsibility: stable BTreeMap-backed schema metadata persistence.
//! Does not own: reconciliation policy, typed snapshot encoding, or generated proposal construction.
//! Boundary: provides the third per-store stable memory alongside row and index stores.

use crate::{
    db::schema::{
        PersistedSchemaSnapshot, SchemaVersion, decode_persisted_schema_snapshot,
        encode_persisted_schema_snapshot, schema_snapshot_integrity_detail,
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
        validate_typed_schema_snapshot_for_store(snapshot)?;

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

// Validate typed schema snapshots before they are encoded into the raw schema
// metadata store. This catches caller-side invariant violations separately from
// raw persisted-byte corruption handled by the codec decode boundary.
fn validate_typed_schema_snapshot_for_store(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    if let Some(detail) = schema_snapshot_integrity_detail(
        "schema snapshot",
        snapshot.version(),
        snapshot.primary_key_field_id(),
        snapshot.row_layout(),
        snapshot.fields(),
    ) {
        return Err(InternalError::store_invariant(detail));
    }

    Ok(())
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
    #[cfg(test)]
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

    /// Load and decode the highest stored schema snapshot version for one entity.
    pub(in crate::db) fn latest_persisted_snapshot(
        &self,
        entity: EntityTag,
    ) -> Result<Option<PersistedSchemaSnapshot>, InternalError> {
        let mut latest = None::<(SchemaVersion, RawSchemaSnapshot)>;
        for entry in self.map.iter() {
            let (key, snapshot) = entry.into_pair();
            if key.entity_tag() != entity {
                continue;
            }

            let version = SchemaVersion::new(key.version());
            if latest
                .as_ref()
                .is_none_or(|(latest_version, _)| version > *latest_version)
            {
                latest = Some((version, snapshot));
            }
        }

        latest
            .map(|(_, snapshot)| snapshot.decode_persisted_snapshot())
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
    #[cfg(test)]
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
            FieldId, PersistedFieldKind, PersistedFieldSnapshot, PersistedNestedLeafSnapshot,
            PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaRowLayout,
            SchemaVersion, encode_persisted_schema_snapshot,
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
    fn schema_store_loads_latest_snapshot_for_entity() {
        let mut store = SchemaStore::init(test_memory(252));
        let initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
        let newer = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Newer");
        let other_entity = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "Other");

        store
            .insert_persisted_snapshot(EntityTag::new(41), &initial)
            .expect("initial schema snapshot should encode");
        store
            .insert_persisted_snapshot(EntityTag::new(42), &other_entity)
            .expect("other entity schema snapshot should encode");
        store
            .insert_persisted_snapshot(EntityTag::new(41), &newer)
            .expect("newer schema snapshot should encode");

        let latest = store
            .latest_persisted_snapshot(EntityTag::new(41))
            .expect("latest schema snapshot should decode")
            .expect("schema snapshot should exist");

        assert_eq!(latest.version(), SchemaVersion::new(2));
        assert_eq!(latest.entity_name(), "Newer");
    }

    #[test]
    fn schema_store_rejects_mismatched_snapshot_and_layout_versions() {
        let mut store = SchemaStore::init(test_memory(253));
        let invalid = persisted_schema_snapshot_with_layout_version_for_test(
            SchemaVersion::new(2),
            SchemaVersion::initial(),
            "Invalid",
        );

        let err = store
            .insert_persisted_snapshot(EntityTag::new(43), &invalid)
            .expect_err("schema store should reject mismatched snapshot/layout versions");

        assert!(
            err.message()
                .contains("schema snapshot row-layout version mismatch"),
            "schema store should preserve the version mismatch diagnostic"
        );
    }

    #[test]
    fn schema_store_rejects_typed_snapshot_with_divergent_field_slots() {
        let mut store = SchemaStore::init(test_memory(254));
        let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "InvalidSlots");
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.primary_key_field_id(),
            SchemaRowLayout::new(
                base.version(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(3)),
                ],
            ),
            base.fields().to_vec(),
        );

        let err = store
            .insert_persisted_snapshot(EntityTag::new(44), &invalid)
            .expect_err("schema store should reject divergent field/layout slots");

        assert!(
            err.message()
                .contains("schema snapshot field slot mismatch"),
            "schema store should report the duplicated slot divergence"
        );
    }

    #[test]
    fn schema_store_rejects_typed_snapshot_with_duplicate_row_layout_slot() {
        let mut store = SchemaStore::init(test_memory(246));
        let base =
            persisted_schema_snapshot_for_test(SchemaVersion::initial(), "DuplicateLayoutSlot");
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.primary_key_field_id(),
            SchemaRowLayout::new(
                base.version(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(0)),
                ],
            ),
            base.fields().to_vec(),
        );

        let err = store
            .insert_persisted_snapshot(EntityTag::new(49), &invalid)
            .expect_err("schema store should reject duplicate row-layout slots");

        assert!(
            err.message()
                .contains("schema snapshot duplicate row-layout slot"),
            "schema store should report the row-layout slot ambiguity"
        );
    }

    #[test]
    fn schema_store_rejects_typed_snapshot_with_missing_primary_key_field() {
        let mut store = SchemaStore::init(test_memory(248));
        let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "MissingPk");
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            FieldId::new(99),
            base.row_layout().clone(),
            base.fields().to_vec(),
        );

        let err = store
            .insert_persisted_snapshot(EntityTag::new(47), &invalid)
            .expect_err("schema store should reject snapshots without the primary-key field");

        assert!(
            err.message()
                .contains("schema snapshot primary key field missing from row layout"),
            "schema store should report the missing primary-key field"
        );
    }

    #[test]
    fn schema_store_does_not_fallback_when_latest_snapshot_is_corrupt() {
        let mut store = SchemaStore::init(test_memory(249));
        let initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
        let corrupt_key =
            RawSchemaKey::from_entity_version(EntityTag::new(45), SchemaVersion::new(3));

        store
            .insert_persisted_snapshot(EntityTag::new(45), &initial)
            .expect("initial schema snapshot should encode");
        store.insert_raw_snapshot(corrupt_key, RawSchemaSnapshot::from_bytes(vec![0xff, 0x00]));

        let err = store
            .latest_persisted_snapshot(EntityTag::new(45))
            .expect_err("latest corrupt schema snapshot must fail closed");

        assert!(
            err.message()
                .contains("failed to decode persisted schema snapshot"),
            "latest-version lookup should report the corrupt newest snapshot"
        );
    }

    #[test]
    fn schema_store_rejects_raw_snapshot_with_divergent_field_slots() {
        let mut store = SchemaStore::init(test_memory(250));
        let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "RawInvalidSlots");
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.primary_key_field_id(),
            SchemaRowLayout::new(
                base.version(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), SchemaFieldSlot::new(3)),
                ],
            ),
            base.fields().to_vec(),
        );
        let raw = encode_persisted_schema_snapshot(&invalid)
            .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
        let key = RawSchemaKey::from_entity_version(EntityTag::new(46), invalid.version());

        store.insert_raw_snapshot(key, RawSchemaSnapshot::from_bytes(raw));

        let err = store
            .latest_persisted_snapshot(EntityTag::new(46))
            .expect_err("raw decode should reject divergent field/layout slots");

        assert!(
            err.message()
                .contains("persisted schema snapshot field slot mismatch"),
            "schema codec should report the raw decoded slot divergence"
        );
    }

    #[test]
    fn schema_store_rejects_raw_snapshot_with_missing_primary_key_field() {
        let mut store = SchemaStore::init(test_memory(247));
        let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "RawMissingPk");
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            FieldId::new(99),
            base.row_layout().clone(),
            base.fields().to_vec(),
        );
        let raw = encode_persisted_schema_snapshot(&invalid)
            .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
        let key = RawSchemaKey::from_entity_version(EntityTag::new(48), invalid.version());

        store.insert_raw_snapshot(key, RawSchemaSnapshot::from_bytes(raw));

        let err = store
            .latest_persisted_snapshot(EntityTag::new(48))
            .expect_err("raw decode should reject snapshots without the primary-key field");

        assert!(
            err.message()
                .contains("persisted schema snapshot primary key field missing from row layout"),
            "schema codec should report the raw decoded missing primary-key field"
        );
    }

    #[test]
    fn schema_store_rejects_raw_snapshot_with_duplicate_field_name() {
        let mut store = SchemaStore::init(test_memory(245));
        let base =
            persisted_schema_snapshot_for_test(SchemaVersion::initial(), "DuplicateFieldName");
        let mut fields = base.fields().to_vec();
        let duplicate = PersistedFieldSnapshot::new(
            fields[1].id(),
            fields[0].name().to_string(),
            fields[1].slot(),
            fields[1].kind().clone(),
            fields[1].nested_leaves().to_vec(),
            fields[1].nullable(),
            fields[1].default(),
            fields[1].storage_decode(),
            fields[1].leaf_codec(),
        );
        fields[1] = duplicate;
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.primary_key_field_id(),
            base.row_layout().clone(),
            fields,
        );
        let raw = encode_persisted_schema_snapshot(&invalid)
            .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
        let key = RawSchemaKey::from_entity_version(EntityTag::new(50), invalid.version());

        store.insert_raw_snapshot(key, RawSchemaSnapshot::from_bytes(raw));

        let err = store
            .latest_persisted_snapshot(EntityTag::new(50))
            .expect_err("raw decode should reject duplicate field names");

        assert!(
            err.message()
                .contains("persisted schema snapshot duplicate field name"),
            "schema codec should report the raw decoded field-name ambiguity"
        );
    }

    #[test]
    fn schema_store_rejects_typed_snapshot_with_empty_nested_leaf_path() {
        let mut store = SchemaStore::init(test_memory(244));
        let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "EmptyNestedLeaf");
        let mut fields = base.fields().to_vec();
        let invalid_field = PersistedFieldSnapshot::new(
            fields[1].id(),
            fields[1].name().to_string(),
            fields[1].slot(),
            fields[1].kind().clone(),
            vec![PersistedNestedLeafSnapshot::new(
                Vec::new(),
                PersistedFieldKind::Blob,
                false,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Blob),
            )],
            fields[1].nullable(),
            fields[1].default(),
            fields[1].storage_decode(),
            fields[1].leaf_codec(),
        );
        fields[1] = invalid_field;
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.primary_key_field_id(),
            base.row_layout().clone(),
            fields,
        );

        let err = store
            .insert_persisted_snapshot(EntityTag::new(51), &invalid)
            .expect_err("schema store should reject empty nested leaf paths");

        assert!(
            err.message()
                .contains("schema snapshot empty nested leaf path"),
            "schema store should report the empty nested leaf path"
        );
    }

    #[test]
    fn schema_store_rejects_raw_snapshot_with_duplicate_nested_leaf_path() {
        let mut store = SchemaStore::init(test_memory(243));
        let base =
            persisted_schema_snapshot_for_test(SchemaVersion::initial(), "DuplicateNestedLeaf");
        let mut fields = base.fields().to_vec();
        let duplicate_leaves = vec![
            PersistedNestedLeafSnapshot::new(
                vec!["bytes".to_string()],
                PersistedFieldKind::Blob,
                false,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Blob),
            ),
            PersistedNestedLeafSnapshot::new(
                vec!["bytes".to_string()],
                PersistedFieldKind::Text { max_len: None },
                false,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Text),
            ),
        ];
        let invalid_field = PersistedFieldSnapshot::new(
            fields[1].id(),
            fields[1].name().to_string(),
            fields[1].slot(),
            fields[1].kind().clone(),
            duplicate_leaves,
            fields[1].nullable(),
            fields[1].default(),
            fields[1].storage_decode(),
            fields[1].leaf_codec(),
        );
        fields[1] = invalid_field;
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.primary_key_field_id(),
            base.row_layout().clone(),
            fields,
        );
        let raw = encode_persisted_schema_snapshot(&invalid)
            .expect("invalid raw schema snapshot should encode for decode-boundary coverage");
        let key = RawSchemaKey::from_entity_version(EntityTag::new(52), invalid.version());

        store.insert_raw_snapshot(key, RawSchemaSnapshot::from_bytes(raw));

        let err = store
            .latest_persisted_snapshot(EntityTag::new(52))
            .expect_err("raw decode should reject duplicate nested leaf paths");

        assert!(
            err.message()
                .contains("persisted schema snapshot duplicate nested leaf path"),
            "schema codec should report the raw decoded nested path ambiguity"
        );
    }

    #[test]
    fn raw_schema_snapshot_encodes_and_decodes_typed_snapshot() {
        let snapshot = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Encoded");

        let raw = RawSchemaSnapshot::from_persisted_snapshot(&snapshot)
            .expect("schema snapshot should encode");
        let decoded = raw
            .decode_persisted_snapshot()
            .expect("schema snapshot should decode");

        assert_eq!(decoded, snapshot);
    }

    // Build one typed schema snapshot used by schema-store tests. The exact
    // field contracts are intentionally rich enough to cover nested metadata,
    // scalar codecs, and structural fallback payloads through the raw store.
    fn persisted_schema_snapshot_for_test(
        version: SchemaVersion,
        entity_name: &str,
    ) -> PersistedSchemaSnapshot {
        persisted_schema_snapshot_with_layout_version_for_test(version, version, entity_name)
    }

    // Build one typed schema snapshot with independently selectable snapshot
    // and row-layout versions. Production snapshots should keep these aligned;
    // tests can deliberately break that invariant at the store boundary.
    fn persisted_schema_snapshot_with_layout_version_for_test(
        version: SchemaVersion,
        layout_version: SchemaVersion,
        entity_name: &str,
    ) -> PersistedSchemaSnapshot {
        PersistedSchemaSnapshot::new(
            version,
            format!("entities::{entity_name}"),
            entity_name.to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                layout_version,
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
                    Vec::new(),
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
                    vec![PersistedNestedLeafSnapshot::new(
                        vec!["bytes".to_string()],
                        PersistedFieldKind::Blob,
                        false,
                        FieldStorageDecode::ByKind,
                        LeafCodec::Scalar(ScalarCodec::Blob),
                    )],
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
            ],
        )
    }
}
