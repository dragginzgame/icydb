//! Module: db::schema::store
//! Responsibility: stable BTreeMap-backed schema metadata persistence.
//! Does not own: reconciliation policy, typed snapshot encoding, or generated proposal construction.
//! Boundary: provides the third per-store stable memory alongside row and index stores.

use crate::{
    db::{
        codec::{
            finalize_hash_sha256, new_hash_sha256, write_hash_len_u32, write_hash_str_u32,
            write_hash_tag_u8, write_hash_u32, write_hash_u64,
        },
        commit::CommitSchemaFingerprint,
        direction::Direction,
        ordered_overlay::{OrderedOverlayEntry, OrderedOverlayVisit, visit_ordered_overlay},
        schema::{
            AcceptedSchemaSnapshot, PersistedFieldKind, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedSchemaSnapshot, SchemaVersion,
            accepted_schema_cache_fingerprint, accepted_schema_cache_fingerprint_from_raw,
            accepted_schema_cache_fingerprint_method_version, decode_persisted_schema_snapshot,
            encode_persisted_schema_snapshot, schema_snapshot_integrity_detail,
        },
    },
    error::InternalError,
    traits::Storable,
    types::EntityTag,
};
use ic_memory::stable_structures::storable::Bound as StorableBound;
use ic_memory::stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, memory_manager::VirtualMemory,
};
use sha2::Digest;
use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;
use std::collections::{BTreeMap as StdBTreeMap, BTreeSet};
use std::convert::Infallible;
use std::ops::Bound as RangeBound;

const SCHEMA_KEY_BYTES_USIZE: usize = 12;
const SCHEMA_KEY_BYTES: u32 = 12;
pub(in crate::db) const MAX_SCHEMA_SNAPSHOT_BYTES: u32 = 512 * 1024;
const SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION: u8 = 1;
const SCHEMA_STORE_DATA_ALLOCATION_FINGERPRINT_VERSION: u8 = 2;
const SCHEMA_STORE_INDEX_ALLOCATION_FINGERPRINT_VERSION: u8 = 3;

#[cfg(test)]
thread_local! {
    static LATEST_RAW_SNAPSHOTS_BY_ENTITY_CALLS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(in crate::db) fn reset_latest_raw_snapshots_by_entity_call_count_for_tests() {
    LATEST_RAW_SNAPSHOTS_BY_ENTITY_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(in crate::db) fn latest_raw_snapshots_by_entity_call_count_for_tests() -> u64 {
    LATEST_RAW_SNAPSHOTS_BY_ENTITY_CALLS.with(Cell::get)
}

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

    fn entity_range_bounds(entity: EntityTag) -> (RangeBound<Self>, RangeBound<Self>) {
        (
            RangeBound::Included(Self::from_entity_version(entity, SchemaVersion::new(0))),
            RangeBound::Included(Self::from_entity_version(
                entity,
                SchemaVersion::new(u32::MAX),
            )),
        )
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

    const BOUND: StorableBound = StorableBound::Bounded {
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
    fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Decode this raw store payload into a typed persisted-schema snapshot.
    fn decode_persisted_snapshot(&self) -> Result<PersistedSchemaSnapshot, InternalError> {
        decode_persisted_schema_snapshot(self.as_bytes())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedCatalogIdentity {
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    accepted_schema_version: SchemaVersion,
    fingerprint_method_version: u8,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
}

impl AcceptedCatalogIdentity {
    #[must_use]
    pub(in crate::db) const fn new(
        entity_tag: EntityTag,
        entity_path: &'static str,
        store_path: &'static str,
        accepted_schema_version: SchemaVersion,
        accepted_schema_fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            entity_tag,
            entity_path,
            store_path,
            accepted_schema_version,
            fingerprint_method_version: accepted_schema_cache_fingerprint_method_version(),
            accepted_schema_fingerprint,
        }
    }

    #[must_use]
    pub(in crate::db) const fn entity_tag(self) -> EntityTag {
        self.entity_tag
    }

    #[must_use]
    pub(in crate::db) const fn entity_path(self) -> &'static str {
        self.entity_path
    }

    #[must_use]
    pub(in crate::db) const fn store_path(self) -> &'static str {
        self.store_path
    }

    #[must_use]
    pub(in crate::db) const fn accepted_schema_version(self) -> SchemaVersion {
        self.accepted_schema_version
    }

    #[must_use]
    pub(in crate::db) const fn fingerprint_method_version(self) -> u8 {
        self.fingerprint_method_version
    }

    #[must_use]
    pub(in crate::db) const fn accepted_schema_fingerprint(self) -> CommitSchemaFingerprint {
        self.accepted_schema_fingerprint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedCatalogSnapshotSelection {
    identity: AcceptedCatalogIdentity,
    raw_snapshot: Vec<u8>,
}

impl AcceptedCatalogSnapshotSelection {
    #[must_use]
    const fn new(identity: AcceptedCatalogIdentity, raw_snapshot: Vec<u8>) -> Self {
        Self {
            identity,
            raw_snapshot,
        }
    }

    #[must_use]
    pub(in crate::db) const fn identity(&self) -> AcceptedCatalogIdentity {
        self.identity
    }

    pub(in crate::db) fn decode_verified(&self) -> Result<AcceptedSchemaSnapshot, InternalError> {
        let snapshot = decode_persisted_schema_snapshot(&self.raw_snapshot)?;
        let accepted = AcceptedSchemaSnapshot::try_new(snapshot)?;
        let identity = self.identity();

        if accepted.persisted_snapshot().version() != identity.accepted_schema_version() {
            return Err(InternalError::store_invariant(
                "accepted catalog identity selected a different schema version than the decoded snapshot",
            ));
        }
        if accepted.entity_path() != identity.entity_path() {
            return Err(InternalError::store_invariant(
                "accepted catalog identity selected a different entity path than the decoded snapshot",
            ));
        }

        let decoded_fingerprint = accepted_schema_cache_fingerprint(&accepted)?;
        if decoded_fingerprint != identity.accepted_schema_fingerprint() {
            return Err(InternalError::store_invariant(
                "accepted catalog identity fingerprint did not match the decoded snapshot",
            ));
        }

        Ok(accepted)
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

    const BOUND: StorableBound = StorableBound::Bounded {
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
        snapshot.primary_key_field_ids(),
        snapshot.row_layout(),
        snapshot.fields(),
    ) {
        return Err(InternalError::store_invariant(detail));
    }

    Ok(())
}

///
/// SchemaStoreFootprint
///
/// Current raw schema metadata footprint for one entity. Reconciliation uses
/// this value to report stable-memory pressure without decoding schema payloads
/// or exposing field-level metadata through metrics.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaStoreFootprint {
    snapshots: u64,
    encoded_bytes: u64,
    latest_snapshot_bytes: u64,
}

///
/// SchemaStoreCatalogMetadata
///
/// Accepted schema-store catalog metadata derived from latest persisted
/// snapshots. This is diagnostic allocation metadata, not allocation identity.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaStoreCatalogMetadata {
    schema_version: SchemaVersion,
    schema_fingerprint_method_version: u8,
    schema_fingerprint: CommitSchemaFingerprint,
    entity_count: u64,
}

impl SchemaStoreCatalogMetadata {
    /// Build catalog metadata from already-derived accepted schema facts.
    #[must_use]
    const fn new(
        schema_version: SchemaVersion,
        schema_fingerprint_method_version: u8,
        schema_fingerprint: CommitSchemaFingerprint,
        entity_count: u64,
    ) -> Self {
        Self {
            schema_version,
            schema_fingerprint_method_version,
            schema_fingerprint,
            entity_count,
        }
    }

    /// Return the maximum latest schema version represented in the catalog.
    #[must_use]
    pub(in crate::db) const fn schema_version(self) -> SchemaVersion {
        self.schema_version
    }

    /// Return the fingerprint method version for this diagnostic metadata row.
    #[must_use]
    pub(in crate::db) const fn schema_fingerprint_method_version(self) -> u8 {
        self.schema_fingerprint_method_version
    }

    /// Return the deterministic catalog fingerprint for latest accepted
    /// snapshots.
    #[must_use]
    pub(in crate::db) const fn schema_fingerprint(self) -> CommitSchemaFingerprint {
        self.schema_fingerprint
    }

    /// Return number of entity schemas represented in this catalog metadata.
    #[must_use]
    pub(in crate::db) const fn entity_count(self) -> u64 {
        self.entity_count
    }
}

///
/// SchemaStoreAllocationMetadata
///
/// Role-specific allocation metadata derived from latest accepted schema-store
/// snapshots. These fingerprints describe the accepted contract that owns each
/// allocation role; they are diagnostics, not allocation identity.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaStoreAllocationMetadata {
    data: SchemaStoreCatalogMetadata,
    index: SchemaStoreCatalogMetadata,
    schema: SchemaStoreCatalogMetadata,
}

impl SchemaStoreAllocationMetadata {
    /// Build one role-specific metadata set from already-derived accepted
    /// schema facts.
    #[must_use]
    const fn new(
        data: SchemaStoreCatalogMetadata,
        index: SchemaStoreCatalogMetadata,
        schema: SchemaStoreCatalogMetadata,
    ) -> Self {
        Self {
            data,
            index,
            schema,
        }
    }

    /// Return accepted row-layout allocation metadata for data memory.
    #[must_use]
    pub(in crate::db) const fn data(self) -> SchemaStoreCatalogMetadata {
        self.data
    }

    /// Return accepted index-catalog allocation metadata for index memory.
    #[must_use]
    pub(in crate::db) const fn index(self) -> SchemaStoreCatalogMetadata {
        self.index
    }

    /// Return accepted full schema-catalog allocation metadata for schema
    /// memory.
    #[must_use]
    pub(in crate::db) const fn schema(self) -> SchemaStoreCatalogMetadata {
        self.schema
    }
}

impl SchemaStoreFootprint {
    /// Build one schema-store footprint from already-counted raw payload facts.
    #[must_use]
    const fn new(snapshots: u64, encoded_bytes: u64, latest_snapshot_bytes: u64) -> Self {
        Self {
            snapshots,
            encoded_bytes,
            latest_snapshot_bytes,
        }
    }

    /// Return the number of raw schema snapshots stored for the entity.
    #[must_use]
    pub(in crate::db) const fn snapshots(self) -> u64 {
        self.snapshots
    }

    /// Return the total encoded payload bytes stored for the entity.
    #[must_use]
    pub(in crate::db) const fn encoded_bytes(self) -> u64 {
        self.encoded_bytes
    }

    /// Return the encoded payload bytes for the highest-version snapshot.
    #[must_use]
    pub(in crate::db) const fn latest_snapshot_bytes(self) -> u64 {
        self.latest_snapshot_bytes
    }
}

///
/// SchemaStore
///
/// Thin persistence wrapper over one stable or heap schema metadata BTreeMap.
/// Startup reconciliation writes and validates encoded schema snapshots here
/// before row/index operations proceed.
///

pub struct SchemaStore {
    backend: SchemaStoreBackend,
}

enum SchemaStoreBackend {
    Stable(StableBTreeMap<RawSchemaKey, RawSchemaSnapshot, VirtualMemory<DefaultMemoryImpl>>),
    Heap(StdBTreeMap<RawSchemaKey, RawSchemaSnapshot>),
    Journaled {
        canonical:
            StableBTreeMap<RawSchemaKey, RawSchemaSnapshot, VirtualMemory<DefaultMemoryImpl>>,
        live: StdBTreeMap<RawSchemaKey, RawSchemaSnapshot>,
        tombstones: BTreeSet<RawSchemaKey>,
    },
}

/// Control-flow result for schema-store traversal visitors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SchemaStoreVisit {
    Continue,
    #[allow(
        dead_code,
        reason = "schema traversal exposes early-stop semantics for bounded future callers; focused tests cover it before live call sites need it"
    )]
    Stop,
}

impl SchemaStoreVisit {
    const fn should_stop(self) -> bool {
        matches!(self, Self::Stop)
    }
}

impl SchemaStore {
    /// Initialize the schema store with the provided backing memory.
    #[must_use]
    pub fn init(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            backend: SchemaStoreBackend::Stable(StableBTreeMap::init(memory)),
        }
    }

    /// Initialize a volatile heap-backed schema store.
    #[must_use]
    pub const fn init_heap() -> Self {
        Self {
            backend: SchemaStoreBackend::Heap(StdBTreeMap::new()),
        }
    }

    /// Initialize a journaled cached-stable schema store.
    ///
    /// Normal schema publication writes only the live projection. Canonical
    /// stable schema history is updated by future journal fold/recovery paths.
    #[must_use]
    pub fn init_journaled(memory: VirtualMemory<DefaultMemoryImpl>) -> Self {
        Self {
            backend: SchemaStoreBackend::Journaled {
                canonical: StableBTreeMap::init(memory),
                live: StdBTreeMap::new(),
                tombstones: BTreeSet::new(),
            },
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

    /// Reset the volatile projection for journaled recovery without mutating
    /// the canonical stable schema base.
    pub(in crate::db) fn reset_journaled_live_projection(&mut self) -> Result<(), InternalError> {
        let SchemaStoreBackend::Journaled {
            live, tombstones, ..
        } = &mut self.backend
        else {
            return Err(InternalError::store_invariant(
                "journaled live projection reset requires a journaled schema store",
            ));
        };

        live.clear();
        tombstones.clear();

        Ok(())
    }

    /// Apply one folded journal schema snapshot into the canonical stable base.
    pub(in crate::db) fn fold_persisted_snapshot(
        &mut self,
        entity: EntityTag,
        snapshot: &PersistedSchemaSnapshot,
    ) -> Result<(), InternalError> {
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant(
                "journal schema fold requires a journaled schema store",
            ));
        };

        let key = RawSchemaKey::from_entity_version(entity, snapshot.version());
        let raw_snapshot = RawSchemaSnapshot::from_persisted_snapshot(snapshot)?;
        canonical.insert(key, raw_snapshot);

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
        self.latest_raw_snapshot(entity)
            .map(|snapshot| snapshot.decode_persisted_snapshot())
            .transpose()
    }

    /// Return the latest accepted catalog identity for one entity without
    /// decoding the selected schema snapshot.
    pub(in crate::db) fn latest_catalog_identity(
        &self,
        entity: EntityTag,
        entity_path: &'static str,
        store_path: &'static str,
    ) -> Option<AcceptedCatalogSnapshotSelection> {
        let (version, raw_snapshot) = self.latest_raw_snapshot_entry(entity)?;
        let fingerprint =
            accepted_schema_cache_fingerprint_from_raw(entity_path, raw_snapshot.as_bytes());
        let identity =
            AcceptedCatalogIdentity::new(entity, entity_path, store_path, version, fingerprint);

        Some(AcceptedCatalogSnapshotSelection::new(
            identity,
            raw_snapshot.into_bytes(),
        ))
    }

    /// Return raw schema-store footprint facts for one entity.
    #[must_use]
    pub(in crate::db) fn entity_footprint(&self, entity: EntityTag) -> SchemaStoreFootprint {
        let mut snapshots = 0u64;
        let mut encoded_bytes = 0u64;
        let mut latest = None::<(SchemaVersion, u64)>;

        let _: Result<(), std::convert::Infallible> = self.visit_raw_snapshots(|key, snapshot| {
            if key.entity_tag() != entity {
                return Ok(SchemaStoreVisit::Continue);
            }

            let snapshot_bytes = u64::try_from(snapshot.as_bytes().len()).unwrap_or(u64::MAX);
            snapshots = snapshots.saturating_add(1);
            encoded_bytes = encoded_bytes.saturating_add(snapshot_bytes);

            let version = SchemaVersion::new(key.version());
            if latest
                .as_ref()
                .is_none_or(|(latest_version, _)| version > *latest_version)
            {
                latest = Some((version, snapshot_bytes));
            }
            Ok(SchemaStoreVisit::Continue)
        });

        SchemaStoreFootprint::new(
            snapshots,
            encoded_bytes,
            latest.map_or(0, |(_, snapshot_bytes)| snapshot_bytes),
        )
    }

    /// Derive accepted catalog metadata from latest persisted schema snapshots.
    ///
    /// This function intentionally reads only the persisted schema store. It
    /// does not reconstruct metadata from generated models when the store has
    /// no accepted snapshots.
    #[cfg(test)]
    pub(in crate::db) fn catalog_metadata(
        &self,
    ) -> Result<Option<SchemaStoreCatalogMetadata>, InternalError> {
        Ok(self
            .allocation_metadata()?
            .map(SchemaStoreAllocationMetadata::schema))
    }

    /// Derive role-specific allocation metadata from latest persisted schema
    /// snapshots.
    ///
    /// This function intentionally reads only accepted schema-store payloads.
    /// It never reconstructs metadata from generated models when the store has
    /// no accepted snapshots.
    pub(in crate::db) fn allocation_metadata(
        &self,
    ) -> Result<Option<SchemaStoreAllocationMetadata>, InternalError> {
        let latest_by_entity = self.latest_raw_snapshots_by_entity();
        if latest_by_entity.is_empty() {
            return Ok(None);
        }

        Ok(Some(SchemaStoreAllocationMetadata::new(
            derive_data_allocation_metadata(&latest_by_entity)?,
            derive_index_allocation_metadata(&latest_by_entity)?,
            derive_schema_catalog_metadata(&latest_by_entity)?,
        )))
    }

    /// Insert or replace one raw schema snapshot.
    fn insert_raw_snapshot(
        &mut self,
        key: RawSchemaKey,
        snapshot: RawSchemaSnapshot,
    ) -> Option<RawSchemaSnapshot> {
        let previous_journaled = if matches!(self.backend, SchemaStoreBackend::Journaled { .. }) {
            self.get_raw_snapshot_for_backend(&key)
        } else {
            None
        };
        match &mut self.backend {
            SchemaStoreBackend::Stable(map) => map.insert(key, snapshot),
            SchemaStoreBackend::Heap(map) => map.insert(key, snapshot),
            SchemaStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                tombstones.remove(&key);
                live.insert(key, snapshot);
                previous_journaled
            }
        }
    }

    /// Load one raw schema snapshot by key.
    #[must_use]
    #[cfg(test)]
    fn get_raw_snapshot(&self, key: &RawSchemaKey) -> Option<RawSchemaSnapshot> {
        match &self.backend {
            SchemaStoreBackend::Stable(map) => map.get(key),
            SchemaStoreBackend::Heap(map) => map.get(key).cloned(),
            SchemaStoreBackend::Journaled { .. } => self.get_raw_snapshot_for_backend(key),
        }
    }

    /// Return whether one schema snapshot key is present.
    #[must_use]
    #[cfg(test)]
    fn contains_raw_snapshot(&self, key: &RawSchemaKey) -> bool {
        match &self.backend {
            SchemaStoreBackend::Stable(map) => map.contains_key(key),
            SchemaStoreBackend::Heap(map) => map.contains_key(key),
            SchemaStoreBackend::Journaled { .. } => {
                self.get_raw_snapshot_for_backend(key).is_some()
            }
        }
    }

    /// Return the number of schema snapshot entries in this store.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn len(&self) -> u64 {
        match &self.backend {
            SchemaStoreBackend::Stable(map) => map.len(),
            SchemaStoreBackend::Heap(map) => u64::try_from(map.len()).unwrap_or(u64::MAX),
            SchemaStoreBackend::Journaled { .. } => {
                let mut count = 0_u64;
                let _: Result<(), Infallible> = self.visit_raw_snapshots(|_key, _snapshot| {
                    count = count.saturating_add(1);
                    Ok(SchemaStoreVisit::Continue)
                });
                count
            }
        }
    }

    /// Return whether this schema store currently has no persisted snapshots.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn is_empty(&self) -> bool {
        match &self.backend {
            SchemaStoreBackend::Stable(map) => map.is_empty(),
            SchemaStoreBackend::Heap(map) => map.is_empty(),
            SchemaStoreBackend::Journaled { .. } => {
                let mut empty = true;
                let _: Result<(), Infallible> = self.visit_raw_snapshots(|_key, _snapshot| {
                    empty = false;
                    Ok(SchemaStoreVisit::Stop)
                });
                empty
            }
        }
    }

    /// Clear all schema metadata entries from the store.
    #[cfg(test)]
    pub(in crate::db) fn clear(&mut self) {
        match &mut self.backend {
            SchemaStoreBackend::Stable(map) => map.clear_new(),
            SchemaStoreBackend::Heap(map) => map.clear(),
            SchemaStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
            } => {
                live.clear();
                tombstones.clear();
                for entry in canonical.iter() {
                    tombstones.insert(*entry.key());
                }
            }
        }
    }

    fn latest_raw_snapshots_by_entity(
        &self,
    ) -> StdBTreeMap<EntityTag, (SchemaVersion, RawSchemaSnapshot)> {
        #[cfg(test)]
        LATEST_RAW_SNAPSHOTS_BY_ENTITY_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));

        let mut latest_by_entity =
            StdBTreeMap::<EntityTag, (SchemaVersion, RawSchemaSnapshot)>::new();

        let _: Result<(), std::convert::Infallible> = self.visit_raw_snapshots(|key, snapshot| {
            let version = SchemaVersion::new(key.version());
            match latest_by_entity.get_mut(&key.entity_tag()) {
                Some((latest_version, latest_snapshot)) if version > *latest_version => {
                    *latest_version = version;
                    *latest_snapshot = snapshot.clone();
                }
                None => {
                    latest_by_entity.insert(key.entity_tag(), (version, snapshot.clone()));
                }
                Some(_) => {}
            }
            Ok(SchemaStoreVisit::Continue)
        });

        latest_by_entity
    }

    /// Visit raw schema snapshots in canonical store order without exposing
    /// the backing stable-map iterator.
    fn visit_raw_snapshots<E>(
        &self,
        visitor: impl FnMut(&RawSchemaKey, &RawSchemaSnapshot) -> Result<SchemaStoreVisit, E>,
    ) -> Result<(), E> {
        match &self.backend {
            SchemaStoreBackend::Stable(map) => {
                let mut visitor = visitor;
                for entry in map.iter() {
                    if visitor(entry.key(), &entry.value())?.should_stop() {
                        break;
                    }
                }
            }
            SchemaStoreBackend::Heap(map) => {
                let mut visitor = visitor;
                for (key, snapshot) in map {
                    if visitor(key, snapshot)?.should_stop() {
                        break;
                    }
                }
            }
            SchemaStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
            } => Self::visit_journaled_raw_snapshot_range(
                canonical,
                live,
                tombstones,
                (RangeBound::Unbounded, RangeBound::Unbounded),
                Direction::Asc,
                visitor,
            )?,
        }

        Ok(())
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn canonical_len_for_tests(&self) -> u64 {
        match &self.backend {
            SchemaStoreBackend::Stable(map)
            | SchemaStoreBackend::Journaled { canonical: map, .. } => map.len(),
            SchemaStoreBackend::Heap(_) => 0,
        }
    }

    fn get_raw_snapshot_for_backend(&self, key: &RawSchemaKey) -> Option<RawSchemaSnapshot> {
        let SchemaStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
        } = &self.backend
        else {
            return None;
        };

        if tombstones.contains(key) {
            return None;
        }
        live.get(key).cloned().or_else(|| canonical.get(key))
    }

    fn latest_raw_snapshot(&self, entity: EntityTag) -> Option<RawSchemaSnapshot> {
        self.latest_raw_snapshot_entry(entity)
            .map(|(_, snapshot)| snapshot)
    }

    fn latest_raw_snapshot_entry(
        &self,
        entity: EntityTag,
    ) -> Option<(SchemaVersion, RawSchemaSnapshot)> {
        let bounds = RawSchemaKey::entity_range_bounds(entity);
        match &self.backend {
            SchemaStoreBackend::Stable(map) => map
                .range((bounds.0, bounds.1))
                .next_back()
                .map(|entry| (SchemaVersion::new(entry.key().version()), entry.value())),
            SchemaStoreBackend::Heap(map) => map
                .range((bounds.0, bounds.1))
                .next_back()
                .map(|(key, snapshot)| (SchemaVersion::new(key.version()), snapshot.clone())),
            SchemaStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
            } => {
                let mut latest = None;
                let _: Result<(), Infallible> = Self::visit_journaled_raw_snapshot_range(
                    canonical,
                    live,
                    tombstones,
                    bounds,
                    Direction::Desc,
                    |key, snapshot| {
                        latest = Some((SchemaVersion::new(key.version()), snapshot.clone()));
                        Ok(SchemaStoreVisit::Stop)
                    },
                );
                latest
            }
        }
    }

    fn visit_journaled_raw_snapshot_range<E>(
        canonical: &StableBTreeMap<
            RawSchemaKey,
            RawSchemaSnapshot,
            VirtualMemory<DefaultMemoryImpl>,
        >,
        live: &StdBTreeMap<RawSchemaKey, RawSchemaSnapshot>,
        tombstones: &BTreeSet<RawSchemaKey>,
        bounds: (RangeBound<RawSchemaKey>, RangeBound<RawSchemaKey>),
        direction: Direction,
        mut visitor: impl FnMut(&RawSchemaKey, &RawSchemaSnapshot) -> Result<SchemaStoreVisit, E>,
    ) -> Result<(), E> {
        match direction {
            Direction::Asc => visit_ordered_overlay(
                canonical.range((bounds.0, bounds.1)),
                live.range((bounds.0, bounds.1)),
                Direction::Asc,
                |canonical_entry, live_entry| canonical_entry.key().cmp(live_entry.0),
                |canonical_entry| !tombstones.contains(canonical_entry.key()),
                |live_entry| !tombstones.contains(live_entry.0),
                |entry| {
                    let visit = match entry {
                        OrderedOverlayEntry::Canonical(canonical_entry) => {
                            visitor(canonical_entry.key(), &canonical_entry.value())?
                        }
                        OrderedOverlayEntry::Live((key, snapshot)) => visitor(key, snapshot)?,
                    };
                    Ok(if visit.should_stop() {
                        OrderedOverlayVisit::Stop
                    } else {
                        OrderedOverlayVisit::Continue
                    })
                },
            ),
            Direction::Desc => visit_ordered_overlay(
                canonical.range((bounds.0, bounds.1)).rev(),
                live.range((bounds.0, bounds.1)).rev(),
                Direction::Desc,
                |canonical_entry, live_entry| canonical_entry.key().cmp(live_entry.0),
                |canonical_entry| !tombstones.contains(canonical_entry.key()),
                |live_entry| !tombstones.contains(live_entry.0),
                |entry| {
                    let visit = match entry {
                        OrderedOverlayEntry::Canonical(canonical_entry) => {
                            visitor(canonical_entry.key(), &canonical_entry.value())?
                        }
                        OrderedOverlayEntry::Live((key, snapshot)) => visitor(key, snapshot)?,
                    };
                    Ok(if visit.should_stop() {
                        OrderedOverlayVisit::Stop
                    } else {
                        OrderedOverlayVisit::Continue
                    })
                },
            ),
        }
    }
}

fn derive_data_allocation_metadata(
    latest_by_entity: &StdBTreeMap<EntityTag, (SchemaVersion, RawSchemaSnapshot)>,
) -> Result<SchemaStoreCatalogMetadata, InternalError> {
    let mut max_version = SchemaVersion::initial();
    let mut hasher = new_hash_sha256();
    write_hash_tag_u8(
        &mut hasher,
        SCHEMA_STORE_DATA_ALLOCATION_FINGERPRINT_VERSION,
    );

    for (entity, (_, snapshot)) in latest_by_entity {
        let persisted = snapshot.decode_persisted_snapshot()?;
        if persisted.version() > max_version {
            max_version = persisted.version();
        }

        let data_projection = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            persisted.version(),
            persisted.entity_path().to_string(),
            persisted.entity_name().to_string(),
            persisted.primary_key_field_ids().to_vec(),
            persisted.row_layout().clone(),
            persisted.fields().to_vec(),
            Vec::new(),
        );
        let encoded = encode_persisted_schema_snapshot(&data_projection)?;

        write_hash_u64(&mut hasher, entity.value());
        write_hash_u32(&mut hasher, persisted.version().get());
        write_hash_len_u32(&mut hasher, encoded.len());
        hasher.update(encoded);
    }

    Ok(finalize_schema_metadata(
        max_version,
        SCHEMA_STORE_DATA_ALLOCATION_FINGERPRINT_VERSION,
        hasher,
        latest_by_entity.len(),
    ))
}

fn derive_index_allocation_metadata(
    latest_by_entity: &StdBTreeMap<EntityTag, (SchemaVersion, RawSchemaSnapshot)>,
) -> Result<SchemaStoreCatalogMetadata, InternalError> {
    let mut max_version = SchemaVersion::initial();
    let mut hasher = new_hash_sha256();
    write_hash_tag_u8(
        &mut hasher,
        SCHEMA_STORE_INDEX_ALLOCATION_FINGERPRINT_VERSION,
    );

    for (entity, (_, snapshot)) in latest_by_entity {
        let persisted = snapshot.decode_persisted_snapshot()?;
        if persisted.version() > max_version {
            max_version = persisted.version();
        }

        write_hash_u64(&mut hasher, entity.value());
        write_hash_u32(&mut hasher, persisted.version().get());
        write_hash_len_u32(&mut hasher, persisted.indexes().len());
        for index in persisted.indexes() {
            write_hash_u32(&mut hasher, u32::from(index.ordinal()));
            write_hash_str_u32(&mut hasher, index.name());
            write_hash_str_u32(&mut hasher, index.store());
            write_hash_tag_u8(&mut hasher, u8::from(index.unique()));
            write_hash_str_u32(&mut hasher, persisted_index_origin_name(index.origin()));
            match index.predicate_sql() {
                Some(predicate_sql) => {
                    write_hash_tag_u8(&mut hasher, 1);
                    write_hash_str_u32(&mut hasher, predicate_sql);
                }
                None => write_hash_tag_u8(&mut hasher, 0),
            }
            hash_persisted_index_key(&mut hasher, index.key());
        }
    }

    Ok(finalize_schema_metadata(
        max_version,
        SCHEMA_STORE_INDEX_ALLOCATION_FINGERPRINT_VERSION,
        hasher,
        latest_by_entity.len(),
    ))
}

fn derive_schema_catalog_metadata(
    latest_by_entity: &StdBTreeMap<EntityTag, (SchemaVersion, RawSchemaSnapshot)>,
) -> Result<SchemaStoreCatalogMetadata, InternalError> {
    let mut max_version = SchemaVersion::initial();
    let mut hasher = new_hash_sha256();
    write_hash_tag_u8(&mut hasher, SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION);

    for (entity, (version, snapshot)) in latest_by_entity {
        let persisted = snapshot.decode_persisted_snapshot()?;
        if persisted.version() > max_version {
            max_version = persisted.version();
        }

        write_hash_u64(&mut hasher, entity.value());
        write_hash_u32(&mut hasher, version.get());
        write_hash_len_u32(&mut hasher, snapshot.as_bytes().len());
        hasher.update(snapshot.as_bytes());
    }

    Ok(finalize_schema_metadata(
        max_version,
        SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION,
        hasher,
        latest_by_entity.len(),
    ))
}

fn finalize_schema_metadata(
    schema_version: SchemaVersion,
    schema_fingerprint_method_version: u8,
    hasher: sha2::Sha256,
    entity_count: usize,
) -> SchemaStoreCatalogMetadata {
    let digest = finalize_hash_sha256(hasher);
    let mut schema_fingerprint = [0u8; 16];
    schema_fingerprint.copy_from_slice(&digest[..16]);

    SchemaStoreCatalogMetadata::new(
        schema_version,
        schema_fingerprint_method_version,
        schema_fingerprint,
        u64::try_from(entity_count).unwrap_or(u64::MAX),
    )
}

fn hash_persisted_index_key(hasher: &mut sha2::Sha256, key: &PersistedIndexKeySnapshot) {
    match key {
        PersistedIndexKeySnapshot::FieldPath(paths) => {
            write_hash_tag_u8(hasher, 1);
            write_hash_len_u32(hasher, paths.len());
            for path in paths {
                hash_persisted_index_field_path(hasher, path);
            }
        }
        PersistedIndexKeySnapshot::Items(items) => {
            write_hash_tag_u8(hasher, 2);
            write_hash_len_u32(hasher, items.len());
            for item in items {
                match item {
                    PersistedIndexKeyItemSnapshot::FieldPath(path) => {
                        write_hash_tag_u8(hasher, 1);
                        hash_persisted_index_field_path(hasher, path);
                    }
                    PersistedIndexKeyItemSnapshot::Expression(expression) => {
                        write_hash_tag_u8(hasher, 2);
                        write_hash_str_u32(hasher, persisted_expression_op_name(expression.op()));
                        hash_persisted_index_field_path(hasher, expression.source());
                        hash_persisted_field_kind(hasher, expression.input_kind());
                        hash_persisted_field_kind(hasher, expression.output_kind());
                        write_hash_str_u32(hasher, expression.canonical_text());
                    }
                }
            }
        }
    }
}

fn hash_persisted_index_field_path(
    hasher: &mut sha2::Sha256,
    path: &crate::db::schema::PersistedIndexFieldPathSnapshot,
) {
    write_hash_u32(hasher, path.field_id().get());
    write_hash_u32(hasher, u32::from(path.slot().get()));
    write_hash_len_u32(hasher, path.path().len());
    for segment in path.path() {
        write_hash_str_u32(hasher, segment);
    }
    hash_persisted_field_kind(hasher, path.kind());
    write_hash_tag_u8(hasher, u8::from(path.nullable()));
}

fn hash_persisted_field_kind(hasher: &mut sha2::Sha256, kind: &PersistedFieldKind) {
    match kind {
        PersistedFieldKind::Account => write_hash_tag_u8(hasher, 1),
        PersistedFieldKind::Blob { max_len } => {
            write_hash_tag_u8(hasher, 2);
            hash_optional_u32(hasher, *max_len);
        }
        PersistedFieldKind::Bool => write_hash_tag_u8(hasher, 3),
        PersistedFieldKind::Date => write_hash_tag_u8(hasher, 4),
        PersistedFieldKind::Decimal { scale } => {
            write_hash_tag_u8(hasher, 5);
            write_hash_u32(hasher, *scale);
        }
        PersistedFieldKind::Duration => write_hash_tag_u8(hasher, 6),
        PersistedFieldKind::Enum { path, variants } => {
            write_hash_tag_u8(hasher, 7);
            write_hash_str_u32(hasher, path);
            write_hash_len_u32(hasher, variants.len());
            for variant in variants {
                write_hash_str_u32(hasher, variant.ident());
                match variant.payload_kind() {
                    Some(payload_kind) => {
                        write_hash_tag_u8(hasher, 1);
                        hash_persisted_field_kind(hasher, payload_kind);
                    }
                    None => write_hash_tag_u8(hasher, 0),
                }
                write_hash_str_u32(
                    hasher,
                    field_storage_decode_name(variant.payload_storage_decode()),
                );
            }
        }
        PersistedFieldKind::Float32 => write_hash_tag_u8(hasher, 8),
        PersistedFieldKind::Float64 => write_hash_tag_u8(hasher, 9),
        PersistedFieldKind::Int8 => write_hash_tag_u8(hasher, 10),
        PersistedFieldKind::Int16 => write_hash_tag_u8(hasher, 11),
        PersistedFieldKind::Int32 => write_hash_tag_u8(hasher, 12),
        PersistedFieldKind::Int64 => write_hash_tag_u8(hasher, 13),
        PersistedFieldKind::Int128 => write_hash_tag_u8(hasher, 14),
        PersistedFieldKind::IntBig { max_bytes } => {
            write_hash_tag_u8(hasher, 15);
            write_hash_u32(hasher, *max_bytes);
        }
        PersistedFieldKind::Principal => write_hash_tag_u8(hasher, 16),
        PersistedFieldKind::Subaccount => write_hash_tag_u8(hasher, 17),
        PersistedFieldKind::Text { max_len } => {
            write_hash_tag_u8(hasher, 18);
            hash_optional_u32(hasher, *max_len);
        }
        PersistedFieldKind::Timestamp => write_hash_tag_u8(hasher, 19),
        PersistedFieldKind::Nat8 => write_hash_tag_u8(hasher, 20),
        PersistedFieldKind::Nat16 => write_hash_tag_u8(hasher, 21),
        PersistedFieldKind::Nat32 => write_hash_tag_u8(hasher, 22),
        PersistedFieldKind::Nat64 => write_hash_tag_u8(hasher, 23),
        PersistedFieldKind::Nat128 => write_hash_tag_u8(hasher, 24),
        PersistedFieldKind::NatBig { max_bytes } => {
            write_hash_tag_u8(hasher, 25);
            write_hash_u32(hasher, *max_bytes);
        }
        PersistedFieldKind::Ulid => write_hash_tag_u8(hasher, 26),
        PersistedFieldKind::Unit => write_hash_tag_u8(hasher, 27),
        PersistedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
            strength,
        } => {
            write_hash_tag_u8(hasher, 28);
            write_hash_str_u32(hasher, target_path);
            write_hash_str_u32(hasher, target_entity_name);
            write_hash_u64(hasher, target_entity_tag.value());
            write_hash_str_u32(hasher, target_store_path);
            hash_persisted_field_kind(hasher, key_kind);
            write_hash_str_u32(hasher, persisted_relation_strength_name(*strength));
        }
        PersistedFieldKind::List(inner) => {
            write_hash_tag_u8(hasher, 29);
            hash_persisted_field_kind(hasher, inner);
        }
        PersistedFieldKind::Set(inner) => {
            write_hash_tag_u8(hasher, 30);
            hash_persisted_field_kind(hasher, inner);
        }
        PersistedFieldKind::Map { key, value } => {
            write_hash_tag_u8(hasher, 31);
            hash_persisted_field_kind(hasher, key);
            hash_persisted_field_kind(hasher, value);
        }
        PersistedFieldKind::Structured { queryable } => {
            write_hash_tag_u8(hasher, 32);
            write_hash_tag_u8(hasher, u8::from(*queryable));
        }
    }
}

fn hash_optional_u32(hasher: &mut sha2::Sha256, value: Option<u32>) {
    match value {
        Some(value) => {
            write_hash_tag_u8(hasher, 1);
            write_hash_u32(hasher, value);
        }
        None => write_hash_tag_u8(hasher, 0),
    }
}

const fn persisted_index_origin_name(
    origin: crate::db::schema::PersistedIndexOrigin,
) -> &'static str {
    match origin {
        crate::db::schema::PersistedIndexOrigin::Generated => "generated",
        crate::db::schema::PersistedIndexOrigin::SqlDdl => "sql_ddl",
    }
}

const fn persisted_expression_op_name(
    op: crate::db::schema::PersistedIndexExpressionOp,
) -> &'static str {
    match op {
        crate::db::schema::PersistedIndexExpressionOp::Lower => "lower",
        crate::db::schema::PersistedIndexExpressionOp::Upper => "upper",
        crate::db::schema::PersistedIndexExpressionOp::Trim => "trim",
        crate::db::schema::PersistedIndexExpressionOp::LowerTrim => "lower_trim",
        crate::db::schema::PersistedIndexExpressionOp::Date => "date",
        crate::db::schema::PersistedIndexExpressionOp::Year => "year",
        crate::db::schema::PersistedIndexExpressionOp::Month => "month",
        crate::db::schema::PersistedIndexExpressionOp::Day => "day",
    }
}

const fn persisted_relation_strength_name(
    strength: crate::db::schema::PersistedRelationStrength,
) -> &'static str {
    match strength {
        crate::db::schema::PersistedRelationStrength::Strong => "strong",
        crate::db::schema::PersistedRelationStrength::Weak => "weak",
    }
}

const fn field_storage_decode_name(
    decode: crate::model::field::FieldStorageDecode,
) -> &'static str {
    match decode {
        crate::model::field::FieldStorageDecode::ByKind => "by_kind",
        crate::model::field::FieldStorageDecode::Value => "value",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        RawSchemaKey, RawSchemaSnapshot, SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION,
        SCHEMA_STORE_DATA_ALLOCATION_FINGERPRINT_VERSION,
        SCHEMA_STORE_INDEX_ALLOCATION_FINGERPRINT_VERSION, SchemaStore, SchemaStoreBackend,
        SchemaStoreVisit,
    };
    use crate::{
        db::{
            direction::Direction,
            schema::{
                FieldId, PersistedFieldKind, PersistedFieldSnapshot,
                PersistedIndexFieldPathSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
                PersistedNestedLeafSnapshot, PersistedSchemaSnapshot, SchemaFieldDefault,
                SchemaFieldSlot, SchemaRowLayout, SchemaVersion, encode_persisted_schema_snapshot,
            },
        },
        model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
        testing::test_memory,
        traits::Storable,
        types::EntityTag,
    };
    use std::borrow::Cow;
    use std::convert::Infallible;

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
    fn schema_store_entity_footprint_counts_raw_snapshots_without_decoding() {
        let mut store = SchemaStore::init(test_memory(242));
        store.insert_raw_snapshot(
            RawSchemaKey::from_entity_version(EntityTag::new(71), SchemaVersion::initial()),
            RawSchemaSnapshot::from_bytes(vec![1, 2, 3]),
        );
        store.insert_raw_snapshot(
            RawSchemaKey::from_entity_version(EntityTag::new(72), SchemaVersion::new(3)),
            RawSchemaSnapshot::from_bytes(vec![5, 8]),
        );
        store.insert_raw_snapshot(
            RawSchemaKey::from_entity_version(EntityTag::new(71), SchemaVersion::new(2)),
            RawSchemaSnapshot::from_bytes(vec![13, 21, 34, 55]),
        );

        let footprint = store.entity_footprint(EntityTag::new(71));

        assert_eq!(footprint.snapshots(), 2);
        assert_eq!(footprint.encoded_bytes(), 7);
        assert_eq!(footprint.latest_snapshot_bytes(), 4);
    }

    #[test]
    fn schema_store_visit_raw_snapshots_preserves_key_order() {
        let mut store = SchemaStore::init(test_memory(235));
        store.insert_raw_snapshot(
            RawSchemaKey::from_entity_version(EntityTag::new(3), SchemaVersion::new(2)),
            RawSchemaSnapshot::from_bytes(vec![32]),
        );
        store.insert_raw_snapshot(
            RawSchemaKey::from_entity_version(EntityTag::new(1), SchemaVersion::new(3)),
            RawSchemaSnapshot::from_bytes(vec![13]),
        );
        store.insert_raw_snapshot(
            RawSchemaKey::from_entity_version(EntityTag::new(1), SchemaVersion::new(1)),
            RawSchemaSnapshot::from_bytes(vec![11]),
        );

        let mut visited = Vec::new();
        let _: Result<(), Infallible> = store.visit_raw_snapshots(|key, snapshot| {
            visited.push((
                key.entity_tag().value(),
                key.version(),
                snapshot.as_bytes()[0],
            ));
            Ok(SchemaStoreVisit::Continue)
        });

        assert_eq!(visited, vec![(1, 1, 11), (1, 3, 13), (3, 2, 32)]);
    }

    #[test]
    fn schema_store_visit_raw_snapshots_can_stop_without_error() {
        let mut store = SchemaStore::init(test_memory(234));
        store.insert_raw_snapshot(
            RawSchemaKey::from_entity_version(EntityTag::new(2), SchemaVersion::new(1)),
            RawSchemaSnapshot::from_bytes(vec![21]),
        );
        store.insert_raw_snapshot(
            RawSchemaKey::from_entity_version(EntityTag::new(2), SchemaVersion::new(2)),
            RawSchemaSnapshot::from_bytes(vec![22]),
        );

        let mut visited = Vec::new();
        let _: Result<(), Infallible> = store.visit_raw_snapshots(|key, _| {
            visited.push(key.version());
            Ok(SchemaStoreVisit::Stop)
        });

        assert_eq!(visited, vec![1]);
    }

    #[test]
    fn heap_schema_store_preserves_order_latest_snapshot_and_early_stop() {
        let mut store = SchemaStore::init_heap();
        let initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
        let newer = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Newer");
        let other_entity = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "Other");

        store
            .insert_persisted_snapshot(EntityTag::new(41), &initial)
            .expect("initial heap schema snapshot should encode");
        store
            .insert_persisted_snapshot(EntityTag::new(42), &other_entity)
            .expect("other heap schema snapshot should encode");
        store
            .insert_persisted_snapshot(EntityTag::new(41), &newer)
            .expect("newer heap schema snapshot should encode");

        let latest = store
            .latest_persisted_snapshot(EntityTag::new(41))
            .expect("latest heap schema snapshot should decode")
            .expect("heap schema snapshot should exist");
        assert_eq!(latest.version(), SchemaVersion::new(2));
        assert_eq!(latest.entity_name(), "Newer");

        let mut visited = Vec::new();
        let _: Result<(), Infallible> = store.visit_raw_snapshots(|key, snapshot| {
            visited.push((
                key.entity_tag().value(),
                key.version(),
                snapshot.as_bytes().len(),
            ));
            Ok(if visited.len() == 2 {
                SchemaStoreVisit::Stop
            } else {
                SchemaStoreVisit::Continue
            })
        });
        assert_eq!(
            visited
                .iter()
                .map(|(entity, version, _)| (*entity, *version))
                .collect::<Vec<_>>(),
            vec![(41, 1), (41, 2)]
        );
    }

    #[test]
    fn journaled_schema_store_streams_overlay_latest_snapshot_and_early_stop() {
        let mut store = SchemaStore::init_journaled(test_memory(233));
        let canonical_initial =
            persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
        let canonical_replaced =
            persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Canonical");
        let live_replacement = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Live");
        let live_newer = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "LiveNewer");
        let other_entity = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Other");

        store
            .fold_persisted_snapshot(EntityTag::new(61), &canonical_initial)
            .expect("initial canonical schema snapshot should encode");
        store
            .fold_persisted_snapshot(EntityTag::new(61), &canonical_replaced)
            .expect("canonical schema snapshot should encode");
        store
            .fold_persisted_snapshot(EntityTag::new(62), &other_entity)
            .expect("other canonical schema snapshot should encode");
        store
            .insert_persisted_snapshot(EntityTag::new(61), &live_replacement)
            .expect("live replacement schema snapshot should encode");
        store
            .insert_persisted_snapshot(EntityTag::new(61), &live_newer)
            .expect("live newer schema snapshot should encode");

        let latest = store
            .latest_persisted_snapshot(EntityTag::new(61))
            .expect("latest journaled schema snapshot should decode")
            .expect("journaled schema snapshot should exist");
        assert_eq!(latest.version(), SchemaVersion::new(3));
        assert_eq!(latest.entity_name(), "LiveNewer");
        assert_eq!(store.len(), 4);

        let mut visited = Vec::new();
        let _: Result<(), Infallible> = store.visit_raw_snapshots(|key, snapshot| {
            let decoded = snapshot
                .decode_persisted_snapshot()
                .expect("visited schema snapshot should decode");
            visited.push((
                key.entity_tag().value(),
                key.version(),
                decoded.entity_name().to_string(),
            ));
            Ok(if visited.len() == 3 {
                SchemaStoreVisit::Stop
            } else {
                SchemaStoreVisit::Continue
            })
        });
        assert_eq!(
            visited,
            vec![
                (61, 1, "Initial".to_string()),
                (61, 2, "Live".to_string()),
                (61, 3, "LiveNewer".to_string()),
            ],
        );

        store.clear();
        assert!(store.is_empty());
        assert!(
            store
                .latest_persisted_snapshot(EntityTag::new(61))
                .expect("cleared journaled latest snapshot lookup should decode")
                .is_none(),
        );
    }

    #[test]
    fn journaled_schema_store_latest_snapshot_reads_each_overlay_source() {
        let entity = EntityTag::new(71);

        let mut canonical_only = SchemaStore::init_journaled(test_memory(231));
        let canonical =
            persisted_schema_snapshot_for_test(SchemaVersion::initial(), "CanonicalOnly");
        canonical_only
            .fold_persisted_snapshot(entity, &canonical)
            .expect("canonical-only schema snapshot should encode");
        assert_latest_schema(
            &canonical_only,
            entity,
            SchemaVersion::initial(),
            "CanonicalOnly",
        );

        let mut live_only = SchemaStore::init_journaled(test_memory(230));
        let live = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "LiveOnly");
        live_only
            .insert_persisted_snapshot(entity, &live)
            .expect("live-only schema snapshot should encode");
        assert_latest_schema(&live_only, entity, SchemaVersion::new(2), "LiveOnly");

        let mut live_override = SchemaStore::init_journaled(test_memory(229));
        let canonical_duplicate =
            persisted_schema_snapshot_for_test(SchemaVersion::new(3), "CanonicalDuplicate");
        let live_duplicate =
            persisted_schema_snapshot_for_test(SchemaVersion::new(3), "LiveDuplicate");
        live_override
            .fold_persisted_snapshot(entity, &canonical_duplicate)
            .expect("canonical duplicate schema snapshot should encode");
        live_override
            .insert_persisted_snapshot(entity, &live_duplicate)
            .expect("live duplicate schema snapshot should encode");
        assert_latest_schema(
            &live_override,
            entity,
            SchemaVersion::new(3),
            "LiveDuplicate",
        );
    }

    #[test]
    fn journaled_schema_store_descending_range_orders_live_between_canonical_versions() {
        let mut store = SchemaStore::init_journaled(test_memory(228));
        let entity = EntityTag::new(72);
        let lower_entity = EntityTag::new(71);
        let higher_entity = EntityTag::new(73);
        let canonical_initial =
            persisted_schema_snapshot_for_test(SchemaVersion::initial(), "CanonicalV1");
        let canonical_duplicate =
            persisted_schema_snapshot_for_test(SchemaVersion::new(2), "CanonicalV2");
        let live_duplicate = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "LiveV2");
        let canonical_latest =
            persisted_schema_snapshot_for_test(SchemaVersion::new(3), "CanonicalV3");
        let unrelated_lower =
            persisted_schema_snapshot_for_test(SchemaVersion::new(9), "UnrelatedLower");

        store
            .fold_persisted_snapshot(entity, &canonical_initial)
            .expect("canonical v1 schema snapshot should encode");
        store
            .fold_persisted_snapshot(entity, &canonical_duplicate)
            .expect("canonical v2 schema snapshot should encode");
        store
            .fold_persisted_snapshot(entity, &canonical_latest)
            .expect("canonical v3 schema snapshot should encode");
        store
            .fold_persisted_snapshot(lower_entity, &unrelated_lower)
            .expect("lower unrelated schema snapshot should encode");
        store
            .insert_persisted_snapshot(entity, &live_duplicate)
            .expect("live v2 schema snapshot should encode");
        store.insert_raw_snapshot(
            RawSchemaKey::from_entity_version(higher_entity, SchemaVersion::new(1)),
            RawSchemaSnapshot::from_bytes(vec![0xff]),
        );

        let visited = visit_journaled_schema_range(&store, entity, Direction::Desc, usize::MAX);
        assert_eq!(
            visited,
            vec![
                (3, "CanonicalV3".to_string()),
                (2, "LiveV2".to_string()),
                (1, "CanonicalV1".to_string()),
            ],
        );

        let early_stop = visit_journaled_schema_range(&store, entity, Direction::Desc, 1);
        assert_eq!(early_stop, vec![(3, "CanonicalV3".to_string())]);
    }

    #[test]
    fn journaled_schema_store_latest_snapshot_skips_tombstoned_latest_version() {
        let entity = EntityTag::new(74);

        let mut canonical_latest_tombstoned = SchemaStore::init_journaled(test_memory(227));
        let canonical_initial =
            persisted_schema_snapshot_for_test(SchemaVersion::initial(), "CanonicalV1");
        let canonical_latest =
            persisted_schema_snapshot_for_test(SchemaVersion::new(2), "CanonicalV2");
        canonical_latest_tombstoned
            .fold_persisted_snapshot(entity, &canonical_initial)
            .expect("canonical v1 schema snapshot should encode");
        canonical_latest_tombstoned
            .fold_persisted_snapshot(entity, &canonical_latest)
            .expect("canonical v2 schema snapshot should encode");
        tombstone_journaled_raw_snapshot(
            &mut canonical_latest_tombstoned,
            entity,
            SchemaVersion::new(2),
        );

        assert_latest_schema(
            &canonical_latest_tombstoned,
            entity,
            SchemaVersion::initial(),
            "CanonicalV1",
        );
        assert!(
            canonical_latest_tombstoned
                .get_persisted_snapshot(entity, SchemaVersion::new(2))
                .expect("tombstoned canonical snapshot lookup should not decode")
                .is_none(),
        );

        let mut live_latest_tombstoned = SchemaStore::init_journaled(test_memory(226));
        let live_latest = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "LiveV2");
        live_latest_tombstoned
            .fold_persisted_snapshot(entity, &canonical_initial)
            .expect("canonical v1 schema snapshot should encode");
        live_latest_tombstoned
            .insert_persisted_snapshot(entity, &live_latest)
            .expect("live v2 schema snapshot should encode");
        tombstone_journaled_raw_snapshot(
            &mut live_latest_tombstoned,
            entity,
            SchemaVersion::new(2),
        );

        assert_latest_schema(
            &live_latest_tombstoned,
            entity,
            SchemaVersion::initial(),
            "CanonicalV1",
        );
        assert!(
            live_latest_tombstoned
                .get_persisted_snapshot(entity, SchemaVersion::new(2))
                .expect("tombstoned live snapshot lookup should not decode")
                .is_none(),
        );
    }

    #[test]
    fn schema_store_catalog_metadata_is_absent_without_accepted_snapshots() {
        let store = SchemaStore::init(test_memory(241));

        assert_eq!(
            store
                .catalog_metadata()
                .expect("empty schema catalog metadata should derive"),
            None
        );
    }

    #[test]
    fn schema_store_catalog_metadata_uses_latest_persisted_snapshots() {
        let mut store = SchemaStore::init(test_memory(240));
        let initial = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "Initial");
        let newer = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "Newer");
        let other = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "Other");

        store
            .insert_persisted_snapshot(EntityTag::new(81), &initial)
            .expect("initial schema snapshot should encode");
        let initial_metadata = store
            .catalog_metadata()
            .expect("initial schema catalog metadata should derive")
            .expect("initial schema catalog metadata should be present");

        store
            .insert_persisted_snapshot(EntityTag::new(81), &newer)
            .expect("newer schema snapshot should encode");
        store
            .insert_persisted_snapshot(EntityTag::new(82), &other)
            .expect("other schema snapshot should encode");
        let updated_metadata = store
            .catalog_metadata()
            .expect("updated schema catalog metadata should derive")
            .expect("updated schema catalog metadata should be present");

        assert_eq!(initial_metadata.schema_version(), SchemaVersion::initial());
        assert_eq!(
            initial_metadata.schema_fingerprint_method_version(),
            SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION
        );
        assert_eq!(initial_metadata.entity_count(), 1);
        assert_eq!(updated_metadata.schema_version(), SchemaVersion::new(3));
        assert_eq!(
            updated_metadata.schema_fingerprint_method_version(),
            SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION
        );
        assert_eq!(updated_metadata.entity_count(), 2);
        assert_ne!(
            initial_metadata.schema_fingerprint(),
            updated_metadata.schema_fingerprint(),
            "catalog fingerprint must change when latest accepted schema catalog changes"
        );
    }

    #[test]
    fn schema_store_catalog_metadata_is_independent_of_insertion_order() {
        let first = persisted_schema_snapshot_for_test(SchemaVersion::new(2), "First");
        let second = persisted_schema_snapshot_for_test(SchemaVersion::new(3), "Second");

        let mut left = SchemaStore::init(test_memory(239));
        left.insert_persisted_snapshot(EntityTag::new(91), &first)
            .expect("first schema snapshot should encode");
        left.insert_persisted_snapshot(EntityTag::new(92), &second)
            .expect("second schema snapshot should encode");

        let mut right = SchemaStore::init(test_memory(238));
        right
            .insert_persisted_snapshot(EntityTag::new(92), &second)
            .expect("second schema snapshot should encode");
        right
            .insert_persisted_snapshot(EntityTag::new(91), &first)
            .expect("first schema snapshot should encode");

        let left_metadata = left
            .catalog_metadata()
            .expect("left schema catalog metadata should derive");
        let right_metadata = right
            .catalog_metadata()
            .expect("right schema catalog metadata should derive");

        assert_eq!(left_metadata, right_metadata);
    }

    #[test]
    fn schema_store_allocation_metadata_uses_role_specific_fingerprints() {
        let without_index =
            persisted_schema_snapshot_for_test(SchemaVersion::initial(), "RoleSpecific");
        let with_index = persisted_schema_snapshot_with_index_for_test(
            SchemaVersion::initial(),
            "RoleSpecific",
            "payload_idx",
        );

        let mut base = SchemaStore::init(test_memory(237));
        base.insert_persisted_snapshot(EntityTag::new(93), &without_index)
            .expect("base schema snapshot should encode");
        let base_metadata = base
            .allocation_metadata()
            .expect("base allocation metadata should derive")
            .expect("base allocation metadata should be present");

        let mut indexed = SchemaStore::init(test_memory(236));
        indexed
            .insert_persisted_snapshot(EntityTag::new(93), &with_index)
            .expect("indexed schema snapshot should encode");
        let indexed_metadata = indexed
            .allocation_metadata()
            .expect("indexed allocation metadata should derive")
            .expect("indexed allocation metadata should be present");

        assert_eq!(
            base_metadata.data().schema_fingerprint(),
            indexed_metadata.data().schema_fingerprint(),
            "data allocation metadata should ignore accepted index catalog changes"
        );
        assert_eq!(
            indexed_metadata.data().schema_fingerprint_method_version(),
            SCHEMA_STORE_DATA_ALLOCATION_FINGERPRINT_VERSION
        );
        assert_eq!(
            indexed_metadata.index().schema_fingerprint_method_version(),
            SCHEMA_STORE_INDEX_ALLOCATION_FINGERPRINT_VERSION
        );
        assert_eq!(
            indexed_metadata
                .schema()
                .schema_fingerprint_method_version(),
            SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION
        );
        assert_ne!(
            base_metadata.index().schema_fingerprint(),
            indexed_metadata.index().schema_fingerprint(),
            "index allocation metadata should change when accepted index catalog changes"
        );
        assert_ne!(
            base_metadata.schema().schema_fingerprint(),
            indexed_metadata.schema().schema_fingerprint(),
            "schema allocation metadata should change when full accepted catalog changes"
        );
        assert_ne!(
            indexed_metadata.data().schema_fingerprint(),
            indexed_metadata.index().schema_fingerprint(),
            "data and index allocation metadata should have distinct role fingerprints"
        );
        assert_ne!(
            indexed_metadata.index().schema_fingerprint(),
            indexed_metadata.schema().schema_fingerprint(),
            "index and schema allocation metadata should have distinct role fingerprints"
        );
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
    fn schema_store_rejects_typed_snapshot_with_zero_schema_version() {
        let mut store = SchemaStore::init(test_memory(254));
        let invalid =
            persisted_schema_snapshot_for_test(SchemaVersion::new(0), "ZeroSchemaVersion");

        let err = store
            .insert_persisted_snapshot(EntityTag::new(44), &invalid)
            .expect_err("schema store should reject non-positive schema versions");

        assert!(
            err.message()
                .contains("schema snapshot schema_version must be positive"),
            "schema store should hard-cut non-positive persisted schema versions"
        );
    }

    #[test]
    fn schema_store_rejects_typed_snapshot_with_divergent_field_slots() {
        let mut store = SchemaStore::init(test_memory(232));
        let base = persisted_schema_snapshot_for_test(SchemaVersion::initial(), "InvalidSlots");
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.first_primary_key_field_id(),
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
            base.first_primary_key_field_id(),
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
            base.first_primary_key_field_id(),
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
            fields[1].default().clone(),
            fields[1].storage_decode(),
            fields[1].leaf_codec(),
        );
        fields[1] = duplicate;
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.first_primary_key_field_id(),
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
                PersistedFieldKind::Blob { max_len: None },
                false,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Blob),
            )],
            fields[1].nullable(),
            fields[1].default().clone(),
            fields[1].storage_decode(),
            fields[1].leaf_codec(),
        );
        fields[1] = invalid_field;
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.first_primary_key_field_id(),
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
                PersistedFieldKind::Blob { max_len: None },
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
            fields[1].default().clone(),
            fields[1].storage_decode(),
            fields[1].leaf_codec(),
        );
        fields[1] = invalid_field;
        let invalid = PersistedSchemaSnapshot::new(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.first_primary_key_field_id(),
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
    fn assert_latest_schema(
        store: &SchemaStore,
        entity: EntityTag,
        version: SchemaVersion,
        entity_name: &str,
    ) {
        let latest = store
            .latest_persisted_snapshot(entity)
            .expect("latest schema snapshot should decode")
            .expect("latest schema snapshot should exist");

        assert_eq!(latest.version(), version);
        assert_eq!(latest.entity_name(), entity_name);
    }

    fn tombstone_journaled_raw_snapshot(
        store: &mut SchemaStore,
        entity: EntityTag,
        version: SchemaVersion,
    ) {
        let key = RawSchemaKey::from_entity_version(entity, version);
        let SchemaStoreBackend::Journaled { tombstones, .. } = &mut store.backend else {
            panic!("schema tombstone test helper requires a journaled store");
        };

        tombstones.insert(key);
    }

    fn visit_journaled_schema_range(
        store: &SchemaStore,
        entity: EntityTag,
        direction: Direction,
        stop_after: usize,
    ) -> Vec<(u32, String)> {
        let SchemaStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
        } = &store.backend
        else {
            panic!("schema range test helper requires a journaled store");
        };

        let mut visited = Vec::new();
        let _: Result<(), Infallible> = SchemaStore::visit_journaled_raw_snapshot_range(
            canonical,
            live,
            tombstones,
            RawSchemaKey::entity_range_bounds(entity),
            direction,
            |key, snapshot| {
                let decoded = snapshot
                    .decode_persisted_snapshot()
                    .expect("visited schema snapshot should decode");
                visited.push((key.version(), decoded.entity_name().to_string()));
                Ok(if visited.len() >= stop_after {
                    SchemaStoreVisit::Stop
                } else {
                    SchemaStoreVisit::Continue
                })
            },
        );

        visited
    }

    fn persisted_schema_snapshot_for_test(
        version: SchemaVersion,
        entity_name: &str,
    ) -> PersistedSchemaSnapshot {
        persisted_schema_snapshot_with_layout_version_for_test(version, version, entity_name)
    }

    fn persisted_schema_snapshot_with_index_for_test(
        version: SchemaVersion,
        entity_name: &str,
        index_name: &str,
    ) -> PersistedSchemaSnapshot {
        let base = persisted_schema_snapshot_for_test(version, entity_name);

        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            base.version(),
            base.entity_path().to_string(),
            base.entity_name().to_string(),
            base.primary_key_field_ids().to_vec(),
            base.row_layout().clone(),
            base.fields().to_vec(),
            vec![PersistedIndexSnapshot::new(
                0,
                index_name.to_string(),
                "RoleSpecificStore".to_string(),
                false,
                PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                    FieldId::new(1),
                    SchemaFieldSlot::new(0),
                    vec!["id".to_string()],
                    PersistedFieldKind::Ulid,
                    false,
                )]),
                None,
            )],
        )
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
                            PersistedFieldKind::Nat64,
                        ))),
                    },
                    vec![PersistedNestedLeafSnapshot::new(
                        vec!["bytes".to_string()],
                        PersistedFieldKind::Blob { max_len: None },
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
