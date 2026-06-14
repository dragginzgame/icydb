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
            accepted_schema_cache_fingerprint,
            accepted_schema_cache_fingerprint_for_persisted_snapshot,
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
const RAW_SCHEMA_SNAPSHOT_MAGIC: &[u8; 8] = b"ICYDBSCH";
const RAW_SCHEMA_SNAPSHOT_VALUE_VERSION: u8 = 1;
const RAW_SCHEMA_SNAPSHOT_HEADER_BYTES: usize = 25;
const RAW_SCHEMA_SNAPSHOT_HEADER_BYTES_U32: u32 = 25;

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
struct RawSchemaSnapshot {
    payload: Vec<u8>,
    accepted_schema_fingerprint: Option<CommitSchemaFingerprint>,
}

impl RawSchemaSnapshot {
    /// Encode one typed persisted-schema snapshot into a raw store payload.
    fn from_persisted_snapshot(snapshot: &PersistedSchemaSnapshot) -> Result<Self, InternalError> {
        validate_typed_schema_snapshot_for_store(snapshot)?;

        let accepted_schema_fingerprint =
            accepted_schema_cache_fingerprint_for_persisted_snapshot(snapshot)?;
        let payload = encode_persisted_schema_snapshot(snapshot)?;

        Ok(Self {
            payload,
            accepted_schema_fingerprint: Some(accepted_schema_fingerprint),
        })
    }

    /// Build one raw schema snapshot from already-encoded bytes.
    #[must_use]
    #[cfg(test)]
    const fn from_bytes(payload: Vec<u8>) -> Self {
        Self {
            payload,
            accepted_schema_fingerprint: None,
        }
    }

    /// Borrow the encoded schema snapshot payload.
    #[must_use]
    const fn as_bytes(&self) -> &[u8] {
        self.payload.as_slice()
    }

    /// Consume the snapshot into its encoded payload bytes.
    #[must_use]
    fn into_bytes(self) -> Vec<u8> {
        self.payload
    }

    /// Return the accepted schema identity fingerprint stored beside the raw
    /// payload, without decoding the persisted snapshot.
    fn accepted_schema_fingerprint(&self) -> Result<CommitSchemaFingerprint, InternalError> {
        self.accepted_schema_fingerprint
            .ok_or_else(InternalError::store_corruption)
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
            return Err(InternalError::store_invariant());
        }
        if accepted.entity_path() != identity.entity_path() {
            return Err(InternalError::store_invariant());
        }

        let decoded_fingerprint = accepted_schema_cache_fingerprint(&accepted)?;
        if decoded_fingerprint != identity.accepted_schema_fingerprint() {
            return Err(InternalError::store_invariant());
        }

        Ok(accepted)
    }
}

impl Storable for RawSchemaSnapshot {
    fn to_bytes(&self) -> Cow<'_, [u8]> {
        let Some(fingerprint) = self.accepted_schema_fingerprint else {
            return Cow::Borrowed(self.as_bytes());
        };

        let mut bytes = Vec::with_capacity(RAW_SCHEMA_SNAPSHOT_HEADER_BYTES + self.payload.len());
        bytes.extend_from_slice(RAW_SCHEMA_SNAPSHOT_MAGIC);
        bytes.push(RAW_SCHEMA_SNAPSHOT_VALUE_VERSION);
        bytes.extend_from_slice(&fingerprint);
        bytes.extend_from_slice(self.as_bytes());

        Cow::Owned(bytes)
    }

    fn from_bytes(bytes: Cow<'_, [u8]>) -> Self {
        let bytes = bytes.into_owned();
        if bytes.len() >= RAW_SCHEMA_SNAPSHOT_HEADER_BYTES
            && &bytes[..RAW_SCHEMA_SNAPSHOT_MAGIC.len()] == RAW_SCHEMA_SNAPSHOT_MAGIC
            && bytes[RAW_SCHEMA_SNAPSHOT_MAGIC.len()] == RAW_SCHEMA_SNAPSHOT_VALUE_VERSION
        {
            let fingerprint_start = RAW_SCHEMA_SNAPSHOT_MAGIC.len() + size_of::<u8>();
            let fingerprint_end = fingerprint_start + size_of::<CommitSchemaFingerprint>();
            let mut fingerprint = [0_u8; size_of::<CommitSchemaFingerprint>()];
            fingerprint.copy_from_slice(&bytes[fingerprint_start..fingerprint_end]);

            return Self {
                payload: bytes[fingerprint_end..].to_vec(),
                accepted_schema_fingerprint: Some(fingerprint),
            };
        }

        Self {
            payload: bytes,
            accepted_schema_fingerprint: None,
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        let Some(fingerprint) = self.accepted_schema_fingerprint else {
            return self.payload;
        };

        let mut bytes = Vec::with_capacity(RAW_SCHEMA_SNAPSHOT_HEADER_BYTES + self.payload.len());
        bytes.extend_from_slice(RAW_SCHEMA_SNAPSHOT_MAGIC);
        bytes.push(RAW_SCHEMA_SNAPSHOT_VALUE_VERSION);
        bytes.extend_from_slice(&fingerprint);
        bytes.extend_from_slice(&self.payload);

        bytes
    }

    const BOUND: StorableBound = StorableBound::Bounded {
        max_size: MAX_SCHEMA_SNAPSHOT_BYTES + RAW_SCHEMA_SNAPSHOT_HEADER_BYTES_U32,
        is_fixed_size: false,
    };
}

// Validate typed schema snapshots before they are encoded into the raw schema
// metadata store. This catches caller-side invariant violations separately from
// raw persisted-byte corruption handled by the codec decode boundary.
fn validate_typed_schema_snapshot_for_store(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    if schema_snapshot_integrity_detail(
        "schema snapshot",
        snapshot.version(),
        snapshot.primary_key_field_ids(),
        snapshot.row_layout(),
        snapshot.fields(),
    )
    .is_some()
    {
        return Err(InternalError::store_invariant());
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
/// Thin persistence wrapper over one journaled or heap schema metadata BTreeMap.
/// Startup reconciliation writes and validates encoded schema snapshots here
/// before row/index operations proceed.
///

pub struct SchemaStore {
    backend: SchemaStoreBackend,
}

enum SchemaStoreBackend {
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

    /// Insert one typed persisted schema snapshot only if the current live
    /// accepted catalog identity still matches the identity captured before
    /// schema mutation planning.
    pub(in crate::db) fn insert_persisted_snapshot_if_latest_identity(
        &mut self,
        expected: AcceptedCatalogIdentity,
        snapshot: &PersistedSchemaSnapshot,
    ) -> Result<(), InternalError> {
        let live = self.latest_catalog_identity(
            expected.entity_tag(),
            expected.entity_path(),
            expected.store_path(),
        )?;
        if live
            .as_ref()
            .map(AcceptedCatalogSnapshotSelection::identity)
            != Some(expected)
        {
            return Err(InternalError::schema_ddl_publication_race_lost(
                expected.entity_path(),
            ));
        }

        self.insert_persisted_snapshot(expected.entity_tag(), snapshot)
    }

    /// Reset the volatile projection for journaled recovery without mutating
    /// the canonical stable schema base.
    pub(in crate::db) fn reset_journaled_live_projection(&mut self) -> Result<(), InternalError> {
        let SchemaStoreBackend::Journaled {
            live, tombstones, ..
        } = &mut self.backend
        else {
            return Err(InternalError::store_invariant());
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
            return Err(InternalError::store_invariant());
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
    ) -> Result<Option<AcceptedCatalogSnapshotSelection>, InternalError> {
        let Some((version, raw_snapshot)) = self.latest_raw_snapshot_entry(entity) else {
            return Ok(None);
        };
        let fingerprint = raw_snapshot.accepted_schema_fingerprint()?;
        let identity =
            AcceptedCatalogIdentity::new(entity, entity_path, store_path, version, fingerprint);

        Ok(Some(AcceptedCatalogSnapshotSelection::new(
            identity,
            raw_snapshot.into_bytes(),
        )))
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
            SchemaStoreBackend::Heap(map) => map.get(key).cloned(),
            SchemaStoreBackend::Journaled { .. } => self.get_raw_snapshot_for_backend(key),
        }
    }

    /// Return whether one schema snapshot key is present.
    #[must_use]
    #[cfg(test)]
    fn contains_raw_snapshot(&self, key: &RawSchemaKey) -> bool {
        match &self.backend {
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
            SchemaStoreBackend::Journaled { canonical: map, .. } => map.len(),
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
mod tests;
