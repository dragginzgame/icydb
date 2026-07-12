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
            AcceptedFieldKind, AcceptedSchemaSnapshot, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedSchemaSnapshot, SchemaVersion,
            accepted_schema_cache_fingerprint,
            accepted_schema_cache_fingerprint_for_persisted_snapshot,
            accepted_schema_cache_fingerprint_method_version, decode_persisted_schema_snapshot,
            encode_persisted_schema_snapshot,
            enum_catalog::{
                AcceptedEnumCatalogHandle, AcceptedSchemaPublicationError, AcceptedSchemaRevision,
                AcceptedSchemaRevisionBundle, AcceptedSchemaRootSelection,
                AcceptedStoreCatalogScope, CandidateSchemaRevision,
                decode_verified_accepted_schema_revision_bundle,
                prepare_accepted_schema_root_publication, select_current_accepted_schema_root,
            },
            schema_snapshot_integrity_detail,
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
use std::cell::{OnceCell, Ref, RefCell};
use std::collections::{BTreeMap as StdBTreeMap, BTreeSet};
use std::convert::Infallible;
use std::ops::Bound as RangeBound;

const SCHEMA_KEY_BYTES_USIZE: usize = 16;
const SCHEMA_KEY_BYTES: u32 = 16;
const SCHEMA_KEY_NAMESPACE_ENTITY_SNAPSHOT: u8 = 0;
const SCHEMA_KEY_NAMESPACE_ACCEPTED_BUNDLE: u8 = 1;
const SCHEMA_KEY_NAMESPACE_ACCEPTED_ROOT: u8 = 2;
pub(in crate::db) const MAX_SCHEMA_SNAPSHOT_BYTES: u32 = 512 * 1024;
const SCHEMA_STORE_CATALOG_FINGERPRINT_VERSION: u8 = 1;
const SCHEMA_STORE_DATA_ALLOCATION_FINGERPRINT_VERSION: u8 = 2;
const SCHEMA_STORE_INDEX_ALLOCATION_FINGERPRINT_VERSION: u8 = 3;
const RAW_SCHEMA_SNAPSHOT_MAGIC: &[u8; 8] = b"ICYDBSCH";
const RAW_SCHEMA_SNAPSHOT_VALUE_VERSION: u8 = 1;
const RAW_SCHEMA_SNAPSHOT_HEADER_BYTES: usize = 25;

#[cfg(test)]
thread_local! {
    static LATEST_RAW_SNAPSHOTS_BY_ENTITY_CALLS: Cell<u64> = const { Cell::new(0) };
    static ACCEPTED_SCHEMA_BUNDLE_CACHE_MISSES: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
pub(in crate::db) fn reset_latest_raw_snapshots_by_entity_call_count_for_tests() {
    LATEST_RAW_SNAPSHOTS_BY_ENTITY_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(in crate::db) fn latest_raw_snapshots_by_entity_call_count_for_tests() -> u64 {
    LATEST_RAW_SNAPSHOTS_BY_ENTITY_CALLS.with(Cell::get)
}

#[cfg(test)]
fn reset_accepted_schema_bundle_cache_miss_count_for_tests() {
    ACCEPTED_SCHEMA_BUNDLE_CACHE_MISSES.with(|misses| misses.set(0));
}

#[cfg(test)]
fn accepted_schema_bundle_cache_miss_count_for_tests() -> u64 {
    ACCEPTED_SCHEMA_BUNDLE_CACHE_MISSES.with(Cell::get)
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
        out[0] = SCHEMA_KEY_NAMESPACE_ENTITY_SNAPSHOT;
        out[4..12].copy_from_slice(&entity.value().to_be_bytes());
        out[12..].copy_from_slice(&version.get().to_be_bytes());

        Self(out)
    }

    fn from_accepted_bundle(bundle_key: super::enum_catalog::AcceptedSchemaBundleKey) -> Self {
        let mut out = [0u8; SCHEMA_KEY_BYTES_USIZE];
        out[0] = SCHEMA_KEY_NAMESPACE_ACCEPTED_BUNDLE;
        out[4..12].copy_from_slice(&bundle_key.get().to_be_bytes());
        Self(out)
    }

    fn from_accepted_root_slot(slot: usize) -> Result<Self, InternalError> {
        let slot = u32::try_from(slot).map_err(|_| InternalError::store_invariant())?;
        if slot > 1 {
            return Err(InternalError::store_invariant());
        }
        let mut out = [0u8; SCHEMA_KEY_BYTES_USIZE];
        out[0] = SCHEMA_KEY_NAMESPACE_ACCEPTED_ROOT;
        out[12..].copy_from_slice(&slot.to_be_bytes());
        Ok(Self(out))
    }

    /// Return the entity tag encoded in this schema key.
    #[must_use]
    fn entity_tag(self) -> EntityTag {
        let mut bytes = [0u8; size_of::<u64>()];
        bytes.copy_from_slice(&self.0[4..12]);

        EntityTag::new(u64::from_be_bytes(bytes))
    }

    /// Return the schema version encoded in this schema key.
    #[must_use]
    fn version(self) -> u32 {
        let mut bytes = [0u8; size_of::<u32>()];
        bytes.copy_from_slice(&self.0[12..]);

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

    const fn all_entity_range_bounds() -> (RangeBound<Self>, RangeBound<Self>) {
        let mut end = [u8::MAX; SCHEMA_KEY_BYTES_USIZE];
        end[0] = SCHEMA_KEY_NAMESPACE_ENTITY_SNAPSHOT;
        (
            RangeBound::Included(Self([0; SCHEMA_KEY_BYTES_USIZE])),
            RangeBound::Included(Self(end)),
        )
    }

    #[cfg(test)]
    const fn is_entity_snapshot(self) -> bool {
        self.0[0] == SCHEMA_KEY_NAMESPACE_ENTITY_SNAPSHOT
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
/// Raw persisted value in the schema metadata store.
///
/// Entity snapshots carry this wrapper's identity header. Accepted catalog
/// bundles and root slots are already-versioned control records and remain
/// opaque here. Key-specific readers decide which representation is required.
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

    /// Store one already-versioned accepted-catalog control record.
    #[must_use]
    const fn from_encoded_control_record(payload: Vec<u8>) -> Self {
        Self {
            payload,
            accepted_schema_fingerprint: None,
        }
    }

    /// Build a framed entity snapshot around deliberately untrusted payload
    /// bytes so decode-boundary tests can exercise current-format corruption.
    #[cfg(test)]
    #[must_use]
    const fn from_unchecked_persisted_snapshot_payload(payload: Vec<u8>) -> Self {
        Self {
            payload,
            accepted_schema_fingerprint: Some([0; size_of::<CommitSchemaFingerprint>()]),
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
        // The identity header is the outer format gate. Do not pass a legacy
        // headerless value or a control record into the schema payload codec.
        let _fingerprint = self.accepted_schema_fingerprint()?;
        decode_persisted_schema_snapshot(self.as_bytes())
    }
}

#[cfg(test)]
pub(in crate::db::schema) fn validate_raw_schema_snapshot_bytes_for_tests(
    bytes: Vec<u8>,
) -> Result<(), InternalError> {
    let raw = <RawSchemaSnapshot as Storable>::from_bytes(Cow::Owned(bytes));
    raw.decode_persisted_snapshot().map(drop)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedCatalogIdentity {
    entity_tag: EntityTag,
    entity_path: &'static str,
    store_path: &'static str,
    accepted_schema_revision: AcceptedSchemaRevision,
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
        accepted_schema_revision: AcceptedSchemaRevision,
        accepted_schema_version: SchemaVersion,
        accepted_schema_fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            entity_tag,
            entity_path,
            store_path,
            accepted_schema_revision,
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
    pub(in crate::db) const fn accepted_schema_revision(self) -> AcceptedSchemaRevision {
        self.accepted_schema_revision
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
    enum_catalog: AcceptedEnumCatalogHandle,
    raw_snapshot: Vec<u8>,
}

impl AcceptedCatalogSnapshotSelection {
    #[must_use]
    const fn new(
        identity: AcceptedCatalogIdentity,
        enum_catalog: AcceptedEnumCatalogHandle,
        raw_snapshot: Vec<u8>,
    ) -> Self {
        Self {
            identity,
            enum_catalog,
            raw_snapshot,
        }
    }

    #[must_use]
    pub(in crate::db) const fn identity(&self) -> AcceptedCatalogIdentity {
        self.identity
    }

    #[must_use]
    pub(in crate::db) const fn enum_catalog(&self) -> &AcceptedEnumCatalogHandle {
        &self.enum_catalog
    }

    /// Re-encode a cached accepted snapshot under its verified catalog identity.
    pub(in crate::db) fn from_accepted_snapshot(
        identity: AcceptedCatalogIdentity,
        enum_catalog: AcceptedEnumCatalogHandle,
        snapshot: &AcceptedSchemaSnapshot,
    ) -> Result<Self, InternalError> {
        let raw_snapshot =
            RawSchemaSnapshot::from_persisted_snapshot(snapshot.persisted_snapshot())?;
        if raw_snapshot.accepted_schema_fingerprint()? != identity.accepted_schema_fingerprint() {
            return Err(InternalError::store_invariant());
        }

        Ok(Self::new(identity, enum_catalog, raw_snapshot.into_bytes()))
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

    const BOUND: StorableBound = StorableBound::Unbounded;
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
    accepted_bundle_cache: RefCell<Option<AcceptedSchemaBundleCache>>,
    accepted_catalog_scope: OnceCell<AcceptedStoreCatalogScope>,
}

struct AcceptedSchemaBundleCache {
    selection: AcceptedSchemaRootSelection,
    bundle: AcceptedSchemaRevisionBundle,
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
            accepted_bundle_cache: RefCell::new(None),
            accepted_catalog_scope: OnceCell::new(),
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
            accepted_bundle_cache: RefCell::new(None),
            accepted_catalog_scope: OnceCell::new(),
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
            return Err(InternalError::store_invariant());
        };

        live.clear();
        tombstones.clear();
        self.accepted_bundle_cache.get_mut().take();

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

    /// Return the current accepted store root selected from its two checksummed slots.
    pub(in crate::db) fn current_accepted_schema_root(
        &self,
    ) -> Result<Option<AcceptedSchemaRootSelection>, InternalError> {
        let first = self.accepted_root_slot_bytes(0)?;
        let second = self.accepted_root_slot_bytes(1)?;
        select_current_accepted_schema_root([first.as_deref(), second.as_deref()])
    }

    /// Load and verify the immutable bundle referenced by the current root.
    pub(in crate::db) fn current_accepted_schema_bundle(
        &self,
    ) -> Result<Option<AcceptedSchemaRevisionBundle>, InternalError> {
        let Some(selection) = self.current_accepted_schema_root()? else {
            return Ok(None);
        };
        let key = RawSchemaKey::from_accepted_bundle(selection.root().bundle_key());
        let raw = self
            .get_raw_snapshot(&key)
            .ok_or_else(InternalError::store_corruption)?;
        decode_verified_accepted_schema_revision_bundle(selection.root(), raw.as_bytes()).map(Some)
    }

    /// Return the current accepted revision without decoding its bundle.
    pub(in crate::db) fn current_accepted_schema_revision(
        &self,
    ) -> Result<Option<AcceptedSchemaRevision>, InternalError> {
        Ok(self
            .current_accepted_schema_root()?
            .map(|selection| selection.root().revision()))
    }

    /// Bootstrap an immutable candidate directly into the schema allocation.
    ///
    /// Journaled online revisions must use `apply_journaled_accepted_schema_candidate`.
    pub(in crate::db) fn publish_accepted_schema_candidate(
        &mut self,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(), InternalError> {
        let first = self.accepted_root_slot_bytes(0)?;
        let second = self.accepted_root_slot_bytes(1)?;
        prepare_accepted_schema_root_publication(
            [first.as_deref(), second.as_deref()],
            expected_revision,
            candidate,
        )
        .map_err(map_schema_publication_error)?;

        self.insert_durable_candidate_snapshots(candidate)?;
        let bundle_key = RawSchemaKey::from_accepted_bundle(candidate.root().bundle_key());
        self.insert_durable_raw_value(bundle_key, candidate.encoded_bundle().to_vec());
        let persisted_bundle = self
            .get_raw_snapshot(&bundle_key)
            .ok_or_else(InternalError::store_corruption)?;
        let _verified = decode_verified_accepted_schema_revision_bundle(
            candidate.root(),
            persisted_bundle.as_bytes(),
        )?;

        // Re-read the root immediately before the inactive-slot write. This is
        // the compare-and-swap check after candidate persistence.
        let first = self.accepted_root_slot_bytes(0)?;
        let second = self.accepted_root_slot_bytes(1)?;
        let publication = prepare_accepted_schema_root_publication(
            [first.as_deref(), second.as_deref()],
            expected_revision,
            candidate,
        )
        .map_err(map_schema_publication_error)?;
        let root_key = RawSchemaKey::from_accepted_root_slot(publication.target_slot())?;
        self.insert_durable_raw_value(root_key, publication.encoded_root().to_vec());

        let selected = self
            .current_accepted_schema_root()?
            .ok_or_else(InternalError::store_corruption)?;
        if selected.root() != candidate.root() {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }

    /// Apply one marker-bound schema candidate to the journaled live projection.
    pub(in crate::db) fn apply_journaled_accepted_schema_candidate(
        &mut self,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(), InternalError> {
        if !matches!(self.backend, SchemaStoreBackend::Journaled { .. }) {
            return Err(InternalError::store_invariant());
        }
        if self.current_root_matches_candidate(candidate)? {
            return Ok(());
        }

        let first = self.accepted_root_slot_bytes(0)?;
        let second = self.accepted_root_slot_bytes(1)?;
        prepare_accepted_schema_root_publication(
            [first.as_deref(), second.as_deref()],
            expected_revision,
            candidate,
        )
        .map_err(map_schema_publication_error)?;

        for (entity_tag, snapshot) in candidate.bundle().entity_snapshots() {
            self.insert_persisted_snapshot(*entity_tag, snapshot)?;
        }
        let bundle_key = RawSchemaKey::from_accepted_bundle(candidate.root().bundle_key());
        self.insert_raw_snapshot(
            bundle_key,
            RawSchemaSnapshot::from_encoded_control_record(candidate.encoded_bundle().to_vec()),
        );
        let persisted_bundle = self
            .get_raw_snapshot(&bundle_key)
            .ok_or_else(InternalError::store_corruption)?;
        let _verified = decode_verified_accepted_schema_revision_bundle(
            candidate.root(),
            persisted_bundle.as_bytes(),
        )?;

        let first = self.accepted_root_slot_bytes(0)?;
        let second = self.accepted_root_slot_bytes(1)?;
        let publication = prepare_accepted_schema_root_publication(
            [first.as_deref(), second.as_deref()],
            expected_revision,
            candidate,
        )
        .map_err(map_schema_publication_error)?;
        let root_key = RawSchemaKey::from_accepted_root_slot(publication.target_slot())?;
        self.insert_raw_snapshot(
            root_key,
            RawSchemaSnapshot::from_encoded_control_record(publication.encoded_root().to_vec()),
        );

        if !self.current_root_matches_candidate(candidate)? {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }

    /// Fold one committed schema candidate into the canonical schema BTree.
    pub(in crate::db) fn fold_journaled_accepted_schema_candidate(
        &mut self,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(), InternalError> {
        if self.canonical_root_matches_candidate(candidate)? {
            return Ok(());
        }

        let first = self.canonical_root_slot_bytes(0)?;
        let second = self.canonical_root_slot_bytes(1)?;
        prepare_accepted_schema_root_publication(
            [first.as_deref(), second.as_deref()],
            expected_revision,
            candidate,
        )
        .map_err(map_schema_publication_error)?;

        for (entity_tag, snapshot) in candidate.bundle().entity_snapshots() {
            self.fold_persisted_snapshot(*entity_tag, snapshot)?;
        }
        let bundle_key = RawSchemaKey::from_accepted_bundle(candidate.root().bundle_key());
        self.insert_canonical_raw_value(bundle_key, candidate.encoded_bundle().to_vec())?;
        let persisted_bundle = self
            .get_canonical_raw_value(&bundle_key)?
            .ok_or_else(InternalError::store_corruption)?;
        let _verified = decode_verified_accepted_schema_revision_bundle(
            candidate.root(),
            persisted_bundle.as_bytes(),
        )?;

        let first = self.canonical_root_slot_bytes(0)?;
        let second = self.canonical_root_slot_bytes(1)?;
        let publication = prepare_accepted_schema_root_publication(
            [first.as_deref(), second.as_deref()],
            expected_revision,
            candidate,
        )
        .map_err(map_schema_publication_error)?;
        let root_key = RawSchemaKey::from_accepted_root_slot(publication.target_slot())?;
        self.insert_canonical_raw_value(root_key, publication.encoded_root().to_vec())?;

        if !self.canonical_root_matches_candidate(candidate)? {
            return Err(InternalError::store_corruption());
        }
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

    /// Load and decode the highest staged schema snapshot for one entity.
    ///
    /// Candidate construction and publication gates use this view. Runtime
    /// execution must use `current_accepted_persisted_snapshot` instead.
    pub(in crate::db) fn latest_staged_persisted_snapshot(
        &self,
        entity: EntityTag,
    ) -> Result<Option<PersistedSchemaSnapshot>, InternalError> {
        self.latest_raw_snapshot(entity)
            .map(|snapshot| snapshot.decode_persisted_snapshot())
            .transpose()
    }

    /// Load one entity snapshot from the immutable bundle selected by the
    /// current accepted root.
    pub(in crate::db) fn current_accepted_persisted_snapshot(
        &self,
        entity: EntityTag,
    ) -> Result<Option<PersistedSchemaSnapshot>, InternalError> {
        let Some(bundle) = self.current_accepted_schema_bundle_ref()? else {
            return Ok(None);
        };

        Ok(bundle.entity_snapshots().get(&entity).cloned())
    }

    /// Return one accepted catalog selection from the current immutable root.
    pub(in crate::db) fn current_accepted_catalog_selection(
        &self,
        entity: EntityTag,
        entity_path: &'static str,
        store_path: &'static str,
    ) -> Result<Option<AcceptedCatalogSnapshotSelection>, InternalError> {
        let Some(bundle) = self.current_accepted_schema_bundle_ref()? else {
            return Ok(None);
        };
        if bundle.store_path() != store_path {
            return Err(InternalError::store_corruption());
        }
        let Some(snapshot) = bundle.entity_snapshots().get(&entity) else {
            return Ok(None);
        };
        if snapshot.entity_path() != entity_path {
            return Err(InternalError::store_corruption());
        }

        let raw_snapshot = RawSchemaSnapshot::from_persisted_snapshot(snapshot)?;
        let fingerprint = raw_snapshot.accepted_schema_fingerprint()?;
        let identity = AcceptedCatalogIdentity::new(
            entity,
            entity_path,
            store_path,
            bundle.revision(),
            snapshot.version(),
            fingerprint,
        );

        Ok(Some(AcceptedCatalogSnapshotSelection::new(
            identity,
            AcceptedEnumCatalogHandle::new(
                bundle.enum_catalog().clone(),
                self.accepted_catalog_scope
                    .get_or_init(AcceptedStoreCatalogScope::new)
                    .clone(),
                bundle.revision(),
                self.current_accepted_schema_root()?
                    .ok_or_else(InternalError::store_corruption)?
                    .root()
                    .fingerprint(),
            ),
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
    fn get_raw_snapshot(&self, key: &RawSchemaKey) -> Option<RawSchemaSnapshot> {
        match &self.backend {
            SchemaStoreBackend::Heap(map) => map.get(key).cloned(),
            SchemaStoreBackend::Journaled { .. } => self.get_raw_snapshot_for_backend(key),
        }
    }

    fn accepted_root_slot_bytes(&self, slot: usize) -> Result<Option<Vec<u8>>, InternalError> {
        let key = RawSchemaKey::from_accepted_root_slot(slot)?;
        Ok(self
            .get_raw_snapshot(&key)
            .map(RawSchemaSnapshot::into_bytes))
    }

    fn canonical_root_slot_bytes(&self, slot: usize) -> Result<Option<Vec<u8>>, InternalError> {
        let key = RawSchemaKey::from_accepted_root_slot(slot)?;
        Ok(self
            .get_canonical_raw_value(&key)?
            .map(RawSchemaSnapshot::into_bytes))
    }

    fn current_root_matches_candidate(
        &self,
        candidate: &CandidateSchemaRevision,
    ) -> Result<bool, InternalError> {
        let Some(selection) = self.current_accepted_schema_root()? else {
            return Ok(false);
        };
        if selection.root() != candidate.root() {
            return Ok(false);
        }
        let key = RawSchemaKey::from_accepted_bundle(candidate.root().bundle_key());
        let bundle = self
            .get_raw_snapshot(&key)
            .ok_or_else(InternalError::store_corruption)?;
        let _verified =
            decode_verified_accepted_schema_revision_bundle(candidate.root(), bundle.as_bytes())?;
        Ok(true)
    }

    fn canonical_root_matches_candidate(
        &self,
        candidate: &CandidateSchemaRevision,
    ) -> Result<bool, InternalError> {
        let first = self.canonical_root_slot_bytes(0)?;
        let second = self.canonical_root_slot_bytes(1)?;
        let Some(selection) =
            select_current_accepted_schema_root([first.as_deref(), second.as_deref()])?
        else {
            return Ok(false);
        };
        if selection.root() != candidate.root() {
            return Ok(false);
        }
        let key = RawSchemaKey::from_accepted_bundle(candidate.root().bundle_key());
        let bundle = self
            .get_canonical_raw_value(&key)?
            .ok_or_else(InternalError::store_corruption)?;
        let _verified =
            decode_verified_accepted_schema_revision_bundle(candidate.root(), bundle.as_bytes())?;
        Ok(true)
    }

    fn get_canonical_raw_value(
        &self,
        key: &RawSchemaKey,
    ) -> Result<Option<RawSchemaSnapshot>, InternalError> {
        match &self.backend {
            SchemaStoreBackend::Journaled { canonical, .. } => Ok(canonical.get(key)),
            SchemaStoreBackend::Heap(_) => Err(InternalError::store_invariant()),
        }
    }

    fn insert_canonical_raw_value(
        &mut self,
        key: RawSchemaKey,
        bytes: Vec<u8>,
    ) -> Result<(), InternalError> {
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant());
        };
        canonical.insert(key, RawSchemaSnapshot::from_encoded_control_record(bytes));
        Ok(())
    }

    // Initial accepted-catalog bootstrap persists immutable bundle/root values
    // directly in the schema allocation. Later online schema mutation will
    // carry the same values through the journal before calling this primitive.
    fn insert_durable_raw_value(&mut self, key: RawSchemaKey, bytes: Vec<u8>) {
        let value = RawSchemaSnapshot::from_encoded_control_record(bytes);
        match &mut self.backend {
            SchemaStoreBackend::Heap(map) => {
                map.insert(key, value);
            }
            SchemaStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
            } => {
                live.remove(&key);
                tombstones.remove(&key);
                canonical.insert(key, value);
            }
        }
    }

    fn insert_durable_candidate_snapshots(
        &mut self,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(), InternalError> {
        for (entity_tag, snapshot) in candidate.bundle().entity_snapshots() {
            let key = RawSchemaKey::from_entity_version(*entity_tag, snapshot.version());
            let value = RawSchemaSnapshot::from_persisted_snapshot(snapshot)?;
            match &mut self.backend {
                SchemaStoreBackend::Heap(map) => {
                    map.insert(key, value);
                }
                SchemaStoreBackend::Journaled {
                    canonical,
                    live,
                    tombstones,
                } => {
                    live.remove(&key);
                    tombstones.remove(&key);
                    canonical.insert(key, value);
                }
            }
        }
        Ok(())
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
        self.accepted_bundle_cache.get_mut().take();
        match &mut self.backend {
            SchemaStoreBackend::Heap(map) => map.clear(),
            SchemaStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
            } => {
                live.clear();
                tombstones.clear();
                let keys = canonical
                    .iter()
                    .map(|entry| *entry.key())
                    .collect::<Vec<_>>();
                for key in keys {
                    if key.is_entity_snapshot() {
                        tombstones.insert(key);
                    } else {
                        canonical.remove(&key);
                    }
                }
            }
        }
    }

    fn current_accepted_schema_bundle_ref(
        &self,
    ) -> Result<Option<Ref<'_, AcceptedSchemaRevisionBundle>>, InternalError> {
        let Some(selection) = self.current_accepted_schema_root()? else {
            self.accepted_bundle_cache
                .try_borrow_mut()
                .map_err(|_| InternalError::store_invariant())?
                .take();
            return Ok(None);
        };

        let cache_matches = self
            .accepted_bundle_cache
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?
            .as_ref()
            .is_some_and(|cached| cached.selection == selection);
        if !cache_matches {
            let key = RawSchemaKey::from_accepted_bundle(selection.root().bundle_key());
            let raw = self
                .get_raw_snapshot(&key)
                .ok_or_else(InternalError::store_corruption)?;
            let bundle =
                decode_verified_accepted_schema_revision_bundle(selection.root(), raw.as_bytes())?;
            #[cfg(test)]
            ACCEPTED_SCHEMA_BUNDLE_CACHE_MISSES
                .with(|misses| misses.set(misses.get().saturating_add(1)));
            *self
                .accepted_bundle_cache
                .try_borrow_mut()
                .map_err(|_| InternalError::store_invariant())? =
                Some(AcceptedSchemaBundleCache { selection, bundle });
        }

        let cache = self
            .accepted_bundle_cache
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?;
        Ref::filter_map(cache, |cache| {
            cache
                .as_ref()
                .filter(|cached| cached.selection == selection)
                .map(|cached| &cached.bundle)
        })
        .map(Some)
        .map_err(|_| InternalError::store_invariant())
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
        let bounds = RawSchemaKey::all_entity_range_bounds();
        match &self.backend {
            SchemaStoreBackend::Heap(map) => {
                let mut visitor = visitor;
                for (key, snapshot) in map.range((bounds.0, bounds.1)) {
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
                bounds,
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

fn map_schema_publication_error(error: AcceptedSchemaPublicationError) -> InternalError {
    match error {
        AcceptedSchemaPublicationError::StaleSchemaRevision { .. }
        | AcceptedSchemaPublicationError::RevisionExhausted => InternalError::store_unsupported(),
        AcceptedSchemaPublicationError::InvalidCandidate => InternalError::store_invariant(),
        AcceptedSchemaPublicationError::CorruptRootSlots => InternalError::store_corruption(),
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
                        hash_accepted_field_kind(hasher, expression.input_kind());
                        hash_accepted_field_kind(hasher, expression.output_kind());
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
    hash_accepted_field_kind(hasher, path.kind());
    write_hash_tag_u8(hasher, u8::from(path.nullable()));
}

fn hash_accepted_field_kind(hasher: &mut sha2::Sha256, kind: &AcceptedFieldKind) {
    match kind {
        AcceptedFieldKind::Account => write_hash_tag_u8(hasher, 1),
        AcceptedFieldKind::Blob { max_len } => {
            write_hash_tag_u8(hasher, 2);
            hash_optional_u32(hasher, *max_len);
        }
        AcceptedFieldKind::Bool => write_hash_tag_u8(hasher, 3),
        AcceptedFieldKind::Date => write_hash_tag_u8(hasher, 4),
        AcceptedFieldKind::Decimal { scale } => {
            write_hash_tag_u8(hasher, 5);
            write_hash_u32(hasher, *scale);
        }
        AcceptedFieldKind::Duration => write_hash_tag_u8(hasher, 6),
        AcceptedFieldKind::Enum { type_id } => {
            write_hash_tag_u8(hasher, 7);
            write_hash_u32(hasher, type_id.get());
        }
        AcceptedFieldKind::Float32 => write_hash_tag_u8(hasher, 8),
        AcceptedFieldKind::Float64 => write_hash_tag_u8(hasher, 9),
        AcceptedFieldKind::Int8 => write_hash_tag_u8(hasher, 10),
        AcceptedFieldKind::Int16 => write_hash_tag_u8(hasher, 11),
        AcceptedFieldKind::Int32 => write_hash_tag_u8(hasher, 12),
        AcceptedFieldKind::Int64 => write_hash_tag_u8(hasher, 13),
        AcceptedFieldKind::Int128 => write_hash_tag_u8(hasher, 14),
        AcceptedFieldKind::IntBig { max_bytes } => {
            write_hash_tag_u8(hasher, 15);
            write_hash_u32(hasher, *max_bytes);
        }
        AcceptedFieldKind::Principal => write_hash_tag_u8(hasher, 16),
        AcceptedFieldKind::Subaccount => write_hash_tag_u8(hasher, 17),
        AcceptedFieldKind::Text { max_len } => {
            write_hash_tag_u8(hasher, 18);
            hash_optional_u32(hasher, *max_len);
        }
        AcceptedFieldKind::Timestamp => write_hash_tag_u8(hasher, 19),
        AcceptedFieldKind::Nat8 => write_hash_tag_u8(hasher, 20),
        AcceptedFieldKind::Nat16 => write_hash_tag_u8(hasher, 21),
        AcceptedFieldKind::Nat32 => write_hash_tag_u8(hasher, 22),
        AcceptedFieldKind::Nat64 => write_hash_tag_u8(hasher, 23),
        AcceptedFieldKind::Nat128 => write_hash_tag_u8(hasher, 24),
        AcceptedFieldKind::NatBig { max_bytes } => {
            write_hash_tag_u8(hasher, 25);
            write_hash_u32(hasher, *max_bytes);
        }
        AcceptedFieldKind::Ulid => write_hash_tag_u8(hasher, 26),
        AcceptedFieldKind::Unit => write_hash_tag_u8(hasher, 27),
        AcceptedFieldKind::Relation {
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
            hash_accepted_field_kind(hasher, key_kind);
            write_hash_str_u32(hasher, accepted_relation_strength_name(*strength));
        }
        AcceptedFieldKind::List(inner) => {
            write_hash_tag_u8(hasher, 29);
            hash_accepted_field_kind(hasher, inner);
        }
        AcceptedFieldKind::Set(inner) => {
            write_hash_tag_u8(hasher, 30);
            hash_accepted_field_kind(hasher, inner);
        }
        AcceptedFieldKind::Map { key, value } => {
            write_hash_tag_u8(hasher, 31);
            hash_accepted_field_kind(hasher, key);
            hash_accepted_field_kind(hasher, value);
        }
        AcceptedFieldKind::Structured { queryable } => {
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

const fn accepted_relation_strength_name(
    strength: crate::db::schema::AcceptedRelationStrength,
) -> &'static str {
    match strength {
        crate::db::schema::AcceptedRelationStrength::Strong => "strong",
        crate::db::schema::AcceptedRelationStrength::Weak => "weak",
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
