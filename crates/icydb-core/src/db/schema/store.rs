//! Module: db::schema::store
//! Responsibility: stable BTreeMap-backed schema metadata persistence.
//! Does not own: reconciliation policy, typed snapshot encoding, or generated proposal construction.
//! Boundary: provides the third per-store stable memory alongside row and index stores.

use crate::db::schema::identity_state::{
    IdentityAdvanceId, IdentityRangeAdvance, IdentityRangeCommitState, IdentityState,
    IdentityStateInventory, IdentityStateLifecycle, IdentityStateTransition,
    IdentityStatementCursor, MAX_IDENTITY_STATE_RECORDS_PER_DATABASE, decode_identity_state,
    encode_identity_state, prepare_identity_state_transition, validate_identity_state_closure,
};
use crate::db::schema::{
    cardinality_build::{
        CardinalityAcceptedDomain, CardinalityReadyCandidate, EmptyCardinalityReadyCandidate,
    },
    cardinality_generation::{
        CardinalityAcceptedRootIdentity, CardinalityBuildCursor, CardinalityCountDigest,
        CardinalityCountRecord, CardinalityCountSlot, CardinalityGenerationHeader,
        CardinalityGenerationId, CardinalityGenerationState, CardinalitySourceIdentity,
    },
};
use crate::{
    db::{
        codec::{
            finalize_hash_sha256, new_hash_sha256, write_hash_len_u32, write_hash_str_u32,
            write_hash_tag_u8, write_hash_u32, write_hash_u64,
        },
        commit::CommitSchemaFingerprint,
        direction::Direction,
        integrity::DatabaseIncarnationId,
        journal::{JournalBatch, JournalRecord},
        ordered_overlay::{OrderedOverlayEntry, ordered_overlay_entries},
        positioned_overlay::{
            JournalOverlayPosition, PositionedOverlayMetadata, PositionedOverlayRetirement,
        },
        runtime_entity_catalog::AcceptedRuntimeEntity,
        schema::{
            AcceptedFieldKind, AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot,
            ConstraintActivationKind, ConstraintActivationState, ConstraintId, ConstraintOrigin,
            ConstraintValidationJob, FieldId, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedSchemaSnapshot, SchemaVersion,
            accepted_schema_cache_fingerprint,
            accepted_schema_cache_fingerprint_for_persisted_snapshot,
            accepted_schema_cache_fingerprint_method_version, decode_constraint_validation_job,
            decode_persisted_schema_snapshot, encode_constraint_validation_job,
            encode_persisted_schema_snapshot,
            enum_catalog::{
                AcceptedSchemaAuthority, AcceptedSchemaPublicationError, AcceptedSchemaRevision,
                AcceptedSchemaRevisionBundle, AcceptedSchemaRootSelection,
                AcceptedStoreCatalogScope, AcceptedValueCatalogHandle, CandidateSchemaRevision,
                decode_verified_accepted_schema_revision_bundle,
                prepare_accepted_schema_root_publication, select_current_accepted_schema_root,
            },
            schema_snapshot_integrity_detail,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use ic_stable_structures::{
    BTreeMap as StableBTreeMap, DefaultMemoryImpl, Storable, memory_manager::VirtualMemory,
    storable::Bound as StorableBound,
};
use sha2::Digest;
use std::borrow::Cow;
#[cfg(test)]
use std::cell::Cell;
use std::cell::{OnceCell, Ref, RefCell};
use std::collections::{BTreeMap as StdBTreeMap, BTreeSet};
#[cfg(test)]
use std::convert::Infallible;
use std::ops::Bound as RangeBound;
use std::rc::Rc;

const SCHEMA_KEY_BYTES_USIZE: usize = 16;
const SCHEMA_KEY_BYTES: u32 = 16;
const SCHEMA_KEY_NAMESPACE_ENTITY_SNAPSHOT: u8 = 0;
const SCHEMA_KEY_NAMESPACE_ACCEPTED_BUNDLE: u8 = 1;
const SCHEMA_KEY_NAMESPACE_ACCEPTED_ROOT: u8 = 2;
const SCHEMA_KEY_NAMESPACE_CONSTRAINT_VALIDATION_JOB: u8 = 3;
const SCHEMA_KEY_NAMESPACE_IDENTITY_STATE: u8 = 4;
// Every role exposes the sole current method version while its separate domain
// tag keeps data, index, and full-catalog fingerprint inputs disjoint.
const SCHEMA_STORE_FINGERPRINT_METHOD_VERSION: u8 = 1;
const SCHEMA_STORE_CATALOG_FINGERPRINT_DOMAIN: u8 = 1;
const SCHEMA_STORE_DATA_ALLOCATION_FINGERPRINT_DOMAIN: u8 = 2;
const SCHEMA_STORE_INDEX_ALLOCATION_FINGERPRINT_DOMAIN: u8 = 3;
const ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_BOOL: u8 = 3;
const ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_LIST: u8 = 29;
const ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_SET: u8 = 30;
const ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_MAP: u8 = 31;
const ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_COMPOSITE: u8 = 32;
const RAW_SCHEMA_SNAPSHOT_MAGIC: &[u8; 8] = b"ICYDBCAT";
const RAW_SCHEMA_SNAPSHOT_VALUE_VERSION: u8 = 1;
const RAW_SCHEMA_SNAPSHOT_HEADER_BYTES: usize = 25;

/// Load one accepted entity snapshot through the current immutable bundle.
///
/// The persisted root and row-layout contract are both validated before the
/// snapshot can become runtime authority.
pub(in crate::db) fn load_accepted_schema_snapshot(
    schema_store: &SchemaStore,
    entity_tag: EntityTag,
    entity_path: &str,
) -> Result<AcceptedSchemaSnapshot, InternalError> {
    let bundle = schema_store
        .current_accepted_schema_bundle()?
        .ok_or_else(InternalError::store_corruption)?;
    let snapshot = bundle
        .entity_snapshots()
        .get(&entity_tag)
        .cloned()
        .ok_or_else(InternalError::store_corruption)?;
    if snapshot.entity_path() != entity_path {
        return Err(InternalError::store_corruption());
    }
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot)?;
    let _runtime_contract = AcceptedRowLayoutRuntimeContract::from_accepted_schema(&accepted)?;

    Ok(accepted)
}

#[cfg(test)]
thread_local! {
    static ACCEPTED_SCHEMA_BUNDLE_CACHE_MISSES: Cell<u64> = const { Cell::new(0) };
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

    fn from_constraint_validation_job(entity: EntityTag, constraint_id: ConstraintId) -> Self {
        let mut out = [0u8; SCHEMA_KEY_BYTES_USIZE];
        out[0] = SCHEMA_KEY_NAMESPACE_CONSTRAINT_VALIDATION_JOB;
        out[4..12].copy_from_slice(&entity.value().to_be_bytes());
        out[12..].copy_from_slice(&constraint_id.get().to_be_bytes());
        Self(out)
    }

    fn from_identity_state(entity: EntityTag, field_id: FieldId) -> Self {
        let mut out = [0u8; SCHEMA_KEY_BYTES_USIZE];
        out[0] = SCHEMA_KEY_NAMESPACE_IDENTITY_STATE;
        out[4..12].copy_from_slice(&entity.value().to_be_bytes());
        out[12..].copy_from_slice(&field_id.get().to_be_bytes());
        Self(out)
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

    const fn all_entity_range_bounds() -> (RangeBound<Self>, RangeBound<Self>) {
        let mut end = [u8::MAX; SCHEMA_KEY_BYTES_USIZE];
        end[0] = SCHEMA_KEY_NAMESPACE_ENTITY_SNAPSHOT;
        (
            RangeBound::Included(Self([0; SCHEMA_KEY_BYTES_USIZE])),
            RangeBound::Included(Self(end)),
        )
    }

    #[cfg(test)]
    fn entity_range_bounds(entity: EntityTag) -> (RangeBound<Self>, RangeBound<Self>) {
        (
            RangeBound::Included(Self::from_entity_version(entity, SchemaVersion::initial())),
            RangeBound::Included(Self::from_entity_version(
                entity,
                SchemaVersion::new(u32::MAX),
            )),
        )
    }

    const fn all_constraint_validation_job_range_bounds() -> (RangeBound<Self>, RangeBound<Self>) {
        let mut start = [0u8; SCHEMA_KEY_BYTES_USIZE];
        start[0] = SCHEMA_KEY_NAMESPACE_CONSTRAINT_VALIDATION_JOB;
        let mut end = [u8::MAX; SCHEMA_KEY_BYTES_USIZE];
        end[0] = SCHEMA_KEY_NAMESPACE_CONSTRAINT_VALIDATION_JOB;
        (
            RangeBound::Included(Self(start)),
            RangeBound::Included(Self(end)),
        )
    }

    const fn all_identity_state_range_bounds() -> (RangeBound<Self>, RangeBound<Self>) {
        let mut start = [0u8; SCHEMA_KEY_BYTES_USIZE];
        start[0] = SCHEMA_KEY_NAMESPACE_IDENTITY_STATE;
        let mut end = [u8::MAX; SCHEMA_KEY_BYTES_USIZE];
        end[0] = SCHEMA_KEY_NAMESPACE_IDENTITY_STATE;
        (
            RangeBound::Included(Self(start)),
            RangeBound::Included(Self(end)),
        )
    }

    #[cfg(test)]
    const fn is_entity_snapshot(self) -> bool {
        self.0[0] == SCHEMA_KEY_NAMESPACE_ENTITY_SNAPSHOT
    }

    const fn is_accepted_root(self) -> bool {
        self.0[0] == SCHEMA_KEY_NAMESPACE_ACCEPTED_ROOT
    }

    const fn is_constraint_validation_job(self) -> bool {
        self.0[0] == SCHEMA_KEY_NAMESPACE_CONSTRAINT_VALIDATION_JOB
    }

    const fn is_identity_state(self) -> bool {
        self.0[0] == SCHEMA_KEY_NAMESPACE_IDENTITY_STATE
    }

    fn constraint_id(self) -> Option<ConstraintId> {
        self.is_constraint_validation_job()
            .then(|| ConstraintId::new(self.version()))
            .flatten()
    }
}

impl RawSchemaKey {
    const NAMESPACE_CARDINALITY_CONTROL: u8 = 5;
    const NAMESPACE_CARDINALITY_COUNT_A: u8 = 6;
    const NAMESPACE_CARDINALITY_COUNT_B: u8 = 7;
    const CARDINALITY_HEADER_DISCRIMINATOR: u8 = 0;
    const CARDINALITY_BUILD_CURSOR_DISCRIMINATOR: u8 = 1;

    const fn from_cardinality_generation_header() -> Self {
        let mut out = [0u8; SCHEMA_KEY_BYTES_USIZE];
        out[0] = Self::NAMESPACE_CARDINALITY_CONTROL;
        out[SCHEMA_KEY_BYTES_USIZE - 1] = Self::CARDINALITY_HEADER_DISCRIMINATOR;
        Self(out)
    }

    const fn from_cardinality_build_cursor() -> Self {
        let mut out = [0u8; SCHEMA_KEY_BYTES_USIZE];
        out[0] = Self::NAMESPACE_CARDINALITY_CONTROL;
        out[SCHEMA_KEY_BYTES_USIZE - 1] = Self::CARDINALITY_BUILD_CURSOR_DISCRIMINATOR;
        Self(out)
    }

    fn from_cardinality_count(slot: CardinalityCountSlot, digest: CardinalityCountDigest) -> Self {
        let mut out = [0u8; SCHEMA_KEY_BYTES_USIZE];
        out[0] = match slot {
            CardinalityCountSlot::A => Self::NAMESPACE_CARDINALITY_COUNT_A,
            CardinalityCountSlot::B => Self::NAMESPACE_CARDINALITY_COUNT_B,
        };
        out[1..].copy_from_slice(&digest.as_bytes()[..SCHEMA_KEY_BYTES_USIZE - 1]);
        Self(out)
    }

    const fn cardinality_count_range_bounds(
        slot: CardinalityCountSlot,
    ) -> (RangeBound<Self>, RangeBound<Self>) {
        let namespace = match slot {
            CardinalityCountSlot::A => Self::NAMESPACE_CARDINALITY_COUNT_A,
            CardinalityCountSlot::B => Self::NAMESPACE_CARDINALITY_COUNT_B,
        };
        let mut start = [0_u8; SCHEMA_KEY_BYTES_USIZE];
        start[0] = namespace;
        let mut end = [u8::MAX; SCHEMA_KEY_BYTES_USIZE];
        end[0] = namespace;
        (
            RangeBound::Included(Self(start)),
            RangeBound::Included(Self(end)),
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
        // The identity header is the outer format gate. Do not pass a
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedCatalogIdentity {
    entity_tag: EntityTag,
    entity_path: Rc<str>,
    store_path: &'static str,
    accepted_schema_revision: AcceptedSchemaRevision,
    accepted_schema_version: SchemaVersion,
    fingerprint_method_version: u8,
    accepted_schema_fingerprint: CommitSchemaFingerprint,
}

impl AcceptedCatalogIdentity {
    #[must_use]
    pub(in crate::db) fn new(
        entity_tag: EntityTag,
        entity_path: impl Into<Rc<str>>,
        store_path: &'static str,
        accepted_schema_revision: AcceptedSchemaRevision,
        accepted_schema_version: SchemaVersion,
        accepted_schema_fingerprint: CommitSchemaFingerprint,
    ) -> Self {
        Self {
            entity_tag,
            entity_path: entity_path.into(),
            store_path,
            accepted_schema_revision,
            accepted_schema_version,
            fingerprint_method_version: accepted_schema_cache_fingerprint_method_version(),
            accepted_schema_fingerprint,
        }
    }

    #[must_use]
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    #[must_use]
    pub(in crate::db) fn entity_path(&self) -> &str {
        self.entity_path.as_ref()
    }

    #[must_use]
    pub(in crate::db) fn entity_path_handle(&self) -> Rc<str> {
        self.entity_path.clone()
    }

    #[must_use]
    pub(in crate::db) const fn store_path(&self) -> &'static str {
        self.store_path
    }

    #[must_use]
    pub(in crate::db) const fn accepted_schema_revision(&self) -> AcceptedSchemaRevision {
        self.accepted_schema_revision
    }

    #[must_use]
    pub(in crate::db) const fn accepted_schema_version(&self) -> SchemaVersion {
        self.accepted_schema_version
    }

    #[must_use]
    pub(in crate::db) const fn fingerprint_method_version(&self) -> u8 {
        self.fingerprint_method_version
    }

    #[must_use]
    pub(in crate::db) const fn accepted_schema_fingerprint(&self) -> CommitSchemaFingerprint {
        self.accepted_schema_fingerprint
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedCatalogSnapshotSelection {
    identity: AcceptedCatalogIdentity,
    value_catalog: AcceptedValueCatalogHandle,
    raw_snapshot: Rc<[u8]>,
}

impl AcceptedCatalogSnapshotSelection {
    #[must_use]
    const fn new(
        identity: AcceptedCatalogIdentity,
        value_catalog: AcceptedValueCatalogHandle,
        raw_snapshot: Rc<[u8]>,
    ) -> Self {
        Self {
            identity,
            value_catalog,
            raw_snapshot,
        }
    }

    #[must_use]
    pub(in crate::db) fn identity(&self) -> AcceptedCatalogIdentity {
        self.identity.clone()
    }

    #[must_use]
    pub(in crate::db) const fn value_catalog_handle(&self) -> &AcceptedValueCatalogHandle {
        &self.value_catalog
    }

    /// Select one entity snapshot and catalog directly from a verified schema
    /// candidate while recovery is still applying its accepted root.
    pub(in crate::db) fn from_candidate(
        candidate: &CandidateSchemaRevision,
        entity_tag: EntityTag,
        entity_path: &str,
        store_path: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        if candidate.store_path() != store_path {
            return Err(InternalError::store_corruption());
        }
        let Some(snapshot) = candidate.bundle().entity_snapshots().get(&entity_tag) else {
            return Ok(None);
        };
        if snapshot.entity_path() != entity_path {
            return Err(InternalError::store_corruption());
        }

        let raw_snapshot = RawSchemaSnapshot::from_persisted_snapshot(snapshot)?;
        let fingerprint = raw_snapshot.accepted_schema_fingerprint()?;
        let identity = AcceptedCatalogIdentity::new(
            entity_tag,
            entity_path,
            store_path,
            candidate.revision(),
            snapshot.version(),
            fingerprint,
        );

        Ok(Some(Self::new(
            identity,
            AcceptedValueCatalogHandle::new(
                candidate.bundle().enum_catalog().clone(),
                candidate.bundle().composite_catalog().clone(),
                AcceptedStoreCatalogScope::new(),
                candidate.revision(),
                candidate.root().fingerprint(),
            ),
            Rc::from(raw_snapshot.into_bytes()),
        )))
    }

    pub(in crate::db) fn decode_verified(&self) -> Result<AcceptedSchemaSnapshot, InternalError> {
        let snapshot = decode_persisted_schema_snapshot(self.raw_snapshot.as_ref())?;
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

///
/// PendingRelationActivationDeleteBarrier
///
/// Accepted activation identity that blocks target deletion until a candidate
/// reverse-relation generation is proven and promoted.
///

pub(in crate::db) struct PendingRelationActivationDeleteBarrier {
    accepted_schema_fingerprint: CommitSchemaFingerprint,
    source_entity_tag: EntityTag,
    constraint_id: ConstraintId,
}

impl PendingRelationActivationDeleteBarrier {
    #[must_use]
    pub(in crate::db) const fn accepted_schema_fingerprint(&self) -> CommitSchemaFingerprint {
        self.accepted_schema_fingerprint
    }

    #[must_use]
    pub(in crate::db) const fn source_entity_tag(&self) -> EntityTag {
        self.source_entity_tag
    }

    /// Return the stable accepted constraint identity.
    #[must_use]
    pub(in crate::db) const fn constraint_id(&self) -> ConstraintId {
        self.constraint_id
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
    cardinality_header_cache: RefCell<Option<(Vec<u8>, CardinalityGenerationHeader)>>,
    accepted_catalog_scope: OnceCell<AcceptedStoreCatalogScope>,
}

struct AcceptedSchemaBundleCache {
    selection: AcceptedSchemaRootSelection,
    bundle: AcceptedSchemaRevisionBundle,
    cardinality_domain: Rc<CardinalityAcceptedDomain>,
    value_catalog: AcceptedValueCatalogHandle,
    entity_selections: RefCell<StdBTreeMap<EntityTag, AcceptedCatalogSnapshotSelection>>,
}

enum SchemaStoreBackend {
    Heap(StdBTreeMap<RawSchemaKey, RawSchemaSnapshot>),
    Journaled {
        canonical:
            StableBTreeMap<RawSchemaKey, RawSchemaSnapshot, VirtualMemory<DefaultMemoryImpl>>,
        live: StdBTreeMap<RawSchemaKey, RawSchemaSnapshot>,
        tombstones: BTreeSet<RawSchemaKey>,
        positions: PositionedOverlayMetadata<RawSchemaKey>,
    },
}

/// Control-flow result for schema-store traversal visitors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SchemaStoreVisit {
    Continue,
    #[cfg(test)]
    Stop,
}

impl SchemaStoreVisit {
    const fn should_stop(self) -> bool {
        match self {
            Self::Continue => false,
            #[cfg(test)]
            Self::Stop => true,
        }
    }
}

#[derive(Clone, Copy)]
enum IdentityStateStorageView {
    Effective,
    Canonical,
}

/// Exact schema/control keys whose live values belong to one journal batch.
#[derive(Clone)]
pub(in crate::db) struct PreparedSchemaPositionPublication {
    keys: Vec<RawSchemaKey>,
    position: JournalOverlayPosition,
}

/// Preflighted exact schema/control retirement for one complete journal batch.
pub(in crate::db) struct PreparedSchemaPositionRetirement {
    entries: Vec<(RawSchemaKey, PositionedOverlayRetirement)>,
}

/// Preflighted count-record changes for one isolated cardinality build page.
pub(in crate::db) struct PreparedCardinalityCountWrites {
    slot: CardinalityCountSlot,
    generation: CardinalityGenerationId,
    entries: Vec<(RawSchemaKey, RawSchemaSnapshot)>,
    new_count_keys: u64,
}

impl PreparedCardinalityCountWrites {
    #[must_use]
    pub(in crate::db) const fn new_count_keys(&self) -> u64 {
        self.new_count_keys
    }
}

/// Fully preflighted atomic count-and-cursor publication for one build page.
pub(in crate::db) struct PreparedCardinalityBuildPage {
    count_entries: Vec<(RawSchemaKey, RawSchemaSnapshot)>,
    cursor: (RawSchemaKey, RawSchemaSnapshot),
}

/// Fully preflighted exact count and Ready-watermark transition for one fold.
pub(in crate::db) struct PreparedCardinalityMaintenance {
    count_entries: Vec<(RawSchemaKey, Option<RawSchemaSnapshot>)>,
    header: (RawSchemaKey, RawSchemaSnapshot),
}

#[derive(Clone, Copy)]
enum IdentityStateWriteTarget {
    Durable,
    Materialized,
    Canonical,
}

impl SchemaStore {
    /// Initialize a volatile heap-backed schema store.
    #[must_use]
    pub const fn init_heap() -> Self {
        Self {
            backend: SchemaStoreBackend::Heap(StdBTreeMap::new()),
            accepted_bundle_cache: RefCell::new(None),
            cardinality_header_cache: RefCell::new(None),
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
                positions: PositionedOverlayMetadata::new(),
            },
            accepted_bundle_cache: RefCell::new(None),
            cardinality_header_cache: RefCell::new(None),
            accepted_catalog_scope: OnceCell::new(),
        }
    }

    /// Load the sole current durable cardinality-generation header.
    pub(in crate::db) fn cardinality_generation_header(
        &self,
    ) -> Result<Option<CardinalityGenerationHeader>, InternalError> {
        let key = RawSchemaKey::from_cardinality_generation_header();
        let raw = self.get_canonical_raw_value(&key)?;
        if let Some(raw) = raw.as_ref() {
            self.decode_cardinality_header_cached(raw).map(Some)
        } else {
            self.cardinality_header_cache
                .try_borrow_mut()
                .map_err(|_| InternalError::store_invariant())?
                .take();
            Ok(None)
        }
    }

    /// Load the sole bounded cardinality build cursor.
    pub(in crate::db) fn cardinality_build_cursor(
        &self,
    ) -> Result<Option<CardinalityBuildCursor>, InternalError> {
        let key = RawSchemaKey::from_cardinality_build_cursor();
        self.get_canonical_raw_value(&key)?
            .map(|raw| CardinalityBuildCursor::decode(raw.as_bytes()))
            .transpose()
    }

    /// Load the generation header and build cursor through one bounded control range.
    pub(in crate::db) fn cardinality_generation_control(
        &self,
    ) -> Result<
        (
            Option<CardinalityGenerationHeader>,
            Option<CardinalityBuildCursor>,
        ),
        InternalError,
    > {
        let SchemaStoreBackend::Journaled { canonical, .. } = &self.backend else {
            return Err(InternalError::store_invariant());
        };
        let header_key = RawSchemaKey::from_cardinality_generation_header();
        let cursor_key = RawSchemaKey::from_cardinality_build_cursor();
        let mut header = None;
        let mut cursor = None;
        for entry in canonical.range(header_key..=cursor_key) {
            if *entry.key() == header_key {
                header = Some(self.decode_cardinality_header_cached(&entry.value())?);
            } else if *entry.key() == cursor_key {
                cursor = Some(CardinalityBuildCursor::decode(entry.value().as_bytes())?);
            } else {
                return Err(InternalError::store_corruption());
            }
        }
        Ok((header, cursor))
    }

    /// Load only lifecycle identity needed by advisory cardinality consumers.
    ///
    /// Cursor payload progress is deliberately not decoded or retained here:
    /// Building progress does not change whether exact evidence is available.
    pub(in crate::db) fn cardinality_generation_lifecycle_control(
        &self,
    ) -> Result<(Option<CardinalityGenerationHeader>, bool), InternalError> {
        let SchemaStoreBackend::Journaled { canonical, .. } = &self.backend else {
            return Err(InternalError::store_invariant());
        };
        let header_key = RawSchemaKey::from_cardinality_generation_header();
        let cursor_key = RawSchemaKey::from_cardinality_build_cursor();
        let mut header = None;
        let mut cursor_present = false;
        for entry in canonical.range(header_key..=cursor_key) {
            if *entry.key() == header_key {
                header = Some(self.decode_cardinality_header_cached(&entry.value())?);
            } else if *entry.key() == cursor_key {
                cursor_present = true;
            } else {
                return Err(InternalError::store_corruption());
            }
        }

        Ok((header, cursor_present))
    }

    fn decode_cardinality_header_cached(
        &self,
        raw: &RawSchemaSnapshot,
    ) -> Result<CardinalityGenerationHeader, InternalError> {
        if let Some((_, header)) = self
            .cardinality_header_cache
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?
            .as_ref()
            .filter(|(bytes, _)| bytes.as_slice() == raw.as_bytes())
        {
            return Ok(*header);
        }
        let header = CardinalityGenerationHeader::decode(raw.as_bytes())?;
        *self
            .cardinality_header_cache
            .try_borrow_mut()
            .map_err(|_| InternalError::store_invariant())? =
            Some((raw.as_bytes().to_vec(), header));
        Ok(header)
    }

    /// Prove that no current cardinality authority or orphaned slot data exists.
    pub(in crate::db) fn cardinality_storage_is_pristine(&self) -> Result<bool, InternalError> {
        if self.cardinality_generation_header()?.is_some()
            || self.cardinality_build_cursor()?.is_some()
        {
            return Ok(false);
        }
        Ok(
            self.cardinality_count_slot_is_empty(CardinalityCountSlot::A)?
                && self.cardinality_count_slot_is_empty(CardinalityCountSlot::B)?,
        )
    }

    /// Persist one already-validated cardinality header into canonical control storage.
    pub(in crate::db) fn write_cardinality_generation_header(
        &mut self,
        header: CardinalityGenerationHeader,
    ) -> Result<(), InternalError> {
        self.insert_canonical_raw_value(
            RawSchemaKey::from_cardinality_generation_header(),
            header.encode(),
        )
    }

    /// Replace stale cardinality evidence with a fresh isolated Building generation.
    ///
    /// Every fallible read, generation increment, and backend check happens
    /// before the header switch. Removing the obsolete cursor afterward is a
    /// mechanical stable-map operation in the same replicated message.
    pub(in crate::db) fn restart_cardinality_generation(
        &mut self,
        current: CardinalityGenerationHeader,
        source: CardinalitySourceIdentity,
    ) -> Result<CardinalityGenerationHeader, InternalError> {
        if self.cardinality_generation_header()? != Some(current) {
            return Err(InternalError::store_corruption());
        }
        if current.validate_source(source).is_ok() {
            return Err(InternalError::store_invariant());
        }
        if let Some(cursor) = self.cardinality_build_cursor()? {
            cursor.validate_header(current)?;
        }
        let next = CardinalityGenerationHeader::new(
            current.generation().checked_next()?,
            CardinalityGenerationState::Building,
            current.slot().alternate(),
            source,
        );
        let encoded = RawSchemaSnapshot::from_encoded_control_record(next.encode());
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant());
        };
        canonical.insert(RawSchemaKey::from_cardinality_generation_header(), encoded);
        canonical.remove(&RawSchemaKey::from_cardinality_build_cursor());
        Ok(next)
    }

    /// Publish exact zero for a physically empty canonical row/index domain.
    pub(in crate::db) fn publish_empty_cardinality_generation(
        &mut self,
        candidate: &EmptyCardinalityReadyCandidate,
    ) -> Result<CardinalityGenerationHeader, InternalError> {
        if !self.cardinality_storage_is_pristine()? {
            return Err(InternalError::store_corruption());
        }
        let ready = CardinalityGenerationHeader::new(
            CardinalityGenerationId::INITIAL,
            CardinalityGenerationState::Ready,
            CardinalityCountSlot::A,
            candidate.source(),
        );
        let encoded = RawSchemaSnapshot::from_encoded_control_record(ready.encode());
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant());
        };
        canonical.insert(RawSchemaKey::from_cardinality_generation_header(), encoded);
        Ok(ready)
    }

    /// Atomically make one completely exhausted candidate planner-visible.
    pub(in crate::db) fn publish_ready_cardinality_generation(
        &mut self,
        candidate: &CardinalityReadyCandidate,
        source: CardinalitySourceIdentity,
    ) -> Result<CardinalityGenerationHeader, InternalError> {
        let building = candidate.header();
        candidate.cursor().validate_header(building)?;
        if building.state() != CardinalityGenerationState::Building
            || building.validate_source(source).is_err()
            || self.cardinality_generation_header()? != Some(building)
            || self.cardinality_build_cursor()?.as_ref() != Some(candidate.cursor())
        {
            return Err(InternalError::store_corruption());
        }
        let ready = CardinalityGenerationHeader::new(
            building.generation(),
            CardinalityGenerationState::Ready,
            building.slot(),
            building.source(),
        );
        let encoded = RawSchemaSnapshot::from_encoded_control_record(ready.encode());
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant());
        };
        canonical.insert(RawSchemaKey::from_cardinality_generation_header(), encoded);
        canonical.remove(&RawSchemaKey::from_cardinality_build_cursor());
        Ok(ready)
    }

    /// Clear at most `limit` records from one inactive count slot.
    ///
    /// When the slot becomes empty, the initial Rows cursor is installed in
    /// the same mutation boundary so a resumed builder never confuses clearing
    /// with scanning.
    pub(in crate::db) fn clear_cardinality_count_slot_page(
        &mut self,
        header: CardinalityGenerationHeader,
        initial_cursor: &CardinalityBuildCursor,
        limit: usize,
    ) -> Result<bool, InternalError> {
        if limit == 0 {
            return Err(InternalError::store_invariant());
        }
        initial_cursor.validate_header(header)?;
        let encoded_cursor = initial_cursor.encode()?;
        if self.cardinality_generation_header()? != Some(header)
            || self.cardinality_build_cursor()?.is_some()
        {
            return Err(InternalError::store_corruption());
        }
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant());
        };
        let bounds = RawSchemaKey::cardinality_count_range_bounds(header.slot());
        let collect_limit = limit
            .checked_add(1)
            .ok_or_else(InternalError::store_unsupported)?;
        let mut keys = Vec::new();
        keys.try_reserve_exact(collect_limit)
            .map_err(|_| InternalError::store_unsupported())?;
        for entry in canonical.range(bounds).take(collect_limit) {
            keys.push(*entry.key());
        }
        let has_more = keys.len() > limit;
        for key in keys.into_iter().take(limit) {
            canonical.remove(&key);
        }
        if !has_more {
            canonical.insert(
                RawSchemaKey::from_cardinality_build_cursor(),
                RawSchemaSnapshot::from_encoded_control_record(encoded_cursor),
            );
        }
        Ok(has_more)
    }

    /// Preflight coalesced positive count increments for one isolated page.
    pub(in crate::db) fn prepare_cardinality_count_increments(
        &self,
        slot: CardinalityCountSlot,
        generation: CardinalityGenerationId,
        increments: &[(CardinalityCountDigest, u64)],
    ) -> Result<PreparedCardinalityCountWrites, InternalError> {
        if !matches!(self.backend, SchemaStoreBackend::Journaled { .. }) {
            return Err(InternalError::store_invariant());
        }
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(increments.len())
            .map_err(|_| InternalError::store_unsupported())?;
        let mut physical_keys = StdBTreeMap::new();
        let mut new_count_keys = 0_u64;
        for (digest, increment) in increments {
            if *increment == 0 {
                return Err(InternalError::store_invariant());
            }
            let key = RawSchemaKey::from_cardinality_count(slot, *digest);
            if let Some(previous_digest) = physical_keys.insert(key, *digest) {
                return Err(if previous_digest == *digest {
                    InternalError::store_invariant()
                } else {
                    InternalError::store_corruption()
                });
            }
            let current = self.get_canonical_raw_value(&key)?;
            let count = if let Some(raw) = current {
                let record = CardinalityCountRecord::decode(raw.as_bytes())?;
                record
                    .validate_identity(generation, *digest)
                    .map_err(|_| InternalError::store_corruption())?
                    .checked_add(*increment)
                    .ok_or_else(InternalError::store_unsupported)?
            } else {
                new_count_keys = new_count_keys
                    .checked_add(1)
                    .ok_or_else(InternalError::store_unsupported)?;
                *increment
            };
            let record = CardinalityCountRecord::new(generation, *digest, count)?;
            entries.push((
                key,
                RawSchemaSnapshot::from_encoded_control_record(record.encode().to_vec()),
            ));
        }
        Ok(PreparedCardinalityCountWrites {
            slot,
            generation,
            entries,
            new_count_keys,
        })
    }

    /// Bind preflighted count writes to the exact current cursor transition.
    pub(in crate::db) fn prepare_cardinality_build_page(
        &self,
        header: CardinalityGenerationHeader,
        current_cursor: &CardinalityBuildCursor,
        counts: PreparedCardinalityCountWrites,
        next_cursor: &CardinalityBuildCursor,
    ) -> Result<PreparedCardinalityBuildPage, InternalError> {
        current_cursor.validate_header(header)?;
        next_cursor.validate_header(header)?;
        if counts.slot != header.slot() || counts.generation != header.generation() {
            return Err(InternalError::store_invariant());
        }
        if self.cardinality_generation_header()? != Some(header)
            || self.cardinality_build_cursor()?.as_ref() != Some(current_cursor)
        {
            return Err(InternalError::store_corruption());
        }
        let encoded_cursor = next_cursor.encode()?;
        Ok(PreparedCardinalityBuildPage {
            count_entries: counts.entries,
            cursor: (
                RawSchemaKey::from_cardinality_build_cursor(),
                RawSchemaSnapshot::from_encoded_control_record(encoded_cursor),
            ),
        })
    }

    /// Mechanically publish one fully preflighted count-and-cursor page.
    pub(in crate::db) fn apply_prepared_cardinality_build_page(
        &mut self,
        prepared: PreparedCardinalityBuildPage,
    ) -> Result<(), InternalError> {
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant());
        };
        for (key, value) in prepared.count_entries {
            canonical.insert(key, value);
        }
        canonical.insert(prepared.cursor.0, prepared.cursor.1);
        Ok(())
    }

    /// Load one exact nonzero count from an isolated generation.
    pub(in crate::db) fn cardinality_count(
        &self,
        slot: CardinalityCountSlot,
        generation: CardinalityGenerationId,
        digest: CardinalityCountDigest,
    ) -> Result<Option<u64>, InternalError> {
        let key = RawSchemaKey::from_cardinality_count(slot, digest);
        self.get_canonical_raw_value(&key)?
            .map(|raw| {
                CardinalityCountRecord::decode(raw.as_bytes())?
                    .validate_identity(generation, digest)
                    .map_err(|_| InternalError::store_corruption())
            })
            .transpose()
    }

    /// Preflight exact coalesced count changes and the next complete fold watermark.
    pub(in crate::db) fn prepare_cardinality_maintenance(
        &self,
        current: CardinalityGenerationHeader,
        current_source: CardinalitySourceIdentity,
        next_source: CardinalitySourceIdentity,
        changes: &[(CardinalityCountDigest, i64)],
    ) -> Result<PreparedCardinalityMaintenance, InternalError> {
        if current.state() != CardinalityGenerationState::Ready
            || current.validate_source(current_source).is_err()
            || self.cardinality_generation_header()? != Some(current)
            || self.cardinality_build_cursor()?.is_some()
        {
            return Err(InternalError::store_corruption());
        }
        let next = CardinalityGenerationHeader::new(
            current.generation(),
            CardinalityGenerationState::Ready,
            current.slot(),
            next_source,
        );
        let mut count_entries = Vec::new();
        count_entries
            .try_reserve_exact(changes.len())
            .map_err(|_| InternalError::store_unsupported())?;
        let mut physical_keys = StdBTreeMap::new();
        for (digest, delta) in changes {
            if *delta == 0 {
                return Err(InternalError::store_invariant());
            }
            let key = RawSchemaKey::from_cardinality_count(current.slot(), *digest);
            if let Some(previous_digest) = physical_keys.insert(key, *digest) {
                return Err(if previous_digest == *digest {
                    InternalError::store_invariant()
                } else {
                    InternalError::store_corruption()
                });
            }
            let base = self
                .cardinality_count(current.slot(), current.generation(), *digest)?
                .unwrap_or(0);
            let count = if *delta > 0 {
                base.checked_add(
                    u64::try_from(*delta).map_err(|_| InternalError::store_invariant())?,
                )
            } else {
                base.checked_sub(delta.unsigned_abs())
            }
            .ok_or_else(InternalError::store_corruption)?;
            let value = if count == 0 {
                None
            } else {
                Some(RawSchemaSnapshot::from_encoded_control_record(
                    CardinalityCountRecord::new(current.generation(), *digest, count)?
                        .encode()
                        .to_vec(),
                ))
            };
            count_entries.push((key, value));
        }
        Ok(PreparedCardinalityMaintenance {
            count_entries,
            header: (
                RawSchemaKey::from_cardinality_generation_header(),
                RawSchemaSnapshot::from_encoded_control_record(next.encode()),
            ),
        })
    }

    /// Mechanically publish one completely preflighted count/watermark transition.
    pub(in crate::db) fn apply_prepared_cardinality_maintenance(
        &mut self,
        prepared: PreparedCardinalityMaintenance,
    ) -> Result<(), InternalError> {
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant());
        };
        for (key, value) in prepared.count_entries {
            if let Some(value) = value {
                canonical.insert(key, value);
            } else {
                canonical.remove(&key);
            }
        }
        canonical.insert(prepared.header.0, prepared.header.1);
        Ok(())
    }

    pub(in crate::db) fn cardinality_count_slot_is_empty(
        &self,
        slot: CardinalityCountSlot,
    ) -> Result<bool, InternalError> {
        let SchemaStoreBackend::Journaled { canonical, .. } = &self.backend else {
            return Err(InternalError::store_invariant());
        };
        Ok(canonical
            .range(RawSchemaKey::cardinality_count_range_bounds(slot))
            .next()
            .is_none())
    }

    /// Prove that recovered journal metadata can fold into canonical storage.
    pub(in crate::db) fn preflight_fold_recovered_journal(&self) -> Result<(), InternalError> {
        match self.backend {
            SchemaStoreBackend::Journaled { .. } => Ok(()),
            SchemaStoreBackend::Heap(_) => Err(InternalError::store_invariant()),
        }
    }

    fn prepare_identity_state_transition(
        &self,
        incarnation: DatabaseIncarnationId,
        candidate: &CandidateSchemaRevision,
        view: IdentityStateStorageView,
    ) -> Result<IdentityStateTransition, InternalError> {
        let current = match view {
            IdentityStateStorageView::Effective => self
                .current_accepted_schema_bundle_ref()?
                .as_ref()
                .map(|bundle| (*bundle).clone()),
            IdentityStateStorageView::Canonical => {
                self.current_canonical_accepted_schema_bundle()?
            }
        };
        let inventory = self.identity_state_inventory(view)?;
        prepare_identity_state_transition(
            incarnation,
            current.as_ref(),
            candidate.bundle(),
            inventory,
        )
    }

    fn validate_identity_state_closure(
        &self,
        bundle: &AcceptedSchemaRevisionBundle,
    ) -> Result<(), InternalError> {
        let inventory = self.identity_state_inventory(IdentityStateStorageView::Effective)?;
        validate_identity_state_closure(bundle, &inventory)
    }

    /// Read one accepted active Identity owner into statement-local allocation state.
    pub(in crate::db) fn identity_statement_cursor(
        &self,
        database_incarnation_id: DatabaseIncarnationId,
        entity_tag: EntityTag,
        field_id: FieldId,
        accepted_kind: &AcceptedFieldKind,
    ) -> Result<IdentityStatementCursor, InternalError> {
        let key = RawSchemaKey::from_identity_state(entity_tag, field_id);
        let raw = self
            .get_raw_snapshot(&key)
            .ok_or_else(InternalError::identity_state_corruption)?;
        let state = decode_identity_state(raw.as_bytes())?;
        let owner = state.owner();
        if owner.database_incarnation_id() != database_incarnation_id
            || owner.entity_tag() != entity_tag
            || owner.field_id() != field_id
            || state.accepted_kind() != accepted_kind
            || state.lifecycle() != IdentityStateLifecycle::Active
        {
            return Err(InternalError::identity_state_corruption());
        }
        IdentityStatementCursor::from_active_state(&state)
    }

    /// Read one quiescent materialized high-water for bounded row integrity.
    pub(in crate::db) fn identity_high_water_for_integrity(
        &self,
        database_incarnation_id: DatabaseIncarnationId,
        entity_tag: EntityTag,
        field_id: FieldId,
        accepted_kind: &AcceptedFieldKind,
    ) -> Result<u128, InternalError> {
        let key = RawSchemaKey::from_identity_state(entity_tag, field_id);
        let raw = self
            .get_raw_snapshot(&key)
            .ok_or_else(InternalError::identity_state_corruption)?;
        let state = decode_identity_state(raw.as_bytes())?;
        let owner = state.owner();
        if owner.database_incarnation_id() != database_incarnation_id
            || owner.entity_tag() != entity_tag
            || owner.field_id() != field_id
            || state.accepted_kind() != accepted_kind
            || state.lifecycle() != IdentityStateLifecycle::Active
        {
            return Err(InternalError::identity_state_corruption());
        }
        Ok(state.materialized_high_water())
    }

    /// Revalidate one tentative range against the quiescent effective state.
    pub(in crate::db) fn preflight_identity_range_advance(
        &self,
        range: IdentityRangeAdvance,
    ) -> Result<(), InternalError> {
        let state =
            self.identity_state_for_owner(range.owner(), IdentityStateStorageView::Effective)?;
        state.preflight_range_advance(range)
    }

    /// Materialize one marker-owned range in the effective live projection.
    pub(in crate::db) fn apply_identity_range_advance(
        &mut self,
        range: IdentityRangeAdvance,
        advance_id: IdentityAdvanceId,
    ) -> Result<(), InternalError> {
        self.apply_identity_range_advance_to(
            range,
            advance_id,
            IdentityStateStorageView::Effective,
            IdentityStateWriteTarget::Materialized,
        )
    }

    /// Fold one marker-owned range into canonical journaled state.
    pub(in crate::db) fn fold_identity_range_advance(
        &mut self,
        range: IdentityRangeAdvance,
        advance_id: IdentityAdvanceId,
    ) -> Result<(), InternalError> {
        self.apply_identity_range_advance_to(
            range,
            advance_id,
            IdentityStateStorageView::Canonical,
            IdentityStateWriteTarget::Canonical,
        )
    }

    /// Preflight one canonical Identity range fold without changing storage.
    pub(in crate::db) fn preflight_fold_identity_range_advance(
        &self,
        range: IdentityRangeAdvance,
        advance_id: IdentityAdvanceId,
    ) -> Result<(), InternalError> {
        if !matches!(self.backend, SchemaStoreBackend::Journaled { .. }) {
            return Err(InternalError::store_invariant());
        }
        let state =
            self.identity_state_for_owner(range.owner(), IdentityStateStorageView::Canonical)?;
        let advanced = state.apply_range_advance(range, advance_id)?;
        let _encoded = encode_identity_state(&advanced)?;
        Ok(())
    }

    /// Verify one exact range identity against effective state.
    pub(in crate::db) fn verify_identity_range_advance(
        &self,
        range: IdentityRangeAdvance,
        advance_id: IdentityAdvanceId,
    ) -> Result<(), InternalError> {
        let state =
            self.identity_state_for_owner(range.owner(), IdentityStateStorageView::Effective)?;
        if state.materialized_high_water() != range.new_high_water()
            || state.last_applied_advance() != Some(advance_id)
        {
            return Err(InternalError::recovery_effect_verification_failed());
        }
        Ok(())
    }

    /// Resolve committed versus materialized range state without changing it.
    pub(in crate::db) fn identity_range_commit_state(
        &self,
        range: IdentityRangeAdvance,
        advance_id: IdentityAdvanceId,
        canonical: bool,
    ) -> Result<IdentityRangeCommitState, InternalError> {
        let view = if canonical {
            IdentityStateStorageView::Canonical
        } else {
            IdentityStateStorageView::Effective
        };
        self.identity_state_for_owner(range.owner(), view)?
            .range_commit_state(range, advance_id)
    }

    /// Enumerate and validate the complete current-form active/retired state
    /// inventory for bounded database-wide integrity inspection.
    pub(in crate::db) fn identity_state_inventory_for_integrity(
        &self,
        incarnation: DatabaseIncarnationId,
    ) -> Result<Vec<IdentityState>, InternalError> {
        let has_accepted_bundle = self.current_accepted_schema_bundle_ref()?.is_some();
        let inventory = self.identity_state_inventory(IdentityStateStorageView::Effective)?;
        if !has_accepted_bundle && !inventory.is_empty() {
            return Err(InternalError::identity_state_corruption());
        }
        if inventory
            .values()
            .any(|state| state.owner().database_incarnation_id() != incarnation)
        {
            return Err(InternalError::identity_state_corruption());
        }
        Ok(inventory.into_values().collect())
    }

    fn identity_state_for_owner(
        &self,
        owner: crate::db::schema::identity_state::IdentityStateOwner,
        view: IdentityStateStorageView,
    ) -> Result<IdentityState, InternalError> {
        let key = RawSchemaKey::from_identity_state(owner.entity_tag(), owner.field_id());
        let raw = match view {
            IdentityStateStorageView::Effective => self.get_raw_snapshot(&key),
            IdentityStateStorageView::Canonical => self.get_canonical_raw_value(&key)?,
        }
        .ok_or_else(InternalError::identity_state_corruption)?;
        let state = decode_identity_state(raw.as_bytes())?;
        if state.owner() != owner {
            return Err(InternalError::identity_state_corruption());
        }
        Ok(state)
    }

    fn apply_identity_range_advance_to(
        &mut self,
        range: IdentityRangeAdvance,
        advance_id: IdentityAdvanceId,
        view: IdentityStateStorageView,
        target: IdentityStateWriteTarget,
    ) -> Result<(), InternalError> {
        let state = self.identity_state_for_owner(range.owner(), view)?;
        let advanced = state.apply_range_advance(range, advance_id)?;
        let key = RawSchemaKey::from_identity_state(
            advanced.owner().entity_tag(),
            advanced.owner().field_id(),
        );
        let bytes = encode_identity_state(&advanced)?;
        match target {
            IdentityStateWriteTarget::Materialized => {
                self.insert_raw_snapshot(
                    key,
                    RawSchemaSnapshot::from_encoded_control_record(bytes),
                );
            }
            IdentityStateWriteTarget::Canonical => {
                self.insert_canonical_raw_value(key, bytes)?;
            }
            IdentityStateWriteTarget::Durable => {
                return Err(InternalError::store_invariant());
            }
        }
        Ok(())
    }

    fn identity_state_inventory(
        &self,
        view: IdentityStateStorageView,
    ) -> Result<IdentityStateInventory, InternalError> {
        let bounds = RawSchemaKey::all_identity_state_range_bounds();
        let mut inventory = StdBTreeMap::new();
        let mut collect = |key: &RawSchemaKey,
                           raw: &RawSchemaSnapshot|
         -> Result<SchemaStoreVisit, InternalError> {
            if inventory.len() >= MAX_IDENTITY_STATE_RECORDS_PER_DATABASE {
                return Err(InternalError::identity_state_corruption());
            }
            let state = decode_identity_state(raw.as_bytes())?;
            let state_key = (key.entity_tag(), FieldId::new(key.version()));
            if !key.is_identity_state()
                || state.owner().entity_tag() != state_key.0
                || state.owner().field_id() != state_key.1
                || inventory.insert(state_key, state).is_some()
            {
                return Err(InternalError::identity_state_corruption());
            }
            Ok(SchemaStoreVisit::Continue)
        };

        match (&self.backend, view) {
            (SchemaStoreBackend::Heap(map), IdentityStateStorageView::Effective) => {
                for (key, raw) in map.range((bounds.0, bounds.1)) {
                    collect(key, raw)?;
                }
            }
            (
                SchemaStoreBackend::Journaled {
                    canonical,
                    live,
                    tombstones,
                    ..
                },
                IdentityStateStorageView::Effective,
            ) => Self::visit_journaled_raw_snapshot_range(
                canonical,
                live,
                tombstones,
                bounds,
                Direction::Asc,
                &mut collect,
            )?,
            (
                SchemaStoreBackend::Journaled { canonical, .. },
                IdentityStateStorageView::Canonical,
            ) => {
                for entry in canonical.range((bounds.0, bounds.1)) {
                    collect(entry.key(), &entry.value())?;
                }
            }
            (SchemaStoreBackend::Heap(_), IdentityStateStorageView::Canonical) => {
                return Err(InternalError::store_invariant());
            }
        }

        Ok(inventory)
    }

    fn apply_identity_state_transition(
        &mut self,
        transition: IdentityStateTransition,
        target: IdentityStateWriteTarget,
    ) -> Result<(), InternalError> {
        for state in transition.into_updates() {
            let key = RawSchemaKey::from_identity_state(
                state.owner().entity_tag(),
                state.owner().field_id(),
            );
            let bytes = encode_identity_state(&state)?;
            match target {
                IdentityStateWriteTarget::Durable => {
                    self.insert_durable_raw_value(key, bytes);
                }
                IdentityStateWriteTarget::Materialized => {
                    self.insert_raw_snapshot(
                        key,
                        RawSchemaSnapshot::from_encoded_control_record(bytes),
                    );
                }
                IdentityStateWriteTarget::Canonical => {
                    self.insert_canonical_raw_value(key, bytes)?;
                }
            }
        }
        Ok(())
    }

    pub(in crate::db) fn current_canonical_accepted_schema_bundle(
        &self,
    ) -> Result<Option<AcceptedSchemaRevisionBundle>, InternalError> {
        self.current_canonical_accepted_schema_authority()
            .map(|authority| authority.map(|(_, bundle)| bundle))
    }

    /// Load one canonical accepted root and its verified immutable bundle.
    pub(in crate::db) fn current_canonical_accepted_schema_authority(
        &self,
    ) -> Result<Option<(AcceptedSchemaRootSelection, AcceptedSchemaRevisionBundle)>, InternalError>
    {
        let Some(selection) = self.current_canonical_accepted_schema_root()? else {
            return Ok(None);
        };
        let bundle_key = RawSchemaKey::from_accepted_bundle(selection.root().bundle_key());
        let raw = self
            .get_canonical_raw_value(&bundle_key)?
            .ok_or_else(InternalError::store_corruption)?;
        let bundle =
            decode_verified_accepted_schema_revision_bundle(selection.root(), raw.as_bytes())?;
        Ok(Some((selection, bundle)))
    }

    /// Return the accepted root selected only from canonical predecessor slots.
    pub(in crate::db) fn current_canonical_accepted_schema_root(
        &self,
    ) -> Result<Option<AcceptedSchemaRootSelection>, InternalError> {
        let first = self.canonical_root_slot_bytes(0)?;
        let second = self.canonical_root_slot_bytes(1)?;
        select_current_accepted_schema_root([first.as_deref(), second.as_deref()])
    }

    /// Select effective and canonical roots from one canonical slot read.
    pub(in crate::db) fn current_effective_and_canonical_accepted_schema_roots(
        &self,
    ) -> Result<
        (
            Option<AcceptedSchemaRootSelection>,
            Option<AcceptedSchemaRootSelection>,
        ),
        InternalError,
    > {
        let SchemaStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
        } = &self.backend
        else {
            return Err(InternalError::store_invariant());
        };
        let first_key = RawSchemaKey::from_accepted_root_slot(0)?;
        let second_key = RawSchemaKey::from_accepted_root_slot(1)?;
        let mut canonical_first = None;
        let mut canonical_second = None;
        for entry in canonical.range(first_key..=second_key) {
            if *entry.key() == first_key {
                canonical_first = Some(entry.value().clone());
            } else if *entry.key() == second_key {
                canonical_second = Some(entry.value().clone());
            } else {
                return Err(InternalError::store_corruption());
            }
        }
        let effective_first = if tombstones.contains(&first_key) {
            None
        } else {
            live.get(&first_key)
                .cloned()
                .or_else(|| canonical_first.clone())
        };
        let effective_second = if tombstones.contains(&second_key) {
            None
        } else {
            live.get(&second_key)
                .cloned()
                .or_else(|| canonical_second.clone())
        };
        let effective_first = effective_first.map(RawSchemaSnapshot::into_bytes);
        let effective_second = effective_second.map(RawSchemaSnapshot::into_bytes);
        let canonical_first = canonical_first.map(RawSchemaSnapshot::into_bytes);
        let canonical_second = canonical_second.map(RawSchemaSnapshot::into_bytes);
        Ok((
            select_current_accepted_schema_root([
                effective_first.as_deref(),
                effective_second.as_deref(),
            ])?,
            select_current_accepted_schema_root([
                canonical_first.as_deref(),
                canonical_second.as_deref(),
            ])?,
        ))
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

    /// Load one schema-owned constraint validation job.
    pub(in crate::db) fn constraint_validation_job(
        &self,
        entity: EntityTag,
        constraint_id: ConstraintId,
    ) -> Result<Option<ConstraintValidationJob>, InternalError> {
        let key = RawSchemaKey::from_constraint_validation_job(entity, constraint_id);
        self.get_raw_snapshot(&key)
            .map(|raw| decode_constraint_validation_job(raw.as_bytes()))
            .transpose()
    }

    /// Apply one marker-authorized validation job to the live schema projection.
    pub(in crate::db) fn apply_constraint_validation_job(
        &mut self,
        job: &ConstraintValidationJob,
    ) -> Result<(), InternalError> {
        let key =
            RawSchemaKey::from_constraint_validation_job(job.entity_tag(), job.constraint_id());
        let bytes = encode_constraint_validation_job(job)?;
        let _ =
            self.insert_raw_snapshot(key, RawSchemaSnapshot::from_encoded_control_record(bytes));
        Ok(())
    }

    /// Remove one marker-authorized validation job from the live projection.
    #[expect(
        clippy::unnecessary_wraps,
        reason = "marker apply operations share one fallible callback contract"
    )]
    pub(in crate::db) fn apply_constraint_validation_job_removal(
        &mut self,
        entity: EntityTag,
        constraint_id: ConstraintId,
    ) -> Result<(), InternalError> {
        let key = RawSchemaKey::from_constraint_validation_job(entity, constraint_id);
        match &mut self.backend {
            SchemaStoreBackend::Heap(map) => {
                map.remove(&key);
            }
            SchemaStoreBackend::Journaled {
                live, tombstones, ..
            } => {
                live.remove(&key);
                tombstones.insert(key);
            }
        }
        Ok(())
    }

    /// Fold one committed validation job into the canonical stable base.
    pub(in crate::db) fn fold_constraint_validation_job(
        &mut self,
        job: &ConstraintValidationJob,
    ) -> Result<(), InternalError> {
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant());
        };
        let key =
            RawSchemaKey::from_constraint_validation_job(job.entity_tag(), job.constraint_id());
        let bytes = encode_constraint_validation_job(job)?;
        canonical.insert(key, RawSchemaSnapshot::from_encoded_control_record(bytes));
        Ok(())
    }

    /// Preflight one canonical validation-job fold without changing storage.
    pub(in crate::db) fn preflight_fold_constraint_validation_job(
        &self,
        job: &ConstraintValidationJob,
    ) -> Result<(), InternalError> {
        if !matches!(self.backend, SchemaStoreBackend::Journaled { .. }) {
            return Err(InternalError::store_invariant());
        }
        let _encoded = encode_constraint_validation_job(job)?;
        Ok(())
    }

    /// Fold one committed validation-job removal into the canonical stable base.
    pub(in crate::db) fn fold_constraint_validation_job_removal(
        &mut self,
        entity: EntityTag,
        constraint_id: ConstraintId,
    ) -> Result<(), InternalError> {
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant());
        };
        canonical.remove(&RawSchemaKey::from_constraint_validation_job(
            entity,
            constraint_id,
        ));
        Ok(())
    }

    /// Preflight one canonical validation-job removal without changing storage.
    pub(in crate::db) fn preflight_fold_constraint_validation_job_removal(
        &self,
    ) -> Result<(), InternalError> {
        match self.backend {
            SchemaStoreBackend::Journaled { .. } => Ok(()),
            SchemaStoreBackend::Heap(_) => Err(InternalError::store_invariant()),
        }
    }

    /// Reset the volatile projection for journaled recovery without mutating
    /// the canonical stable schema base.
    pub(in crate::db) fn reset_journaled_live_projection(&mut self) -> Result<(), InternalError> {
        let SchemaStoreBackend::Journaled {
            live,
            tombstones,
            positions,
            ..
        } = &mut self.backend
        else {
            return Err(InternalError::store_invariant());
        };

        live.clear();
        tombstones.clear();
        positions.clear();
        self.accepted_bundle_cache.get_mut().take();

        Ok(())
    }

    /// Preflight every schema/control position represented by one online batch.
    pub(in crate::db) fn prepare_positioned_journal_batch_publication(
        &self,
        incarnation: DatabaseIncarnationId,
        batch: &JournalBatch,
        position: JournalOverlayPosition,
    ) -> Result<PreparedSchemaPositionPublication, InternalError> {
        let SchemaStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(InternalError::store_invariant());
        };
        let keys = self.positioned_journal_batch_keys(
            incarnation,
            batch,
            IdentityStateStorageView::Effective,
        )?;
        for key in &keys {
            positions.preflight_publish(key, position)?;
        }
        Ok(PreparedSchemaPositionPublication {
            keys: keys.into_iter().collect(),
            position,
        })
    }

    /// Preflight exact schema/control retirement before canonical mutation.
    pub(in crate::db) fn prepare_positioned_journal_batch_retirement(
        &self,
        incarnation: DatabaseIncarnationId,
        batch: &JournalBatch,
        position: JournalOverlayPosition,
    ) -> Result<PreparedSchemaPositionRetirement, InternalError> {
        let keys = self.positioned_journal_batch_keys(
            incarnation,
            batch,
            IdentityStateStorageView::Canonical,
        )?;
        self.prepare_positioned_key_retirements(keys, position)
    }

    fn prepare_positioned_key_retirements(
        &self,
        keys: impl IntoIterator<Item = RawSchemaKey>,
        position: JournalOverlayPosition,
    ) -> Result<PreparedSchemaPositionRetirement, InternalError> {
        let SchemaStoreBackend::Journaled {
            live,
            tombstones,
            positions,
            ..
        } = &self.backend
        else {
            return Err(InternalError::store_invariant());
        };
        let mut entries = Vec::new();
        for key in keys {
            if !positions.is_positioned(&key) {
                // A prior row fold can create canonical-only derived metadata
                // after this older schema batch publishes. Its canonical fold
                // owns that key; there is no overlay for this batch to retire.
                if live.contains_key(&key) || tombstones.contains(&key) {
                    return Err(InternalError::store_invariant());
                }
                continue;
            }
            let retirement = positions.preflight_retirement(&key, position)?;
            entries.push((key, retirement));
        }
        Ok(PreparedSchemaPositionRetirement { entries })
    }

    /// Publish schema positions after their values have been mechanically applied.
    pub(in crate::db) fn publish_prepared_journal_batch_positions(
        &mut self,
        prepared: PreparedSchemaPositionPublication,
    ) {
        let SchemaStoreBackend::Journaled { positions, .. } = &mut self.backend else {
            debug_assert!(
                false,
                "preflighted schema positions require a journaled store"
            );
            return;
        };
        for key in prepared.keys {
            positions.publish_preflighted(key, prepared.position);
        }
    }

    /// Retire only exact schema/control overlays after canonical mutation.
    pub(in crate::db) fn apply_prepared_journal_batch_retirement(
        &mut self,
        prepared: PreparedSchemaPositionRetirement,
    ) {
        for (key, retirement) in prepared.entries {
            if retirement != PositionedOverlayRetirement::Exact {
                continue;
            }
            self.invalidate_accepted_bundle_cache_for_key(key);
            let SchemaStoreBackend::Journaled {
                live,
                tombstones,
                positions,
                ..
            } = &mut self.backend
            else {
                debug_assert!(
                    false,
                    "preflighted schema retirement requires a journaled store"
                );
                return;
            };
            live.remove(&key);
            tombstones.remove(&key);
            positions.retire_preflighted(&key, retirement);
        }
    }

    #[cfg(test)]
    fn publish_positioned_journal_entry(
        &mut self,
        key: RawSchemaKey,
        snapshot: Option<RawSchemaSnapshot>,
        position: JournalOverlayPosition,
    ) -> Result<Option<RawSchemaSnapshot>, InternalError> {
        let SchemaStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(InternalError::store_invariant());
        };
        positions.preflight_publish(&key, position)?;
        self.invalidate_accepted_bundle_cache_for_key(key);
        let SchemaStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            positions,
        } = &mut self.backend
        else {
            return Err(InternalError::store_invariant());
        };
        let previous = if tombstones.contains(&key) {
            None
        } else {
            live.get(&key).cloned().or_else(|| canonical.get(&key))
        };
        if let Some(snapshot) = snapshot {
            tombstones.remove(&key);
            live.insert(key, snapshot);
        } else {
            live.remove(&key);
            tombstones.insert(key);
        }
        positions.publish_preflighted(key, position);
        Ok(previous)
    }

    #[cfg(test)]
    fn retire_positioned_journal_effect(
        &mut self,
        key: RawSchemaKey,
        position: JournalOverlayPosition,
    ) -> Result<PositionedOverlayRetirement, InternalError> {
        let SchemaStoreBackend::Journaled { positions, .. } = &self.backend else {
            return Err(InternalError::store_invariant());
        };
        let retirement = positions.preflight_retirement(&key, position)?;
        let prepared = PreparedSchemaPositionRetirement {
            entries: vec![(key, retirement)],
        };
        self.apply_prepared_journal_batch_retirement(prepared);
        Ok(retirement)
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

    /// Preflight one canonical schema-snapshot fold without changing storage.
    pub(in crate::db) fn preflight_fold_persisted_snapshot(
        &self,
        snapshot: &PersistedSchemaSnapshot,
    ) -> Result<(), InternalError> {
        if !matches!(self.backend, SchemaStoreBackend::Journaled { .. }) {
            return Err(InternalError::store_invariant());
        }
        let _encoded = RawSchemaSnapshot::from_persisted_snapshot(snapshot)?;
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
        let Some(bundle) = self.current_accepted_schema_bundle_ref()? else {
            return Ok(None);
        };
        self.validate_constraint_validation_job_closure(&bundle)?;
        Ok(Some(bundle.clone()))
    }

    /// Project current accepted entity identity onto one registry-owned store path.
    pub(in crate::db) fn current_accepted_runtime_entities(
        &self,
        registered_store_path: &'static str,
    ) -> Result<Vec<AcceptedRuntimeEntity>, InternalError> {
        let Some(bundle) = self.current_accepted_schema_bundle_ref()? else {
            return Ok(Vec::new());
        };
        if bundle.store_path() != registered_store_path {
            return Err(InternalError::store_corruption());
        }

        bundle
            .entity_snapshots()
            .iter()
            .map(|(entity_tag, snapshot)| {
                AcceptedRuntimeEntity::from_accepted_snapshot(
                    &bundle,
                    *entity_tag,
                    snapshot,
                    registered_store_path,
                )
            })
            .collect()
    }

    /// Resolve one accepted entity tag without materializing the full store catalog.
    pub(in crate::db) fn current_accepted_runtime_entity_for_tag(
        &self,
        registered_store_path: &'static str,
        entity_tag: EntityTag,
    ) -> Result<Option<AcceptedRuntimeEntity>, InternalError> {
        let Some(bundle) = self.current_accepted_schema_bundle_ref()? else {
            return Ok(None);
        };
        if bundle.store_path() != registered_store_path {
            return Err(InternalError::store_corruption());
        }
        let Some(snapshot) = bundle.entity_snapshots().get(&entity_tag) else {
            return Ok(None);
        };

        AcceptedRuntimeEntity::from_accepted_snapshot(
            &bundle,
            entity_tag,
            snapshot,
            registered_store_path,
        )
        .map(Some)
    }

    /// Resolve one entity tag from the canonical accepted predecessor.
    pub(in crate::db) fn current_canonical_accepted_runtime_entity_for_tag(
        &self,
        registered_store_path: &'static str,
        entity_tag: EntityTag,
    ) -> Result<Option<AcceptedRuntimeEntity>, InternalError> {
        let Some(bundle) = self.current_canonical_accepted_schema_bundle()? else {
            return Ok(None);
        };
        if bundle.store_path() != registered_store_path {
            return Err(InternalError::store_corruption());
        }
        let Some(snapshot) = bundle.entity_snapshots().get(&entity_tag) else {
            return Ok(None);
        };

        AcceptedRuntimeEntity::from_accepted_snapshot(
            &bundle,
            entity_tag,
            snapshot,
            registered_store_path,
        )
        .map(Some)
    }

    /// Resolve one accepted entity source path without materializing the full store catalog.
    pub(in crate::db) fn current_accepted_runtime_entity_for_path(
        &self,
        registered_store_path: &'static str,
        entity_path: &str,
    ) -> Result<Option<AcceptedRuntimeEntity>, InternalError> {
        self.current_accepted_runtime_entity_matching(registered_store_path, |snapshot_path, _| {
            snapshot_path == entity_path
        })
    }

    /// Resolve one entity path from the canonical accepted predecessor.
    pub(in crate::db) fn current_canonical_accepted_runtime_entity_for_path(
        &self,
        registered_store_path: &'static str,
        entity_path: &str,
    ) -> Result<Option<AcceptedRuntimeEntity>, InternalError> {
        let Some(bundle) = self.current_canonical_accepted_schema_bundle()? else {
            return Ok(None);
        };
        if bundle.store_path() != registered_store_path {
            return Err(InternalError::store_corruption());
        }

        let mut matched = None;
        for (entity_tag, snapshot) in bundle.entity_snapshots() {
            if snapshot.entity_path() != entity_path {
                continue;
            }
            let entity = AcceptedRuntimeEntity::from_accepted_snapshot(
                &bundle,
                *entity_tag,
                snapshot,
                registered_store_path,
            )?;
            if matched.replace(entity).is_some() {
                return Err(InternalError::store_corruption());
            }
        }

        Ok(matched)
    }

    /// Resolve one accepted entity display name without materializing the full store catalog.
    #[cfg(test)]
    pub(in crate::db) fn current_accepted_runtime_entity_for_name(
        &self,
        registered_store_path: &'static str,
        entity_name: &str,
    ) -> Result<Option<AcceptedRuntimeEntity>, InternalError> {
        self.current_accepted_runtime_entity_matching(registered_store_path, |_, snapshot_name| {
            snapshot_name == entity_name
        })
    }

    fn current_accepted_runtime_entity_matching(
        &self,
        registered_store_path: &'static str,
        mut predicate: impl FnMut(&str, &str) -> bool,
    ) -> Result<Option<AcceptedRuntimeEntity>, InternalError> {
        let Some(bundle) = self.current_accepted_schema_bundle_ref()? else {
            return Ok(None);
        };
        if bundle.store_path() != registered_store_path {
            return Err(InternalError::store_corruption());
        }

        let mut matched = None;
        for (entity_tag, snapshot) in bundle.entity_snapshots() {
            if !predicate(snapshot.entity_path(), snapshot.entity_name()) {
                continue;
            }
            let entity = AcceptedRuntimeEntity::from_accepted_snapshot(
                &bundle,
                *entity_tag,
                snapshot,
                registered_store_path,
            )?;
            if matched.replace(entity).is_some() {
                return Err(InternalError::store_corruption());
            }
        }

        Ok(matched)
    }

    /// Return the current accepted revision without decoding its bundle.
    pub(in crate::db) fn current_accepted_schema_revision(
        &self,
    ) -> Result<Option<AcceptedSchemaRevision>, InternalError> {
        Ok(self
            .current_accepted_schema_root()?
            .map(|selection| selection.root().revision()))
    }

    /// Return the pending relation activation that blocks deletes from one target.
    ///
    /// This reads the immutable accepted-bundle cache directly so ordinary
    /// deletes do not decode and clone every store catalog merely to prove that
    /// no candidate reverse generation targets the deleted entity.
    pub(in crate::db) fn pending_relation_activation_for_target(
        &self,
        target_path: &str,
    ) -> Result<Option<PendingRelationActivationDeleteBarrier>, InternalError> {
        let Some(bundle) = self.current_accepted_schema_bundle_ref()? else {
            return Ok(None);
        };
        for (entity_tag, snapshot) in bundle.entity_snapshots() {
            let Some(candidate) = snapshot
                .candidate_relations()
                .iter()
                .find(|candidate| candidate.target_path() == target_path)
            else {
                continue;
            };
            let activation = snapshot
                .constraint_activations()
                .iter()
                .find(|activation| {
                    matches!(
                        activation.kind(),
                        ConstraintActivationKind::Relation { relation_id }
                            if *relation_id == candidate.id()
                    )
                })
                .ok_or_else(InternalError::store_corruption)?;
            return Ok(Some(PendingRelationActivationDeleteBarrier {
                accepted_schema_fingerprint:
                    accepted_schema_cache_fingerprint_for_persisted_snapshot(snapshot)?,
                source_entity_tag: *entity_tag,
                constraint_id: activation.id(),
            }));
        }

        Ok(None)
    }

    /// Return whether one accepted source entity owns a live relation to a target.
    pub(in crate::db) fn entity_has_relation_to_target(
        &self,
        source_entity: EntityTag,
        target_path: &str,
    ) -> Result<bool, InternalError> {
        let Some(bundle) = self.current_accepted_schema_bundle_ref()? else {
            return Ok(false);
        };
        let Some(snapshot) = bundle.entity_snapshots().get(&source_entity) else {
            return Ok(false);
        };

        Ok(snapshot
            .relations()
            .iter()
            .any(|relation| relation.target_path() == target_path))
    }

    /// Reject any same-entity schema change beside one exact activation lifecycle step.
    pub(in crate::db) fn validate_live_activation_transition(
        &self,
        candidate: &AcceptedSchemaRevisionBundle,
    ) -> Result<(), InternalError> {
        let Some(current) = self.current_accepted_schema_bundle()? else {
            return Ok(());
        };
        Self::validate_activation_transition_from(&current, candidate)
    }

    /// Validate one transition against the canonical accepted predecessor.
    pub(in crate::db) fn validate_canonical_activation_transition(
        &self,
        candidate: &AcceptedSchemaRevisionBundle,
    ) -> Result<(), InternalError> {
        let Some(current) = self.current_canonical_accepted_schema_bundle()? else {
            return Ok(());
        };
        Self::validate_activation_transition_from(&current, candidate)
    }

    fn validate_activation_transition_from(
        current: &AcceptedSchemaRevisionBundle,
        candidate: &AcceptedSchemaRevisionBundle,
    ) -> Result<(), InternalError> {
        for (entity_tag, before) in current.entity_snapshots() {
            if before.constraint_activations().is_empty() {
                continue;
            }
            let after = candidate
                .entity_snapshots()
                .get(entity_tag)
                .ok_or_else(InternalError::store_invariant)?;
            if before == after {
                continue;
            }
            let expected_shape = before
                .clone()
                .with_constraint_catalog(after.constraint_catalog().clone());
            let catalog_only_transition = expected_shape == *after
                && before
                    .constraint_catalog()
                    .permits_live_activation_transition_to(after.constraint_catalog());
            let sql_row_local_abort_with_version =
                before.constraint_activations().iter().any(|activation| {
                    activation.origin() == ConstraintOrigin::SqlDdl
                        && matches!(
                            activation.kind(),
                            ConstraintActivationKind::Check { .. }
                                | ConstraintActivationKind::NotNull { .. }
                        )
                        && before.version().get().checked_add(1) == Some(after.version().get())
                        && before
                            .constraint_catalog()
                            .clone()
                            .with_aborted_activation(activation.id())
                            .is_ok_and(|catalog| catalog == *after.constraint_catalog())
                        && before
                            .clone()
                            .with_constraint_catalog(after.constraint_catalog().clone())
                            .with_schema_version(after.version())
                            == *after
                });
            let sql_unique_abort_with_version =
                before.constraint_activations().iter().any(|activation| {
                    activation.origin() == ConstraintOrigin::SqlDdl
                        && matches!(activation.kind(), ConstraintActivationKind::Unique { .. })
                        && before.version().get().checked_add(1) == Some(after.version().get())
                        && before
                            .with_aborted_unique_activation(activation.id(), after.version())
                            .is_ok_and(|expected| expected == *after)
                });
            let not_null_promotion = before.constraint_activations().iter().any(|activation| {
                matches!(activation.kind(), ConstraintActivationKind::NotNull { .. })
                    && before
                        .with_promoted_not_null_activation(activation.id(), after.version())
                        .is_ok_and(|expected| expected == *after)
            });
            let unique_promotion = before.constraint_activations().iter().any(|activation| {
                matches!(activation.kind(), ConstraintActivationKind::Unique { .. })
                    && before
                        .with_promoted_unique_activation(activation.id(), after.version())
                        .is_ok_and(|expected| expected == *after)
            });
            let relation_promotion = before.constraint_activations().iter().any(|activation| {
                matches!(activation.kind(), ConstraintActivationKind::Relation { .. })
                    && before
                        .with_promoted_relation_activation(activation.id(), after.version())
                        .is_ok_and(|expected| expected == *after)
            });
            if !catalog_only_transition
                && !sql_row_local_abort_with_version
                && !sql_unique_abort_with_version
                && !not_null_promotion
                && !unique_promotion
                && !relation_promotion
            {
                return Err(InternalError::store_invariant());
            }
        }
        Ok(())
    }

    /// Prove exact pairing between live activations and durable validation jobs.
    pub(in crate::db) fn validate_constraint_validation_job_closure(
        &self,
        bundle: &AcceptedSchemaRevisionBundle,
    ) -> Result<(), InternalError> {
        self.validate_constraint_validation_job_closure_with_change(bundle, None, None)
    }

    /// Prove the activation/job closure that would exist after one bounded
    /// marker-owned job replacement or removal.
    pub(in crate::db) fn validate_constraint_validation_job_closure_with_change(
        &self,
        bundle: &AcceptedSchemaRevisionBundle,
        replacement: Option<&ConstraintValidationJob>,
        removal: Option<(EntityTag, ConstraintId)>,
    ) -> Result<(), InternalError> {
        self.validate_constraint_validation_job_closure_with_change_in_view(
            bundle,
            replacement,
            removal,
            IdentityStateStorageView::Effective,
        )
    }

    /// Prove activation/job closure against the canonical predecessor view.
    pub(in crate::db) fn validate_canonical_constraint_validation_job_closure_with_change(
        &self,
        bundle: &AcceptedSchemaRevisionBundle,
        replacement: Option<&ConstraintValidationJob>,
        removal: Option<(EntityTag, ConstraintId)>,
    ) -> Result<(), InternalError> {
        self.validate_constraint_validation_job_closure_with_change_in_view(
            bundle,
            replacement,
            removal,
            IdentityStateStorageView::Canonical,
        )
    }

    fn validate_constraint_validation_job_closure_with_change_in_view(
        &self,
        bundle: &AcceptedSchemaRevisionBundle,
        replacement: Option<&ConstraintValidationJob>,
        removal: Option<(EntityTag, ConstraintId)>,
        view: IdentityStateStorageView,
    ) -> Result<(), InternalError> {
        if replacement.is_some() && removal.is_some() {
            return Err(InternalError::store_invariant());
        }
        let replacement_key = replacement.map(|job| {
            RawSchemaKey::from_constraint_validation_job(job.entity_tag(), job.constraint_id())
        });
        let removal_key = removal.map(|(entity_tag, constraint_id)| {
            RawSchemaKey::from_constraint_validation_job(entity_tag, constraint_id)
        });
        let mut expected = BTreeSet::new();
        for (entity_tag, snapshot) in bundle.entity_snapshots() {
            for activation in snapshot.constraint_activations() {
                let key =
                    RawSchemaKey::from_constraint_validation_job(*entity_tag, activation.id());
                match activation.state() {
                    ConstraintActivationState::EnforcingNewWrites => {
                        if self
                            .constraint_validation_job_after_change(
                                key,
                                replacement,
                                replacement_key,
                                removal_key,
                                view,
                            )?
                            .is_some()
                        {
                            return Err(InternalError::store_corruption());
                        }
                    }
                    ConstraintActivationState::Validating => {
                        let job = self
                            .constraint_validation_job_after_change(
                                key,
                                replacement,
                                replacement_key,
                                removal_key,
                                view,
                            )?
                            .ok_or_else(InternalError::store_corruption)?;
                        if job.entity_tag() != *entity_tag
                            || job.entity_path() != snapshot.entity_path()
                        {
                            return Err(InternalError::store_corruption());
                        }
                        job.validate(Some(activation))?;
                        expected.insert(key);
                    }
                }
            }
        }

        self.visit_constraint_validation_jobs_in_view(view, |key, raw| {
            if removal_key == Some(*key) || replacement_key == Some(*key) {
                return Ok(SchemaStoreVisit::Continue);
            }
            if !expected.contains(key) {
                return Err(InternalError::store_corruption());
            }
            let job = decode_constraint_validation_job(raw.as_bytes())?;
            if job.entity_tag() != key.entity_tag()
                || key.constraint_id() != Some(job.constraint_id())
            {
                return Err(InternalError::store_corruption());
            }
            Ok(SchemaStoreVisit::Continue)
        })?;

        if let Some(key) = replacement_key
            && !expected.contains(&key)
        {
            return Err(InternalError::store_corruption());
        }
        if let Some(key) = removal_key
            && expected.contains(&key)
        {
            return Err(InternalError::store_corruption());
        }

        Ok(())
    }

    fn constraint_validation_job_after_change(
        &self,
        key: RawSchemaKey,
        replacement: Option<&ConstraintValidationJob>,
        replacement_key: Option<RawSchemaKey>,
        removal_key: Option<RawSchemaKey>,
        view: IdentityStateStorageView,
    ) -> Result<Option<ConstraintValidationJob>, InternalError> {
        if removal_key == Some(key) {
            return Ok(None);
        }
        if replacement_key == Some(key) {
            return Ok(replacement.cloned());
        }
        let raw = match view {
            IdentityStateStorageView::Effective => self.get_raw_snapshot(&key),
            IdentityStateStorageView::Canonical => self.get_canonical_raw_value(&key)?,
        };
        raw.map(|raw| decode_constraint_validation_job(raw.as_bytes()))
            .transpose()
    }

    /// Return whether one retained schema authority still names this store's
    /// current immutable accepted root.
    pub(in crate::db) fn current_accepted_schema_authority_matches(
        &self,
        expected: &AcceptedSchemaAuthority,
    ) -> Result<bool, InternalError> {
        let Some(store_scope) = self.accepted_catalog_scope.get() else {
            return Ok(false);
        };

        // Root-writing primitives invalidate this cache before publication,
        // so a retained selection is the current in-memory authority.
        if let Some(cached) = self
            .accepted_bundle_cache
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?
            .as_ref()
        {
            let root = cached.selection.root();
            return Ok(expected.matches_store_root(
                store_scope,
                root.revision(),
                root.fingerprint(),
            ));
        }

        let Some(selection) = self.current_accepted_schema_root()? else {
            return Ok(false);
        };
        let root = selection.root();

        Ok(expected.matches_store_root(store_scope, root.revision(), root.fingerprint()))
    }

    /// Publish a candidate directly into its canonical schema allocation.
    ///
    /// Journaled online revisions must use
    /// `apply_journaled_accepted_schema_candidate`; this path owns initial
    /// bootstrap and marker-owned live-projection updates.
    pub(in crate::db) fn publish_accepted_schema_candidate(
        &mut self,
        incarnation: DatabaseIncarnationId,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(), InternalError> {
        let identity_transition = self.prepare_identity_state_transition(
            incarnation,
            candidate,
            IdentityStateStorageView::Effective,
        )?;
        if self.current_root_matches_candidate(candidate)? {
            if !identity_transition.is_empty() {
                return Err(InternalError::identity_state_corruption());
            }
            let selection = self
                .current_accepted_schema_root()?
                .ok_or_else(InternalError::store_corruption)?;
            self.retain_durable_candidate_entries(candidate, selection.slot())?;
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
        self.apply_identity_state_transition(
            identity_transition,
            IdentityStateWriteTarget::Durable,
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
        self.retain_durable_candidate_entries(candidate, selected.slot())?;
        Ok(())
    }

    /// Restore one current accepted candidate into an empty live-only schema
    /// store from its durable database-control checkpoint.
    pub(in crate::db) fn restore_live_accepted_schema_checkpoint(
        &mut self,
        incarnation: DatabaseIncarnationId,
        candidate: &CandidateSchemaRevision,
        checkpoint_identity_states: &IdentityStateInventory,
    ) -> Result<(), InternalError> {
        if !matches!(self.backend, SchemaStoreBackend::Heap(_)) {
            return Err(InternalError::store_invariant());
        }
        let checkpoint_validation = prepare_identity_state_transition(
            incarnation,
            Some(candidate.bundle()),
            candidate.bundle(),
            checkpoint_identity_states.clone(),
        )?;
        if !checkpoint_validation.is_empty() {
            return Err(InternalError::identity_state_corruption());
        }
        if self.current_root_matches_candidate(candidate)? {
            for state in checkpoint_identity_states.values() {
                let key = RawSchemaKey::from_identity_state(
                    state.owner().entity_tag(),
                    state.owner().field_id(),
                );
                self.insert_durable_raw_value(key, encode_identity_state(state)?);
            }
            if self.identity_state_inventory(IdentityStateStorageView::Effective)?
                != *checkpoint_identity_states
            {
                return Err(InternalError::identity_state_corruption());
            }
            let selection = self
                .current_accepted_schema_root()?
                .ok_or_else(InternalError::store_corruption)?;
            self.retain_durable_candidate_entries(candidate, selection.slot())?;
            return Ok(());
        }
        if self.current_accepted_schema_root()?.is_some()
            || !self
                .identity_state_inventory(IdentityStateStorageView::Effective)?
                .is_empty()
        {
            return Err(InternalError::store_corruption());
        }

        self.insert_durable_candidate_snapshots(candidate)?;
        let bundle_key = RawSchemaKey::from_accepted_bundle(candidate.root().bundle_key());
        self.insert_durable_raw_value(bundle_key, candidate.encoded_bundle().to_vec());
        for state in checkpoint_identity_states.values() {
            let key = RawSchemaKey::from_identity_state(
                state.owner().entity_tag(),
                state.owner().field_id(),
            );
            self.insert_durable_raw_value(key, encode_identity_state(state)?);
        }
        let root_key = RawSchemaKey::from_accepted_root_slot(0)?;
        self.insert_durable_raw_value(root_key, candidate.encoded_root().to_vec());

        let selected = self
            .current_accepted_schema_root()?
            .ok_or_else(InternalError::store_corruption)?;
        if selected.root() != candidate.root() {
            return Err(InternalError::store_corruption());
        }
        self.retain_durable_candidate_entries(candidate, selected.slot())?;
        Ok(())
    }

    /// Preflight one accepted candidate without changing durable or live
    /// schema state.
    ///
    /// Returns `true` only when this exact candidate is already authoritative.
    /// Multi-store publication uses that distinction to reject partial replay
    /// before opening one marker-owned commit window.
    pub(in crate::db) fn preflight_accepted_schema_candidate(
        &self,
        incarnation: DatabaseIncarnationId,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<bool, InternalError> {
        let identity_transition = self.prepare_identity_state_transition(
            incarnation,
            candidate,
            IdentityStateStorageView::Effective,
        )?;
        if self.current_root_matches_candidate(candidate)? {
            if !identity_transition.is_empty() {
                return Err(InternalError::identity_state_corruption());
            }
            return Ok(true);
        }
        let first = self.accepted_root_slot_bytes(0)?;
        let second = self.accepted_root_slot_bytes(1)?;
        prepare_accepted_schema_root_publication(
            [first.as_deref(), second.as_deref()],
            expected_revision,
            candidate,
        )
        .map_err(map_schema_publication_error)?;

        Ok(false)
    }

    /// Preflight one accepted candidate against canonical journaled authority.
    pub(in crate::db) fn preflight_fold_journaled_accepted_schema_candidate(
        &self,
        incarnation: DatabaseIncarnationId,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(), InternalError> {
        if !matches!(self.backend, SchemaStoreBackend::Journaled { .. }) {
            return Err(InternalError::store_invariant());
        }
        let identity_transition = self.prepare_identity_state_transition(
            incarnation,
            candidate,
            IdentityStateStorageView::Canonical,
        )?;
        let candidate_is_current = self.canonical_root_matches_candidate(candidate)?;
        if candidate_is_current && !identity_transition.is_empty() {
            return Err(InternalError::identity_state_corruption());
        }
        for state in identity_transition.into_updates() {
            let _encoded = encode_identity_state(&state)?;
        }
        for snapshot in candidate.bundle().entity_snapshots().values() {
            let _encoded = RawSchemaSnapshot::from_persisted_snapshot(snapshot)?;
        }

        let first = self.canonical_root_slot_bytes(0)?;
        let second = self.canonical_root_slot_bytes(1)?;
        let root_slot = if candidate_is_current {
            select_current_accepted_schema_root([first.as_deref(), second.as_deref()])?
                .ok_or_else(InternalError::store_corruption)?
                .slot()
        } else {
            prepare_accepted_schema_root_publication(
                [first.as_deref(), second.as_deref()],
                expected_revision,
                candidate,
            )
            .map_err(map_schema_publication_error)?
            .target_slot()
        };
        let _retained = Self::candidate_entry_keys(candidate, root_slot)?;
        Ok(())
    }

    /// Return the retained Identity owner count after admitting one candidate.
    pub(in crate::db) fn projected_identity_state_count(
        &self,
        incarnation: DatabaseIncarnationId,
        candidate: &CandidateSchemaRevision,
    ) -> Result<usize, InternalError> {
        Ok(self
            .prepare_identity_state_transition(
                incarnation,
                candidate,
                IdentityStateStorageView::Effective,
            )?
            .projected_inventory_len())
    }

    /// Apply one marker-bound schema candidate to the journaled live projection.
    pub(in crate::db) fn apply_journaled_accepted_schema_candidate(
        &mut self,
        incarnation: DatabaseIncarnationId,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(), InternalError> {
        if !matches!(self.backend, SchemaStoreBackend::Journaled { .. }) {
            return Err(InternalError::store_invariant());
        }
        let identity_transition = self.prepare_identity_state_transition(
            incarnation,
            candidate,
            IdentityStateStorageView::Effective,
        )?;
        if self.current_root_matches_candidate(candidate)? {
            if !identity_transition.is_empty() {
                return Err(InternalError::identity_state_corruption());
            }
            let selection = self
                .current_accepted_schema_root()?
                .ok_or_else(InternalError::store_corruption)?;
            self.retain_materialized_candidate_entries(candidate, selection.slot())?;
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
        self.apply_identity_state_transition(
            identity_transition,
            IdentityStateWriteTarget::Materialized,
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
        let selection = self
            .current_accepted_schema_root()?
            .ok_or_else(InternalError::store_corruption)?;
        self.retain_materialized_candidate_entries(candidate, selection.slot())?;
        Ok(())
    }

    /// Fold one committed schema candidate into the canonical schema BTree.
    pub(in crate::db) fn fold_journaled_accepted_schema_candidate(
        &mut self,
        incarnation: DatabaseIncarnationId,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
    ) -> Result<(), InternalError> {
        let identity_transition = self.prepare_identity_state_transition(
            incarnation,
            candidate,
            IdentityStateStorageView::Canonical,
        )?;
        if self.canonical_root_matches_candidate(candidate)? {
            if !identity_transition.is_empty() {
                return Err(InternalError::identity_state_corruption());
            }
            let first = self.canonical_root_slot_bytes(0)?;
            let second = self.canonical_root_slot_bytes(1)?;
            let selection =
                select_current_accepted_schema_root([first.as_deref(), second.as_deref()])?
                    .ok_or_else(InternalError::store_corruption)?;
            self.retain_canonical_candidate_entries(candidate, selection.slot())?;
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
        self.apply_identity_state_transition(
            identity_transition,
            IdentityStateWriteTarget::Canonical,
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
        let first = self.canonical_root_slot_bytes(0)?;
        let second = self.canonical_root_slot_bytes(1)?;
        let selection = select_current_accepted_schema_root([first.as_deref(), second.as_deref()])?
            .ok_or_else(InternalError::store_corruption)?;
        self.retain_canonical_candidate_entries(candidate, selection.slot())?;
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

    #[cfg(test)]
    fn latest_staged_persisted_snapshot(
        &self,
        entity: EntityTag,
    ) -> Result<Option<PersistedSchemaSnapshot>, InternalError> {
        self.latest_raw_snapshots_by_entity()
            .remove(&entity)
            .map(|(_, snapshot)| snapshot.decode_persisted_snapshot())
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
        entity_path: &str,
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

        let cache = self
            .accepted_bundle_cache
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?;
        let cached = cache.as_ref().ok_or_else(InternalError::store_invariant)?;
        if let Some(selection) = cached
            .entity_selections
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?
            .get(&entity)
            .cloned()
        {
            return Ok(Some(selection));
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

        let selected = AcceptedCatalogSnapshotSelection::new(
            identity,
            cached.value_catalog.clone(),
            Rc::from(raw_snapshot.into_bytes()),
        );
        cached
            .entity_selections
            .try_borrow_mut()
            .map_err(|_| InternalError::store_invariant())?
            .insert(entity, selected.clone());

        Ok(Some(selected))
    }

    /// Return one accepted catalog selection from the canonical journal base.
    /// Recovery uses this while folding historical row batches whose schema
    /// revision can precede the current live accepted root.
    pub(in crate::db) fn current_canonical_accepted_catalog_selection(
        &self,
        entity: EntityTag,
        entity_path: &str,
        store_path: &'static str,
    ) -> Result<Option<AcceptedCatalogSnapshotSelection>, InternalError> {
        let first = self.canonical_root_slot_bytes(0)?;
        let second = self.canonical_root_slot_bytes(1)?;
        let Some(selection) =
            select_current_accepted_schema_root([first.as_deref(), second.as_deref()])?
        else {
            return Ok(None);
        };
        let bundle_key = RawSchemaKey::from_accepted_bundle(selection.root().bundle_key());
        let raw_bundle = self
            .get_canonical_raw_value(&bundle_key)?
            .ok_or_else(InternalError::store_corruption)?;
        let bundle = decode_verified_accepted_schema_revision_bundle(
            selection.root(),
            raw_bundle.as_bytes(),
        )?;
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
            AcceptedValueCatalogHandle::new(
                bundle.enum_catalog().clone(),
                bundle.composite_catalog().clone(),
                self.accepted_catalog_scope
                    .get_or_init(AcceptedStoreCatalogScope::new)
                    .clone(),
                bundle.revision(),
                selection.root().fingerprint(),
            ),
            Rc::from(raw_snapshot.into_bytes()),
        )))
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
        self.invalidate_accepted_bundle_cache_for_key(key);
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
        self.invalidate_accepted_bundle_cache_for_key(key);
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
        self.invalidate_accepted_bundle_cache_for_key(key);
        let value = RawSchemaSnapshot::from_encoded_control_record(bytes);
        match &mut self.backend {
            SchemaStoreBackend::Heap(map) => {
                map.insert(key, value);
            }
            SchemaStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
                ..
            } => {
                live.remove(&key);
                tombstones.remove(&key);
                canonical.insert(key, value);
            }
        }
    }

    fn invalidate_accepted_bundle_cache_for_key(&mut self, key: RawSchemaKey) {
        if key.is_accepted_root() {
            self.accepted_bundle_cache.get_mut().take();
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
                    ..
                } => {
                    live.remove(&key);
                    tombstones.remove(&key);
                    canonical.insert(key, value);
                }
            }
        }
        Ok(())
    }

    fn candidate_entry_keys(
        candidate: &CandidateSchemaRevision,
        root_slot: usize,
    ) -> Result<BTreeSet<RawSchemaKey>, InternalError> {
        let mut keys = candidate
            .bundle()
            .entity_snapshots()
            .iter()
            .map(|(entity_tag, snapshot)| {
                RawSchemaKey::from_entity_version(*entity_tag, snapshot.version())
            })
            .collect::<BTreeSet<_>>();
        keys.insert(RawSchemaKey::from_accepted_bundle(
            candidate.root().bundle_key(),
        ));
        keys.insert(RawSchemaKey::from_accepted_root_slot(root_slot)?);
        for (entity_tag, snapshot) in candidate.bundle().entity_snapshots() {
            for activation in snapshot
                .constraint_activations()
                .iter()
                .filter(|activation| activation.state() == ConstraintActivationState::Validating)
            {
                keys.insert(RawSchemaKey::from_constraint_validation_job(
                    *entity_tag,
                    activation.id(),
                ));
            }
        }
        Ok(keys)
    }

    fn positioned_candidate_effect_keys(
        &self,
        incarnation: DatabaseIncarnationId,
        expected_revision: AcceptedSchemaRevision,
        candidate: &CandidateSchemaRevision,
        view: IdentityStateStorageView,
    ) -> Result<BTreeSet<RawSchemaKey>, InternalError> {
        let identity_transition =
            self.prepare_identity_state_transition(incarnation, candidate, view)?;
        let (first, second, candidate_is_current) = match view {
            IdentityStateStorageView::Effective => (
                self.accepted_root_slot_bytes(0)?,
                self.accepted_root_slot_bytes(1)?,
                self.current_root_matches_candidate(candidate)?,
            ),
            IdentityStateStorageView::Canonical => (
                self.canonical_root_slot_bytes(0)?,
                self.canonical_root_slot_bytes(1)?,
                self.canonical_root_matches_candidate(candidate)?,
            ),
        };
        let root_slot = if candidate_is_current {
            select_current_accepted_schema_root([first.as_deref(), second.as_deref()])?
                .ok_or_else(InternalError::store_corruption)?
                .slot()
        } else {
            prepare_accepted_schema_root_publication(
                [first.as_deref(), second.as_deref()],
                expected_revision,
                candidate,
            )
            .map_err(map_schema_publication_error)?
            .target_slot()
        };
        let mut keys = Self::candidate_entry_keys(candidate, root_slot)?;
        for state in identity_transition.into_updates() {
            keys.insert(RawSchemaKey::from_identity_state(
                state.owner().entity_tag(),
                state.owner().field_id(),
            ));
        }

        let SchemaStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            positions,
        } = &self.backend
        else {
            return Err(InternalError::store_invariant());
        };
        for entry in canonical.iter() {
            let has_relevant_overlay = matches!(view, IdentityStateStorageView::Effective)
                || positions.is_positioned(entry.key())
                || live.contains_key(entry.key())
                || tombstones.contains(entry.key());
            if has_relevant_overlay
                && !keys.contains(entry.key())
                && !entry.key().is_identity_state()
            {
                keys.insert(*entry.key());
            }
        }
        if matches!(view, IdentityStateStorageView::Effective) {
            for key in live.keys() {
                if !keys.contains(key) && !key.is_identity_state() {
                    keys.insert(*key);
                }
            }
        }
        Ok(keys)
    }

    fn positioned_journal_batch_keys(
        &self,
        incarnation: DatabaseIncarnationId,
        batch: &JournalBatch,
        view: IdentityStateStorageView,
    ) -> Result<BTreeSet<RawSchemaKey>, InternalError> {
        let mut keys = BTreeSet::new();
        for record in batch.records() {
            match record {
                JournalRecord::SchemaPut {
                    schema_snapshot_bytes,
                    ..
                } => {
                    let snapshot = decode_persisted_schema_snapshot(schema_snapshot_bytes)?;
                    let entity_tag = match view {
                        IdentityStateStorageView::Effective => self
                            .current_accepted_schema_bundle_ref()?
                            .ok_or_else(InternalError::store_corruption)?
                            .entity_snapshots()
                            .iter()
                            .find_map(|(entity_tag, accepted)| {
                                (accepted.entity_path() == snapshot.entity_path())
                                    .then_some(*entity_tag)
                            }),
                        IdentityStateStorageView::Canonical => self
                            .current_canonical_accepted_schema_bundle()?
                            .ok_or_else(InternalError::store_corruption)?
                            .entity_snapshots()
                            .iter()
                            .find_map(|(entity_tag, accepted)| {
                                (accepted.entity_path() == snapshot.entity_path())
                                    .then_some(*entity_tag)
                            }),
                    }
                    .ok_or_else(InternalError::store_corruption)?;
                    keys.insert(RawSchemaKey::from_entity_version(
                        entity_tag,
                        snapshot.version(),
                    ));
                }
                JournalRecord::AcceptedSchemaPublish {
                    expected_revision,
                    schema_bundle_bytes,
                    schema_root_bytes,
                    ..
                } => {
                    let candidate = CandidateSchemaRevision::from_encoded(
                        schema_bundle_bytes.clone(),
                        schema_root_bytes.clone(),
                    )?;
                    keys.extend(self.positioned_candidate_effect_keys(
                        incarnation,
                        *expected_revision,
                        &candidate,
                        view,
                    )?);
                }
                JournalRecord::ConstraintValidationJobPut {
                    entity_tag,
                    constraint_id,
                    ..
                }
                | JournalRecord::ConstraintValidationJobDelete {
                    entity_tag,
                    constraint_id,
                    ..
                } => {
                    keys.insert(RawSchemaKey::from_constraint_validation_job(
                        *entity_tag,
                        *constraint_id,
                    ));
                }
                JournalRecord::IdentityRangeAdvance { range } => {
                    keys.insert(RawSchemaKey::from_identity_state(
                        range.owner().entity_tag(),
                        range.owner().field_id(),
                    ));
                }
                JournalRecord::RowPut { .. }
                | JournalRecord::RowDelete { .. }
                | JournalRecord::AcceptedSchemaIndexDelete { .. }
                | JournalRecord::AcceptedSchemaIndexPut { .. }
                | JournalRecord::ConstraintValidationIndexPut { .. } => {}
                #[cfg(any(test, feature = "migration"))]
                JournalRecord::SchemaMigrationRowPut { .. }
                | JournalRecord::SchemaMigrationIndexPut { .. } => {}
            }
        }
        Ok(keys)
    }

    // Keep only the current entity snapshots, immutable bundle, and selected
    // root. The inactive root is needed only during publication and is removed
    // after the new root has been verified.
    fn retain_durable_candidate_entries(
        &mut self,
        candidate: &CandidateSchemaRevision,
        root_slot: usize,
    ) -> Result<(), InternalError> {
        let keep = Self::candidate_entry_keys(candidate, root_slot)?;
        self.accepted_bundle_cache.get_mut().take();
        match &mut self.backend {
            SchemaStoreBackend::Heap(map) => {
                map.retain(|key, _| keep.contains(key) || key.is_identity_state());
            }
            SchemaStoreBackend::Journaled {
                canonical,
                live,
                tombstones,
                ..
            } => {
                let stale = canonical
                    .iter()
                    .filter_map(|entry| {
                        (!keep.contains(entry.key()) && !entry.key().is_identity_state())
                            .then_some(*entry.key())
                    })
                    .collect::<Vec<_>>();
                for key in stale {
                    canonical.remove(&key);
                }
                live.retain(|key, _| keep.contains(key) || key.is_identity_state());
                tombstones.clear();
            }
        }
        Ok(())
    }

    fn retain_materialized_candidate_entries(
        &mut self,
        candidate: &CandidateSchemaRevision,
        root_slot: usize,
    ) -> Result<(), InternalError> {
        let keep = Self::candidate_entry_keys(candidate, root_slot)?;
        self.accepted_bundle_cache.get_mut().take();
        let SchemaStoreBackend::Journaled {
            canonical,
            live,
            tombstones,
            ..
        } = &mut self.backend
        else {
            return Err(InternalError::store_invariant());
        };
        live.retain(|key, _| keep.contains(key) || key.is_identity_state());
        let canonical_keys = canonical
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for key in canonical_keys {
            if keep.contains(&key) || key.is_identity_state() {
                tombstones.remove(&key);
            } else {
                tombstones.insert(key);
            }
        }
        Ok(())
    }

    fn retain_canonical_candidate_entries(
        &mut self,
        candidate: &CandidateSchemaRevision,
        root_slot: usize,
    ) -> Result<(), InternalError> {
        let keep = Self::candidate_entry_keys(candidate, root_slot)?;
        self.accepted_bundle_cache.get_mut().take();
        let SchemaStoreBackend::Journaled { canonical, .. } = &mut self.backend else {
            return Err(InternalError::store_invariant());
        };
        let stale = canonical
            .iter()
            .filter_map(|entry| {
                (!keep.contains(entry.key()) && !entry.key().is_identity_state())
                    .then_some(*entry.key())
            })
            .collect::<Vec<_>>();
        for key in stale {
            canonical.remove(&key);
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
                ..
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
        self.current_accepted_schema_authority_ref()
            .map(|authority| authority.map(|(_selection, bundle)| bundle))
    }

    /// Borrow the effective accepted root and its cached, verified immutable bundle together.
    pub(in crate::db) fn current_accepted_schema_authority_ref(
        &self,
    ) -> Result<
        Option<(
            AcceptedSchemaRootSelection,
            Ref<'_, AcceptedSchemaRevisionBundle>,
        )>,
        InternalError,
    > {
        let selection = self.current_accepted_schema_root()?;
        self.accepted_schema_authority_ref_for_selection(selection)
    }

    fn accepted_schema_authority_ref_for_selection(
        &self,
        selection: Option<AcceptedSchemaRootSelection>,
    ) -> Result<
        Option<(
            AcceptedSchemaRootSelection,
            Ref<'_, AcceptedSchemaRevisionBundle>,
        )>,
        InternalError,
    > {
        let Some(selection) = selection else {
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
            self.validate_constraint_validation_job_closure(&bundle)?;
            #[cfg(test)]
            ACCEPTED_SCHEMA_BUNDLE_CACHE_MISSES
                .with(|misses| misses.set(misses.get().saturating_add(1)));
            let value_catalog = AcceptedValueCatalogHandle::new(
                bundle.enum_catalog().clone(),
                bundle.composite_catalog().clone(),
                self.accepted_catalog_scope
                    .get_or_init(AcceptedStoreCatalogScope::new)
                    .clone(),
                bundle.revision(),
                selection.root().fingerprint(),
            );
            let cardinality_domain = Rc::new(CardinalityAcceptedDomain::derive(&bundle)?);
            *self
                .accepted_bundle_cache
                .try_borrow_mut()
                .map_err(|_| InternalError::store_invariant())? = Some(AcceptedSchemaBundleCache {
                selection,
                bundle,
                cardinality_domain,
                value_catalog,
                entity_selections: RefCell::new(StdBTreeMap::new()),
            });
        }

        let cache = self
            .accepted_bundle_cache
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?;
        let bundle = Ref::filter_map(cache, |cache| {
            cache
                .as_ref()
                .filter(|cached| cached.selection == selection)
                .map(|cached| &cached.bundle)
        })
        .map_err(|_| InternalError::store_invariant())?;
        self.validate_identity_state_closure(&bundle)?;
        Ok(Some((selection, bundle)))
    }

    /// Reuse the accepted-domain projection for one already-selected effective root.
    pub(in crate::db) fn accepted_cardinality_domain_for_selection(
        &self,
        selection: Option<AcceptedSchemaRootSelection>,
    ) -> Result<Option<(AcceptedSchemaRootSelection, Rc<CardinalityAcceptedDomain>)>, InternalError>
    {
        let Some(selection) = selection else {
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
            let authority = self
                .accepted_schema_authority_ref_for_selection(Some(selection))?
                .ok_or_else(InternalError::store_invariant)?;
            drop(authority);
        }
        let cache = self
            .accepted_bundle_cache
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?;
        let domain = cache
            .as_ref()
            .filter(|cached| cached.selection == selection)
            .map(|cached| Rc::clone(&cached.cardinality_domain))
            .ok_or_else(InternalError::store_invariant)?;
        Ok(Some((selection, domain)))
    }

    /// Borrow the cached accepted-domain projection for an already-admitted root.
    pub(in crate::db) fn cached_cardinality_domain_for_root(
        &self,
        root: CardinalityAcceptedRootIdentity,
    ) -> Result<Option<Rc<CardinalityAcceptedDomain>>, InternalError> {
        let cache = self
            .accepted_bundle_cache
            .try_borrow()
            .map_err(|_| InternalError::store_invariant())?;
        Ok(cache
            .as_ref()
            .filter(|cached| root.matches(cached.selection.root()))
            .map(|cached| Rc::clone(&cached.cardinality_domain)))
    }

    fn latest_raw_snapshots_by_entity(
        &self,
    ) -> StdBTreeMap<EntityTag, (SchemaVersion, RawSchemaSnapshot)> {
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
                ..
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

    fn visit_constraint_validation_jobs_in_view<E>(
        &self,
        view: IdentityStateStorageView,
        visitor: impl FnMut(&RawSchemaKey, &RawSchemaSnapshot) -> Result<SchemaStoreVisit, E>,
    ) -> Result<(), E> {
        let bounds = RawSchemaKey::all_constraint_validation_job_range_bounds();
        match (&self.backend, view) {
            (SchemaStoreBackend::Heap(map), _) => {
                let mut visitor = visitor;
                for (key, snapshot) in map.range((bounds.0, bounds.1)) {
                    if visitor(key, snapshot)?.should_stop() {
                        break;
                    }
                }
            }
            (
                SchemaStoreBackend::Journaled {
                    canonical,
                    live,
                    tombstones,
                    ..
                },
                IdentityStateStorageView::Effective,
            ) => Self::visit_journaled_raw_snapshot_range(
                canonical,
                live,
                tombstones,
                bounds,
                Direction::Asc,
                visitor,
            )?,
            (
                SchemaStoreBackend::Journaled { canonical, .. },
                IdentityStateStorageView::Canonical,
            ) => {
                let mut visitor = visitor;
                for entry in canonical.range((bounds.0, bounds.1)) {
                    if visitor(entry.key(), &entry.value())?.should_stop() {
                        break;
                    }
                }
            }
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
            ..
        } = &self.backend
        else {
            return None;
        };

        if tombstones.contains(key) {
            return None;
        }
        live.get(key).cloned().or_else(|| canonical.get(key))
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
            Direction::Asc => {
                for entry in ordered_overlay_entries(
                    canonical.range((bounds.0, bounds.1)),
                    live.range((bounds.0, bounds.1)),
                    Direction::Asc,
                    |entry| entry.key(),
                    |entry| entry.0,
                    tombstones,
                ) {
                    let visit = match entry {
                        OrderedOverlayEntry::Canonical(canonical_entry) => {
                            visitor(canonical_entry.key(), &canonical_entry.value())?
                        }
                        OrderedOverlayEntry::Live((key, snapshot)) => visitor(key, snapshot)?,
                    };
                    if visit.should_stop() {
                        return Ok(());
                    }
                }
            }
            Direction::Desc => {
                for entry in ordered_overlay_entries(
                    canonical.range((bounds.0, bounds.1)).rev(),
                    live.range((bounds.0, bounds.1)).rev(),
                    Direction::Desc,
                    |entry| entry.key(),
                    |entry| entry.0,
                    tombstones,
                ) {
                    let visit = match entry {
                        OrderedOverlayEntry::Canonical(canonical_entry) => {
                            visitor(canonical_entry.key(), &canonical_entry.value())?
                        }
                        OrderedOverlayEntry::Live((key, snapshot)) => visitor(key, snapshot)?,
                    };
                    if visit.should_stop() {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
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
    write_hash_tag_u8(&mut hasher, SCHEMA_STORE_DATA_ALLOCATION_FINGERPRINT_DOMAIN);

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
        let constraint_catalog = crate::db::schema::AcceptedConstraintCatalog::initial(
            data_projection.fields(),
            data_projection.indexes(),
            data_projection.relations(),
        )
        .map_err(|_| InternalError::store_invariant())?;
        let data_projection = data_projection.with_constraint_catalog(constraint_catalog);
        let encoded = encode_persisted_schema_snapshot(&data_projection)?;

        write_hash_u64(&mut hasher, entity.value());
        write_hash_u32(&mut hasher, persisted.version().get());
        write_hash_len_u32(&mut hasher, encoded.len());
        hasher.update(encoded);
    }

    Ok(finalize_schema_metadata(
        max_version,
        SCHEMA_STORE_FINGERPRINT_METHOD_VERSION,
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
        SCHEMA_STORE_INDEX_ALLOCATION_FINGERPRINT_DOMAIN,
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
        SCHEMA_STORE_FINGERPRINT_METHOD_VERSION,
        hasher,
        latest_by_entity.len(),
    ))
}

fn derive_schema_catalog_metadata(
    latest_by_entity: &StdBTreeMap<EntityTag, (SchemaVersion, RawSchemaSnapshot)>,
) -> Result<SchemaStoreCatalogMetadata, InternalError> {
    let mut max_version = SchemaVersion::initial();
    let mut hasher = new_hash_sha256();
    write_hash_tag_u8(&mut hasher, SCHEMA_STORE_CATALOG_FINGERPRINT_DOMAIN);

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
        SCHEMA_STORE_FINGERPRINT_METHOD_VERSION,
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
        AcceptedFieldKind::Bool => {
            write_hash_tag_u8(hasher, ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_BOOL);
        }
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
        } => {
            write_hash_tag_u8(hasher, 28);
            write_hash_str_u32(hasher, target_path);
            write_hash_str_u32(hasher, target_entity_name);
            write_hash_u64(hasher, target_entity_tag.value());
            write_hash_str_u32(hasher, target_store_path);
            hash_accepted_field_kind(hasher, key_kind);
        }
        AcceptedFieldKind::List(inner) => {
            write_hash_tag_u8(hasher, ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_LIST);
            hash_accepted_field_kind(hasher, inner);
        }
        AcceptedFieldKind::Set(inner) => {
            write_hash_tag_u8(hasher, ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_SET);
            hash_accepted_field_kind(hasher, inner);
        }
        AcceptedFieldKind::Map { key, value } => {
            write_hash_tag_u8(hasher, ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_MAP);
            hash_accepted_field_kind(hasher, key);
            hash_accepted_field_kind(hasher, value);
        }
        AcceptedFieldKind::Composite { type_id } => {
            write_hash_tag_u8(hasher, ACCEPTED_FIELD_KIND_FINGERPRINT_TAG_COMPOSITE);
            write_hash_u32(hasher, type_id.get());
        }
        AcceptedFieldKind::U256 => write_hash_tag_u8(hasher, 33),
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

///
/// TESTS
///

#[cfg(test)]
mod tests;
