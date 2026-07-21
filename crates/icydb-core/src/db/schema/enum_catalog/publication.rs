//! Module: db::schema::enum_catalog::publication
//! Responsibility: immutable accepted-schema revision bundles and root publication records.
//! Does not own: schema-store physical keys, journal commits, or runtime plan guards.
//! Boundary: canonical catalog/snapshots <-> bounded bundle and checksummed root bytes.

#[cfg(test)]
mod tests;

use super::{
    AcceptedEnumCatalog, AcceptedEnumVariantBody, decode_accepted_enum_catalog,
    encode_accepted_enum_catalog,
    equality_key::{EqualityCapability, enum_equality_capability},
};
use crate::db::schema::composite_catalog::{
    AcceptedCompositeCatalog, AcceptedCompositeElement, AcceptedCompositeShape,
    decode_accepted_composite_catalog, encode_accepted_composite_catalog,
};
use crate::{
    db::{
        codec::{finalize_hash_sha256, new_hash_sha256},
        data::validate_default_payload_for_accepted_field_contract,
        database_format::crc32c,
        schema::{
            AcceptedFieldDecodeContract, AcceptedFieldKind, AcceptedSchemaSnapshot,
            MAX_ACCEPTED_RECURSIVE_DEPTH, MAX_SCHEMA_SNAPSHOT_BYTES, PersistedFieldSnapshot,
            PersistedIndexFieldPathSnapshot, PersistedIndexKeyItemSnapshot,
            PersistedIndexKeySnapshot, PersistedSchemaSnapshot, classify_accepted_field_kind,
            decode_persisted_schema_snapshot, encode_persisted_schema_snapshot,
        },
    },
    error::InternalError,
    types::EntityTag,
};
use sha2::Digest;
use std::collections::BTreeMap;

const ACCEPTED_SCHEMA_BUNDLE_MAGIC: &[u8; 8] = b"ICYDBAEB";
const ACCEPTED_SCHEMA_BUNDLE_CODEC_VERSION: u16 = 1;
const ACCEPTED_SCHEMA_BUNDLE_HEADER_BYTES: usize = 30;
const ACCEPTED_SCHEMA_ROOT_MAGIC: &[u8; 8] = b"ICYDBAER";
const ACCEPTED_SCHEMA_ROOT_CODEC_VERSION: u16 = 1;
const ACCEPTED_SCHEMA_ROOT_BYTES: usize = 94;
const ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET: usize = ACCEPTED_SCHEMA_ROOT_BYTES - size_of::<u32>();
const ACCEPTED_SCHEMA_FINGERPRINT_PROFILE: &[u8] = b"icydb.accepted-schema.semantic.v1";
const MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES: usize = 16 * 1024 * 1024;
const MAX_SCHEMA_STORE_PATH_BYTES: usize = 4 * 1024;

/// Monotonic publication identity for one store-local accepted schema.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct AcceptedSchemaRevision(u64);

impl AcceptedSchemaRevision {
    pub(in crate::db) const NONE: Self = Self(0);
    pub(in crate::db) const INITIAL: Self = Self(1);

    #[must_use]
    pub(in crate::db) const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub(in crate::db) const fn get(self) -> u64 {
        self.0
    }

    pub(in crate::db) const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Semantic hash of accepted catalog and entity snapshot content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSchemaFingerprint([u8; 32]);

impl AcceptedSchemaFingerprint {
    #[must_use]
    pub(in crate::db) const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub(in crate::db) const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Immutable bundle lookup identity within one store's schema allocation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct AcceptedSchemaBundleKey(u64);

impl AcceptedSchemaBundleKey {
    fn new(revision: AcceptedSchemaRevision) -> Option<Self> {
        (revision != AcceptedSchemaRevision::NONE).then_some(Self(revision.get()))
    }

    #[must_use]
    pub(in crate::db) const fn get(self) -> u64 {
        self.0
    }
}

/// One immutable store-local accepted schema revision bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSchemaRevisionBundle {
    revision: AcceptedSchemaRevision,
    store_path: String,
    enum_catalog: AcceptedEnumCatalog,
    composite_catalog: AcceptedCompositeCatalog,
    entity_snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
}

impl AcceptedSchemaRevisionBundle {
    pub(in crate::db::schema) fn new(
        revision: AcceptedSchemaRevision,
        store_path: impl Into<String>,
        enum_catalog: AcceptedEnumCatalog,
        composite_catalog: AcceptedCompositeCatalog,
        entity_snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    ) -> Result<Self, InternalError> {
        let bundle = Self {
            revision,
            store_path: store_path.into(),
            enum_catalog,
            composite_catalog,
            entity_snapshots,
        };
        bundle.validate()?;
        Ok(bundle)
    }

    #[must_use]
    pub(in crate::db) const fn revision(&self) -> AcceptedSchemaRevision {
        self.revision
    }

    #[must_use]
    pub(in crate::db) fn store_path(&self) -> &str {
        &self.store_path
    }

    #[must_use]
    pub(in crate::db::schema) const fn enum_catalog(&self) -> &AcceptedEnumCatalog {
        &self.enum_catalog
    }

    #[must_use]
    pub(in crate::db::schema) const fn composite_catalog(&self) -> &AcceptedCompositeCatalog {
        &self.composite_catalog
    }

    #[must_use]
    pub(in crate::db) const fn entity_snapshots(
        &self,
    ) -> &BTreeMap<EntityTag, PersistedSchemaSnapshot> {
        &self.entity_snapshots
    }

    fn validate(&self) -> Result<(), InternalError> {
        if self.revision == AcceptedSchemaRevision::NONE
            || self.store_path.is_empty()
            || self.store_path.len() > MAX_SCHEMA_STORE_PATH_BYTES
        {
            return Err(InternalError::store_invariant());
        }
        let catalog_bytes = encode_accepted_enum_catalog(&self.enum_catalog)?;
        if decode_accepted_enum_catalog(&catalog_bytes)? != self.enum_catalog {
            return Err(InternalError::store_invariant());
        }
        let composite_catalog_bytes =
            encode_accepted_composite_catalog(&self.composite_catalog, &self.enum_catalog)?;
        if decode_accepted_composite_catalog(&composite_catalog_bytes, &self.enum_catalog)?
            != self.composite_catalog
        {
            return Err(InternalError::store_invariant());
        }
        if !catalog_relation_key_contracts_are_supported(
            &self.enum_catalog,
            &self.composite_catalog,
        ) {
            return Err(InternalError::store_invariant());
        }
        for snapshot in self.entity_snapshots.values() {
            let encoded = encode_persisted_schema_snapshot(snapshot)?;
            if encoded.len() > MAX_SCHEMA_SNAPSHOT_BYTES as usize {
                return Err(InternalError::store_unsupported());
            }
            AcceptedSchemaSnapshot::try_new(snapshot.clone())?;
            for field in snapshot.fields() {
                if !self
                    .composite_catalog
                    .matches_kind(&self.enum_catalog, field.kind())
                    || field.nested_leaves().iter().any(|leaf| {
                        !self
                            .composite_catalog
                            .matches_kind(&self.enum_catalog, leaf.kind())
                    })
                {
                    return Err(InternalError::store_invariant());
                }
                if !nested_leaf_contracts_match_composite_catalog(&self.composite_catalog, field)
                    || !relation_key_contracts_are_supported(field.kind())
                {
                    return Err(InternalError::store_invariant());
                }
                let contract = AcceptedFieldDecodeContract::new(
                    field.name(),
                    field.kind(),
                    field.nullable(),
                    field.storage_decode(),
                    field.leaf_codec(),
                );
                for payload in [
                    field.insert_default().slot_payload(),
                    field.historical_fill().slot_payload(),
                ]
                .into_iter()
                .flatten()
                {
                    validate_default_payload_for_accepted_field_contract(
                        &self.enum_catalog,
                        &self.composite_catalog,
                        contract,
                        payload,
                    )?;
                }
            }
            validate_primary_key_capabilities(snapshot)?;
            validate_index_capabilities(&self.enum_catalog, snapshot)?;
        }
        Ok(())
    }

    fn semantic_fingerprint(&self) -> Result<AcceptedSchemaFingerprint, InternalError> {
        let catalog_bytes = encode_accepted_enum_catalog(&self.enum_catalog)?;
        let composite_catalog_bytes =
            encode_accepted_composite_catalog(&self.composite_catalog, &self.enum_catalog)?;
        let mut hasher = new_hash_sha256();
        hasher.update(ACCEPTED_SCHEMA_FINGERPRINT_PROFILE);
        hash_len_prefixed(&mut hasher, self.store_path.as_bytes())?;
        hash_len_prefixed(&mut hasher, &catalog_bytes)?;
        hash_len_prefixed(&mut hasher, &composite_catalog_bytes)?;
        hash_len(&mut hasher, self.entity_snapshots.len())?;
        for (entity_tag, snapshot) in &self.entity_snapshots {
            hasher.update(entity_tag.value().to_be_bytes());
            hash_len_prefixed(&mut hasher, &encode_persisted_schema_snapshot(snapshot)?)?;
        }
        Ok(AcceptedSchemaFingerprint::new(finalize_hash_sha256(hasher)))
    }
}

fn nested_leaf_contracts_match_composite_catalog(
    catalog: &AcceptedCompositeCatalog,
    field: &PersistedFieldSnapshot,
) -> bool {
    let AcceptedFieldKind::Composite { type_id } = field.kind() else {
        return field.nested_leaves().is_empty();
    };
    let Some(definition) = catalog.composite_type(*type_id) else {
        return false;
    };
    let AcceptedCompositeShape::Record(fields) = definition.shape() else {
        return field.nested_leaves().is_empty();
    };

    let mut path = Vec::new();
    let mut expected_count = 0usize;
    fields.iter().all(|member| {
        nested_leaf_contract_matches(
            catalog,
            field,
            member.name(),
            member.contract(),
            &mut path,
            &mut expected_count,
            0,
        )
    }) && expected_count == field.nested_leaves().len()
}

fn nested_leaf_contract_matches(
    catalog: &AcceptedCompositeCatalog,
    field: &PersistedFieldSnapshot,
    name: &str,
    contract: &AcceptedCompositeElement,
    path: &mut Vec<String>,
    expected_count: &mut usize,
    depth: usize,
) -> bool {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return false;
    }
    path.push(name.to_string());
    let leaf_index = *expected_count;
    let Some(next_count) = expected_count.checked_add(1) else {
        path.pop();
        return false;
    };
    *expected_count = next_count;
    let matches = field.nested_leaves().get(leaf_index).is_some_and(|leaf| {
        let path_matches = leaf.path() == path.as_slice();
        let kind_matches = leaf.kind() == contract.kind();
        let nullability_matches = leaf.nullable() == contract.nullable();
        path_matches && kind_matches && nullability_matches
    });
    if !matches {
        path.pop();
        return false;
    }

    let nested_matches =
        match contract.kind() {
            AcceptedFieldKind::Composite { type_id } => catalog
                .composite_type(*type_id)
                .is_some_and(|definition| match definition.shape() {
                    AcceptedCompositeShape::Record(fields) => fields.iter().all(|member| {
                        nested_leaf_contract_matches(
                            catalog,
                            field,
                            member.name(),
                            member.contract(),
                            path,
                            expected_count,
                            depth.saturating_add(1),
                        )
                    }),
                    AcceptedCompositeShape::Tuple(_) | AcceptedCompositeShape::Newtype(_) => true,
                }),
            _ => true,
        };
    path.pop();
    nested_matches
}

// Relation roles may be nested inside nominal catalog definitions rather than
// appearing directly in an entity field. Validate every definition once at the
// publication boundary so runtime capability projection never sees a relation
// whose key contract is unsupported.
fn catalog_relation_key_contracts_are_supported(
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> bool {
    let enum_contracts_are_supported = enum_catalog.by_id.values().all(|definition| {
        definition
            .variants_by_id
            .values()
            .all(|variant| match variant.body() {
                AcceptedEnumVariantBody::Unit => true,
                AcceptedEnumVariantBody::Payload { contract } => {
                    relation_key_contracts_are_supported(contract.kind())
                }
            })
    });
    let composite_contracts_are_supported =
        composite_catalog.id_by_path().values().all(|type_id| {
            composite_catalog
                .composite_type(*type_id)
                .is_some_and(|definition| match definition.shape() {
                    AcceptedCompositeShape::Record(fields) => fields
                        .iter()
                        .all(|field| relation_key_contracts_are_supported(field.contract().kind())),
                    AcceptedCompositeShape::Tuple(elements) => elements
                        .iter()
                        .all(|element| relation_key_contracts_are_supported(element.kind())),
                    AcceptedCompositeShape::Newtype(inner) => {
                        relation_key_contracts_are_supported(inner.kind())
                    }
                })
        });

    enum_contracts_are_supported && composite_contracts_are_supported
}

fn relation_key_contracts_are_supported(kind: &AcceptedFieldKind) -> bool {
    match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => {
            classify_accepted_field_kind(key_kind).is_relation_key_eligible()
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            relation_key_contracts_are_supported(inner)
        }
        AcceptedFieldKind::Map { key, value } => {
            relation_key_contracts_are_supported(key) && relation_key_contracts_are_supported(value)
        }
        _ => true,
    }
}

fn validate_primary_key_capabilities(
    snapshot: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    for field_id in snapshot.primary_key_field_ids() {
        let Some(field) = snapshot
            .fields()
            .iter()
            .find(|field| field.id() == *field_id)
        else {
            return Err(InternalError::store_invariant());
        };
        let semantics = classify_accepted_field_kind(field.kind());
        if semantics.is_collection()
            || semantics.is_composite()
            || matches!(
                semantics.category(),
                crate::db::schema::AcceptedFieldKindCategory::Relation(None)
            )
        {
            return Err(InternalError::store_unsupported());
        }
    }

    Ok(())
}

fn validate_index_capabilities(
    catalog: &AcceptedEnumCatalog,
    snapshot: &PersistedSchemaSnapshot,
) -> Result<(), InternalError> {
    for index in snapshot.indexes() {
        match index.key() {
            PersistedIndexKeySnapshot::FieldPath(paths) => {
                for path in paths {
                    validate_index_path(catalog, path)?;
                }
            }
            PersistedIndexKeySnapshot::Items(items) => {
                for item in items {
                    match item {
                        PersistedIndexKeyItemSnapshot::FieldPath(path) => {
                            validate_index_path(catalog, path)?;
                        }
                        PersistedIndexKeyItemSnapshot::Expression(expression) => {
                            validate_index_path(catalog, expression.source())?;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn validate_index_path(
    catalog: &AcceptedEnumCatalog,
    path: &PersistedIndexFieldPathSnapshot,
) -> Result<(), InternalError> {
    let semantics = classify_accepted_field_kind(path.kind());
    if semantics.is_collection() || semantics.is_composite() {
        return Err(InternalError::store_unsupported());
    }
    let AcceptedFieldKind::Enum { type_id } = path.kind() else {
        return Ok(());
    };
    let capability = enum_equality_capability(catalog, *type_id)
        .map_err(|_| InternalError::store_invariant())?;
    if capability != EqualityCapability::CanonicalStableKey {
        return Err(InternalError::store_unsupported());
    }

    Ok(())
}

/// Failure-atomic current-root record. Two physical slots carry this codec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSchemaRoot {
    revision: AcceptedSchemaRevision,
    fingerprint: AcceptedSchemaFingerprint,
    bundle_key: AcceptedSchemaBundleKey,
    bundle_hash: [u8; 32],
}

impl AcceptedSchemaRoot {
    #[must_use]
    pub(in crate::db) const fn revision(self) -> AcceptedSchemaRevision {
        self.revision
    }

    #[must_use]
    pub(in crate::db::schema) const fn fingerprint(self) -> AcceptedSchemaFingerprint {
        self.fingerprint
    }

    #[must_use]
    pub(in crate::db) const fn bundle_key(self) -> AcceptedSchemaBundleKey {
        self.bundle_key
    }

    fn validate(self) -> Result<(), InternalError> {
        if self.revision == AcceptedSchemaRevision::NONE
            || self.bundle_key.get() != self.revision.get()
        {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }
}

/// Fully encoded candidate whose bundle and root agree byte-for-byte.
#[derive(Clone, Debug)]
pub(in crate::db) struct CandidateSchemaRevision {
    store_path: String,
    bundle: AcceptedSchemaRevisionBundle,
    encoded_bundle: Vec<u8>,
    root: AcceptedSchemaRoot,
    encoded_root: Vec<u8>,
}

impl CandidateSchemaRevision {
    pub(in crate::db::schema) fn new(
        bundle: AcceptedSchemaRevisionBundle,
    ) -> Result<Self, InternalError> {
        let store_path = bundle.store_path().to_string();
        let encoded_bundle = encode_accepted_schema_revision_bundle(&bundle)?;
        let decoded_bundle = decode_accepted_schema_revision_bundle(&encoded_bundle)?;
        if decoded_bundle != bundle {
            return Err(InternalError::store_invariant());
        }

        let bundle_hash = hash_bytes(&encoded_bundle);
        let root = AcceptedSchemaRoot {
            revision: bundle.revision(),
            fingerprint: bundle.semantic_fingerprint()?,
            bundle_key: AcceptedSchemaBundleKey::new(bundle.revision())
                .ok_or_else(InternalError::store_invariant)?,
            bundle_hash,
        };
        let encoded_root = encode_accepted_schema_root(root)?;
        if decode_accepted_schema_root(&encoded_root)? != root {
            return Err(InternalError::store_invariant());
        }

        Ok(Self {
            store_path,
            bundle,
            encoded_bundle,
            root,
            encoded_root,
        })
    }

    /// Reconstruct one candidate exclusively from persisted journal bytes.
    pub(in crate::db) fn from_encoded(
        encoded_bundle: Vec<u8>,
        encoded_root: Vec<u8>,
    ) -> Result<Self, InternalError> {
        let root = decode_accepted_schema_root(&encoded_root)?;
        let bundle = decode_verified_accepted_schema_revision_bundle(root, &encoded_bundle)?;
        Ok(Self {
            store_path: bundle.store_path().to_string(),
            bundle,
            encoded_bundle,
            root,
            encoded_root,
        })
    }

    #[must_use]
    pub(in crate::db) const fn revision(&self) -> AcceptedSchemaRevision {
        self.root.revision()
    }

    #[must_use]
    pub(in crate::db) fn store_path(&self) -> &str {
        &self.store_path
    }

    #[must_use]
    pub(in crate::db) const fn bundle(&self) -> &AcceptedSchemaRevisionBundle {
        &self.bundle
    }

    #[must_use]
    pub(in crate::db) const fn root(&self) -> AcceptedSchemaRoot {
        self.root
    }

    #[must_use]
    pub(in crate::db) fn encoded_bundle(&self) -> &[u8] {
        &self.encoded_bundle
    }

    #[must_use]
    pub(in crate::db) fn encoded_root(&self) -> &[u8] {
        &self.encoded_root
    }
}

/// Selected current root and the physical slot that supplied it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSchemaRootSelection {
    slot: usize,
    root: AcceptedSchemaRoot,
}

impl AcceptedSchemaRootSelection {
    #[must_use]
    pub(in crate::db) const fn slot(self) -> usize {
        self.slot
    }

    #[must_use]
    pub(in crate::db) const fn root(self) -> AcceptedSchemaRoot {
        self.root
    }
}

/// Prepared inactive-slot write after expected-revision validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSchemaRootPublication {
    target_slot: usize,
    encoded_root: Vec<u8>,
}

impl AcceptedSchemaRootPublication {
    #[must_use]
    pub(in crate::db) const fn target_slot(&self) -> usize {
        self.target_slot
    }

    #[must_use]
    pub(in crate::db) fn encoded_root(&self) -> &[u8] {
        &self.encoded_root
    }
}

/// Typed expected-revision publication failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedSchemaPublicationError {
    StaleSchemaRevision {
        expected: AcceptedSchemaRevision,
        found: AcceptedSchemaRevision,
    },
    RevisionExhausted,
    InvalidCandidate,
    CorruptRootSlots,
}

/// Select the highest valid root while tolerating one torn/corrupt slot.
pub(in crate::db::schema) fn select_current_accepted_schema_root(
    slots: [Option<&[u8]>; 2],
) -> Result<Option<AcceptedSchemaRootSelection>, InternalError> {
    let first = classify_root_slot(slots[0])?;
    let second = classify_root_slot(slots[1])?;
    let selected = match (first, second) {
        (RootSlotState::Absent, RootSlotState::Absent) => None,
        (RootSlotState::Valid(root), RootSlotState::Absent | RootSlotState::Invalid) => {
            Some(AcceptedSchemaRootSelection { slot: 0, root })
        }
        (RootSlotState::Absent | RootSlotState::Invalid, RootSlotState::Valid(root)) => {
            Some(AcceptedSchemaRootSelection { slot: 1, root })
        }
        (RootSlotState::Valid(first), RootSlotState::Valid(second)) => {
            if first.revision() == second.revision() && first != second {
                return Err(InternalError::store_corruption());
            }
            let (slot, root) = if second.revision() >= first.revision() {
                (1, second)
            } else {
                (0, first)
            };
            Some(AcceptedSchemaRootSelection { slot, root })
        }
        (RootSlotState::Absent | RootSlotState::Invalid, RootSlotState::Invalid)
        | (RootSlotState::Invalid, RootSlotState::Absent) => {
            return Err(InternalError::store_corruption());
        }
    };
    Ok(selected)
}

/// Validate expected revision and prepare the inactive root-slot write.
pub(in crate::db::schema) fn prepare_accepted_schema_root_publication(
    slots: [Option<&[u8]>; 2],
    expected_revision: AcceptedSchemaRevision,
    candidate: &CandidateSchemaRevision,
) -> Result<AcceptedSchemaRootPublication, AcceptedSchemaPublicationError> {
    let current = select_current_accepted_schema_root(slots)
        .map_err(|_| AcceptedSchemaPublicationError::CorruptRootSlots)?;
    let found_revision = current.map_or(AcceptedSchemaRevision::NONE, |selection| {
        selection.root().revision()
    });
    if found_revision != expected_revision {
        return Err(AcceptedSchemaPublicationError::StaleSchemaRevision {
            expected: expected_revision,
            found: found_revision,
        });
    }
    let expected_candidate_revision = expected_revision
        .checked_next()
        .ok_or(AcceptedSchemaPublicationError::RevisionExhausted)?;
    if candidate.revision() != expected_candidate_revision
        || hash_bytes(candidate.encoded_bundle()) != candidate.root.bundle_hash
        || decode_accepted_schema_revision_bundle(candidate.encoded_bundle()).is_err()
    {
        return Err(AcceptedSchemaPublicationError::InvalidCandidate);
    }

    Ok(AcceptedSchemaRootPublication {
        target_slot: current.map_or(0, |selection| 1usize.saturating_sub(selection.slot())),
        encoded_root: candidate.encoded_root.clone(),
    })
}

pub(in crate::db::schema) fn decode_accepted_schema_revision_bundle(
    bytes: &[u8],
) -> Result<AcceptedSchemaRevisionBundle, InternalError> {
    if bytes.len() < ACCEPTED_SCHEMA_BUNDLE_HEADER_BYTES
        || bytes.len() > MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES
    {
        return Err(InternalError::store_corruption());
    }

    let mut reader = BundleReader::new(bytes);
    if reader.read_array::<8>()? != *ACCEPTED_SCHEMA_BUNDLE_MAGIC {
        return Err(InternalError::store_corruption());
    }
    if reader.read_u16()? != ACCEPTED_SCHEMA_BUNDLE_CODEC_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }
    let revision = AcceptedSchemaRevision::new(reader.read_u64()?);
    let store_path = reader.read_string()?;
    let catalog = decode_accepted_enum_catalog(reader.read_len_prefixed_bytes()?)?;
    let composite_catalog =
        decode_accepted_composite_catalog(reader.read_len_prefixed_bytes()?, &catalog)?;
    let entity_count = reader.read_count()?;
    let mut entity_snapshots = BTreeMap::new();
    let mut previous_entity_tag = None;
    for _ in 0..entity_count {
        let entity_tag = EntityTag::new(reader.read_u64()?);
        if previous_entity_tag.is_some_and(|previous| entity_tag <= previous) {
            return Err(InternalError::store_corruption());
        }
        let snapshot_bytes = reader.read_len_prefixed_bytes()?;
        if snapshot_bytes.len() > MAX_SCHEMA_SNAPSHOT_BYTES as usize {
            return Err(InternalError::store_corruption());
        }
        let snapshot = decode_persisted_schema_snapshot(snapshot_bytes)?;
        if entity_snapshots.insert(entity_tag, snapshot).is_some() {
            return Err(InternalError::store_corruption());
        }
        previous_entity_tag = Some(entity_tag);
    }
    reader.finish()?;

    let bundle = AcceptedSchemaRevisionBundle {
        revision,
        store_path,
        enum_catalog: catalog,
        composite_catalog,
        entity_snapshots,
    };
    bundle
        .validate()
        .map_err(|_| InternalError::store_corruption())?;
    Ok(bundle)
}

/// Decode one bundle and verify every root-bound identity before returning it.
pub(in crate::db::schema) fn decode_verified_accepted_schema_revision_bundle(
    root: AcceptedSchemaRoot,
    bytes: &[u8],
) -> Result<AcceptedSchemaRevisionBundle, InternalError> {
    if root.bundle_hash != hash_bytes(bytes) {
        return Err(InternalError::store_corruption());
    }
    let bundle = decode_accepted_schema_revision_bundle(bytes)?;
    if bundle.revision() != root.revision() || bundle.semantic_fingerprint()? != root.fingerprint {
        return Err(InternalError::store_corruption());
    }
    Ok(bundle)
}

fn encode_accepted_schema_revision_bundle(
    bundle: &AcceptedSchemaRevisionBundle,
) -> Result<Vec<u8>, InternalError> {
    bundle.validate()?;
    let catalog_bytes = encode_accepted_enum_catalog(&bundle.enum_catalog)?;
    let composite_catalog_bytes =
        encode_accepted_composite_catalog(&bundle.composite_catalog, &bundle.enum_catalog)?;
    let mut writer = BundleWriter::new();
    writer.push_bytes(ACCEPTED_SCHEMA_BUNDLE_MAGIC);
    writer.push_u16(ACCEPTED_SCHEMA_BUNDLE_CODEC_VERSION);
    writer.push_u64(bundle.revision().get());
    writer.push_string(bundle.store_path())?;
    writer.push_len_prefixed_bytes(&catalog_bytes)?;
    writer.push_len_prefixed_bytes(&composite_catalog_bytes)?;
    writer.push_len(bundle.entity_snapshots.len())?;
    for (entity_tag, snapshot) in &bundle.entity_snapshots {
        writer.push_u64(entity_tag.value());
        writer.push_len_prefixed_bytes(&encode_persisted_schema_snapshot(snapshot)?)?;
    }
    writer.finish()
}

fn encode_accepted_schema_root(root: AcceptedSchemaRoot) -> Result<Vec<u8>, InternalError> {
    root.validate()
        .map_err(|_| InternalError::store_invariant())?;
    let mut bytes = Vec::with_capacity(ACCEPTED_SCHEMA_ROOT_BYTES);
    bytes.extend_from_slice(ACCEPTED_SCHEMA_ROOT_MAGIC);
    bytes.extend_from_slice(&ACCEPTED_SCHEMA_ROOT_CODEC_VERSION.to_be_bytes());
    bytes.extend_from_slice(&root.revision().get().to_be_bytes());
    bytes.extend_from_slice(&root.fingerprint.as_bytes());
    bytes.extend_from_slice(&root.bundle_key.get().to_be_bytes());
    bytes.extend_from_slice(&root.bundle_hash);
    bytes.extend_from_slice(&crc32c(&bytes).to_be_bytes());
    if bytes.len() != ACCEPTED_SCHEMA_ROOT_BYTES {
        return Err(InternalError::store_invariant());
    }
    Ok(bytes)
}

fn decode_accepted_schema_root(bytes: &[u8]) -> Result<AcceptedSchemaRoot, InternalError> {
    if bytes.len() != ACCEPTED_SCHEMA_ROOT_BYTES
        || bytes.get(..ACCEPTED_SCHEMA_ROOT_MAGIC.len()) != Some(ACCEPTED_SCHEMA_ROOT_MAGIC)
    {
        return Err(InternalError::store_corruption());
    }
    let version_offset = ACCEPTED_SCHEMA_ROOT_MAGIC.len();
    let version = read_u16_at(bytes, version_offset)?;
    if version != ACCEPTED_SCHEMA_ROOT_CODEC_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }
    let checksum = read_u32_at(bytes, ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET)?;
    if checksum != crc32c(&bytes[..ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET]) {
        return Err(InternalError::store_corruption());
    }

    let revision_offset = version_offset + size_of::<u16>();
    let revision = AcceptedSchemaRevision::new(read_u64_at(bytes, revision_offset)?);
    let fingerprint_offset = revision_offset + size_of::<u64>();
    let fingerprint = AcceptedSchemaFingerprint::new(read_array_at(bytes, fingerprint_offset)?);
    let bundle_key_offset = fingerprint_offset + size_of::<[u8; 32]>();
    let bundle_key = AcceptedSchemaBundleKey(read_u64_at(bytes, bundle_key_offset)?);
    let bundle_hash_offset = bundle_key_offset + size_of::<u64>();
    let bundle_hash = read_array_at(bytes, bundle_hash_offset)?;
    let root = AcceptedSchemaRoot {
        revision,
        fingerprint,
        bundle_key,
        bundle_hash,
    };
    root.validate()?;
    Ok(root)
}

enum RootSlotState {
    Absent,
    Invalid,
    Valid(AcceptedSchemaRoot),
}

fn classify_root_slot(bytes: Option<&[u8]>) -> Result<RootSlotState, InternalError> {
    let Some(bytes) = bytes else {
        return Ok(RootSlotState::Absent);
    };
    if bytes.len() != ACCEPTED_SCHEMA_ROOT_BYTES {
        return Ok(RootSlotState::Invalid);
    }
    if bytes.get(..ACCEPTED_SCHEMA_ROOT_MAGIC.len()) != Some(ACCEPTED_SCHEMA_ROOT_MAGIC) {
        return Ok(RootSlotState::Invalid);
    }
    let checksum = read_u32_at(bytes, ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET)?;
    if checksum != crc32c(&bytes[..ACCEPTED_SCHEMA_ROOT_CHECKSUM_OFFSET]) {
        return Ok(RootSlotState::Invalid);
    }
    let version = read_u16_at(bytes, ACCEPTED_SCHEMA_ROOT_MAGIC.len())?;
    if version != ACCEPTED_SCHEMA_ROOT_CODEC_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }
    match decode_accepted_schema_root(bytes) {
        Ok(root) => Ok(RootSlotState::Valid(root)),
        Err(_) => Ok(RootSlotState::Invalid),
    }
}

fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = new_hash_sha256();
    hasher.update(bytes);
    finalize_hash_sha256(hasher)
}

fn hash_len(hasher: &mut sha2::Sha256, len: usize) -> Result<(), InternalError> {
    let len = u32::try_from(len).map_err(|_| InternalError::store_unsupported())?;
    hasher.update(len.to_be_bytes());
    Ok(())
}

fn hash_len_prefixed(hasher: &mut sha2::Sha256, bytes: &[u8]) -> Result<(), InternalError> {
    hash_len(hasher, bytes.len())?;
    hasher.update(bytes);
    Ok(())
}

fn read_u16_at(bytes: &[u8], offset: usize) -> Result<u16, InternalError> {
    Ok(u16::from_be_bytes(read_array_at(bytes, offset)?))
}

fn read_u32_at(bytes: &[u8], offset: usize) -> Result<u32, InternalError> {
    Ok(u32::from_be_bytes(read_array_at(bytes, offset)?))
}

fn read_u64_at(bytes: &[u8], offset: usize) -> Result<u64, InternalError> {
    Ok(u64::from_be_bytes(read_array_at(bytes, offset)?))
}

fn read_array_at<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], InternalError> {
    let end = offset
        .checked_add(N)
        .ok_or_else(InternalError::store_corruption)?;
    let slice = bytes
        .get(offset..end)
        .ok_or_else(InternalError::store_corruption)?;
    let mut value = [0_u8; N];
    value.copy_from_slice(slice);
    Ok(value)
}

struct BundleWriter {
    bytes: Vec<u8>,
    overflowed: bool,
}

impl BundleWriter {
    const fn new() -> Self {
        Self {
            bytes: Vec::new(),
            overflowed: false,
        }
    }

    fn push_u16(&mut self, value: u16) {
        self.push_bytes(&value.to_be_bytes());
    }

    fn push_u64(&mut self, value: u64) {
        self.push_bytes(&value.to_be_bytes());
    }

    fn push_len(&mut self, value: usize) -> Result<(), InternalError> {
        let value = u32::try_from(value).map_err(|_| InternalError::store_unsupported())?;
        self.push_bytes(&value.to_be_bytes());
        Ok(())
    }

    fn push_string(&mut self, value: &str) -> Result<(), InternalError> {
        self.push_len_prefixed_bytes(value.as_bytes())
    }

    fn push_len_prefixed_bytes(&mut self, value: &[u8]) -> Result<(), InternalError> {
        self.push_len(value.len())?;
        self.push_bytes(value);
        Ok(())
    }

    fn push_bytes(&mut self, value: &[u8]) {
        if value.len() > MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES.saturating_sub(self.bytes.len()) {
            self.overflowed = true;
            return;
        }
        self.bytes.extend_from_slice(value);
    }

    fn finish(self) -> Result<Vec<u8>, InternalError> {
        if self.overflowed {
            return Err(InternalError::store_unsupported());
        }
        Ok(self.bytes)
    }
}

struct BundleReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> BundleReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_u16(&mut self) -> Result<u16, InternalError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    fn read_u32(&mut self) -> Result<u32, InternalError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, InternalError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_count(&mut self) -> Result<usize, InternalError> {
        let count = self.read_u32()? as usize;
        if count > self.remaining() {
            return Err(InternalError::store_corruption());
        }
        Ok(count)
    }

    fn read_string(&mut self) -> Result<String, InternalError> {
        let bytes = self.read_len_prefixed_bytes()?;
        let value = std::str::from_utf8(bytes).map_err(|_| InternalError::store_corruption())?;
        Ok(value.to_string())
    }

    fn read_len_prefixed_bytes(&mut self) -> Result<&'a [u8], InternalError> {
        let len = self.read_u32()? as usize;
        self.read_slice(len)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], InternalError> {
        let slice = self.read_slice(N)?;
        let mut value = [0_u8; N];
        value.copy_from_slice(slice);
        Ok(value)
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], InternalError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(InternalError::store_corruption)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(InternalError::store_corruption)?;
        self.offset = end;
        Ok(slice)
    }

    fn finish(self) -> Result<(), InternalError> {
        if self.offset != self.bytes.len() {
            return Err(InternalError::store_corruption());
        }
        Ok(())
    }
}

#[cfg(test)]
pub(in crate::db) fn empty_accepted_schema_candidate_for_tests(
    store_path: &str,
    revision: AcceptedSchemaRevision,
) -> CandidateSchemaRevision {
    let bundle = AcceptedSchemaRevisionBundle::new(
        revision,
        store_path,
        AcceptedEnumCatalog {
            by_id: BTreeMap::new(),
            id_by_path: BTreeMap::new(),
        },
        AcceptedCompositeCatalog::empty(),
        BTreeMap::new(),
    )
    .expect("empty accepted schema candidate fixture should build");
    CandidateSchemaRevision::new(bundle)
        .expect("empty accepted schema candidate fixture should encode")
}
