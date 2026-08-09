//! Durable accepted per-entity generated-source lineage.
//!
//! This module owns the bounded current lineage value. The generalized
//! database-control map owns its reserved key and publication.

use std::collections::BTreeMap;

use icydb_schema::{
    EntitySourceDigest, ExpectedAcceptedHead, ExpectedSchemaFingerprint, TargetStoreIdentity,
};

use crate::{
    db::{
        database_format::crc32c,
        schema::wire::{SchemaWireReader, SchemaWireWriter},
    },
    error::InternalError,
    types::EntityTag,
};

pub(in crate::db::schema) const MAX_ENTITY_SOURCE_LINEAGE_BYTES: usize = 2 * 1024 * 1024;
pub(in crate::db::schema) const MAX_ENTITY_SOURCE_LINEAGE_ENTRIES: usize = 4_096;
const LINEAGE_MAGIC: &[u8; 8] = b"ICYSLIN1";
const LINEAGE_VERSION: u8 = 1;
const LINEAGE_HEADER_BYTES: usize = 8 + 1 + 4 + 4;
const LINEAGE_CHECKSUM_BYTES: usize = 4;

type LineageWriter = SchemaWireWriter<MAX_ENTITY_SOURCE_LINEAGE_BYTES>;
type LineageReader<'a> = SchemaWireReader<'a>;

/// Positive accepted source version for one generated-owned entity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db::schema) struct AcceptedEntitySourceVersion(u32);

impl AcceptedEntitySourceVersion {
    pub(in crate::db::schema) fn try_new(value: u32) -> Result<Self, InternalError> {
        if value == 0 {
            return Err(InternalError::store_invariant());
        }
        Ok(Self(value))
    }

    #[must_use]
    pub(in crate::db::schema) const fn get(self) -> u32 {
        self.0
    }
}

/// Accepted lineage state. Unadopted never invents a version or digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum AcceptedEntitySourceLineageState {
    Unadopted,
    Adopted {
        version: AcceptedEntitySourceVersion,
        source_digest: EntitySourceDigest,
    },
}

/// One exact accepted entity-source lineage fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct AcceptedEntitySourceLineage {
    accepted_head: ExpectedAcceptedHead,
    state: AcceptedEntitySourceLineageState,
}

impl AcceptedEntitySourceLineage {
    pub(in crate::db::schema) fn unadopted(
        accepted_head: ExpectedAcceptedHead,
    ) -> Result<Self, InternalError> {
        validate_lineage_head(&accepted_head)?;
        Ok(Self {
            accepted_head,
            state: AcceptedEntitySourceLineageState::Unadopted,
        })
    }

    pub(in crate::db::schema) fn adopted(
        accepted_head: ExpectedAcceptedHead,
        version: AcceptedEntitySourceVersion,
        source_digest: EntitySourceDigest,
    ) -> Result<Self, InternalError> {
        validate_lineage_head(&accepted_head)?;
        if source_digest.to_bytes() == [0; 32] {
            return Err(InternalError::store_invariant());
        }
        Ok(Self {
            accepted_head,
            state: AcceptedEntitySourceLineageState::Adopted {
                version,
                source_digest,
            },
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn accepted_head(&self) -> &ExpectedAcceptedHead {
        &self.accepted_head
    }

    #[must_use]
    pub(in crate::db::schema) const fn state(&self) -> &AcceptedEntitySourceLineageState {
        &self.state
    }
}

/// Canonical database-wide accepted entity-source lineage catalog.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db::schema) struct AcceptedEntitySourceLineageCatalog {
    entries: BTreeMap<(TargetStoreIdentity, EntityTag), AcceptedEntitySourceLineage>,
}

/// Exact compare-and-replace effect for the reserved lineage record.
///
/// The encoded values are retained in the commit marker so recovery never
/// reconstructs lineage from generated models or the current deployment.
#[derive(Clone, Debug)]
pub(in crate::db) struct EntitySourceLineageCatalogOp {
    before: Option<Vec<u8>>,
    after: Vec<u8>,
}

impl EntitySourceLineageCatalogOp {
    pub(in crate::db::schema) fn replace(
        before: Option<&AcceptedEntitySourceLineageCatalog>,
        after: &AcceptedEntitySourceLineageCatalog,
    ) -> Result<Self, InternalError> {
        Self::from_encoded(
            before
                .map(encode_entity_source_lineage_catalog)
                .transpose()?,
            encode_entity_source_lineage_catalog(after)?,
        )
    }

    pub(in crate::db) fn from_encoded(
        before: Option<Vec<u8>>,
        after: Vec<u8>,
    ) -> Result<Self, InternalError> {
        let operation = Self { before, after };
        operation.validate()?;
        Ok(operation)
    }

    pub(in crate::db) fn before_bytes(&self) -> Option<&[u8]> {
        self.before.as_deref()
    }

    pub(in crate::db) const fn after_bytes(&self) -> &[u8] {
        self.after.as_slice()
    }

    pub(in crate::db) fn validate(&self) -> Result<(), InternalError> {
        let after = decode_entity_source_lineage_catalog(&self.after)?;
        if let Some(before) = self.before.as_deref() {
            let before = decode_entity_source_lineage_catalog(before)?;
            if before == after {
                return Err(InternalError::store_corruption());
            }
        }
        Ok(())
    }
}

impl AcceptedEntitySourceLineageCatalog {
    pub(in crate::db::schema) fn try_new(
        entries: BTreeMap<(TargetStoreIdentity, EntityTag), AcceptedEntitySourceLineage>,
    ) -> Result<Self, InternalError> {
        if entries.len() > MAX_ENTITY_SOURCE_LINEAGE_ENTRIES
            || entries.keys().any(|(_, entity)| entity.value() == 0)
        {
            return Err(InternalError::store_unsupported());
        }
        let catalog = Self { entries };
        let _ = encode_entity_source_lineage_catalog(&catalog)?;
        Ok(catalog)
    }

    #[must_use]
    pub(in crate::db::schema) const fn entries(
        &self,
    ) -> &BTreeMap<(TargetStoreIdentity, EntityTag), AcceptedEntitySourceLineage> {
        &self.entries
    }

    #[must_use]
    pub(in crate::db::schema) fn get(
        &self,
        store: TargetStoreIdentity,
        entity: EntityTag,
    ) -> Option<&AcceptedEntitySourceLineage> {
        self.entries.get(&(store, entity))
    }

    #[cfg(test)]
    pub(in crate::db::schema) fn insert(
        &mut self,
        store: TargetStoreIdentity,
        entity: EntityTag,
        lineage: AcceptedEntitySourceLineage,
    ) -> Result<(), InternalError> {
        if entity.value() == 0
            || self.entries.len() >= MAX_ENTITY_SOURCE_LINEAGE_ENTRIES
            || self.entries.insert((store, entity), lineage).is_some()
        {
            return Err(InternalError::store_unsupported());
        }
        Ok(())
    }
}

#[cfg(test)]
pub(in crate::db) fn unadopted_entity_source_lineage_op_for_tests(
    store: TargetStoreIdentity,
    entity: EntityTag,
    accepted_head: ExpectedAcceptedHead,
) -> Result<EntitySourceLineageCatalogOp, InternalError> {
    let catalog = AcceptedEntitySourceLineageCatalog::try_new(BTreeMap::from([(
        (store, entity),
        AcceptedEntitySourceLineage::unadopted(accepted_head)?,
    )]))?;
    EntitySourceLineageCatalogOp::replace(None, &catalog)
}

pub(in crate::db::schema) fn encode_entity_source_lineage_catalog(
    catalog: &AcceptedEntitySourceLineageCatalog,
) -> Result<Vec<u8>, InternalError> {
    if catalog.entries.len() > MAX_ENTITY_SOURCE_LINEAGE_ENTRIES {
        return Err(InternalError::store_unsupported());
    }
    let mut writer = LineageWriter::new();
    writer.push_bytes(LINEAGE_MAGIC);
    writer.push_u8(LINEAGE_VERSION);
    writer.push_u32(0);
    writer.push_u32(
        u32::try_from(catalog.entries.len()).map_err(|_| InternalError::store_unsupported())?,
    );
    for ((store, entity), lineage) in catalog.entries() {
        writer.push_bytes(&store.to_bytes());
        writer.push_u64(entity.value());
        encode_lineage_head(&mut writer, lineage.accepted_head())?;
        match lineage.state() {
            AcceptedEntitySourceLineageState::Unadopted => writer.push_u8(0),
            AcceptedEntitySourceLineageState::Adopted {
                version,
                source_digest,
            } => {
                writer.push_u8(1);
                writer.push_u32(version.get());
                writer.push_bytes(&source_digest.to_bytes());
            }
        }
    }
    let mut encoded = writer.finish()?;
    let payload_len = encoded
        .len()
        .checked_sub(LINEAGE_HEADER_BYTES)
        .ok_or_else(InternalError::store_invariant)?;
    encoded[9..13].copy_from_slice(
        &u32::try_from(payload_len)
            .map_err(|_| InternalError::store_unsupported())?
            .to_be_bytes(),
    );
    if encoded.len() > MAX_ENTITY_SOURCE_LINEAGE_BYTES.saturating_sub(LINEAGE_CHECKSUM_BYTES) {
        return Err(InternalError::store_unsupported());
    }
    encoded.extend_from_slice(&crc32c(&encoded).to_be_bytes());
    Ok(encoded)
}

pub(in crate::db::schema) fn decode_entity_source_lineage_catalog(
    bytes: &[u8],
) -> Result<AcceptedEntitySourceLineageCatalog, InternalError> {
    if bytes.len() < LINEAGE_HEADER_BYTES + LINEAGE_CHECKSUM_BYTES
        || bytes.len() > MAX_ENTITY_SOURCE_LINEAGE_BYTES
    {
        return Err(InternalError::store_corruption());
    }
    let checksum_offset = bytes
        .len()
        .checked_sub(LINEAGE_CHECKSUM_BYTES)
        .ok_or_else(InternalError::store_corruption)?;
    let (body, checksum) = bytes.split_at(checksum_offset);
    if crc32c(body)
        != u32::from_be_bytes(
            checksum
                .try_into()
                .map_err(|_| InternalError::store_corruption())?,
        )
    {
        return Err(InternalError::store_corruption());
    }
    let mut reader = LineageReader::new(body);
    if reader.read_array::<8>()? != *LINEAGE_MAGIC || reader.read_u8()? != LINEAGE_VERSION {
        return Err(InternalError::store_corruption());
    }
    let payload_len =
        usize::try_from(reader.read_u32()?).map_err(|_| InternalError::store_corruption())?;
    if payload_len != body.len().saturating_sub(LINEAGE_HEADER_BYTES) {
        return Err(InternalError::store_corruption());
    }
    let count =
        usize::try_from(reader.read_u32()?).map_err(|_| InternalError::store_corruption())?;
    if count > MAX_ENTITY_SOURCE_LINEAGE_ENTRIES {
        return Err(InternalError::store_corruption());
    }
    let mut entries = BTreeMap::new();
    let mut prior = None;
    for _ in 0..count {
        let store = TargetStoreIdentity::from_bytes(reader.read_array()?);
        let entity = EntityTag::new(reader.read_u64()?);
        let key = (store, entity);
        if prior.is_some_and(|prior| prior >= key) {
            return Err(InternalError::store_corruption());
        }
        let accepted_head = decode_lineage_head(&mut reader)?;
        let lineage = match reader.read_u8()? {
            0 => AcceptedEntitySourceLineage::unadopted(accepted_head)
                .map_err(|_| InternalError::store_corruption())?,
            1 => AcceptedEntitySourceLineage::adopted(
                accepted_head,
                AcceptedEntitySourceVersion::try_new(reader.read_u32()?)
                    .map_err(|_| InternalError::store_corruption())?,
                EntitySourceDigest::from_bytes(reader.read_array()?),
            )
            .map_err(|_| InternalError::store_corruption())?,
            _ => return Err(InternalError::store_corruption()),
        };
        if entries.insert(key, lineage).is_some() {
            return Err(InternalError::store_corruption());
        }
        prior = Some(key);
    }
    reader.finish()?;
    let catalog = AcceptedEntitySourceLineageCatalog::try_new(entries)
        .map_err(|_| InternalError::store_corruption())?;
    if encode_entity_source_lineage_catalog(&catalog)? != bytes {
        return Err(InternalError::store_corruption());
    }
    Ok(catalog)
}

fn encode_lineage_head(
    writer: &mut LineageWriter,
    head: &ExpectedAcceptedHead,
) -> Result<(), InternalError> {
    let ExpectedAcceptedHead::Exact {
        revision,
        fingerprint,
    } = head
    else {
        return Err(InternalError::store_invariant());
    };
    if *revision == 0 || fingerprint.to_bytes() == [0; 32] {
        return Err(InternalError::store_invariant());
    }
    writer.push_u64(*revision);
    writer.push_bytes(&fingerprint.to_bytes());
    Ok(())
}

fn decode_lineage_head(
    reader: &mut LineageReader<'_>,
) -> Result<ExpectedAcceptedHead, InternalError> {
    let revision = reader.read_u64()?;
    let fingerprint = ExpectedSchemaFingerprint::from_bytes(reader.read_array()?);
    let head = ExpectedAcceptedHead::Exact {
        revision,
        fingerprint,
    };
    validate_lineage_head(&head).map_err(|_| InternalError::store_corruption())?;
    Ok(head)
}

fn validate_lineage_head(head: &ExpectedAcceptedHead) -> Result<(), InternalError> {
    match head {
        ExpectedAcceptedHead::Exact {
            revision,
            fingerprint,
        } if *revision != 0 && fingerprint.to_bytes() != [0; 32] => Ok(()),
        ExpectedAcceptedHead::Empty | ExpectedAcceptedHead::Exact { .. } => {
            Err(InternalError::store_invariant())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exact_head(byte: u8) -> ExpectedAcceptedHead {
        ExpectedAcceptedHead::Exact {
            revision: u64::from(byte),
            fingerprint: ExpectedSchemaFingerprint::from_bytes([byte; 32]),
        }
    }

    #[test]
    fn lineage_codec_round_trips_adopted_and_unadopted_current_form() {
        let store = TargetStoreIdentity::from_bytes([7; 32]);
        let mut catalog = AcceptedEntitySourceLineageCatalog::default();
        catalog
            .insert(
                store,
                EntityTag::new(1),
                AcceptedEntitySourceLineage::unadopted(exact_head(3))
                    .expect("unadopted lineage should admit"),
            )
            .expect("entry should insert");
        catalog
            .insert(
                store,
                EntityTag::new(2),
                AcceptedEntitySourceLineage::adopted(
                    exact_head(3),
                    AcceptedEntitySourceVersion::try_new(4).expect("version should admit"),
                    EntitySourceDigest::from_bytes([9; 32]),
                )
                .expect("adopted lineage should admit"),
            )
            .expect("entry should insert");

        let encoded =
            encode_entity_source_lineage_catalog(&catalog).expect("lineage catalog should encode");
        assert_eq!(
            decode_entity_source_lineage_catalog(&encoded).expect("lineage catalog should decode"),
            catalog,
        );
    }

    #[test]
    fn lineage_codec_rejects_checksum_length_version_and_noncanonical_order() {
        let encoded =
            encode_entity_source_lineage_catalog(&AcceptedEntitySourceLineageCatalog::default())
                .expect("empty lineage should encode");
        for offset in [0, 8, 9, encoded.len() - 1] {
            let mut malformed = encoded.clone();
            malformed[offset] ^= 0x80;
            assert!(decode_entity_source_lineage_catalog(&malformed).is_err());
        }

        let store = TargetStoreIdentity::from_bytes([7; 32]);
        let mut catalog = AcceptedEntitySourceLineageCatalog::default();
        for entity in [1, 2] {
            catalog
                .insert(
                    store,
                    EntityTag::new(entity),
                    AcceptedEntitySourceLineage::unadopted(exact_head(3))
                        .expect("lineage should admit"),
                )
                .expect("entry should insert");
        }
        let mut out_of_order =
            encode_entity_source_lineage_catalog(&catalog).expect("lineage catalog should encode");
        let body_len = out_of_order.len() - LINEAGE_CHECKSUM_BYTES;
        let record_bytes = 32 + 8 + 8 + 32 + 1;
        let first = LINEAGE_HEADER_BYTES;
        let second = first + record_bytes;
        for offset in 0..record_bytes {
            out_of_order.swap(first + offset, second + offset);
        }
        let checksum = crc32c(&out_of_order[..body_len]).to_be_bytes();
        out_of_order[body_len..].copy_from_slice(&checksum);
        assert!(decode_entity_source_lineage_catalog(&out_of_order).is_err());
    }

    #[test]
    fn lineage_catalog_enforces_exact_entry_and_identity_bounds() {
        assert!(
            AcceptedEntitySourceLineage::adopted(
                exact_head(1),
                AcceptedEntitySourceVersion::try_new(1).expect("version should admit"),
                EntitySourceDigest::from_bytes([0; 32]),
            )
            .is_err(),
        );
        let store = TargetStoreIdentity::from_bytes([5; 32]);
        let lineage =
            AcceptedEntitySourceLineage::unadopted(exact_head(1)).expect("lineage should admit");
        let mut catalog = AcceptedEntitySourceLineageCatalog::default();
        assert!(
            catalog
                .insert(store, EntityTag::new(0), lineage.clone())
                .is_err()
        );
        for entity in 1..=MAX_ENTITY_SOURCE_LINEAGE_ENTRIES {
            catalog
                .insert(
                    store,
                    EntityTag::new(u64::try_from(entity).expect("entity should fit")),
                    lineage.clone(),
                )
                .expect("boundary entry should insert");
        }
        assert_eq!(catalog.entries().len(), MAX_ENTITY_SOURCE_LINEAGE_ENTRIES);
        assert!(encode_entity_source_lineage_catalog(&catalog).is_ok());
        assert!(
            catalog
                .insert(
                    store,
                    EntityTag::new(
                        u64::try_from(MAX_ENTITY_SOURCE_LINEAGE_ENTRIES + 1)
                            .expect("entity should fit"),
                    ),
                    lineage,
                )
                .is_err(),
        );
    }
}
