//! Durable accepted per-entity generated-source lineage.
//!
//! This module owns the bounded current lineage value. The generalized
//! database-control map owns its reserved key and publication.

use crate::db::codec::BoundedByteWriter;
use std::collections::BTreeMap;

use icydb_schema::{
    EntitySourceDigest, ExpectedAcceptedHead, ExpectedSchemaFingerprint, SchemaFragment,
    SchemaProposal, TargetStoreIdentity,
};

use crate::{
    db::{
        database_format::{crc32c, verify_crc32c_trailer},
        schema::{
            AcceptedSchemaRevisionBundle, SchemaChangeActivation, migration_record::encode_head,
            wire::SchemaWireReader,
        },
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

type LineageWriter = BoundedByteWriter<MAX_ENTITY_SOURCE_LINEAGE_BYTES>;
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

/// Source meaning captured when one generated activation is admitted.
/// The enclosing application record binds its single store/entity activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct PendingEntitySourceLineage {
    version: AcceptedEntitySourceVersion,
    prior_digest: EntitySourceDigest,
    proposed_digest: EntitySourceDigest,
}

impl PendingEntitySourceLineage {
    pub(in crate::db::schema) fn new(
        version: AcceptedEntitySourceVersion,
        prior_digest: EntitySourceDigest,
        proposed_digest: EntitySourceDigest,
    ) -> Result<Self, InternalError> {
        if prior_digest.to_bytes() == [0; 32] || proposed_digest.to_bytes() == [0; 32] {
            return Err(InternalError::store_corruption());
        }
        Ok(Self {
            version,
            prior_digest,
            proposed_digest,
        })
    }

    pub(in crate::db::schema) const fn version(&self) -> AcceptedEntitySourceVersion {
        self.version
    }
    pub(in crate::db::schema) const fn prior_digest(&self) -> EntitySourceDigest {
        self.prior_digest
    }
    pub(in crate::db::schema) const fn proposed_digest(&self) -> EntitySourceDigest {
        self.proposed_digest
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
    // Exact catalog head when this lineage record was published. SQL-owned
    // catalog changes do not change generated-source meaning or this provenance.
    publication_head: ExpectedAcceptedHead,
    state: AcceptedEntitySourceLineageState,
}

impl AcceptedEntitySourceLineage {
    pub(in crate::db::schema) fn unadopted(
        accepted_head: ExpectedAcceptedHead,
    ) -> Result<Self, InternalError> {
        validate_lineage_head(&accepted_head)?;
        Ok(Self {
            publication_head: accepted_head,
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
            publication_head: accepted_head,
            state: AcceptedEntitySourceLineageState::Adopted {
                version,
                source_digest,
            },
        })
    }

    #[must_use]
    pub(in crate::db::schema) const fn publication_head(&self) -> &ExpectedAcceptedHead {
        &self.publication_head
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
}

/// Refresh only existing adopted sources after ordinary lowering has admitted
/// their complete proposed contract. Source versions require a plan even when
/// validation is deferred. Pending validation keeps the prior digest because it
/// can still abort; only that entity waits while other completed edits refresh.
/// Return its admitted source binding for the enclosing application record.
pub(in crate::db::schema) fn prepare_ordinary_source_lineage(
    before: AcceptedEntitySourceLineageCatalog,
    proposal: &SchemaProposal,
    current: &[Option<AcceptedSchemaRevisionBundle>],
    head: &ExpectedAcceptedHead,
    deferred: Option<&SchemaChangeActivation>,
) -> Result<
    (
        AcceptedEntitySourceLineageCatalog,
        Option<PendingEntitySourceLineage>,
    ),
    InternalError,
> {
    let mut entries = before.entries;
    let mut pending = None;
    for assignment in proposal.assignments() {
        let Some(tag) = current
            .iter()
            .flatten()
            .find_map(|bundle| bundle.source_bindings().entity(assignment.entity()))
        else {
            // New entity lineage remains with the explicit creation/adoption
            // owner. Never adopt an existing untracked source by refreshing it.
            continue;
        };
        let key = (assignment.store(), tag);
        let Some(entry) = entries.get(&key) else {
            continue;
        };
        let AcceptedEntitySourceLineageState::Adopted {
            version,
            source_digest,
        } = entry.state()
        else {
            continue;
        };
        let version = *version;
        let prior_digest = *source_digest;
        let entity = proposal
            .fragments()
            .iter()
            .flat_map(SchemaFragment::entities)
            .find(|entity| entity.source_key() == assignment.entity())
            .ok_or_else(InternalError::store_invariant)?;
        if version.get() != entity.version().get() {
            return Err(InternalError::schema_migration(
                icydb_diagnostic_code::SchemaMigrationCode::MissingMigration,
            ));
        }
        let digest = proposal
            .entity_source_digest(assignment.entity())
            .map_err(|_| InternalError::store_unsupported())?;
        if deferred.is_some_and(|activation| {
            activation.store() == key.0 && activation.entity_tag() == tag.value()
        }) {
            pending = Some(PendingEntitySourceLineage::new(
                version,
                prior_digest,
                digest,
            )?);
        } else {
            let next = AcceptedEntitySourceLineage::adopted(head.clone(), version, digest)?;
            entries.insert(key, next);
        }
    }
    Ok((
        AcceptedEntitySourceLineageCatalog::try_new(entries)?,
        pending,
    ))
}

/// Refuse missing or superseded retained source authority before advancement.
/// Unadopted entities remain unadopted; completion never invents a source digest.
pub(in crate::db::schema) fn require_pending_source_lineage(
    catalog: Option<&AcceptedEntitySourceLineageCatalog>,
    activation: &SchemaChangeActivation,
    pending: Option<&PendingEntitySourceLineage>,
) -> Result<(), InternalError> {
    let entry = catalog.and_then(|catalog| {
        catalog.get(activation.store(), EntityTag::new(activation.entity_tag()))
    });
    match (pending, entry.map(AcceptedEntitySourceLineage::state)) {
        (None, None | Some(AcceptedEntitySourceLineageState::Unadopted)) => Ok(()),
        (
            Some(pending),
            Some(AcceptedEntitySourceLineageState::Adopted {
                version,
                source_digest,
            }),
        ) if *version == pending.version
            && (*source_digest == pending.prior_digest
                || *source_digest == pending.proposed_digest) =>
        {
            Ok(())
        }
        _ => Err(InternalError::store_corruption()),
    }
}

/// Apply only the admitted entity's source meaning after its activation proved.
/// Other entities and intervening publication provenance retain their owners.
pub(in crate::db::schema) fn complete_pending_source_lineage(
    before: &AcceptedEntitySourceLineageCatalog,
    activation: &SchemaChangeActivation,
    pending: &PendingEntitySourceLineage,
    head: &ExpectedAcceptedHead,
) -> Result<AcceptedEntitySourceLineageCatalog, InternalError> {
    require_pending_source_lineage(Some(before), activation, Some(pending))?;
    let mut entries = before.entries.clone();
    entries.insert(
        (activation.store(), EntityTag::new(activation.entity_tag())),
        AcceptedEntitySourceLineage::adopted(
            head.clone(),
            pending.version,
            pending.proposed_digest,
        )?,
    );
    AcceptedEntitySourceLineageCatalog::try_new(entries)
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
        encode_lineage_head(&mut writer, lineage.publication_head())?;
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
    let body = verify_crc32c_trailer(bytes).ok_or_else(InternalError::store_corruption)?;
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
    validate_lineage_head(head)?;
    encode_head(writer, head)
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
mod tests;

#[cfg(test)]
mod helper_tests;

#[cfg(test)]
pub(in crate::db) use helper_tests::unadopted_entity_source_lineage_op_for_tests;
