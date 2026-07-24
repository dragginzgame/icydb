//! Module: db::schema::source_binding
//! Responsibility: durable source-key to accepted-identity bindings.
//! Does not own: accepted structural semantics, identity allocation, proposal lowering, or publication.
//! Boundary: immutable proposal source keys <-> existing store-local accepted identities.

use std::collections::{BTreeMap, BTreeSet};

use icydb_schema::{
    ConstraintSourceKey, EntitySourceKey, FieldSourceKey, IndexSourceKey, RelationSourceKey,
    TypeSourceKey,
};

use crate::{
    db::schema::{
        AcceptedCompositeCatalog, AcceptedConstraintKind, AcceptedEnumCatalog,
        ConstraintActivationKind, ConstraintId, FieldId, PersistedSchemaSnapshot, RelationId,
        SchemaIndexId,
        composite_catalog::CompositeTypeId,
        wire::{SchemaWireReader, SchemaWireWriter},
    },
    error::InternalError,
    types::EntityTag,
    value::EnumTypeId,
};

const ACCEPTED_SOURCE_BINDING_MAGIC: &[u8; 8] = b"ICYDBASB";
const ACCEPTED_SOURCE_BINDING_CODEC_VERSION: u16 = 1;
const ACCEPTED_SOURCE_BINDING_HEADER_BYTES: usize = 34;
const MAX_ACCEPTED_SOURCE_BINDING_BYTES: usize = 2 * 1024 * 1024;
const TYPE_ENUM: u8 = 0;
const TYPE_COMPOSITE: u8 = 1;
type BindingWriter = SchemaWireWriter<MAX_ACCEPTED_SOURCE_BINDING_BYTES>;
type BindingReader<'a> = SchemaWireReader<'a>;

///
/// AcceptedNamedTypeIdentity
///
/// Store-local accepted identity selected by one immutable named-type source
/// key. The variant preserves the structural catalog that owns the definition.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db) enum AcceptedNamedTypeIdentity {
    /// Definition owned by the accepted enum catalog.
    Enum(EnumTypeId),
    /// Definition owned by the accepted composite catalog.
    Composite(CompositeTypeId),
}

///
/// AcceptedSourceBindingCatalog
///
/// Canonical store-local bindings from immutable authorship keys to accepted
/// structural identities. The referenced snapshots and value catalogs remain
/// the sole semantic owners; this catalog carries identity only.
///
/// During the 0.213 hard cut, a structural owner becomes source-addressable
/// only when its admission path supplies an immutable source key. The catalog
/// may therefore be empty, but every binding it does contain must close
/// exactly over the same accepted revision bundle.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSourceBindingCatalog {
    entities: BTreeMap<EntitySourceKey, EntityTag>,
    types: BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
    fields: BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
    constraints: BTreeMap<(EntityTag, ConstraintSourceKey), ConstraintId>,
    indexes: BTreeMap<(EntityTag, IndexSourceKey), SchemaIndexId>,
    relations: BTreeMap<(EntityTag, RelationSourceKey), RelationId>,
}

impl AcceptedSourceBindingCatalog {
    const fn from_parts(
        entities: BTreeMap<EntitySourceKey, EntityTag>,
        types: BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
        fields: BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
        constraints: BTreeMap<(EntityTag, ConstraintSourceKey), ConstraintId>,
        indexes: BTreeMap<(EntityTag, IndexSourceKey), SchemaIndexId>,
        relations: BTreeMap<(EntityTag, RelationSourceKey), RelationId>,
    ) -> Self {
        Self {
            entities,
            types,
            fields,
            constraints,
            indexes,
            relations,
        }
    }

    fn validate(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
        composite_catalog: &AcceptedCompositeCatalog,
        entities: &BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    ) -> bool {
        unique_values(self.entities.values().copied())
            && unique_values(self.types.values().copied())
            && unique_values(
                self.fields
                    .iter()
                    .map(|((entity, _), field)| (*entity, *field)),
            )
            && unique_values(
                self.constraints
                    .iter()
                    .map(|((entity, _), constraint)| (*entity, *constraint)),
            )
            && unique_values(
                self.indexes
                    .iter()
                    .map(|((entity, _), index)| (*entity, *index)),
            )
            && unique_values(
                self.relations
                    .iter()
                    .map(|((entity, _), relation)| (*entity, *relation)),
            )
            && self
                .entities
                .values()
                .all(|entity| entities.contains_key(entity))
            && self.types.values().all(|identity| match identity {
                AcceptedNamedTypeIdentity::Enum(id) => enum_catalog.enum_type(*id).is_some(),
                AcceptedNamedTypeIdentity::Composite(id) => {
                    composite_catalog.composite_type(*id).is_some()
                }
            })
            && self.fields.iter().all(|((entity, _), field_id)| {
                entities.get(entity).is_some_and(|snapshot| {
                    snapshot
                        .fields()
                        .iter()
                        .any(|field| field.id() == *field_id)
                })
            })
            && self.constraints.iter().all(|((entity, _), constraint_id)| {
                entities.get(entity).is_some_and(|snapshot| {
                    snapshot
                        .constraint_catalog()
                        .constraints()
                        .iter()
                        .any(|constraint| {
                            constraint.id() == *constraint_id
                                && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
                        })
                        || snapshot
                            .constraint_catalog()
                            .activations()
                            .iter()
                            .any(|activation| {
                                activation.id() == *constraint_id
                                    && matches!(
                                        activation.kind(),
                                        ConstraintActivationKind::Check { .. }
                                    )
                            })
                })
            })
            && self.indexes.iter().all(|((entity, _), index_id)| {
                entities.get(entity).is_some_and(|snapshot| {
                    snapshot
                        .indexes()
                        .iter()
                        .chain(snapshot.candidate_indexes())
                        .any(|index| index.schema_id() == *index_id)
                })
            })
            && self.relations.iter().all(|((entity, _), relation_id)| {
                entities.get(entity).is_some_and(|snapshot| {
                    snapshot
                        .relations()
                        .iter()
                        .chain(snapshot.candidate_relations())
                        .any(|relation| relation.id() == *relation_id)
                })
            })
    }
}

fn unique_values<T: Ord>(values: impl Iterator<Item = T>) -> bool {
    let mut seen = BTreeSet::new();
    values.into_iter().all(|value| seen.insert(value))
}

/// Encode one source-binding catalog after proving exact structural closure.
///
/// # Errors
///
/// Returns a typed internal error when a binding is not one-to-one, references
/// an absent accepted owner, or exceeds the persisted byte bound.
pub(in crate::db::schema) fn encode_accepted_source_bindings(
    catalog: &AcceptedSourceBindingCatalog,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    entities: &BTreeMap<EntityTag, PersistedSchemaSnapshot>,
) -> Result<Vec<u8>, InternalError> {
    if !catalog.validate(enum_catalog, composite_catalog, entities) {
        return Err(InternalError::store_invariant());
    }

    let mut writer = BindingWriter::new();
    writer.push_bytes(ACCEPTED_SOURCE_BINDING_MAGIC);
    writer.push_u16(ACCEPTED_SOURCE_BINDING_CODEC_VERSION);
    writer.push_len(catalog.entities.len())?;
    for (source_key, entity) in &catalog.entities {
        writer.push_string(source_key.as_str())?;
        writer.push_u64(entity.value());
    }
    writer.push_len(catalog.types.len())?;
    for (source_key, identity) in &catalog.types {
        writer.push_string(source_key.as_str())?;
        match identity {
            AcceptedNamedTypeIdentity::Enum(id) => {
                writer.push_u8(TYPE_ENUM);
                writer.push_u32(id.get());
            }
            AcceptedNamedTypeIdentity::Composite(id) => {
                writer.push_u8(TYPE_COMPOSITE);
                writer.push_u32(id.get());
            }
        }
    }
    writer.push_len(catalog.fields.len())?;
    for ((entity, source_key), field) in &catalog.fields {
        writer.push_u64(entity.value());
        writer.push_string(source_key.as_str())?;
        writer.push_u32(field.get());
    }
    writer.push_len(catalog.constraints.len())?;
    for ((entity, source_key), constraint) in &catalog.constraints {
        writer.push_u64(entity.value());
        writer.push_string(source_key.as_str())?;
        writer.push_u32(constraint.get());
    }
    writer.push_len(catalog.indexes.len())?;
    for ((entity, source_key), index) in &catalog.indexes {
        writer.push_u64(entity.value());
        writer.push_string(source_key.as_str())?;
        writer.push_u32(index.get());
    }
    writer.push_len(catalog.relations.len())?;
    for ((entity, source_key), relation) in &catalog.relations {
        writer.push_u64(entity.value());
        writer.push_string(source_key.as_str())?;
        writer.push_u32(relation.get());
    }
    writer.finish()
}

/// Decode one bounded current-form source-binding catalog and prove closure.
///
/// # Errors
///
/// Returns a typed internal error for malformed, obsolete, oversized,
/// non-canonical, or structurally unclosed bytes.
pub(in crate::db::schema) fn decode_accepted_source_bindings(
    bytes: &[u8],
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    entities: &BTreeMap<EntityTag, PersistedSchemaSnapshot>,
) -> Result<AcceptedSourceBindingCatalog, InternalError> {
    if bytes.len() < ACCEPTED_SOURCE_BINDING_HEADER_BYTES
        || bytes.len() > MAX_ACCEPTED_SOURCE_BINDING_BYTES
    {
        return Err(InternalError::store_corruption());
    }

    let mut reader = BindingReader::new(bytes);
    if reader.read_array::<8>()? != *ACCEPTED_SOURCE_BINDING_MAGIC {
        return Err(InternalError::store_corruption());
    }
    if reader.read_u16()? != ACCEPTED_SOURCE_BINDING_CODEC_VERSION {
        return Err(InternalError::serialize_incompatible_persisted_format());
    }

    let mut entity_bindings = BTreeMap::new();
    for _ in 0..reader.read_count()? {
        let source = EntitySourceKey::try_new(reader.read_string()?)
            .map_err(|_| InternalError::store_corruption())?;
        if entity_bindings
            .insert(source, EntityTag::new(reader.read_u64()?))
            .is_some()
        {
            return Err(InternalError::store_corruption());
        }
    }

    let mut type_bindings = BTreeMap::new();
    for _ in 0..reader.read_count()? {
        let source = TypeSourceKey::try_new(reader.read_string()?)
            .map_err(|_| InternalError::store_corruption())?;
        let tag = reader.read_u8()?;
        let raw = reader.read_u32()?;
        let identity = match tag {
            TYPE_ENUM => AcceptedNamedTypeIdentity::Enum(
                EnumTypeId::new(raw).ok_or_else(InternalError::store_corruption)?,
            ),
            TYPE_COMPOSITE => AcceptedNamedTypeIdentity::Composite(
                CompositeTypeId::new(raw).ok_or_else(InternalError::store_corruption)?,
            ),
            _ => return Err(InternalError::store_corruption()),
        };
        if type_bindings.insert(source, identity).is_some() {
            return Err(InternalError::store_corruption());
        }
    }

    let field_bindings =
        read_entity_local_bindings(&mut reader, FieldSourceKey::try_new, FieldId::new)?;
    let constraint_bindings = read_nonzero_entity_local_bindings(
        &mut reader,
        ConstraintSourceKey::try_new,
        ConstraintId::new,
    )?;
    let index_bindings = read_nonzero_entity_local_bindings(
        &mut reader,
        IndexSourceKey::try_new,
        SchemaIndexId::new,
    )?;
    let relation_bindings = read_nonzero_entity_local_bindings(
        &mut reader,
        RelationSourceKey::try_new,
        RelationId::new,
    )?;
    reader.finish()?;

    let catalog = AcceptedSourceBindingCatalog::from_parts(
        entity_bindings,
        type_bindings,
        field_bindings,
        constraint_bindings,
        index_bindings,
        relation_bindings,
    );
    if !catalog.validate(enum_catalog, composite_catalog, entities) {
        return Err(InternalError::store_corruption());
    }
    let canonical =
        encode_accepted_source_bindings(&catalog, enum_catalog, composite_catalog, entities)
            .map_err(|_| InternalError::store_corruption())?;
    if canonical != bytes {
        return Err(InternalError::store_corruption());
    }
    Ok(catalog)
}

fn read_entity_local_bindings<K: Ord>(
    reader: &mut BindingReader<'_>,
    source_key: impl Fn(String) -> Result<K, icydb_schema::SchemaContractError>,
    identity: impl Fn(u32) -> FieldId,
) -> Result<BTreeMap<(EntityTag, K), FieldId>, InternalError> {
    let mut bindings = BTreeMap::new();
    for _ in 0..reader.read_count()? {
        let entity = EntityTag::new(reader.read_u64()?);
        let source =
            source_key(reader.read_string()?).map_err(|_| InternalError::store_corruption())?;
        let accepted = identity(reader.read_u32()?);
        if bindings.insert((entity, source), accepted).is_some() {
            return Err(InternalError::store_corruption());
        }
    }
    Ok(bindings)
}

fn read_nonzero_entity_local_bindings<K: Ord, V>(
    reader: &mut BindingReader<'_>,
    source_key: impl Fn(String) -> Result<K, icydb_schema::SchemaContractError>,
    identity: impl Fn(u32) -> Option<V>,
) -> Result<BTreeMap<(EntityTag, K), V>, InternalError> {
    let mut bindings = BTreeMap::new();
    for _ in 0..reader.read_count()? {
        let entity = EntityTag::new(reader.read_u64()?);
        let source =
            source_key(reader.read_string()?).map_err(|_| InternalError::store_corruption())?;
        let accepted = identity(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
        if bindings.insert((entity, source), accepted).is_some() {
            return Err(InternalError::store_corruption());
        }
    }
    Ok(bindings)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        AcceptedSourceBindingCatalog, decode_accepted_source_bindings,
        encode_accepted_source_bindings,
    };
    use crate::{
        db::schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
            AcceptedSchemaRevisionBundle, CandidateSchemaRevision, FieldId, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout,
            SchemaVersion, build_initial_accepted_enum_catalog_from_kinds_for_tests,
        },
        model::field::{FieldStorageDecode, LeafCodec, ScalarCodec},
        types::EntityTag,
    };

    #[test]
    fn empty_source_binding_catalog_has_one_canonical_current_form() {
        let enums = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[])
            .expect("empty enum catalog should build");
        let composites = AcceptedCompositeCatalog::empty();
        let entities = BTreeMap::new();
        let catalog = AcceptedSourceBindingCatalog::default();

        let encoded = encode_accepted_source_bindings(&catalog, &enums, &composites, &entities)
            .expect("empty source bindings should encode");
        let decoded = decode_accepted_source_bindings(&encoded, &enums, &composites, &entities)
            .expect("empty source bindings should decode");

        assert_eq!(decoded, catalog);
    }

    #[test]
    fn source_binding_catalog_rejects_missing_structural_owner() {
        let enums = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[])
            .expect("empty enum catalog should build");
        let composites = AcceptedCompositeCatalog::empty();
        let entities = BTreeMap::new();
        let mut catalog = AcceptedSourceBindingCatalog::default();
        catalog.entities.insert(
            icydb_schema::EntitySourceKey::try_new("test:missing")
                .expect("source key should build"),
            EntityTag::new(7),
        );

        assert!(encode_accepted_source_bindings(&catalog, &enums, &composites, &entities).is_err());
    }

    #[test]
    fn source_binding_catalog_round_trips_existing_accepted_identities() {
        let enums = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[])
            .expect("empty enum catalog should build");
        let composites = AcceptedCompositeCatalog::empty();
        let entity = EntityTag::new(7);
        let field = FieldId::new(1);
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "test::Bound".to_string(),
            "Bound".to_string(),
            field,
            SchemaRowLayout::initial(vec![(field, SchemaFieldSlot::new(0))]),
            vec![PersistedFieldSnapshot::new_initial(
                field,
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            )],
        );
        let entities = BTreeMap::from([(entity, snapshot)]);
        let mut catalog = AcceptedSourceBindingCatalog::default();
        catalog.entities.insert(
            icydb_schema::EntitySourceKey::try_new("test:bound")
                .expect("entity source key should build"),
            entity,
        );
        catalog.fields.insert(
            (
                entity,
                icydb_schema::FieldSourceKey::try_new("test:bound:id")
                    .expect("field source key should build"),
            ),
            field,
        );

        let encoded = encode_accepted_source_bindings(&catalog, &enums, &composites, &entities)
            .expect("closed source bindings should encode");
        let decoded = decode_accepted_source_bindings(&encoded, &enums, &composites, &entities)
            .expect("closed source bindings should decode");

        assert_eq!(decoded, catalog);
    }

    #[test]
    fn accepted_schema_fingerprint_covers_source_bindings() {
        let enums = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[])
            .expect("empty enum catalog should build");
        let composites = AcceptedCompositeCatalog::empty();
        let entity = EntityTag::new(7);
        let field = FieldId::new(1);
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "test::Bound".to_string(),
            "Bound".to_string(),
            field,
            SchemaRowLayout::initial(vec![(field, SchemaFieldSlot::new(0))]),
            vec![PersistedFieldSnapshot::new_initial(
                field,
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Nat64,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            )],
        );
        let entities = BTreeMap::from([(entity, snapshot)]);
        let mut first_bindings = AcceptedSourceBindingCatalog::default();
        first_bindings.entities.insert(
            icydb_schema::EntitySourceKey::try_new("test:first")
                .expect("first source key should build"),
            entity,
        );
        let mut second_bindings = AcceptedSourceBindingCatalog::default();
        second_bindings.entities.insert(
            icydb_schema::EntitySourceKey::try_new("test:second")
                .expect("second source key should build"),
            entity,
        );

        let first = CandidateSchemaRevision::new(
            AcceptedSchemaRevisionBundle::new_with_source_bindings(
                AcceptedSchemaRevision::INITIAL,
                "test::Store",
                enums.clone(),
                composites.clone(),
                first_bindings,
                entities.clone(),
            )
            .expect("first bundle should close"),
        )
        .expect("first candidate should encode");
        let second = CandidateSchemaRevision::new(
            AcceptedSchemaRevisionBundle::new_with_source_bindings(
                AcceptedSchemaRevision::INITIAL,
                "test::Store",
                enums,
                composites,
                second_bindings,
                entities,
            )
            .expect("second bundle should close"),
        )
        .expect("second candidate should encode");

        assert_ne!(
            first.root().fingerprint().as_bytes(),
            second.root().fingerprint().as_bytes(),
        );
    }
}
