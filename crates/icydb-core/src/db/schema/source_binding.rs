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
    value::{EnumTypeId, EnumVariantId},
};

const ACCEPTED_SOURCE_BINDING_MAGIC: &[u8; 8] = b"ICYDBASB";
const ACCEPTED_SOURCE_BINDING_CODEC_VERSION: u16 = 1;
const ACCEPTED_SOURCE_BINDING_HEADER_BYTES: usize = 38;
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
    enum_variants: BTreeMap<(EnumTypeId, TypeSourceKey), EnumVariantId>,
    fields: BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
    constraints: BTreeMap<(EntityTag, ConstraintSourceKey), ConstraintId>,
    indexes: BTreeMap<(EntityTag, IndexSourceKey), SchemaIndexId>,
    relations: BTreeMap<(EntityTag, RelationSourceKey), RelationId>,
}

impl AcceptedSourceBindingCatalog {
    const fn from_parts(
        entities: BTreeMap<EntitySourceKey, EntityTag>,
        types: BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
        enum_variants: BTreeMap<(EnumTypeId, TypeSourceKey), EnumVariantId>,
        fields: BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
        constraints: BTreeMap<(EntityTag, ConstraintSourceKey), ConstraintId>,
        indexes: BTreeMap<(EntityTag, IndexSourceKey), SchemaIndexId>,
        relations: BTreeMap<(EntityTag, RelationSourceKey), RelationId>,
    ) -> Self {
        Self {
            entities,
            types,
            enum_variants,
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
        self.identities_are_one_to_one()
            && self.type_bindings_close(enum_catalog, composite_catalog)
            && self.entity_bindings_close(entities)
    }

    fn identities_are_one_to_one(&self) -> bool {
        unique_values(self.entities.values().copied())
            && unique_values(self.types.values().copied())
            && unique_values(
                self.enum_variants
                    .iter()
                    .map(|((enum_type, _), variant)| (*enum_type, *variant)),
            )
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
    }

    fn type_bindings_close(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
        composite_catalog: &AcceptedCompositeCatalog,
    ) -> bool {
        self.types.values().all(|identity| match identity {
            AcceptedNamedTypeIdentity::Enum(id) => enum_catalog.enum_type(*id).is_some(),
            AcceptedNamedTypeIdentity::Composite(id) => {
                composite_catalog.composite_type(*id).is_some()
            }
        }) && self.enum_variants.iter().all(|((enum_type, _), variant)| {
            self.types
                .values()
                .any(|identity| *identity == AcceptedNamedTypeIdentity::Enum(*enum_type))
                && enum_catalog
                    .enum_type(*enum_type)
                    .is_some_and(|definition| definition.variant(*variant).is_some())
        }) && self.types.values().all(|identity| match identity {
            AcceptedNamedTypeIdentity::Enum(enum_type) => enum_catalog
                .enum_type(*enum_type)
                .is_some_and(|definition| {
                    self.enum_variants
                        .keys()
                        .filter(|(bound_type, _)| bound_type == enum_type)
                        .count()
                        == definition.variant_count()
                }),
            AcceptedNamedTypeIdentity::Composite(_) => true,
        })
    }

    fn entity_bindings_close(
        &self,
        entities: &BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    ) -> bool {
        self.entities
            .values()
            .all(|entity| entities.contains_key(entity))
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
    writer.push_len(catalog.enum_variants.len())?;
    for ((enum_type, source_key), variant) in &catalog.enum_variants {
        writer.push_u32(enum_type.get());
        writer.push_string(source_key.as_str())?;
        writer.push_u32(variant.get());
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

    let mut enum_variant_bindings = BTreeMap::new();
    for _ in 0..reader.read_count()? {
        let enum_type =
            EnumTypeId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
        let source = TypeSourceKey::try_new(reader.read_string()?)
            .map_err(|_| InternalError::store_corruption())?;
        let variant =
            EnumVariantId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
        if enum_variant_bindings
            .insert((enum_type, source), variant)
            .is_some()
        {
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
        enum_variant_bindings,
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
        AcceptedNamedTypeIdentity, AcceptedSourceBindingCatalog, decode_accepted_source_bindings,
        encode_accepted_source_bindings,
    };
    use crate::{
        db::schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
            AcceptedSchemaRevisionBundle, CandidateSchemaRevision, FieldId, PersistedFieldSnapshot,
            PersistedSchemaSnapshot, SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout,
            SchemaVersion, build_initial_accepted_enum_catalog_from_kinds_for_tests,
        },
        model::field::{EnumVariantModel, FieldKind, FieldStorageDecode, LeafCodec, ScalarCodec},
        types::EntityTag,
    };

    static STATUS_VARIANTS: [EnumVariantModel; 2] = [
        EnumVariantModel::new("Active", None, FieldStorageDecode::ByKind),
        EnumVariantModel::new("Disabled", None, FieldStorageDecode::ByKind),
    ];
    const STATUS_KIND: FieldKind = FieldKind::Enum {
        path: "test::Status",
        variants: &STATUS_VARIANTS,
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
    fn superseded_source_binding_shape_fails_closed() {
        let enums = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[])
            .expect("empty enum catalog should build");
        let composites = AcceptedCompositeCatalog::empty();
        let entities = BTreeMap::new();
        let mut superseded = b"ICYDBASB".to_vec();
        superseded.extend_from_slice(&1u16.to_be_bytes());
        for _ in 0..6 {
            superseded.extend_from_slice(&0u32.to_be_bytes());
        }

        assert!(
            decode_accepted_source_bindings(superseded.as_slice(), &enums, &composites, &entities,)
                .is_err(),
            "the pre-variant-binding development shape must not decode",
        );
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
    fn source_binding_catalog_closes_enum_variant_source_identities() {
        let enums = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[STATUS_KIND])
            .expect("enum catalog should build");
        let composites = AcceptedCompositeCatalog::empty();
        let entities = BTreeMap::new();
        let enum_type = enums
            .type_id("test::Status")
            .expect("accepted enum type should exist");
        let definition = enums
            .enum_type(enum_type)
            .expect("accepted enum definition should exist");
        let active = definition
            .variant_id("Active")
            .expect("accepted active variant should exist");
        let disabled = definition
            .variant_id("Disabled")
            .expect("accepted disabled variant should exist");
        let mut catalog = AcceptedSourceBindingCatalog::default();
        catalog.types.insert(
            icydb_schema::TypeSourceKey::try_new("test:status")
                .expect("type source key should build"),
            AcceptedNamedTypeIdentity::Enum(enum_type),
        );
        catalog.enum_variants.insert(
            (
                enum_type,
                icydb_schema::TypeSourceKey::try_new("test:status:active")
                    .expect("variant source key should build"),
            ),
            active,
        );
        catalog.enum_variants.insert(
            (
                enum_type,
                icydb_schema::TypeSourceKey::try_new("test:status:disabled")
                    .expect("variant source key should build"),
            ),
            disabled,
        );

        let encoded = encode_accepted_source_bindings(&catalog, &enums, &composites, &entities)
            .expect("closed enum bindings should encode");
        let decoded = decode_accepted_source_bindings(&encoded, &enums, &composites, &entities)
            .expect("closed enum bindings should decode");

        assert_eq!(decoded, catalog);

        let mut incomplete = catalog.clone();
        incomplete
            .enum_variants
            .retain(|(_, source), _| source.as_str() != "test:status:disabled");
        assert!(
            encode_accepted_source_bindings(&incomplete, &enums, &composites, &entities).is_err(),
            "a bound enum type must bind every accepted variant",
        );
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
