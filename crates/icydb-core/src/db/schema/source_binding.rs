//! Module: db::schema::source_binding
//! Responsibility: durable source-key to accepted-identity bindings.
//! Does not own: accepted structural semantics, accepted-ID allocation, proposal lowering, or publication.
//! Boundary: immutable proposal source keys <-> existing store-local accepted identities.

use crate::db::codec::BoundedByteWriter;
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
        composite_catalog::{AcceptedCompositeShape, CompositeFieldId, CompositeTypeId},
        wire::SchemaWireReader,
    },
    error::InternalError,
    types::EntityTag,
    value::{EnumTypeId, EnumVariantId},
};

#[cfg(feature = "sql")]
use crate::db::schema::{
    AcceptedSchemaRevision, ConstraintOrigin, PersistedFieldOrigin, PersistedIndexOrigin,
    PersistedIndexSnapshot,
};

const ACCEPTED_SOURCE_BINDING_MAGIC: &[u8; 8] = b"ICYDBASB";
const ACCEPTED_SOURCE_BINDING_CODEC_VERSION: u16 = 1;
const ACCEPTED_SOURCE_BINDING_HEADER_BYTES: usize = 42;
const MAX_ACCEPTED_SOURCE_BINDING_BYTES: usize = 2 * 1024 * 1024;
const TYPE_ENUM: u8 = 0;
const TYPE_COMPOSITE: u8 = 1;
type BindingWriter = BoundedByteWriter<MAX_ACCEPTED_SOURCE_BINDING_BYTES>;
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
/// A structural owner becomes source-addressable only when its admission path
/// supplies an immutable source key. The catalog may therefore be empty, but
/// every binding it does contain must close exactly over the same accepted
/// revision bundle.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSourceBindingCatalog {
    entities: BTreeMap<EntitySourceKey, EntityTag>,
    types: BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
    enum_variants: BTreeMap<(EnumTypeId, TypeSourceKey), EnumVariantId>,
    composite_fields: BTreeMap<(CompositeTypeId, FieldSourceKey), CompositeFieldId>,
    fields: BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
    constraints: BTreeMap<(EntityTag, ConstraintSourceKey), ConstraintId>,
    indexes: BTreeMap<(EntityTag, IndexSourceKey), SchemaIndexId>,
    relations: BTreeMap<(EntityTag, RelationSourceKey), RelationId>,
}

///
/// AcceptedTypedAdapterNames
///
/// Read-only accepted display-name projection for one opaque generated adapter
/// binding. Source identities remain the keys; catalogs remain semantic owners.
///

pub(in crate::db) struct AcceptedTypedAdapterNames {
    pub(in crate::db) composite_fields: Vec<(String, String, String)>,
    pub(in crate::db) enum_variants: Vec<(String, String, String)>,
    pub(in crate::db) named_types: Vec<(String, String)>,
}

#[cfg(any(test, feature = "migration"))]
fn rekey_exact<K, V>(
    bindings: &mut BTreeMap<K, V>,
    from: &K,
    to: K,
    expected: V,
) -> Result<(), InternalError>
where
    K: Ord,
    V: Copy + Eq,
{
    if from == &to || bindings.contains_key(&to) {
        return Err(InternalError::store_unsupported());
    }
    match bindings.get(from) {
        Some(actual) if *actual == expected => {}
        Some(_) | None => return Err(InternalError::store_unsupported()),
    }
    if bindings
        .iter()
        .any(|(key, actual)| key != from && *actual == expected)
    {
        return Err(InternalError::store_invariant());
    }
    let _ = bindings.remove(from);
    if bindings.insert(to, expected).is_some() {
        return Err(InternalError::store_invariant());
    }
    Ok(())
}

#[cfg(any(test, feature = "migration"))]
fn rekey_scoped_exact<S, K, V>(
    bindings: &mut BTreeMap<(S, K), V>,
    scope: S,
    from: &K,
    to: K,
    expected: V,
) -> Result<(), InternalError>
where
    S: Copy + Ord,
    K: Clone + Ord,
    V: Copy + Eq,
{
    let from_key = (scope, from.clone());
    let to_key = (scope, to);
    if from_key == to_key || bindings.contains_key(&to_key) {
        return Err(InternalError::store_unsupported());
    }
    match bindings.get(&from_key) {
        Some(actual) if *actual == expected => {}
        Some(_) | None => return Err(InternalError::store_unsupported()),
    }
    if bindings
        .iter()
        .any(|(key, actual)| key != &from_key && key.0 == scope && *actual == expected)
    {
        return Err(InternalError::store_invariant());
    }
    let _ = bindings.remove(&from_key);
    if bindings.insert(to_key, expected).is_some() {
        return Err(InternalError::store_invariant());
    }
    Ok(())
}

impl AcceptedSourceBindingCatalog {
    /// Construct the exact source-addressable identity closure for one initial
    /// catalog-native proposal.
    pub(in crate::db::schema) const fn initial(
        entities: BTreeMap<EntitySourceKey, EntityTag>,
        fields: BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
        constraints: BTreeMap<(EntityTag, ConstraintSourceKey), ConstraintId>,
        indexes: BTreeMap<(EntityTag, IndexSourceKey), SchemaIndexId>,
        relations: BTreeMap<(EntityTag, RelationSourceKey), RelationId>,
    ) -> Self {
        Self {
            entities,
            types: BTreeMap::new(),
            enum_variants: BTreeMap::new(),
            composite_fields: BTreeMap::new(),
            fields,
            constraints,
            indexes,
            relations,
        }
    }

    /// Attach the exact named-type and enum-variant identities allocated for
    /// the same initial accepted candidate.
    #[must_use]
    pub(in crate::db::schema) fn with_initial_named_types(
        mut self,
        types: BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
        enum_variants: BTreeMap<(EnumTypeId, TypeSourceKey), EnumVariantId>,
        composite_fields: BTreeMap<(CompositeTypeId, FieldSourceKey), CompositeFieldId>,
    ) -> Self {
        self.types = types;
        self.enum_variants = enum_variants;
        self.composite_fields = composite_fields;
        self
    }

    /// Resolve one immutable entity source identity.
    #[must_use]
    pub(in crate::db) fn entity(&self, source: &EntitySourceKey) -> Option<EntityTag> {
        self.entities.get(source).copied()
    }

    /// Borrow accepted named identities for proposal-local allocation and references.
    pub(in crate::db::schema) const fn named_types(
        &self,
    ) -> &BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity> {
        &self.types
    }

    /// Append bindings only within fresh named-type scopes.
    pub(in crate::db::schema) fn append_new_named_types(
        &mut self,
        mut types: BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
        mut variants: BTreeMap<(EnumTypeId, TypeSourceKey), EnumVariantId>,
        mut fields: BTreeMap<(CompositeTypeId, FieldSourceKey), CompositeFieldId>,
    ) -> Result<(), InternalError> {
        let identities = types.values().copied().collect::<BTreeSet<_>>();
        if identities.len() != types.len()
            || types.iter().any(|(source, id)| {
                self.types.contains_key(source) || self.types.values().any(|old| old == id)
            })
            || variants
                .keys()
                .any(|(id, _)| !identities.contains(&AcceptedNamedTypeIdentity::Enum(*id)))
            || fields
                .keys()
                .any(|(id, _)| !identities.contains(&AcceptedNamedTypeIdentity::Composite(*id)))
        {
            return Err(InternalError::store_invariant());
        }
        self.types.append(&mut types);
        self.enum_variants.append(&mut variants);
        self.composite_fields.append(&mut fields);
        Ok(())
    }

    /// Attach complete entity-scoped bindings for newly allocated empty entities.
    /// Named-type identities remain owned by the current accepted value catalogs.
    pub(in crate::db::schema) fn append_new_entities(
        &mut self,
        mut additions: Self,
    ) -> Result<(), InternalError> {
        if !additions.types.is_empty()
            || !additions.enum_variants.is_empty()
            || !additions.composite_fields.is_empty()
            || additions.entities.iter().any(|(source, tag)| {
                self.entities.contains_key(source) || self.entities.values().any(|old| old == tag)
            })
        {
            return Err(InternalError::store_invariant());
        }
        // Fresh entity scopes make every child key disjoint. Validate closure
        // here rather than letting map append overwrite an existing binding.
        let tags = additions
            .entities
            .values()
            .copied()
            .collect::<BTreeSet<_>>();
        if tags.len() != additions.entities.len()
            || additions.fields.keys().any(|(tag, _)| !tags.contains(tag))
            || additions
                .constraints
                .keys()
                .any(|(tag, _)| !tags.contains(tag))
            || additions.indexes.keys().any(|(tag, _)| !tags.contains(tag))
            || additions
                .relations
                .keys()
                .any(|(tag, _)| !tags.contains(tag))
        {
            return Err(InternalError::store_invariant());
        }
        self.entities.append(&mut additions.entities);
        self.fields.append(&mut additions.fields);
        self.constraints.append(&mut additions.constraints);
        self.indexes.append(&mut additions.indexes);
        self.relations.append(&mut additions.relations);
        Ok(())
    }

    /// Replace one entity source key while preserving its exact accepted ID.
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn rekey_entity(
        &mut self,
        from: &EntitySourceKey,
        to: EntitySourceKey,
        expected: EntityTag,
    ) -> Result<(), InternalError> {
        rekey_exact(&mut self.entities, from, to, expected)
    }

    /// Remove one exact entity binding and every entity-scoped child binding.
    pub(in crate::db::schema) fn remove_entity(
        &mut self,
        source: &EntitySourceKey,
        expected: EntityTag,
    ) -> Result<(), InternalError> {
        match self.entities.get(source) {
            Some(actual) if *actual == expected => {}
            Some(_) | None => return Err(InternalError::store_invariant()),
        }
        let _ = self.entities.remove(source);
        self.fields.retain(|(entity, _), _| *entity != expected);
        self.constraints
            .retain(|(entity, _), _| *entity != expected);
        self.indexes.retain(|(entity, _), _| *entity != expected);
        self.relations.retain(|(entity, _), _| *entity != expected);
        Ok(())
    }

    /// Resolve one immutable field source identity inside an accepted entity.
    #[must_use]
    pub(in crate::db) fn field(
        &self,
        entity: EntityTag,
        source: &FieldSourceKey,
    ) -> Option<FieldId> {
        self.fields.get(&(entity, source.clone())).copied()
    }

    /// Resolve the source owner of one accepted field before a dense remap.
    #[must_use]
    pub(in crate::db::schema) fn field_source(
        &self,
        entity: EntityTag,
        field: FieldId,
    ) -> Option<&FieldSourceKey> {
        self.fields.iter().find_map(|((bound_entity, source), id)| {
            (*bound_entity == entity && *id == field).then_some(source)
        })
    }

    /// Add one exact generated-field binding allocated by a physical source
    /// migration. Ordinary reconciliation never calls this path.
    #[cfg(feature = "migration")]
    pub(in crate::db::schema) fn insert_migration_field(
        &mut self,
        entity: EntityTag,
        source: FieldSourceKey,
        field: FieldId,
    ) -> Result<(), InternalError> {
        let key = (entity, source);
        if self.fields.contains_key(&key)
            || self
                .fields
                .iter()
                .any(|((bound_entity, _), accepted)| *bound_entity == entity && *accepted == field)
        {
            return Err(InternalError::store_invariant());
        }
        self.fields.insert(key, field);
        Ok(())
    }

    /// Replace one entity-local field source key while preserving its ID.
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn rekey_field(
        &mut self,
        entity: EntityTag,
        from: &FieldSourceKey,
        to: FieldSourceKey,
        expected: FieldId,
    ) -> Result<(), InternalError> {
        rekey_scoped_exact(&mut self.fields, entity, from, to, expected)
    }

    /// Derive the accepted editable names needed by an opaque typed adapter.
    pub(in crate::db) fn typed_adapter_names(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
        composite_catalog: &AcceptedCompositeCatalog,
    ) -> Result<AcceptedTypedAdapterNames, InternalError> {
        let mut named_types = Vec::with_capacity(self.types.len());
        let mut enum_variants = Vec::with_capacity(self.enum_variants.len());
        let mut composite_fields = Vec::with_capacity(self.composite_fields.len());

        for (source, identity) in &self.types {
            match identity {
                AcceptedNamedTypeIdentity::Enum(type_id) => {
                    let definition = enum_catalog
                        .enum_type(*type_id)
                        .ok_or_else(InternalError::store_invariant)?;
                    named_types.push((source.as_str().to_string(), definition.path().to_string()));
                    for ((bound_type, variant_source), variant_id) in &self.enum_variants {
                        if bound_type != type_id {
                            continue;
                        }
                        let variant = definition
                            .variant(*variant_id)
                            .ok_or_else(InternalError::store_invariant)?;
                        enum_variants.push((
                            source.as_str().to_string(),
                            variant_source.as_str().to_string(),
                            variant.name().to_string(),
                        ));
                    }
                }
                AcceptedNamedTypeIdentity::Composite(type_id) => {
                    let definition = composite_catalog
                        .composite_type(*type_id)
                        .ok_or_else(InternalError::store_invariant)?;
                    named_types.push((source.as_str().to_string(), definition.path().to_string()));
                    let AcceptedCompositeShape::Record(fields) = definition.shape() else {
                        continue;
                    };
                    for ((bound_type, field_source), field_id) in &self.composite_fields {
                        if bound_type != type_id {
                            continue;
                        }
                        let field = fields
                            .iter()
                            .find(|field| field.id() == *field_id)
                            .ok_or_else(InternalError::store_invariant)?;
                        composite_fields.push((
                            source.as_str().to_string(),
                            field_source.as_str().to_string(),
                            field.name().to_string(),
                        ));
                    }
                }
            }
        }

        Ok(AcceptedTypedAdapterNames {
            composite_fields,
            enum_variants,
            named_types,
        })
    }

    /// Remove one exact field binding and move every retained binding through
    /// the dense accepted field-ID reassignment owned by the schema candidate.
    pub(in crate::db::schema) fn remove_field_and_remap(
        &mut self,
        entity: EntityTag,
        source: &FieldSourceKey,
        expected: FieldId,
        mut retained_field_id: impl FnMut(FieldId) -> Option<FieldId>,
    ) -> Result<(), InternalError> {
        match self.fields.remove(&(entity, source.clone())) {
            Some(actual) if actual == expected => {}
            Some(_) | None => return Err(InternalError::store_invariant()),
        }
        for ((bound_entity, _), field_id) in &mut self.fields {
            if *bound_entity != entity {
                continue;
            }
            *field_id = retained_field_id(*field_id).ok_or_else(InternalError::store_invariant)?;
        }
        Ok(())
    }

    /// Resolve one immutable named-type source identity.
    #[must_use]
    pub(in crate::db) fn named_type(
        &self,
        source: &TypeSourceKey,
    ) -> Option<AcceptedNamedTypeIdentity> {
        self.types.get(source).copied()
    }

    /// Replace one named-type source key while preserving its catalog ID.
    #[cfg(feature = "migration")]
    pub(in crate::db::schema) fn rekey_named_type(
        &mut self,
        from: &TypeSourceKey,
        to: TypeSourceKey,
        expected: AcceptedNamedTypeIdentity,
    ) -> Result<(), InternalError> {
        rekey_exact(&mut self.types, from, to, expected)
    }

    /// Remove one exact named-type binding and its child identity closure.
    pub(in crate::db::schema) fn remove_named_type(
        &mut self,
        source: &TypeSourceKey,
        expected: AcceptedNamedTypeIdentity,
    ) -> Result<(), InternalError> {
        match self.types.remove(source) {
            Some(actual) if actual == expected => {}
            Some(_) | None => return Err(InternalError::store_invariant()),
        }
        match expected {
            AcceptedNamedTypeIdentity::Enum(type_id) => {
                self.enum_variants
                    .retain(|(bound_type, _), _| *bound_type != type_id);
            }
            AcceptedNamedTypeIdentity::Composite(type_id) => {
                self.composite_fields
                    .retain(|(bound_type, _), _| *bound_type != type_id);
            }
        }
        Ok(())
    }

    /// Resolve one immutable unit-variant identity inside an accepted enum.
    #[must_use]
    pub(in crate::db::schema) fn enum_variant(
        &self,
        enum_type: EnumTypeId,
        source: &TypeSourceKey,
    ) -> Option<EnumVariantId> {
        self.enum_variants
            .get(&(enum_type, source.clone()))
            .copied()
    }

    /// Replace one enum-variant source key while preserving its accepted ID.
    #[cfg(feature = "migration")]
    pub(in crate::db::schema) fn rekey_enum_variant(
        &mut self,
        enum_type: EnumTypeId,
        from: &TypeSourceKey,
        to: TypeSourceKey,
        expected: EnumVariantId,
    ) -> Result<(), InternalError> {
        rekey_scoped_exact(&mut self.enum_variants, enum_type, from, to, expected)
    }

    /// Resolve one immutable member source identity inside an accepted record.
    #[must_use]
    pub(in crate::db::schema) fn composite_field(
        &self,
        composite_type: CompositeTypeId,
        source: &FieldSourceKey,
    ) -> Option<CompositeFieldId> {
        self.composite_fields
            .get(&(composite_type, source.clone()))
            .copied()
    }

    /// Replace one record-member source key while preserving its accepted ID.
    #[cfg(feature = "migration")]
    pub(in crate::db::schema) fn rekey_composite_field(
        &mut self,
        composite_type: CompositeTypeId,
        from: &FieldSourceKey,
        to: FieldSourceKey,
        expected: CompositeFieldId,
    ) -> Result<(), InternalError> {
        rekey_scoped_exact(
            &mut self.composite_fields,
            composite_type,
            from,
            to,
            expected,
        )
    }

    /// Resolve one immutable accepted-check source identity.
    #[must_use]
    pub(in crate::db::schema) fn constraint(
        &self,
        entity: EntityTag,
        source: &ConstraintSourceKey,
    ) -> Option<ConstraintId> {
        self.constraints.get(&(entity, source.clone())).copied()
    }

    /// Replace one entity-local constraint source key while preserving its ID.
    #[cfg(feature = "migration")]
    pub(in crate::db::schema) fn rekey_constraint(
        &mut self,
        entity: EntityTag,
        from: &ConstraintSourceKey,
        to: ConstraintSourceKey,
        expected: ConstraintId,
    ) -> Result<(), InternalError> {
        rekey_scoped_exact(&mut self.constraints, entity, from, to, expected)
    }

    /// Add one exact source binding for a newly reserved accepted check.
    pub(in crate::db::schema) fn insert_constraint(
        &mut self,
        entity: EntityTag,
        source: ConstraintSourceKey,
        constraint: ConstraintId,
    ) -> Result<(), InternalError> {
        let key = (entity, source);
        if self
            .constraints
            .iter()
            .any(|((bound_entity, _), accepted)| *bound_entity == entity && *accepted == constraint)
            || self.constraints.contains_key(&key)
        {
            return Err(InternalError::store_invariant());
        }
        self.constraints.insert(key, constraint);
        Ok(())
    }

    /// Remove one exact accepted-check source binding with its structural owner.
    pub(in crate::db::schema) fn remove_constraint(
        &mut self,
        entity: EntityTag,
        source: &ConstraintSourceKey,
        expected: ConstraintId,
    ) -> Result<(), InternalError> {
        match self.constraints.remove(&(entity, source.clone())) {
            Some(actual) if actual == expected => Ok(()),
            Some(_) | None => Err(InternalError::store_invariant()),
        }
    }

    /// Remove the unique generated-check binding selected by accepted identity.
    ///
    /// Abort owns only the durable application record's accepted identity, so
    /// it resolves the source key from this catalog rather than consulting the
    /// authored proposal again.
    pub(in crate::db::schema) fn remove_constraint_identity(
        &mut self,
        entity: EntityTag,
        expected: ConstraintId,
    ) -> Result<(), InternalError> {
        let mut matching = self
            .constraints
            .iter()
            .filter(|((bound_entity, _), constraint)| {
                *bound_entity == entity && **constraint == expected
            })
            .map(|((_, source), _)| source.clone());
        let source = matching.next().ok_or_else(InternalError::store_invariant)?;
        if matching.next().is_some() {
            return Err(InternalError::store_invariant());
        }
        self.remove_constraint(entity, &source, expected)
    }

    /// Resolve one immutable secondary-index source identity.
    #[must_use]
    pub(in crate::db::schema) fn index(
        &self,
        entity: EntityTag,
        source: &IndexSourceKey,
    ) -> Option<SchemaIndexId> {
        self.indexes.get(&(entity, source.clone())).copied()
    }

    /// Replace one entity-local index source key while preserving its ID.
    #[cfg(feature = "migration")]
    pub(in crate::db::schema) fn rekey_index(
        &mut self,
        entity: EntityTag,
        from: &IndexSourceKey,
        to: IndexSourceKey,
        expected: SchemaIndexId,
    ) -> Result<(), InternalError> {
        rekey_scoped_exact(&mut self.indexes, entity, from, to, expected)
    }

    /// Add one exact generated-index binding reserved by a physical source
    /// migration. The candidate catalog remains unpublished until completion.
    #[cfg(feature = "migration")]
    pub(in crate::db::schema) fn insert_migration_index(
        &mut self,
        entity: EntityTag,
        source: IndexSourceKey,
        index: SchemaIndexId,
    ) -> Result<(), InternalError> {
        let key = (entity, source);
        if self.indexes.contains_key(&key)
            || self
                .indexes
                .iter()
                .any(|((bound_entity, _), accepted)| *bound_entity == entity && *accepted == index)
        {
            return Err(InternalError::store_invariant());
        }
        self.indexes.insert(key, index);
        Ok(())
    }

    /// Return canonical index bindings for planner-local rename inference.
    #[cfg(feature = "migration")]
    pub(in crate::db::schema) fn index_bindings(
        &self,
        entity: EntityTag,
    ) -> impl Iterator<Item = (&IndexSourceKey, SchemaIndexId)> {
        self.indexes
            .iter()
            .filter_map(move |((owner, source), id)| (*owner == entity).then_some((source, *id)))
    }

    /// Remove one exact accepted secondary-index source binding.
    pub(in crate::db::schema) fn remove_index(
        &mut self,
        entity: EntityTag,
        source: &IndexSourceKey,
        expected: SchemaIndexId,
    ) -> Result<(), InternalError> {
        match self.indexes.remove(&(entity, source.clone())) {
            Some(actual) if actual == expected => Ok(()),
            Some(_) | None => Err(InternalError::store_invariant()),
        }
    }

    /// Resolve one immutable relation source identity.
    #[must_use]
    pub(in crate::db::schema) fn relation(
        &self,
        entity: EntityTag,
        source: &RelationSourceKey,
    ) -> Option<RelationId> {
        self.relations.get(&(entity, source.clone())).copied()
    }

    /// Replace one entity-local relation source key while preserving its ID.
    #[cfg(feature = "migration")]
    pub(in crate::db::schema) fn rekey_relation(
        &mut self,
        entity: EntityTag,
        from: &RelationSourceKey,
        to: RelationSourceKey,
        expected: RelationId,
    ) -> Result<(), InternalError> {
        rekey_scoped_exact(&mut self.relations, entity, from, to, expected)
    }

    /// Add one exact generated-relation binding reserved by a physical source
    /// migration. The candidate catalog remains unpublished until completion.
    #[cfg(feature = "migration")]
    pub(in crate::db::schema) fn insert_migration_relation(
        &mut self,
        entity: EntityTag,
        source: RelationSourceKey,
        relation: RelationId,
    ) -> Result<(), InternalError> {
        let key = (entity, source);
        if self.relations.contains_key(&key)
            || self.relations.iter().any(|((bound_entity, _), accepted)| {
                *bound_entity == entity && *accepted == relation
            })
        {
            return Err(InternalError::store_invariant());
        }
        self.relations.insert(key, relation);
        Ok(())
    }

    /// Remove one exact accepted relation source binding.
    pub(in crate::db::schema) fn remove_relation(
        &mut self,
        entity: EntityTag,
        source: &RelationSourceKey,
        expected: RelationId,
    ) -> Result<(), InternalError> {
        match self.relations.remove(&(entity, source.clone())) {
            Some(actual) if actual == expected => Ok(()),
            Some(_) | None => Err(InternalError::store_invariant()),
        }
    }

    /// Apply one catalog-native SQL-DDL entity transition.
    ///
    /// Existing immutable keys follow their structural owner through rename
    /// and dense field-ID reassignment. A newly admitted SQL-owned object gets
    /// one key derived from the monotonic accepted publication revision;
    /// removed owners lose their binding. The method deliberately does not
    /// backfill unbound owners from earlier development formats.
    #[cfg(feature = "sql")]
    pub(in crate::db::schema) fn with_sql_ddl_entity_transition(
        mut self,
        entity: EntityTag,
        before: &PersistedSchemaSnapshot,
        after: &PersistedSchemaSnapshot,
        publication_revision: AcceptedSchemaRevision,
    ) -> Result<Self, InternalError> {
        if before.entity_path() != after.entity_path() {
            return Err(InternalError::store_invariant());
        }

        self.apply_sql_ddl_field_transition(entity, before, after, publication_revision)?;
        self.apply_sql_ddl_index_transition(entity, before, after, publication_revision)?;
        self.apply_sql_ddl_check_transition(entity, before, after, publication_revision)?;

        Ok(self)
    }

    #[cfg(feature = "sql")]
    fn apply_sql_ddl_field_transition(
        &mut self,
        entity: EntityTag,
        before: &PersistedSchemaSnapshot,
        after: &PersistedSchemaSnapshot,
        publication_revision: AcceptedSchemaRevision,
    ) -> Result<(), InternalError> {
        let field_lineage = field_lineage(before, after)?;
        let mut removed_keys = Vec::new();
        for ((bound_entity, source_key), field_id) in &mut self.fields {
            if *bound_entity != entity {
                continue;
            }
            if !before.fields().iter().any(|field| field.id() == *field_id) {
                return Err(InternalError::store_invariant());
            }
            match field_lineage.get(field_id) {
                Some(after_id) => *field_id = *after_id,
                None => removed_keys.push((*bound_entity, source_key.clone())),
            }
        }
        for key in removed_keys {
            self.fields.remove(&key);
        }

        let added = after
            .fields()
            .iter()
            .filter(|field| {
                field.origin() == PersistedFieldOrigin::SqlDdl
                    && !field_lineage
                        .values()
                        .any(|after_id| *after_id == field.id())
            })
            .collect::<Vec<_>>();
        if added.len() > 1 {
            return Err(InternalError::store_invariant());
        }
        if let Some(field) = added.first() {
            let source =
                FieldSourceKey::try_new(sql_ddl_source_key("field", entity, publication_revision))
                    .map_err(|_| InternalError::store_invariant())?;
            if self.fields.insert((entity, source), field.id()).is_some() {
                return Err(InternalError::store_invariant());
            }
        }
        Ok(())
    }

    #[cfg(feature = "sql")]
    fn apply_sql_ddl_index_transition(
        &mut self,
        entity: EntityTag,
        before: &PersistedSchemaSnapshot,
        after: &PersistedSchemaSnapshot,
        publication_revision: AcceptedSchemaRevision,
    ) -> Result<(), InternalError> {
        let before_ids = before
            .indexes()
            .iter()
            .chain(before.candidate_indexes())
            .map(PersistedIndexSnapshot::schema_id)
            .collect::<BTreeSet<_>>();
        let after_ids = after
            .indexes()
            .iter()
            .chain(after.candidate_indexes())
            .map(PersistedIndexSnapshot::schema_id)
            .collect::<BTreeSet<_>>();
        self.indexes.retain(|(bound_entity, _), index_id| {
            *bound_entity != entity || after_ids.contains(index_id)
        });

        let added = after
            .indexes()
            .iter()
            .chain(after.candidate_indexes())
            .filter(|index| {
                index.origin() == PersistedIndexOrigin::SqlDdl
                    && !before_ids.contains(&index.schema_id())
            })
            .collect::<Vec<_>>();
        if added.len() > 1 {
            return Err(InternalError::store_invariant());
        }
        if let Some(index) = added.first() {
            let source =
                IndexSourceKey::try_new(sql_ddl_source_key("index", entity, publication_revision))
                    .map_err(|_| InternalError::store_invariant())?;
            if self
                .indexes
                .insert((entity, source), index.schema_id())
                .is_some()
            {
                return Err(InternalError::store_invariant());
            }
        }
        Ok(())
    }

    #[cfg(feature = "sql")]
    fn apply_sql_ddl_check_transition(
        &mut self,
        entity: EntityTag,
        before: &PersistedSchemaSnapshot,
        after: &PersistedSchemaSnapshot,
        publication_revision: AcceptedSchemaRevision,
    ) -> Result<(), InternalError> {
        let before_ids = check_ids(before);
        let after_ids = check_ids(after);
        self.constraints
            .retain(|(bound_entity, _), id| *bound_entity != entity || after_ids.contains(id));

        let added = sql_ddl_check_ids(after)
            .difference(&before_ids)
            .copied()
            .collect::<Vec<_>>();
        if added.len() > 1 {
            return Err(InternalError::store_invariant());
        }
        if let Some(id) = added.first() {
            let source = ConstraintSourceKey::try_new(sql_ddl_source_key(
                "constraint",
                entity,
                publication_revision,
            ))
            .map_err(|_| InternalError::store_invariant())?;
            if self.constraints.insert((entity, source), *id).is_some() {
                return Err(InternalError::store_invariant());
            }
        }
        Ok(())
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
                self.composite_fields
                    .iter()
                    .map(|((composite_type, _), field)| (*composite_type, *field)),
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
        }) && self.enum_variant_bindings_close(enum_catalog)
            && self.composite_field_bindings_close(composite_catalog)
    }

    fn enum_variant_bindings_close(&self, enum_catalog: &AcceptedEnumCatalog) -> bool {
        self.enum_variants.iter().all(|((enum_type, _), variant)| {
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

    fn composite_field_bindings_close(&self, composite_catalog: &AcceptedCompositeCatalog) -> bool {
        self.composite_fields
            .iter()
            .all(|((composite_type, _), field_id)| {
                self.types.values().any(|identity| {
                    *identity == AcceptedNamedTypeIdentity::Composite(*composite_type)
                }) && composite_catalog
                    .composite_type(*composite_type)
                    .is_some_and(|definition| {
                        matches!(
                            definition.shape(),
                            AcceptedCompositeShape::Record(fields)
                                if fields.iter().any(|field| field.id() == *field_id)
                        )
                    })
            })
            && self.types.values().all(|identity| match identity {
                AcceptedNamedTypeIdentity::Composite(composite_type) => composite_catalog
                    .composite_type(*composite_type)
                    .is_some_and(|definition| {
                        let binding_count = self
                            .composite_fields
                            .keys()
                            .filter(|(bound_type, _)| bound_type == composite_type)
                            .count();
                        match definition.shape() {
                            AcceptedCompositeShape::Record(fields) => binding_count == fields.len(),
                            AcceptedCompositeShape::Tuple(_)
                            | AcceptedCompositeShape::Newtype(_) => binding_count == 0,
                        }
                    }),
                AcceptedNamedTypeIdentity::Enum(_) => true,
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
                                && matches!(
                                    constraint.kind(),
                                    AcceptedConstraintKind::Check { .. }
                                        | AcceptedConstraintKind::TargetedRule { .. }
                                )
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
                                            | ConstraintActivationKind::TargetedRule { .. }
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

#[cfg(feature = "sql")]
fn sql_ddl_source_key(
    object_kind: &str,
    entity: EntityTag,
    publication_revision: AcceptedSchemaRevision,
) -> String {
    format!(
        "icydb:sql-ddl:{object_kind}:{}:{}",
        entity.value(),
        publication_revision.get(),
    )
}

#[cfg(feature = "sql")]
fn field_lineage(
    before: &PersistedSchemaSnapshot,
    after: &PersistedSchemaSnapshot,
) -> Result<BTreeMap<FieldId, FieldId>, InternalError> {
    let mut lineage = BTreeMap::new();
    let mut used_after = BTreeSet::new();

    for before_field in before.fields() {
        if let Some(after_field) = after
            .fields()
            .iter()
            .find(|after_field| after_field.name() == before_field.name())
        {
            lineage.insert(before_field.id(), after_field.id());
            if !used_after.insert(after_field.id()) {
                return Err(InternalError::store_invariant());
            }
        }
    }
    for before_field in before.fields() {
        if lineage.contains_key(&before_field.id()) {
            continue;
        }
        if let Some(after_field) = after.fields().iter().find(|after_field| {
            after_field.id() == before_field.id() && !used_after.contains(&after_field.id())
        }) {
            lineage.insert(before_field.id(), after_field.id());
            used_after.insert(after_field.id());
        }
    }

    Ok(lineage)
}

#[cfg(feature = "sql")]
fn check_ids(snapshot: &PersistedSchemaSnapshot) -> BTreeSet<ConstraintId> {
    check_ids_with_origins(snapshot).map(|(id, _)| id).collect()
}

#[cfg(feature = "sql")]
fn sql_ddl_check_ids(snapshot: &PersistedSchemaSnapshot) -> BTreeSet<ConstraintId> {
    check_ids_with_origins(snapshot)
        .filter_map(|(id, origin)| (origin == ConstraintOrigin::SqlDdl).then_some(id))
        .collect()
}

// Keep accepted and activating check identities in one catalog traversal.
#[cfg(feature = "sql")]
fn check_ids_with_origins(
    snapshot: &PersistedSchemaSnapshot,
) -> impl Iterator<Item = (ConstraintId, ConstraintOrigin)> + '_ {
    snapshot
        .constraint_catalog()
        .constraints()
        .iter()
        .filter_map(|constraint| {
            matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
                .then_some((constraint.id(), constraint.origin()))
        })
        .chain(
            snapshot
                .constraint_catalog()
                .activations()
                .iter()
                .filter_map(|activation| {
                    matches!(activation.kind(), ConstraintActivationKind::Check { .. })
                        .then_some((activation.id(), activation.origin()))
                }),
        )
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
    writer.push_len(catalog.composite_fields.len())?;
    for ((composite_type, source_key), field) in &catalog.composite_fields {
        writer.push_u32(composite_type.get());
        writer.push_string(source_key.as_str())?;
        writer.push_u32(field.get());
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

    let type_bindings = read_named_type_bindings(&mut reader)?;
    let enum_variant_bindings = read_enum_variant_bindings(&mut reader)?;
    let composite_field_bindings = read_composite_field_bindings(&mut reader)?;

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

    let catalog = AcceptedSourceBindingCatalog {
        entities: entity_bindings,
        types: type_bindings,
        enum_variants: enum_variant_bindings,
        composite_fields: composite_field_bindings,
        fields: field_bindings,
        constraints: constraint_bindings,
        indexes: index_bindings,
        relations: relation_bindings,
    };
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

fn read_named_type_bindings(
    reader: &mut BindingReader<'_>,
) -> Result<BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>, InternalError> {
    let mut bindings = BTreeMap::new();
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
        if bindings.insert(source, identity).is_some() {
            return Err(InternalError::store_corruption());
        }
    }
    Ok(bindings)
}

fn read_enum_variant_bindings(
    reader: &mut BindingReader<'_>,
) -> Result<BTreeMap<(EnumTypeId, TypeSourceKey), EnumVariantId>, InternalError> {
    let mut bindings = BTreeMap::new();
    for _ in 0..reader.read_count()? {
        let enum_type =
            EnumTypeId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
        let source = TypeSourceKey::try_new(reader.read_string()?)
            .map_err(|_| InternalError::store_corruption())?;
        let variant =
            EnumVariantId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
        if bindings.insert((enum_type, source), variant).is_some() {
            return Err(InternalError::store_corruption());
        }
    }
    Ok(bindings)
}

fn read_composite_field_bindings(
    reader: &mut BindingReader<'_>,
) -> Result<BTreeMap<(CompositeTypeId, FieldSourceKey), CompositeFieldId>, InternalError> {
    let mut bindings = BTreeMap::new();
    for _ in 0..reader.read_count()? {
        let composite_type =
            CompositeTypeId::new(reader.read_u32()?).ok_or_else(InternalError::store_corruption)?;
        let source = FieldSourceKey::try_new(reader.read_string()?)
            .map_err(|_| InternalError::store_corruption())?;
        let field = CompositeFieldId::new(reader.read_u32()?)
            .ok_or_else(InternalError::store_corruption)?;
        if bindings.insert((composite_type, source), field).is_some() {
            return Err(InternalError::store_corruption());
        }
    }
    Ok(bindings)
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
mod tests;

#[cfg(test)]
mod helper_tests;
