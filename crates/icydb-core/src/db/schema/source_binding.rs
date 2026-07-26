//! Module: db::schema::source_binding
//! Responsibility: durable source-key to accepted-identity bindings.
//! Does not own: accepted structural semantics, accepted-ID allocation, proposal lowering, or publication.
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
        composite_catalog::{AcceptedCompositeShape, CompositeFieldId, CompositeTypeId},
        wire::{SchemaWireReader, SchemaWireWriter},
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

    /// Copy the named-type identity closure from the same staged initial
    /// candidate while adding entity-scoped structural bindings.
    #[must_use]
    pub(in crate::db::schema) fn with_initial_named_types_from(
        mut self,
        named_types: &Self,
    ) -> Self {
        self.types.clone_from(&named_types.types);
        self.enum_variants.clone_from(&named_types.enum_variants);
        self.composite_fields
            .clone_from(&named_types.composite_fields);
        self
    }

    /// Resolve one immutable entity source identity.
    #[must_use]
    pub(in crate::db) fn entity(&self, source: &EntitySourceKey) -> Option<EntityTag> {
        self.entities.get(source).copied()
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

    /// Resolve one immutable accepted-check source identity.
    #[must_use]
    pub(in crate::db::schema) fn constraint(
        &self,
        entity: EntityTag,
        source: &ConstraintSourceKey,
    ) -> Option<ConstraintId> {
        self.constraints.get(&(entity, source.clone())).copied()
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

    /// Resolve one immutable secondary-index source identity.
    #[must_use]
    pub(in crate::db::schema) fn index(
        &self,
        entity: EntityTag,
        source: &IndexSourceKey,
    ) -> Option<SchemaIndexId> {
        self.indexes.get(&(entity, source.clone())).copied()
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

    #[cfg(test)]
    pub(in crate::db) fn field_source_key_for_tests(
        &self,
        entity: EntityTag,
        field_id: FieldId,
    ) -> Option<&str> {
        self.fields.iter().find_map(|((bound_entity, source), id)| {
            (*bound_entity == entity && *id == field_id).then(|| source.as_str())
        })
    }

    #[cfg(test)]
    pub(in crate::db) fn field_binding_count_for_tests(&self, entity: EntityTag) -> usize {
        self.fields
            .keys()
            .filter(|(bound_entity, _)| *bound_entity == entity)
            .count()
    }

    #[cfg(test)]
    pub(in crate::db) fn entity_binding_count_for_tests(&self) -> usize {
        self.entities.len()
    }

    #[cfg(test)]
    pub(in crate::db) fn named_type_binding_count_for_tests(&self) -> usize {
        self.types.len()
    }

    #[cfg(test)]
    pub(in crate::db) fn enum_variant_binding_count_for_tests(&self) -> usize {
        self.enum_variants.len()
    }

    #[cfg(test)]
    pub(in crate::db) fn composite_field_binding_count_for_tests(
        &self,
        composite_type: CompositeTypeId,
    ) -> usize {
        self.composite_fields
            .keys()
            .filter(|(bound_type, _)| *bound_type == composite_type)
            .count()
    }

    #[cfg(test)]
    pub(in crate::db) fn constraint_binding_count_for_tests(&self, entity: EntityTag) -> usize {
        self.constraints
            .keys()
            .filter(|(bound_entity, _)| *bound_entity == entity)
            .count()
    }

    #[cfg(test)]
    pub(in crate::db) fn relation_binding_count_for_tests(&self, entity: EntityTag) -> usize {
        self.relations
            .keys()
            .filter(|(bound_entity, _)| *bound_entity == entity)
            .count()
    }

    #[cfg(test)]
    pub(in crate::db) fn index_binding_count_for_tests(&self, entity: EntityTag) -> usize {
        self.indexes
            .keys()
            .filter(|(bound_entity, _)| *bound_entity == entity)
            .count()
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
    snapshot
        .constraint_catalog()
        .constraints()
        .iter()
        .filter_map(|constraint| {
            matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
                .then_some(constraint.id())
        })
        .chain(
            snapshot
                .constraint_catalog()
                .activations()
                .iter()
                .filter_map(|activation| {
                    matches!(activation.kind(), ConstraintActivationKind::Check { .. })
                        .then_some(activation.id())
                }),
        )
        .collect()
}

#[cfg(feature = "sql")]
fn sql_ddl_check_ids(snapshot: &PersistedSchemaSnapshot) -> BTreeSet<ConstraintId> {
    snapshot
        .constraint_catalog()
        .constraints()
        .iter()
        .filter_map(|constraint| {
            (constraint.origin() == ConstraintOrigin::SqlDdl
                && matches!(constraint.kind(), AcceptedConstraintKind::Check { .. }))
            .then_some(constraint.id())
        })
        .chain(
            snapshot
                .constraint_catalog()
                .activations()
                .iter()
                .filter_map(|activation| {
                    (activation.origin() == ConstraintOrigin::SqlDdl
                        && matches!(activation.kind(), ConstraintActivationKind::Check { .. }))
                    .then_some(activation.id())
                }),
        )
        .collect()
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
mod tests {
    use std::collections::BTreeMap;

    use super::{
        AcceptedNamedTypeIdentity, AcceptedSourceBindingCatalog, decode_accepted_source_bindings,
        encode_accepted_source_bindings,
    };
    use crate::{
        db::schema::{
            AcceptedCompositeCatalog, AcceptedFieldKind, AcceptedSchemaRevision,
            AcceptedSchemaRevisionBundle, CandidateSchemaRevision, ConstraintId, FieldId,
            PersistedFieldSnapshot, PersistedSchemaSnapshot, SchemaFieldSlot, SchemaInsertDefault,
            SchemaRowLayout, SchemaVersion,
            build_initial_accepted_enum_catalog_from_kinds_for_tests,
            composite_catalog::{
                AcceptedCompositeElement, AcceptedCompositeField, AcceptedCompositeShape,
                CompositeFieldId, CompositeTypeId,
            },
        },
        model::field::{EnumVariantModel, FieldKind, FieldStorageDecode, LeafCodec, ScalarCodec},
        types::EntityTag,
    };
    use icydb_schema::{ConstraintSourceKey, FieldSourceKey};

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
    fn constraint_binding_insertion_enforces_entity_local_identity() {
        let mut catalog = AcceptedSourceBindingCatalog::default();
        let first_entity = EntityTag::new(7);
        let second_entity = EntityTag::new(8);
        let first_source = ConstraintSourceKey::try_new("test:first")
            .expect("first constraint source should build");
        let second_source = ConstraintSourceKey::try_new("test:second")
            .expect("second constraint source should build");
        let third_source = ConstraintSourceKey::try_new("test:third")
            .expect("third constraint source should build");
        let local_id = ConstraintId::new(1).expect("local constraint ID should build");

        catalog
            .insert_constraint(first_entity, first_source.clone(), local_id)
            .expect("first entity should accept its local ID");
        catalog
            .insert_constraint(second_entity, second_source, local_id)
            .expect("another entity may reuse the same local ID");

        assert!(
            catalog
                .insert_constraint(first_entity, third_source, local_id)
                .is_err(),
        );
        assert!(
            catalog
                .insert_constraint(
                    first_entity,
                    first_source,
                    ConstraintId::new(2).expect("second local constraint ID should build"),
                )
                .is_err(),
        );
    }

    #[test]
    fn field_removal_carries_retained_source_identity_through_dense_ids() {
        let entity = EntityTag::new(7);
        let first = FieldSourceKey::try_new("test:first").expect("first field source should build");
        let removed =
            FieldSourceKey::try_new("test:removed").expect("removed field source should build");
        let retained =
            FieldSourceKey::try_new("test:retained").expect("retained field source should build");
        let mut catalog = AcceptedSourceBindingCatalog::default();
        catalog
            .fields
            .insert((entity, first.clone()), FieldId::new(1));
        catalog
            .fields
            .insert((entity, removed.clone()), FieldId::new(2));
        catalog
            .fields
            .insert((entity, retained.clone()), FieldId::new(3));

        catalog
            .remove_field_and_remap(entity, &removed, FieldId::new(2), |field_id| match field_id
                .get()
            {
                1 => Some(FieldId::new(1)),
                3 => Some(FieldId::new(2)),
                _ => None,
            })
            .expect("exact removal lineage should apply");

        assert_eq!(catalog.field(entity, &first), Some(FieldId::new(1)));
        assert_eq!(catalog.field(entity, &retained), Some(FieldId::new(2)));
        assert_eq!(catalog.field(entity, &removed), None);
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
        let adapter_names = catalog
            .typed_adapter_names(&enums, &composites)
            .expect("accepted adapter names should resolve");
        assert_eq!(
            adapter_names.named_types,
            vec![("test:status".to_string(), "test::Status".to_string())],
        );
        assert_eq!(
            adapter_names.enum_variants,
            vec![
                (
                    "test:status".to_string(),
                    "test:status:active".to_string(),
                    "Active".to_string(),
                ),
                (
                    "test:status".to_string(),
                    "test:status:disabled".to_string(),
                    "Disabled".to_string(),
                ),
            ],
        );

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
    fn source_binding_catalog_closes_record_member_source_identities() {
        let enums = build_initial_accepted_enum_catalog_from_kinds_for_tests(&[])
            .expect("empty enum catalog should build");
        let composite_type = CompositeTypeId::new(1).expect("one is non-zero");
        let alpha = CompositeFieldId::new(1).expect("one is non-zero");
        let zeta = CompositeFieldId::new(2).expect("two is non-zero");
        let composites = AcceptedCompositeCatalog::from_initial_definitions(
            BTreeMap::from([(
                composite_type,
                (
                    "test::Record".to_string(),
                    AcceptedCompositeShape::Record(vec![
                        AcceptedCompositeField::new(
                            alpha,
                            "alpha".to_string(),
                            AcceptedCompositeElement::new(AcceptedFieldKind::Nat64, false),
                        ),
                        AcceptedCompositeField::new(
                            zeta,
                            "zeta".to_string(),
                            AcceptedCompositeElement::new(AcceptedFieldKind::Bool, false),
                        ),
                    ]),
                ),
            )]),
            &enums,
        )
        .expect("record catalog should build");
        let entities = BTreeMap::new();
        let mut catalog = AcceptedSourceBindingCatalog::default();
        catalog.types.insert(
            icydb_schema::TypeSourceKey::try_new("test:record")
                .expect("type source key should build"),
            AcceptedNamedTypeIdentity::Composite(composite_type),
        );
        catalog.composite_fields.insert(
            (
                composite_type,
                icydb_schema::FieldSourceKey::try_new("test:record:alpha")
                    .expect("field source key should build"),
            ),
            alpha,
        );
        catalog.composite_fields.insert(
            (
                composite_type,
                icydb_schema::FieldSourceKey::try_new("test:record:zeta")
                    .expect("field source key should build"),
            ),
            zeta,
        );

        let encoded = encode_accepted_source_bindings(&catalog, &enums, &composites, &entities)
            .expect("closed record bindings should encode");
        let decoded = decode_accepted_source_bindings(&encoded, &enums, &composites, &entities)
            .expect("closed record bindings should decode");

        assert_eq!(decoded, catalog);
        let adapter_names = catalog
            .typed_adapter_names(&enums, &composites)
            .expect("accepted adapter names should resolve");
        assert_eq!(
            adapter_names.named_types,
            vec![("test:record".to_string(), "test::Record".to_string())],
        );
        assert_eq!(
            adapter_names.composite_fields,
            vec![
                (
                    "test:record".to_string(),
                    "test:record:alpha".to_string(),
                    "alpha".to_string(),
                ),
                (
                    "test:record".to_string(),
                    "test:record:zeta".to_string(),
                    "zeta".to_string(),
                ),
            ],
        );

        let mut incomplete = catalog;
        incomplete
            .composite_fields
            .retain(|(_, source), _| source.as_str() != "test:record:zeta");
        assert!(
            encode_accepted_source_bindings(&incomplete, &enums, &composites, &entities).is_err(),
            "a bound record type must bind every accepted member",
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
