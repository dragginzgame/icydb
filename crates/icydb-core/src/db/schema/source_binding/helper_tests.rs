//! Test-only constructors and adapters for the parent module.

use super::*;

impl AcceptedSourceBindingCatalog {
    /// Build exact source bindings in focused cross-boundary tests.
    #[must_use]
    pub(in crate::db) const fn initial_for_tests(
        entities: BTreeMap<EntitySourceKey, EntityTag>,
        fields: BTreeMap<(EntityTag, FieldSourceKey), FieldId>,
        constraints: BTreeMap<(EntityTag, ConstraintSourceKey), ConstraintId>,
        indexes: BTreeMap<(EntityTag, IndexSourceKey), SchemaIndexId>,
        relations: BTreeMap<(EntityTag, RelationSourceKey), RelationId>,
    ) -> Self {
        Self::initial(entities, fields, constraints, indexes, relations)
    }

    /// Attach exact named-type identities in focused cross-boundary tests.
    #[must_use]
    pub(in crate::db) fn with_initial_named_types_for_tests(
        self,
        types: BTreeMap<TypeSourceKey, AcceptedNamedTypeIdentity>,
        enum_variants: BTreeMap<(EnumTypeId, TypeSourceKey), EnumVariantId>,
        composite_fields: BTreeMap<(CompositeTypeId, FieldSourceKey), CompositeFieldId>,
    ) -> Self {
        self.with_initial_named_types(types, enum_variants, composite_fields)
    }

    pub(in crate::db) fn field_binding_count_for_tests(&self, entity: EntityTag) -> usize {
        self.fields
            .keys()
            .filter(|(bound_entity, _)| *bound_entity == entity)
            .count()
    }

    pub(in crate::db) fn named_type_binding_count_for_tests(&self) -> usize {
        self.types.len()
    }

    pub(in crate::db) fn enum_variant_binding_count_for_tests(&self) -> usize {
        self.enum_variants.len()
    }

    pub(in crate::db) fn composite_field_binding_count_for_tests(
        &self,
        composite_type: CompositeTypeId,
    ) -> usize {
        self.composite_fields
            .keys()
            .filter(|(bound_type, _)| *bound_type == composite_type)
            .count()
    }

    pub(in crate::db) fn constraint_binding_count_for_tests(&self, entity: EntityTag) -> usize {
        self.constraints
            .keys()
            .filter(|(bound_entity, _)| *bound_entity == entity)
            .count()
    }

    pub(in crate::db) fn index_binding_count_for_tests(&self, entity: EntityTag) -> usize {
        self.indexes
            .keys()
            .filter(|(bound_entity, _)| *bound_entity == entity)
            .count()
    }
}
