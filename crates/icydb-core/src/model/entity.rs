//! Module: model::entity
//! Responsibility: runtime entity metadata emitted by derives and used by the engine.
//! Does not own: full schema graphs, validators, or registry orchestration.
//! Boundary: authoritative entity-level runtime contract for planning and execution.

use crate::model::{field::FieldModel, index::IndexModel};

///
/// PrimaryKeyModel
///
/// Ordered primary-key field metadata for one entity. The current execution
/// engine consumes scalar projections, while this model carries the ordered
/// field-list shape needed for composite primary keys.
///

#[derive(Debug)]
pub struct PrimaryKeyModel {
    fields: PrimaryKeyModelFields,
}

impl PrimaryKeyModel {
    /// Build scalar primary-key metadata for existing generated/test models.
    #[must_use]
    pub const fn scalar(field: &'static FieldModel) -> Self {
        Self {
            fields: PrimaryKeyModelFields::Scalar(field),
        }
    }

    /// Build ordered primary-key metadata from generated field references.
    #[must_use]
    pub const fn ordered(fields: &'static [&'static FieldModel]) -> Self {
        assert!(!fields.is_empty(), "primary key model requires fields");
        Self {
            fields: PrimaryKeyModelFields::Ordered(fields),
        }
    }

    /// Return the number of fields in this primary key.
    #[must_use]
    pub const fn len(&self) -> usize {
        match self.fields {
            PrimaryKeyModelFields::Scalar(_) => 1,
            PrimaryKeyModelFields::Ordered(fields) => fields.len(),
        }
    }

    /// Return whether this primary key has no fields.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return whether this primary key is the scalar one-field case.
    #[must_use]
    pub const fn is_scalar(&self) -> bool {
        self.len() == 1
    }

    /// Return the first primary-key field.
    ///
    /// Composite-aware code should consume `fields()` when it needs full row
    /// identity. This helper exists only for metadata surfaces that still carry
    /// one primary-key field pointer alongside ordered primary-key metadata.
    #[must_use]
    pub const fn first_field(&self) -> &'static FieldModel {
        match self.fields {
            PrimaryKeyModelFields::Scalar(field) => field,
            PrimaryKeyModelFields::Ordered(fields) => fields[0],
        }
    }

    /// Iterate over ordered primary-key fields.
    #[must_use]
    pub const fn fields(&self) -> PrimaryKeyModelFields {
        self.fields
    }
}

///
/// PrimaryKeyModelFields
///
/// Borrowed primary-key field list without allocating on hot metadata paths.
///

#[derive(Clone, Copy, Debug)]
pub enum PrimaryKeyModelFields {
    Scalar(&'static FieldModel),
    Ordered(&'static [&'static FieldModel]),
}

impl PrimaryKeyModelFields {
    /// Return the number of fields represented by this view.
    #[must_use]
    pub const fn len(self) -> usize {
        match self {
            Self::Scalar(_) => 1,
            Self::Ordered(fields) => fields.len(),
        }
    }

    /// Return whether this view has no fields.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Return the field at `index`.
    #[must_use]
    pub fn get(self, index: usize) -> Option<&'static FieldModel> {
        match self {
            Self::Scalar(field) => (index == 0).then_some(field),
            Self::Ordered(fields) => fields.get(index).copied(),
        }
    }

    /// Iterate over ordered primary-key fields.
    #[must_use]
    pub const fn iter(self) -> PrimaryKeyModelFieldIter {
        PrimaryKeyModelFieldIter {
            fields: self,
            index: 0,
        }
    }
}

///
/// PrimaryKeyModelFieldIter
///
/// Iterator over primary-key field model references.
///

#[derive(Clone, Debug)]
pub struct PrimaryKeyModelFieldIter {
    fields: PrimaryKeyModelFields,
    index: usize,
}

impl Iterator for PrimaryKeyModelFieldIter {
    type Item = &'static FieldModel;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.fields.get(self.index)?;
        self.index += 1;
        Some(item)
    }
}

#[cfg(test)]
mod primary_key_model_tests {
    use super::{PrimaryKeyModel, PrimaryKeyModelFields};
    use crate::model::FieldModel;

    static ID_FIELD: FieldModel = FieldModel::generated("id", crate::model::FieldKind::Nat64);
    static TENANT_FIELD: FieldModel =
        FieldModel::generated("tenant_id", crate::model::FieldKind::Nat64);
    static ORDERED_FIELDS: [&FieldModel; 2] = [&ID_FIELD, &TENANT_FIELD];

    #[test]
    fn scalar_primary_key_model_exposes_one_field() {
        let model = PrimaryKeyModel::scalar(&ID_FIELD);

        assert_eq!(model.len(), 1);
        assert!(model.is_scalar());
        assert_eq!(model.first_field().name(), "id");
        assert_eq!(
            model
                .fields()
                .iter()
                .map(FieldModel::name)
                .collect::<Vec<_>>(),
            ["id"]
        );
    }

    #[test]
    fn ordered_primary_key_model_preserves_field_order() {
        let model = PrimaryKeyModel::ordered(&ORDERED_FIELDS);

        assert_eq!(model.len(), 2);
        assert!(!model.is_scalar());
        assert_eq!(model.first_field().name(), "id");
        assert_eq!(
            model
                .fields()
                .iter()
                .map(FieldModel::name)
                .collect::<Vec<_>>(),
            ["id", "tenant_id"],
        );
        assert!(matches!(model.fields(), PrimaryKeyModelFields::Ordered(_)));
    }
}

///
/// RelationEdgeModel
///
/// Generated relation-edge proposal metadata. Runtime accepted-schema paths
/// must still reconcile this into persisted catalog authority before
/// execution consumes it.
///

#[derive(Debug)]
pub struct RelationEdgeModel {
    name: &'static str,
    target_path: &'static str,
    local_fields: &'static [&'static FieldModel],
}

impl RelationEdgeModel {
    /// Build one generated relation-edge proposal from ordered local field
    /// metadata.
    #[must_use]
    pub const fn generated(
        name: &'static str,
        target_path: &'static str,
        local_fields: &'static [&'static FieldModel],
    ) -> Self {
        Self {
            name,
            target_path,
            local_fields,
        }
    }

    /// Borrow the generated relation edge name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Borrow the declared target entity path.
    #[must_use]
    pub const fn target_path(&self) -> &'static str {
        self.target_path
    }

    /// Borrow ordered local relation component field metadata.
    #[must_use]
    pub const fn local_fields(&self) -> &'static [&'static FieldModel] {
        self.local_fields
    }
}

#[cfg(test)]
mod relation_edge_model_tests {
    use super::RelationEdgeModel;
    use crate::model::{FieldKind, FieldModel};

    static TENANT_FIELD: FieldModel = FieldModel::generated("tenant_id", FieldKind::Nat64);
    static USER_FIELD: FieldModel = FieldModel::generated("user_id", FieldKind::Ulid);
    static LOCAL_FIELDS: [&FieldModel; 2] = [&TENANT_FIELD, &USER_FIELD];

    #[test]
    fn relation_edge_model_preserves_ordered_local_fields() {
        let relation = RelationEdgeModel::generated("author", "example::User", &LOCAL_FIELDS);

        assert_eq!(relation.name(), "author");
        assert_eq!(relation.target_path(), "example::User");
        assert_eq!(
            relation
                .local_fields()
                .iter()
                .map(|field| field.name())
                .collect::<Vec<_>>(),
            ["tenant_id", "user_id"],
        );
    }
}

///
/// EntityModel
///
/// Macro-generated runtime schema snapshot for a single entity.
/// The planner and predicate validator consume this model directly.
///

#[derive(Debug)]
pub struct EntityModel {
    /// Fully-qualified Rust type path (for diagnostics).
    pub(crate) path: &'static str,

    /// Stable external name used in keys and routing.
    pub(crate) entity_name: &'static str,

    /// Primary key field (points at an entry in `fields`).
    pub(crate) primary_key: &'static FieldModel,

    /// Stable primary-key slot within `fields`.
    pub(crate) primary_key_slot: usize,

    /// Ordered primary-key field metadata.
    pub(crate) primary_key_model: PrimaryKeyModel,

    /// Ordered field list (authoritative for runtime planning).
    pub(crate) fields: &'static [FieldModel],

    /// Index definitions (field order is significant).
    pub(crate) indexes: &'static [&'static IndexModel],

    /// Generated relation-edge proposal metadata.
    pub(crate) relations: &'static [RelationEdgeModel],
}

impl EntityModel {
    /// Construct one generated runtime entity descriptor.
    ///
    /// This constructor exists for derive/codegen output. Runtime query and
    /// executor code treat `EntityModel` values as already validated build-time
    /// artifacts and do not perform defensive model-shape validation per call.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated(
        path: &'static str,
        entity_name: &'static str,
        primary_key: &'static FieldModel,
        primary_key_slot: usize,
        fields: &'static [FieldModel],
        indexes: &'static [&'static IndexModel],
    ) -> Self {
        Self {
            path,
            entity_name,
            primary_key,
            primary_key_slot,
            primary_key_model: PrimaryKeyModel::scalar(primary_key),
            fields,
            indexes,
            relations: &[],
        }
    }

    /// Construct one generated runtime entity descriptor with explicit
    /// ordered primary-key metadata.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_primary_key_model(
        path: &'static str,
        entity_name: &'static str,
        primary_key_model: PrimaryKeyModel,
        primary_key_slot: usize,
        fields: &'static [FieldModel],
        indexes: &'static [&'static IndexModel],
    ) -> Self {
        Self::generated_with_primary_key_model_and_relations(
            path,
            entity_name,
            primary_key_model,
            primary_key_slot,
            fields,
            indexes,
            &[],
        )
    }

    /// Construct one generated runtime entity descriptor with explicit
    /// ordered primary-key metadata and relation-edge proposal metadata.
    #[must_use]
    #[doc(hidden)]
    pub const fn generated_with_primary_key_model_and_relations(
        path: &'static str,
        entity_name: &'static str,
        primary_key_model: PrimaryKeyModel,
        primary_key_slot: usize,
        fields: &'static [FieldModel],
        indexes: &'static [&'static IndexModel],
        relations: &'static [RelationEdgeModel],
    ) -> Self {
        Self {
            path,
            entity_name,
            primary_key: primary_key_model.first_field(),
            primary_key_slot,
            primary_key_model,
            fields,
            indexes,
            relations,
        }
    }

    /// Return the fully-qualified Rust path for this entity.
    #[must_use]
    pub const fn path(&self) -> &'static str {
        self.path
    }

    /// Return the stable external entity name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.entity_name
    }

    /// Return the primary-key field descriptor.
    #[must_use]
    pub const fn primary_key(&self) -> &'static FieldModel {
        self.primary_key
    }

    /// Return ordered primary-key field metadata.
    #[must_use]
    pub const fn primary_key_model(&self) -> &PrimaryKeyModel {
        &self.primary_key_model
    }

    /// Return ordered primary-key field names.
    #[must_use]
    pub fn primary_key_names(&self) -> Vec<&'static str> {
        self.primary_key_model()
            .fields()
            .iter()
            .map(crate::model::field::FieldModel::name)
            .collect()
    }

    /// Return the stable primary-key slot within the ordered field table.
    #[must_use]
    pub const fn primary_key_slot(&self) -> usize {
        self.primary_key_slot
    }

    /// Return the ordered runtime field descriptors.
    #[must_use]
    pub const fn fields(&self) -> &'static [FieldModel] {
        self.fields
    }

    /// Return the runtime index descriptors.
    #[must_use]
    pub const fn indexes(&self) -> &'static [&'static IndexModel] {
        self.indexes
    }

    /// Return generated relation-edge proposal metadata.
    #[must_use]
    pub const fn relations(&self) -> &'static [RelationEdgeModel] {
        self.relations
    }

    /// Resolve one schema field name into its stable slot index.
    #[must_use]
    pub(crate) fn resolve_field_slot(&self, field_name: &str) -> Option<usize> {
        self.fields
            .iter()
            .position(|field| field.name == field_name)
    }
}
