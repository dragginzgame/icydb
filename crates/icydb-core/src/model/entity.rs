//! Module: model::entity
//! Responsibility: runtime entity metadata emitted by derives and used by the engine.
//! Does not own: full schema graphs, validators, or registry orchestration.
//! Boundary: authoritative entity-level runtime contract for planning and execution.

use crate::model::{field::FieldModel, index::IndexModel};

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

    /// Ordered field list (authoritative for runtime planning).
    pub(crate) fields: &'static [FieldModel],

    /// Index definitions (field order is significant).
    pub(crate) indexes: &'static [&'static IndexModel],
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
            fields,
            indexes,
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
}

/// Resolve one schema field name into its stable slot index.
#[must_use]
pub(crate) fn resolve_field_slot(model: &EntityModel, field_name: &str) -> Option<usize> {
    model
        .fields
        .iter()
        .position(|field| field.name == field_name)
}
