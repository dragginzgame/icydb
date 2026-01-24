use crate::model::{field::EntityFieldModel, index::IndexModel};

///
/// EntityModel
/// Minimal, macro-generated runtime model for one entity.
///

pub struct EntityModel {
    /// Fully-qualified Rust type path (for dispatch and diagnostics).
    pub path: &'static str,
    /// Stable external name used in keys and routing.
    pub entity_name: &'static str,
    /// Primary key field (points at an entry in `fields`).
    pub primary_key: &'static EntityFieldModel,
    /// Ordered field list (authoritative for runtime planning).
    pub fields: &'static [EntityFieldModel],
    /// Index definitions (field order is significant).
    pub indexes: &'static [&'static IndexModel],
}
