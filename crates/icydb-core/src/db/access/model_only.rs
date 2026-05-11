//! Module: access::model_only
//! Responsibility: generated-model-only access-contract adapters.
//! Does not own: accepted runtime index authority.
//! Boundary: standalone model-only planning/explain and tests may use these
//! adapters; accepted session/write/recovery runtime must use accepted schema
//! contracts instead.

use crate::{
    db::access::path::{
        SemanticIndexAccessContract, SemanticIndexAccessContractInner, SemanticIndexKeyItems,
    },
    model::index::IndexModel,
};

impl SemanticIndexAccessContract {
    /// Project one generated index model into the reduced access contract for
    /// explicit generated/model-only planner surfaces and tests.
    ///
    /// Accepted runtime planning, explain, writes, uniqueness validation, and
    /// recovery must use accepted schema/index contract constructors instead.
    #[must_use]
    pub(in crate::db) fn model_only_from_generated_index(index: IndexModel) -> Self {
        Self {
            inner: std::sync::Arc::new(SemanticIndexAccessContractInner {
                ordinal: index.ordinal(),
                name: index.name().to_string(),
                store_path: index.store().to_string(),
                key_items: SemanticIndexKeyItems::Static(index.key_items()),
                unique: index.is_unique(),
                predicate_semantics: index.predicate_semantics().cloned(),
            }),
        }
    }
}
