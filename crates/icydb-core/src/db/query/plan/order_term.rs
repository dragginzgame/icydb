//! Module: db::query::plan::order_term
//! Responsibility: canonical index-order term rendering shared by planner boundaries.
//! Does not own: query expression parsing or executor slot resolution.
//! Boundary: keeps index-key canonicalization in one place.

use crate::db::access::SemanticIndexKeyItem;

/// Return one canonical ORDER BY term list from reduced index key-item facts.
#[must_use]
pub(in crate::db) fn index_key_item_order_terms(key_items: &[SemanticIndexKeyItem]) -> Vec<String> {
    key_items
        .iter()
        .map(|item| item.as_ref().canonical_text())
        .collect()
}
