//! Module: db::query::plan::order_term
//! Responsibility: canonical index-order term rendering shared by planner boundaries.
//! Does not own: query expression parsing or executor slot resolution.
//! Boundary: keeps index-key canonicalization in one place.

use crate::model::index::{IndexKeyItem, IndexKeyItemsRef, IndexModel};

/// Return one canonical ORDER BY term list for an index key sequence.
#[must_use]
pub(in crate::db) fn index_order_terms(index: &IndexModel) -> Vec<String> {
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => {
            canonical_index_order_terms(fields.iter().copied().map(IndexKeyItem::Field))
        }
        IndexKeyItemsRef::Items(items) => canonical_index_order_terms(items.iter().copied()),
    }
}

// Field-only indexes and mixed key-item indexes share the same canonical
// ORDER BY rendering contract; only the source iterator for key items differs.
fn canonical_index_order_terms<I>(key_items: I) -> Vec<String>
where
    I: IntoIterator<Item = IndexKeyItem>,
{
    key_items
        .into_iter()
        .map(|key_item| key_item.canonical_text())
        .collect()
}
