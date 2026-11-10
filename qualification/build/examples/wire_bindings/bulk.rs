//! Consumer decisions for Wire's host-only bulk-field advice.

use std::collections::BTreeMap;

/// Exact endpoint-field decisions consumed by Wire's generator diagnostics.
pub(super) fn acknowledgements() -> BTreeMap<String, String> {
    let mut acknowledgements = BTreeMap::new();
    for method in ["icydb_query", "icydb_ddl", "icydb_update"] {
        acknowledgements.insert(
            format!("/methods/{method}/response/ok/variants/Ddl/fields/constraint_validation/item/fields/findings/item/fields/field_ids/list/u32"),
            "Diagnostic field IDs retain compact varints; no measured bulk benefit justifies changing payload bytes and the client array type.".to_owned(),
        );
    }

    acknowledgements
}

#[cfg(test)]
mod tests;
