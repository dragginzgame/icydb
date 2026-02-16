use crate::db::query::plan::{AccessPath, AccessPlan};

/// Return a human-readable summary of the access plan.
pub fn access_summary<K>(access: &AccessPlan<K>) -> String {
    match access {
        AccessPlan::Path(path) => access_path_summary(path),
        AccessPlan::Union(children) => format!("union of {} access paths", children.len()),
        AccessPlan::Intersection(children) => {
            format!("intersection of {} access paths", children.len())
        }
    }
}

/// Render a compact description for a concrete access path.
pub fn access_path_summary<K>(path: &AccessPath<K>) -> String {
    match path {
        AccessPath::ByKey(_) => "primary key lookup".to_string(),
        AccessPath::ByKeys(keys) => format!("primary key lookup ({} keys)", keys.len()),
        AccessPath::KeyRange { .. } => "primary key range scan".to_string(),
        AccessPath::IndexPrefix { index, values } => format!(
            "index prefix scan ({}, prefix_len={})",
            index.name,
            values.len()
        ),
        AccessPath::IndexRange { index, prefix, .. } => format!(
            "index range scan ({}, prefix_len={})",
            index.name,
            prefix.len()
        ),
        AccessPath::FullScan => "full scan".to_string(),
    }
}

/// Convert a boolean to a concise yes/no label for debug summaries.
pub const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
