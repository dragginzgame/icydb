use crate::db::query::plan::{AccessPath, AccessPlan};

/// Return a human-readable summary of the access plan.
pub(super) fn access_summary<K>(access: &AccessPlan<K>) -> String {
    access.debug_summary()
}

/// Render a compact description for a concrete access path.
pub(super) fn access_path_summary<K>(path: &AccessPath<K>) -> String {
    path.debug_summary()
}

/// Convert a boolean to a concise yes/no label for debug summaries.
pub(super) const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

impl<K> AccessPlan<K> {
    // Render a stable summary for debug logging.
    fn debug_summary(&self) -> String {
        match self {
            Self::Path(path) => access_path_summary(path),
            Self::Union(children) => format!("union of {} access paths", children.len()),
            Self::Intersection(children) => {
                format!("intersection of {} access paths", children.len())
            }
        }
    }
}

impl<K> AccessPath<K> {
    // Render a stable summary for debug logging.
    fn debug_summary(&self) -> String {
        match self {
            Self::ByKey(_) => "primary key lookup".to_string(),
            Self::ByKeys(keys) => format!("primary key lookup ({} keys)", keys.len()),
            Self::KeyRange { .. } => "primary key range scan".to_string(),
            Self::IndexPrefix { index, values } => {
                format!(
                    "index prefix scan ({}, prefix_len={})",
                    index.name,
                    values.len()
                )
            }
            Self::IndexRange { index, prefix, .. } => {
                format!(
                    "index range scan ({}, prefix_len={})",
                    index.name,
                    prefix.len()
                )
            }
            Self::FullScan => "full scan".to_string(),
        }
    }
}
