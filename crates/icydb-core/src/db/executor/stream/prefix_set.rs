//! Module: executor::stream::prefix_set
//! Responsibility: shared prefix-set execution shape selection.
//! Does not own: prefix pruning, payload decoding, or route admission policy.
//! Boundary: classifies already-active sibling prefixes for executor runtimes.

pub(in crate::db::executor) enum PrefixSetExecutionShape<T> {
    Empty,
    Single(T),
    Fallback,
    OrderedConcat(Vec<T>),
    OrderedMerge(Vec<T>),
}

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum PrefixSetMergeSafety {
    RequiresFallback,
    OrderedConcatSafe,
    OrderedMergeSafe,
}

impl<T> PrefixSetExecutionShape<T> {
    #[must_use]
    pub(in crate::db::executor) fn from_active_prefixes(
        mut prefixes: Vec<T>,
        merge_safety: PrefixSetMergeSafety,
    ) -> Self {
        match prefixes.len() {
            0 => Self::Empty,
            1 => match prefixes.pop() {
                Some(prefix) => Self::Single(prefix),
                None => Self::Empty,
            },
            _ if matches!(merge_safety, PrefixSetMergeSafety::OrderedConcatSafe) => {
                Self::OrderedConcat(prefixes)
            }
            _ if matches!(merge_safety, PrefixSetMergeSafety::OrderedMergeSafe) => {
                Self::OrderedMerge(prefixes)
            }
            _ => Self::Fallback,
        }
    }
}

#[cfg(test)]
mod tests;
