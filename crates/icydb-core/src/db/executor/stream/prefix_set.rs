//! Module: executor::stream::prefix_set
//! Responsibility: shared prefix-set execution shape selection.
//! Does not own: prefix pruning, payload decoding, or route admission policy.
//! Boundary: classifies already-active sibling prefixes for executor runtimes.

pub(in crate::db::executor) enum PrefixSetExecutionShape<T> {
    Empty,
    Single(T),
    Materialized(Vec<T>),
    OrderedConcat(Vec<T>),
    OrderedMerge(Vec<T>),
}

#[derive(Clone, Copy)]
pub(in crate::db::executor) enum PrefixSetMergeSafety {
    RequiresMaterialization,
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
            _ => Self::Materialized(prefixes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_set_execution_shape_preserves_empty_shape() {
        let shape: PrefixSetExecutionShape<u8> = PrefixSetExecutionShape::from_active_prefixes(
            Vec::new(),
            PrefixSetMergeSafety::OrderedMergeSafe,
        );

        assert!(matches!(shape, PrefixSetExecutionShape::Empty));
    }

    #[test]
    fn prefix_set_execution_shape_preserves_single_shape() {
        let shape = PrefixSetExecutionShape::from_active_prefixes(
            vec![7],
            PrefixSetMergeSafety::RequiresMaterialization,
        );

        match shape {
            PrefixSetExecutionShape::Single(prefix) => assert_eq!(prefix, 7),
            _ => panic!("one active prefix should stay direct"),
        }
    }

    #[test]
    fn prefix_set_execution_shape_uses_ordered_merge_when_safe() {
        let shape = PrefixSetExecutionShape::from_active_prefixes(
            vec![1, 2, 3],
            PrefixSetMergeSafety::OrderedMergeSafe,
        );

        match shape {
            PrefixSetExecutionShape::OrderedMerge(prefixes) => assert_eq!(prefixes, vec![1, 2, 3]),
            _ => panic!("safe sibling prefixes should use ordered merge"),
        }
    }

    #[test]
    fn prefix_set_execution_shape_uses_ordered_concat_when_safe() {
        let shape = PrefixSetExecutionShape::from_active_prefixes(
            vec![1, 2, 3],
            PrefixSetMergeSafety::OrderedConcatSafe,
        );

        match shape {
            PrefixSetExecutionShape::OrderedConcat(prefixes) => {
                assert_eq!(prefixes, vec![1, 2, 3]);
            }
            _ => panic!("safe branch-ordered sibling prefixes should use ordered concat"),
        }
    }

    #[test]
    fn prefix_set_execution_shape_uses_materialized_fallback_when_unsafe() {
        let shape = PrefixSetExecutionShape::from_active_prefixes(
            vec![1, 2, 3],
            PrefixSetMergeSafety::RequiresMaterialization,
        );

        match shape {
            PrefixSetExecutionShape::Materialized(prefixes) => {
                assert_eq!(prefixes, vec![1, 2, 3]);
            }
            _ => panic!("unsafe sibling prefixes should materialize"),
        }
    }
}
