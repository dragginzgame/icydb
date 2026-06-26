//! Module: executor::stream::flat_merge
//! Responsibility: payload-agnostic flat ordered merge driver.
//! Does not own: child stream decoding, pairwise merge, or intersection semantics.
//! Boundary: shared sibling-stream merge loop for already ordered branch streams.

use crate::{
    db::{data::DecodedDataStoreKey, executor::stream::key::KeyOrderComparator},
    error::InternalError,
};

pub(in crate::db::executor) trait FlatMergeOrderedChild {
    type Item;
    type KeyWitness;

    fn ensure_item(&mut self) -> Result<(), InternalError>;

    fn head_key(&self) -> Option<&DecodedDataStoreKey>;

    fn take_item(&mut self) -> Option<Self::Item>;

    fn clear_item(&mut self);

    fn item_key(item: &Self::Item) -> &DecodedDataStoreKey;

    fn key_witness(key: &DecodedDataStoreKey) -> Self::KeyWitness;

    fn witness_matches_key(witness: &Self::KeyWitness, key: &DecodedDataStoreKey) -> bool;
}

pub(in crate::db::executor) enum FlatMergeSiblingSet<T> {
    Empty,
    Single(T),
    Pair(T, T),
    Many(Vec<T>),
}

impl<T> FlatMergeSiblingSet<T> {
    #[must_use]
    pub(in crate::db::executor) fn from_vec(mut streams: Vec<T>) -> Self {
        match streams.len() {
            0 => Self::Empty,
            1 => match streams.pop() {
                Some(stream) => Self::Single(stream),
                None => Self::Empty,
            },
            2 => {
                let right = streams.pop();
                let left = streams.pop();
                match (left, right) {
                    (Some(left), Some(right)) => Self::Pair(left, right),
                    (Some(left), None) | (None, Some(left)) => Self::Single(left),
                    (None, None) => Self::Empty,
                }
            }
            _ => Self::Many(streams),
        }
    }
}

pub(in crate::db::executor) struct FlatMergeStream<C>
where
    C: FlatMergeOrderedChild,
{
    children: Vec<C>,
    comparator: KeyOrderComparator,
    last_emitted: Option<C::KeyWitness>,
}

impl<C> FlatMergeStream<C>
where
    C: FlatMergeOrderedChild,
{
    pub(in crate::db::executor) const fn new(
        children: Vec<C>,
        comparator: KeyOrderComparator,
    ) -> Self {
        Self {
            children,
            comparator,
            last_emitted: None,
        }
    }

    fn ensure_items(&mut self) -> Result<(), InternalError> {
        for child in &mut self.children {
            child.ensure_item()?;
        }

        Ok(())
    }

    fn next_child_index(&self) -> Option<usize> {
        let mut best = None;
        for (index, child) in self.children.iter().enumerate() {
            let Some(candidate) = child.head_key() else {
                continue;
            };
            let Some(best_index) = best else {
                best = Some(index);
                continue;
            };
            let Some(best_key) = self.children[best_index].head_key() else {
                best = Some(index);
                continue;
            };
            if self
                .comparator
                .compare_data_keys(candidate, best_key)
                .is_lt()
            {
                best = Some(index);
            }
        }

        best
    }

    fn clear_duplicate_heads(&mut self, emitted: &C::KeyWitness) {
        for child in &mut self.children {
            if child
                .head_key()
                .is_some_and(|key| C::witness_matches_key(emitted, key))
            {
                child.clear_item();
            }
        }
    }

    pub(in crate::db::executor) fn next_item(&mut self) -> Result<Option<C::Item>, InternalError> {
        loop {
            self.ensure_items()?;
            let Some(child_index) = self.next_child_index() else {
                return Ok(None);
            };
            let Some(next) = self.children[child_index].take_item() else {
                return Ok(None);
            };

            let emitted_witness = C::key_witness(C::item_key(&next));
            self.clear_duplicate_heads(&emitted_witness);

            if self
                .last_emitted
                .as_ref()
                .is_some_and(|last| C::witness_matches_key(last, C::item_key(&next)))
            {
                continue;
            }

            self.last_emitted = Some(emitted_witness);
            return Ok(Some(next));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_merge_sibling_set_preserves_pair_order() {
        let shape = FlatMergeSiblingSet::from_vec(vec!["left", "right"]);

        match shape {
            FlatMergeSiblingSet::Pair(left, right) => {
                assert_eq!(left, "left");
                assert_eq!(right, "right");
            }
            _ => panic!("two streams should produce a pair shape"),
        }
    }

    #[test]
    fn flat_merge_sibling_set_preserves_many_order() {
        let shape = FlatMergeSiblingSet::from_vec(vec![1, 2, 3, 4]);

        match shape {
            FlatMergeSiblingSet::Many(streams) => assert_eq!(streams, vec![1, 2, 3, 4]),
            _ => panic!("three or more streams should produce a many shape"),
        }
    }
}
