//! Module: executor::stream::flat_merge
//! Responsibility: payload-agnostic flat ordered merge driver.
//! Does not own: child stream decoding, pairwise merge, or intersection semantics.
//! Boundary: shared sibling-stream merge loop for already ordered branch streams.

use crate::{
    db::{
        data::DecodedDataStoreKey,
        executor::{budget::charge_current_execution_budget, stream::key::KeyOrderComparator},
    },
    error::InternalError,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource;
use std::mem::size_of;

pub(in crate::db::executor) trait FlatMergeOrderedChild {
    type Item;
    type KeyWitness;

    fn ensure_item(&mut self) -> Result<(), InternalError>;

    fn head_key(&self) -> Option<&DecodedDataStoreKey>;

    fn take_item(&mut self) -> Option<Self::Item>;

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
    winner_tree: Vec<Option<usize>>,
    winner_tree_leaf_base: usize,
    dirty_child: Option<usize>,
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
            winner_tree: Vec::new(),
            winner_tree_leaf_base: 0,
            dirty_child: None,
        }
    }

    fn initialize_winner_tree(&mut self) -> Result<(), InternalError> {
        if self.winner_tree_leaf_base != 0 || self.children.is_empty() {
            return Ok(());
        }
        for child in &mut self.children {
            child.ensure_item()?;
        }

        let leaf_base = self
            .children
            .len()
            .checked_next_power_of_two()
            .ok_or_else(InternalError::executor_invariant)?;
        let tree_len = leaf_base
            .checked_mul(2)
            .ok_or_else(InternalError::executor_invariant)?;
        let tree_bytes = tree_len
            .checked_mul(size_of::<Option<usize>>())
            .ok_or_else(InternalError::executor_invariant)?;
        charge_current_execution_budget(
            DiagnosticExecutionBudgetResource::TemporaryBytes,
            u64::try_from(tree_bytes).unwrap_or(u64::MAX),
        )?;
        self.winner_tree
            .try_reserve_exact(tree_len)
            .map_err(|_| InternalError::executor_internal())?;
        self.winner_tree.resize(tree_len, None);
        self.winner_tree_leaf_base = leaf_base;
        for index in 0..self.children.len() {
            let has_head = self
                .children
                .get(index)
                .ok_or_else(InternalError::executor_invariant)?
                .head_key()
                .is_some();
            let leaf = self
                .winner_tree
                .get_mut(leaf_base + index)
                .ok_or_else(InternalError::executor_invariant)?;
            *leaf = has_head.then_some(index);
        }
        for node in (1..leaf_base).rev() {
            self.recompute_winner_node(node)?;
        }

        Ok(())
    }

    fn recompute_winner_node(&mut self, node: usize) -> Result<(), InternalError> {
        let left = self.winner_tree.get(node * 2).copied().flatten();
        let right = self.winner_tree.get(node * 2 + 1).copied().flatten();
        let winner = match (left, right) {
            (Some(left), Some(right)) => {
                let left_key = self
                    .children
                    .get(left)
                    .ok_or_else(InternalError::executor_invariant)?
                    .head_key();
                let right_key = self
                    .children
                    .get(right)
                    .ok_or_else(InternalError::executor_invariant)?
                    .head_key();
                match (left_key, right_key) {
                    (Some(left_key), Some(right_key)) => {
                        if self
                            .comparator
                            .compare_data_keys(right_key, left_key)
                            .is_lt()
                        {
                            Some(right)
                        } else {
                            Some(left)
                        }
                    }
                    (Some(_), None) => Some(left),
                    (None, Some(_)) => Some(right),
                    (None, None) => None,
                }
            }
            (Some(index), None) | (None, Some(index)) => Some(index),
            (None, None) => None,
        };
        let slot = self
            .winner_tree
            .get_mut(node)
            .ok_or_else(InternalError::executor_invariant)?;
        *slot = winner;
        Ok(())
    }

    fn refresh_child(&mut self, index: usize) -> Result<(), InternalError> {
        let child = self
            .children
            .get_mut(index)
            .ok_or_else(InternalError::executor_invariant)?;
        child.ensure_item()?;
        let has_head = child.head_key().is_some();
        let mut node = self.winner_tree_leaf_base + index;
        let leaf = self
            .winner_tree
            .get_mut(node)
            .ok_or_else(InternalError::executor_invariant)?;
        *leaf = has_head.then_some(index);
        node /= 2;
        while node != 0 {
            self.recompute_winner_node(node)?;
            node /= 2;
        }

        Ok(())
    }

    pub(in crate::db::executor) fn next_item(&mut self) -> Result<Option<C::Item>, InternalError> {
        self.initialize_winner_tree()?;
        loop {
            if let Some(index) = self.dirty_child.take() {
                self.refresh_child(index)?;
            }
            let Some(child_index) = self.winner_tree.get(1).copied().flatten() else {
                return Ok(None);
            };
            let Some(next) = self
                .children
                .get_mut(child_index)
                .ok_or_else(InternalError::executor_invariant)?
                .take_item()
            else {
                return Err(InternalError::executor_invariant());
            };
            self.dirty_child = Some(child_index);

            let emitted_witness = C::key_witness(C::item_key(&next));

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
