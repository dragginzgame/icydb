//! Module: executor::aggregate::runtime::grouped_fold::utils::bucket
//! Responsibility: compact grouped stable-hash bucket storage.
//! Boundary: stores group indexes for stable-hash side maps only.

use crate::{
    db::executor::group::{retained_vec_element_backing_bytes, try_reserve_vec_elements},
    error::InternalError,
};

///
/// GroupIndexBucket
///
/// GroupIndexBucket keeps the common grouped hash bucket case allocation-free
/// by storing one group index inline and promoting only on real collisions.
///

pub(in crate::db::executor::aggregate::runtime::grouped_fold) enum GroupIndexBucket {
    Single(usize),
    Colliding(Vec<usize>),
}

impl GroupIndexBucket {
    // Return bucket indexes as a slice so lookup code stays independent from
    // whether this bucket has ever observed a stable-hash collision.
    pub(in crate::db::executor::aggregate::runtime::grouped_fold) const fn as_slice(
        &self,
    ) -> &[usize] {
        match self {
            Self::Single(index) => std::slice::from_ref(index),
            Self::Colliding(indexes) => indexes.as_slice(),
        }
    }

    // Return the structural backing reservation required by the next insert.
    // Promotion owns both the former inline index and the new colliding index.
    pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn retained_insert_backing_bytes(
        &self,
    ) -> u64 {
        let retained_elements = match self {
            Self::Single(_) => 2,
            Self::Colliding(_) => 1,
        };
        retained_vec_element_backing_bytes::<usize>().saturating_mul(retained_elements)
    }

    // Insert one group index through fallible storage after its complete
    // backing reservation has been admitted.
    pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn push_index(
        &mut self,
        new_index: usize,
    ) -> Result<(), InternalError> {
        match self {
            Self::Single(existing_index) => {
                let mut indexes = Vec::new();
                try_reserve_vec_elements(&mut indexes, 2)?;
                indexes.push(*existing_index);
                indexes.push(new_index);
                *self = Self::Colliding(indexes);
            }
            Self::Colliding(indexes) => {
                try_reserve_vec_elements(indexes, 1)?;
                indexes.push(new_index);
            }
        }

        Ok(())
    }

    // Build one collision-free bucket.
    pub(in crate::db::executor::aggregate::runtime::grouped_fold) const fn single(
        index: usize,
    ) -> Self {
        Self::Single(index)
    }
}
