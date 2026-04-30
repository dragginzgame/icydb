//! Module: executor::aggregate::runtime::grouped_fold::utils::bucket
//! Responsibility: compact grouped stable-hash bucket storage.
//! Boundary: stores group indexes for stable-hash side maps only.

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

    // Insert one group index, promoting to heap storage only when this stable
    // hash actually has more than one candidate group.
    pub(in crate::db::executor::aggregate::runtime::grouped_fold) fn push_index(
        &mut self,
        new_index: usize,
    ) {
        match self {
            Self::Single(existing_index) => {
                *self = Self::Colliding(vec![*existing_index, new_index]);
            }
            Self::Colliding(indexes) => indexes.push(new_index),
        }
    }

    // Build one collision-free bucket.
    pub(in crate::db::executor::aggregate::runtime::grouped_fold) const fn single(
        index: usize,
    ) -> Self {
        Self::Single(index)
    }
}
