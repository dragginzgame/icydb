//! Module: executor::stream
//! Responsibility: ordered key-stream primitives and physical access-stream boundaries.
//! Does not own: planning semantics or row materialization policy.
//! Boundary: shared key-stream infrastructure consumed by executor load routes.

pub(super) mod access;
mod flat_merge;
pub(super) mod key;

pub(in crate::db::executor) use flat_merge::{
    FlatMergeOrderedChild, FlatMergeSiblingSet, FlatMergeStream,
};

pub(in crate::db::executor) fn reduce_non_empty_streams_pairwise<T, F>(
    mut streams: Vec<T>,
    combiner: F,
) -> Option<T>
where
    F: Fn(T, T) -> T,
{
    if streams.is_empty() {
        return None;
    }
    if streams.len() == 1 {
        return streams.pop();
    }

    while streams.len() > 1 {
        let mut next_round = Vec::with_capacity(streams.len().div_ceil(2));
        let mut iter = streams.into_iter();
        while let Some(left) = iter.next() {
            if let Some(right) = iter.next() {
                next_round.push(combiner(left, right));
            } else {
                next_round.push(left);
            }
        }
        streams = next_round;
    }

    streams.pop()
}
