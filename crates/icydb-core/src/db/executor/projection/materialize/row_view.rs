//! Module: db::executor::projection::materialize::row_view
//! Responsibility: projected-row transport before final response materialization.
//! Does not own: projection evaluation, DISTINCT key storage, or DTO shaping.
//! Boundary: carries either borrowed or owned projected row values locally.

use crate::value::Value;

///
/// RowView
///
/// RowView is the local projection-materialization transport used before the
/// structural boundary builds the public row matrix.
/// It lets future borrowed projection paths avoid row-vector allocation while
/// preserving the owned fallback needed by expression and decoded-row paths.
///

pub(in crate::db::executor::projection::materialize) enum RowView<'a> {
    #[expect(
        dead_code,
        reason = "borrowed row transport is the intended zero-copy extension point"
    )]
    Borrowed(&'a [Value]),
    Owned(Vec<Value>),
}

impl RowView<'_> {
    #[inline]
    pub(in crate::db::executor::projection::materialize) fn get(&self, idx: usize) -> &Value {
        match self {
            Self::Borrowed(slice) => &slice[idx],
            Self::Owned(vec) => &vec[idx],
        }
    }

    pub(in crate::db::executor::projection::materialize) fn into_owned(self) -> Vec<Value> {
        match self {
            Self::Borrowed(slice) => slice.to_vec(),
            Self::Owned(vec) => vec,
        }
    }

    #[inline]
    pub(in crate::db::executor::projection::materialize) const fn values(&self) -> &[Value] {
        match self {
            Self::Borrowed(slice) => slice,
            Self::Owned(vec) => vec.as_slice(),
        }
    }
}
