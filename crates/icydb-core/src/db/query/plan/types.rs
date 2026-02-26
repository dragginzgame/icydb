//! Pure plan-layer data types; must not embed planning semantics or validation.

///
/// OrderDirection
/// Executor-facing ordering direction (applied after filtering).
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

///
/// OrderSpec
/// Executor-facing ordering specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OrderSpec {
    pub(crate) fields: Vec<(String, OrderDirection)>,
}

///
/// DeleteLimitSpec
/// Executor-facing delete bound with no offsets.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteLimitSpec {
    pub max_rows: u32,
}

///
/// PageSpec
/// Executor-facing pagination specification.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PageSpec {
    pub limit: Option<u32>,
    pub offset: u32,
}
